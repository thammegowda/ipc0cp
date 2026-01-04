#pragma once

#include "ipc.hpp"
#include "serialize.hpp"
#include <string>
#include <cstdint>
#include <memory>
#include <optional>
#include <chrono>
#include <map>
#include <vector>
#include <utility>
#include <stdexcept>

namespace ipc0cp {

// Type aliases for backward compatibility
using RingBufferError = IPCError;
using RingBufferException = IPCException;
using RingBufferObject = IPCObject;

/**
 * Shared Ring Buffer Memory Layout
 * =================================
 * 
 * Header (24 bytes):
 *   - write_pos (uint64, 8 bytes): ABSOLUTE byte position (write position / location) of next slot
 *   - read_pos (uint64, 8 bytes): ABSOLUTE byte position (read position / location) of next slot
 *   - total_data_bytes (uint64, 8 bytes): Total size of data region
 * 
 * Data Region (variable size):
 *   Each slot contains:
 *     - next_pos (uint64, 8 bytes): ABSOLUTE byte position (next location) of next slot
 *     - metadata_size (uint32, 4 bytes): Size of JSON metadata
 *     - payload_size (uint64, 8 bytes): Size of payload data
 *     - metadata_json (variable, max 1024 bytes): JSON metadata
 *     - start_sentinel (uint8, 1 byte, value=0x00): Data integrity check
 *     - payload (variable): Binary payload data
 *     - end_sentinel (uint8, 1 byte, value=0x00): Data integrity check
 * 
 * Sentinel bytes detect buffer overruns and data corruption.
 */

/**
 * @brief Result type for operations that can fail
 */
template<typename T>
using Result = std::variant<T, IPCError>;

/**
 * @brief Base class for shared ring buffer
 */
class SharedRingBufferBase {
public:
    virtual ~SharedRingBufferBase();
    
    /**
     * @brief Close the shared memory segment
     * Closes the file descriptor but does not delete the shared memory
     */
    void close();
    
    /**
     * @brief Unlink (delete) the shared memory segment
     * Removes the shared memory segment from the system
     */
    void unlink();
    
    /**
     * @brief Get buffer statistics
     */
    struct Stats {
        uint64_t write_pos;
        uint64_t read_pos;
        size_t available_bytes;
        size_t used_bytes;
        size_t total_data_bytes;
        bool is_empty;
    };
    
    Stats get_stats() const;

protected:
    SharedRingBufferBase(std::string shm_name, size_t total_data_bytes, bool blocking);
    
    // Position operations (ABSOLUTE positions within the mapped region)
    uint64_t get_write_pos() const;
    uint64_t get_read_pos() const;
    size_t available_space(uint64_t write_pos, uint64_t read_pos) const;
    uint64_t normalize_pos(uint64_t pos) const;
    
    // Utility
    bool is_empty() const;
    
    std::string shm_name_;
    size_t total_data_bytes_;
    bool blocking_;
    size_t shm_size_;
    
    int shm_fd_ = -1;
    void* shm_ptr_ = nullptr;
};

/**
 * @brief Consumer-only interface for shared ring buffer
 * 
 * This class only exposes the pop() method for reading objects from the buffer.
 * Use this in consumer processes to prevent accidental producer operations.
 * Always attaches to existing shared memory created by a producer.
 * 
 * Example:
 * @code
 * auto consumer = SharedRingBufferConsumer("my_buffer", 100*1024*1024);
 * if (auto result = consumer.attach(); !result) {
 *     std::cerr << "Failed to attach\n";
 *     return;
 * }
 * 
 * while (true) {
 *     auto obj = consumer.pop(std::chrono::seconds(5));
 *     if (!obj) break;
 *     
 *     // Process object based on type
 *     if (obj->metadata.type == ObjectType::Text) {
 *         std::string text(obj->payload.begin(), obj->payload.end());
 *         std::cout << "Received text: " << text << "\n";
 *     }
 * }
 * @endcode
 */
class SharedRingBufferConsumer : public SharedRingBufferBase, public IPCConsumer {
public:
    /**
     * @brief Construct a consumer
     * @param shm_name Name of the POSIX shared memory segment
     * @param total_data_bytes Total size of data region (must match producer)
     * @param blocking Whether to block when buffer is empty
     * @param auto_attach If true, automatically attach to shared memory in constructor
     * @param auto_unlink If true, automatically unlink (delete) shared memory when EOS is received
     */
    SharedRingBufferConsumer(
        std::string shm_name,
        size_t total_data_bytes = DEFAULT_TOTAL_DATA_BYTES,
        bool blocking = true,
        bool auto_attach = true,
        bool auto_unlink = true
    );
    
    /**
     * @brief Manually attach to existing shared memory segment
     * Only needed if auto_attach=false in constructor
     * @return true on success, false on failure (check errno)
     */
    bool attach();
    
    /**
     * @brief Get last error
     */
    IPCError get_last_error() const override { return last_error_; }
    
    /**
     * @brief Pop an object from the ring buffer
     * @param timeout_ms Timeout in milliseconds (<0 = infinite when blocking)
     * @return Deserialized object, or nullptr if end-of-stream marker received
     * @throws IPCException with error_type indicating the error
     */
    std::unique_ptr<IPCObject> pop(
        int timeout_ms = -1) override;
    
    std::optional<std::pair<std::string, std::vector<uint8_t>>> pop_raw(
        int timeout_ms = -1) override;

    /**
     * @brief Check if end-of-stream was received
     */
    bool eos_received() const override { return eos_received_; }

private:
    void set_read_pos(uint64_t pos);
    uint64_t read_uint64(uint64_t pos);
    uint32_t read_uint32(uint64_t pos);
    std::vector<uint8_t> read_bytes(uint64_t pos, size_t length);
    uint64_t advance_pos(uint64_t pos, size_t delta);
    
    std::optional<std::map<std::string, std::string>> parse_metadata(
        const std::string& metadata_json
    );
    
    IPCError last_error_ = IPCError::NotInitialized;
    bool eos_received_ = false;  ///< Track if end-of-stream was received
    bool auto_unlink_ = true;  ///< Automatically unlink shared memory on EOS
};

/**
 * @brief Producer for SharedRingBuffer
 * 
 * Writes serialized objects to a POSIX shared memory ring buffer.
 * Compatible with Python's SharedRingBufferProducer.
 */
class SharedRingBufferProducer : public IPCProducer {
public:
    /**
     * @brief Construct producer with shared memory name
     * @param shm_name Name of shared memory segment (e.g., "/my_buffer")
     * @param total_data_bytes Total size of ring buffer data area (default 1GB)
     * @param create_new If true, create new shared memory; if false, attach to existing
     */
    explicit SharedRingBufferProducer(
        const std::string& shm_name,
        size_t total_data_bytes = DEFAULT_TOTAL_DATA_BYTES,
        bool create_new = true
    );
    
    ~SharedRingBufferProducer();
    
    // Disable copy
    SharedRingBufferProducer(const SharedRingBufferProducer&) = delete;
    SharedRingBufferProducer& operator=(const SharedRingBufferProducer&) = delete;
    
    // Enable move
    SharedRingBufferProducer(SharedRingBufferProducer&& other) noexcept;
    SharedRingBufferProducer& operator=(SharedRingBufferProducer&& other) noexcept;
    
    /**
     * @brief Push an object to the ring buffer
     * @param obj SerializableObject to push
     * @param timeout_ms Timeout in milliseconds (negative blocks indefinitely)
     * @throws IPCException on errors
     */
    void push(const SerializableObject& obj, 
             int timeout_ms = -1) override;
    
    /**
     * @brief Push pre-serialized data
     * @param metadata_json JSON metadata string
     * @param payload Binary payload
     * @param timeout_ms Maximum time to wait for space in milliseconds (0 = no wait, -1 = infinite)
     * @return true if successful
     */
    bool push_raw(
        const std::string& metadata_json,
        const std::vector<uint8_t>& payload,
        int timeout_ms = -1
    );
    
    /**
     * @brief Close the producer by sending end-of-stream marker (IPCProducer interface).
     * 
     * Sends a slot with payload_size=0 to signal the consumer to stop,
     * then closes and unlinks the shared memory.
     * @throws IPCException on errors
     */
    void close() override;
    
    /**
     * @brief Get available space in buffer
     */
    size_t available_space() const;
    
    /**
     * @brief Check if buffer is initialized
     */
    bool is_initialized() const { return shm_ptr_ != nullptr; }
    
    /**
     * @brief Get shared memory name
     */
    const std::string& shm_name() const { return shm_name_; }

private:
    std::string shm_name_;
    size_t total_data_bytes_;
    size_t shm_size_;  // HEADER_SIZE + total_data_bytes_
    int shm_fd_;
    void* shm_ptr_;
    
    // Initialize shared memory
    bool init_shm(bool create_new);
    
    // Get/set positions (ABSOLUTE positions within the mapped region)
    uint64_t get_write_pos() const;
    uint64_t get_read_pos() const;
    void set_write_pos(uint64_t pos);
    
    // Calculate available space
    size_t available_space(uint64_t write_pos, uint64_t read_pos) const;
    
    // Write slot to buffer
    bool write_slot(
        const std::string& metadata_json,
        const std::vector<uint8_t>& payload,
        uint64_t write_pos
    );
    
    // Helper to write bytes at position
    void write_bytes(size_t pos, const void* data, size_t size);
};

} // namespace ipc0cp

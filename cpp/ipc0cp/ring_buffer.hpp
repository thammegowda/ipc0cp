#pragma once

#include "serialize.hpp"
#include <string>
#include <cstdint>
#include <memory>
#include <optional>
#include <chrono>
#include <map>
#include <vector>
#include <stdexcept>

namespace ipc0cp {

/**
 * @brief Error types for ring buffer operations
 */
enum class RingBufferError {
    None,
    NotInitialized,
    ShmNotFound,
    SizeMismatch,
    InvalidMetadata,
    InvalidSlot,
    Timeout,
    BufferEmpty,
    DeserializationFailed,
    CorruptPayload
};

/**
 * @brief Convert error to string
 */
inline std::string errorToString(RingBufferError error) {
    switch (error) {
        case RingBufferError::None: return "No error";
        case RingBufferError::NotInitialized: return "Shared memory not initialized";
        case RingBufferError::ShmNotFound: return "Shared memory segment not found";
        case RingBufferError::SizeMismatch: return "Shared memory size mismatch";
        case RingBufferError::InvalidMetadata: return "Invalid metadata";
        case RingBufferError::InvalidSlot: return "Invalid slot data";
        case RingBufferError::Timeout: return "Operation timed out";
        case RingBufferError::BufferEmpty: return "Buffer is empty";
        case RingBufferError::DeserializationFailed: return "Deserialization failed";
        case RingBufferError::CorruptPayload: return "Corrupt payload: sentinel bytes mismatch";
        default: return "Unknown error";
    }
}

/**
 * @brief Exception class for ring buffer errors
 */
class RingBufferException : public std::runtime_error {
public:
    RingBufferError error_type;
    
    explicit RingBufferException(RingBufferError error) 
        : std::runtime_error(errorToString(error)), error_type(error) {}
    
    RingBufferException(RingBufferError error, const std::string& msg) 
        : std::runtime_error(msg), error_type(error) {}
};

/**
 * Shared Ring Buffer Memory Layout
 * =================================
 * 
 * Header (24 bytes):
 *   - write_offset (uint64, 8 bytes): Offset where next slot will be written
 *   - read_offset (uint64, 8 bytes): Offset of next slot to read
 *   - total_data_bytes (uint64, 8 bytes): Total size of data region
 * 
 * Data Region (variable size):
 *   Each slot contains:
 *     - next_offset (uint64, 8 bytes): Offset of next slot
 *     - metadata_size (uint32, 4 bytes): Size of JSON metadata
 *     - payload_size (uint64, 8 bytes): Size of payload data
 *     - metadata_json (variable, max 1024 bytes): JSON metadata
 *     - start_sentinel (uint8, 1 byte, value=0x00): Data integrity check
 *     - payload (variable): Binary payload data
 *     - end_sentinel (uint8, 1 byte, value=0x00): Data integrity check
 * 
 * Sentinel bytes detect buffer overruns and data corruption.
 */

// Constants matching Python implementation
constexpr size_t HEADER_SIZE = 24;  // 3 * uint64: write_offset, read_offset, total_data_bytes
constexpr size_t SLOT_HEADER_SIZE = 20;  // next_offset(8) + metadata_size(4) + payload_size(8)
constexpr uint8_t SENTINEL_BYTE = 0x00;  // Null byte for data integrity checking (before and after payload)
constexpr size_t MAX_METADATA_SIZE = 1024;
constexpr size_t MAX_SLOT_SIZE = 10 * 1024 * 1024;  // 10 MB
constexpr size_t DEFAULT_TOTAL_DATA_BYTES = 1024ULL * 1024 * 1024;  // 1 GB

/**
 * @brief A generic object from the ring buffer with deserialized data
 */
struct RingBufferObject {
    std::unique_ptr<SerializableObject> data;
    std::map<std::string, std::string> raw_metadata;  // Original metadata
    
    RingBufferObject() = default;
    explicit RingBufferObject(std::unique_ptr<SerializableObject> d) 
        : data(std::move(d)) {}
    
    ObjectType get_type() const { 
        return data ? data->get_type() : ObjectType::Unknown; 
    }
    
    // Helper accessor with dynamic_cast (throws std::bad_cast on failure)
    template<typename T>
        requires std::derived_from<T, SerializableObject>
    T& as() {
        auto* ptr = dynamic_cast<T*>(data.get());
        if (!ptr) throw std::bad_cast();
        return *ptr;
    }
    
    template<typename T>
        requires std::derived_from<T, SerializableObject>
    const T& as() const {
        auto* ptr = dynamic_cast<const T*>(data.get());
        if (!ptr) throw std::bad_cast();
        return *ptr;
    }
};

/**
 * @brief Result type for operations that can fail
 */
template<typename T>
using Result = std::variant<T, RingBufferError>;

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
        uint64_t write_offset;
        uint64_t read_offset;
        size_t available_bytes;
        size_t used_bytes;
        size_t total_data_bytes;
        bool is_empty;
    };
    
    Stats get_stats() const;

protected:
    SharedRingBufferBase(std::string shm_name, size_t total_data_bytes, bool blocking);
    
    // Offset operations
    uint64_t get_write_offset() const;
    uint64_t get_read_offset() const;
    size_t available_space(uint64_t write_offset, uint64_t read_offset) const;
    uint64_t normalize_offset(uint64_t offset) const;
    
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
class SharedRingBufferConsumer : public SharedRingBufferBase {
public:
    /**
     * @brief Construct a consumer
     * @param shm_name Name of the POSIX shared memory segment
     * @param total_data_bytes Total size of data region (must match producer)
     * @param blocking Whether to block when buffer is empty
     * @param auto_attach If true, automatically attach to shared memory in constructor
     */
    SharedRingBufferConsumer(
        std::string shm_name,
        size_t total_data_bytes = DEFAULT_TOTAL_DATA_BYTES,
        bool blocking = true,
        bool auto_attach = true
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
    RingBufferError get_last_error() const { return last_error_; }
    
    /**
     * @brief Pop an object from the ring buffer
     * @param timeout Maximum time to wait in milliseconds (nullopt = infinite if blocking)
     * @return Deserialized object, or std::nullopt if end-of-stream marker received
     * @throws RingBufferException with error_type indicating the error
     */
    std::optional<RingBufferObject> pop(
        std::optional<std::chrono::milliseconds> timeout = std::nullopt
    );

private:
    void set_read_offset(uint64_t offset);
    uint64_t read_uint64(uint64_t pos);
    uint32_t read_uint32(uint64_t pos);
    std::vector<uint8_t> read_bytes(uint64_t pos, size_t length);
    uint64_t advance_pos(uint64_t pos, size_t offset);
    
    std::optional<std::map<std::string, std::string>> parse_metadata(
        const std::vector<uint8_t>& metadata_bytes
    );
    
    RingBufferError last_error_ = RingBufferError::NotInitialized;
    bool eos_received_ = false;  ///< Track if end-of-stream was received
};

/**
 * @brief Producer for SharedRingBuffer
 * 
 * Writes serialized objects to a POSIX shared memory ring buffer.
 * Compatible with Python's SharedRingBufferProducer.
 */
class SharedRingBufferProducer {
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
     * @param timeout_ms Maximum time to wait for space in milliseconds (0 = no wait, -1 = infinite)
     * @return true if successful, false if buffer full or error
     */
    bool push(const SerializableObject& obj, int timeout_ms = -1);
    
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
     * @brief Close the producer by sending end-of-stream marker.
     * 
     * Sends a slot with payload_size=0 to signal the consumer to stop,
     * then closes and unlinks the shared memory.
     */
    void close();
    
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
    
    // Get/set offsets
    uint64_t get_write_offset() const;
    uint64_t get_read_offset() const;
    void set_write_offset(uint64_t offset);
    
    // Calculate available space
    size_t available_space(uint64_t write_offset, uint64_t read_offset) const;
    
    // Write slot to buffer
    bool write_slot(
        const std::string& metadata_json,
        const std::vector<uint8_t>& payload,
        uint64_t write_offset
    );
    
    // Helper to write bytes at position
    void write_bytes(size_t pos, const void* data, size_t size);
    
    // Helper to write uint64 in little-endian
    void write_uint64_le(uint8_t* ptr, uint64_t value);
    void write_uint32_le(uint8_t* ptr, uint32_t value);
};

} // namespace ipc0cp

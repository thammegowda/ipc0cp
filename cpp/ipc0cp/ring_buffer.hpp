#pragma once

#include "ipc.hpp"
#include "serialize.hpp"
#include "posix_sync.hpp"
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
 * Shared Ring Buffer Memory Layout (MPMC)
 * ========================================
 * 
 * Uses POSIX shared memory (mmap) + POSIX named semaphores for cross-process synchronization.
 * 
 * Header (64 bytes):
 *   - write_pos (uint64, 8 bytes): ABSOLUTE byte position of next write slot
 *   - read_pos (uint64, 8 bytes): ABSOLUTE byte position of next read slot
 *   - total_data_bytes (uint64, 8 bytes): Total size of data region
 *   - active_producers (uint32, 4 bytes): Number of currently attached producers
 *   - active_consumers (uint32, 4 bytes): Number of currently attached consumers
 *   - total_producers_joined (uint32, 4 bytes): Cumulative producers that have joined
 *   - total_consumers_joined (uint32, 4 bytes): Cumulative consumers that have joined
 *   - reserved (24 bytes): Reserved for future use
 * 
 * Synchronization:
 *   Named POSIX semaphores provide cross-process mutual exclusion and condition signaling.
 *   Semaphore names: /{shm_name}_mutex (mutex) and /{shm_name}_wait (condition)
 * 
 * Data Region (variable size):
 *   Each slot contains:
 *     - next_pos (uint64, 8 bytes): ABSOLUTE byte position of next slot
 *     - metadata_size (uint32, 4 bytes): Size of JSON metadata
 *     - payload_size (uint64, 8 bytes): Size of payload data
 *     - metadata_json (variable, max 1024 bytes): JSON metadata
 *     - start_sentinel (uint8, 1 byte, value=0x00): Data integrity check
 *     - payload (variable): Binary payload data
 *     - end_sentinel (uint8, 1 byte, value=0x00): Data integrity check
 * 
 * End-of-Stream (EOS):
 *   Implicit: buffer is EOS when (active_producers == 0) AND (buffer is empty)
 *   pop() returns nullptr only for EOS; errors throw IPCException.
 * 
 * Cleanup Policy:
 *   - Producers NEVER unlink shared memory (ignored with warning).
 *   - Last consumer unlinks shared memory and cleans up semaphores (if auto_unlink=true).
 */

/**
 * @brief Result type for operations that can fail
 */
template<typename T>
using Result = std::variant<T, IPCError>;

/**
 * @brief Base class for shared ring buffer (MPMC)
 * 
 * Handles POSIX shared memory and semaphore management.
 * Subclasses (Producer/Consumer) implement specific push/pop logic.
 */
class SharedRingBufferBase {
public:
    virtual ~SharedRingBufferBase();
    
    /**
     * @brief Close the shared memory mapping
     * Unmaps the shared memory but does not delete it
     */
    void close();
    
    /**
     * @brief Unlink (delete) the shared memory segment and semaphores
     * Only consumers should call this; producers calling this will log a warning and no-op
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
        uint32_t active_producers;
        uint32_t active_consumers;
    };
    
    Stats get_stats() const;

protected:
    SharedRingBufferBase(
        std::string shm_name,
        size_t total_data_bytes,
        bool blocking
    );
    
    // Position operations (ABSOLUTE positions within the mapped region)
    uint64_t get_write_pos() const;
    uint64_t get_read_pos() const;
    void set_write_pos(uint64_t pos);
    void set_read_pos(uint64_t pos);
    
    // Producer/Consumer counters (atomic within semaphore lock)
    uint32_t get_active_producers() const;
    void set_active_producers(uint32_t count);
    uint32_t increment_active_producers();
    uint32_t decrement_active_producers();
    
    uint32_t get_active_consumers() const;
    void set_active_consumers(uint32_t count);
    uint32_t increment_active_consumers();
    uint32_t decrement_active_consumers();
    
    // Space calculations
    size_t available_space(uint64_t write_pos, uint64_t read_pos) const;
    uint64_t normalize_pos(uint64_t pos) const;
    
    // Utility
    bool is_empty() const;
    
    // Synchronization primitives
    void lock_buffer();
    void unlock_buffer();
    bool wait_for_signal(int timeout_ms = -1);
    void notify_all();
    
    // Data access helpers
    uint64_t read_uint64(uint64_t pos) const;
    uint32_t read_uint32(uint64_t pos) const;
    void write_uint64(uint64_t pos, uint64_t value);
    void write_uint32(uint64_t pos, uint32_t value);
    void write_bytes(uint64_t pos, const void* data, size_t size);
    std::vector<uint8_t> read_bytes(uint64_t pos, size_t length) const;
    
    std::string shm_name_;
    size_t total_data_bytes_;
    bool blocking_;
    size_t shm_size_;
    
    std::unique_ptr<PosixSharedMemory> shm_;
    std::unique_ptr<PosixCondition> condition_;
    
    bool is_producer_ = false;  // Track if this is a producer or consumer for cleanup
};

/**
 * @brief Consumer for SharedRingBuffer (MPMC)
 * 
 * Attaches to existing shared memory and pops objects.
 * Only call pop() and close().
 * 
 * Example:
 * @code
 * auto consumer = SharedRingBufferConsumer("my_buffer");
 * 
 * while (true) {
 *     try {
 *         auto obj = consumer.pop(std::chrono::seconds(5));
 *         if (!obj) break;  // End-of-stream
 *         
 *         // Process object
 *         if (obj->get_type() == ObjectType::Text) {
 *             // ... handle text ...
 *         }
 *     } catch (const IPCException& e) {
 *         std::cerr << "Error: " << e.what() << "\n";
 *         break;
 *     }
 * }
 * @endcode
 */
class SharedRingBufferConsumer : public SharedRingBufferBase, public IPCConsumer {
public:
    /**
     * @brief Construct a consumer
     * @param shm_name Name of the POSIX shared memory segment
     * @param blocking Whether to block when buffer is empty
     * @param auto_unlink If true, automatically unlink SHM + semaphores when EOS is received
     */
    SharedRingBufferConsumer(
        std::string shm_name,
        bool blocking = true,
        bool auto_unlink = true
    );
    
    ~SharedRingBufferConsumer() override;
    
    // Disable copy, allow move
    SharedRingBufferConsumer(const SharedRingBufferConsumer&) = delete;
    SharedRingBufferConsumer& operator=(const SharedRingBufferConsumer&) = delete;
    SharedRingBufferConsumer(SharedRingBufferConsumer&& other) noexcept;
    SharedRingBufferConsumer& operator=(SharedRingBufferConsumer&& other) noexcept;
    
    /**
     * @brief Get last error code
     */
    IPCError get_last_error() const override { return last_error_; }
    
    /**
     * @brief Pop an object from the ring buffer
     * @param timeout_ms Timeout in milliseconds (<0 = infinite when blocking)
     * @return Deserialized object, or nullptr if end-of-stream reached
     * @throws IPCException on errors (timeout, corruption, deserialization failure)
     */
    std::unique_ptr<IPCObject> pop(
        int timeout_ms = -1) override;
    
    /**
     * @brief Pop raw metadata and payload
     * @param timeout_ms Timeout in milliseconds
     * @return Pair of (metadata_json, payload_bytes), or empty optional if EOS
     */
    std::optional<std::pair<std::string, std::vector<uint8_t>>> pop_raw(
        int timeout_ms = -1) override;

    /**
     * @brief Check if end-of-stream was received
     */
    bool eos_received() const override { return eos_received_; }

public:
    /**
     * @brief Manually attach to existing shared memory (for tests)
     * @return true on success, false on failure
     */
    bool attach();

    /**
     * @brief Close this consumer
     *
     * Decrements active_consumers (once) and detaches from SHM/semaphores.
     * If auto_unlink=true, the last consumer will also unlink SHM + semaphores
     * once there are no active producers.
     */
    void close();

private:
    
    IPCError last_error_ = IPCError::NotInitialized;
    bool eos_received_ = false;
    bool auto_unlink_ = true;
    bool registered_ = false;
};

/**
 * @brief Producer for SharedRingBuffer (MPMC)
 * 
 * Creates or attaches to shared memory and pushes objects.
 * 
 * Example:
 * @code
 * auto producer = SharedRingBufferProducer("my_buffer", 100*1024*1024);
 * 
 * std::string text = "Hello, World!";
 * auto data = std::make_unique<TextData>(text);
 * producer.push(*data);
 * 
 * producer.close();  // Signals EOS to consumers
 * @endcode
 */
class SharedRingBufferProducer : public SharedRingBufferBase, public IPCProducer {
public:
    /**
     * @brief Construct producer
     * @param shm_name Name of shared memory segment
     * @param total_data_bytes Total size of data region (default 1GB)
        * @param create_new If true, create-or-attach; if false, attach-only
     * @param blocking If true, block when buffer is full; if false, fail fast with NO_CONSUMERS
     */
    explicit SharedRingBufferProducer(
        const std::string& shm_name,
        size_t total_data_bytes = DEFAULT_TOTAL_DATA_BYTES,
        bool create_new = true,
        bool blocking = true
    );
    
    ~SharedRingBufferProducer() override;
    
    // Disable copy, allow move
    SharedRingBufferProducer(const SharedRingBufferProducer&) = delete;
    SharedRingBufferProducer& operator=(const SharedRingBufferProducer&) = delete;
    SharedRingBufferProducer(SharedRingBufferProducer&& other) noexcept;
    SharedRingBufferProducer& operator=(SharedRingBufferProducer&& other) noexcept;
    
    /**
     * @brief Push an object to the ring buffer
     * @param obj SerializableObject to push
     * @param timeout_ms Timeout in milliseconds (<0 = infinite)
     * @throws IPCException on errors
     */
    void push(const SerializableObject& obj, 
             int timeout_ms = -1) override;
    
    /**
     * @brief Push pre-serialized data
     * @param metadata_json JSON metadata string
     * @param payload Binary payload
     * @param timeout_ms Timeout in milliseconds (<0 = infinite)
     * @return true on success
     * @throws IPCException on errors
     */
    bool push_raw(
        const std::string& metadata_json,
        const std::vector<uint8_t>& payload,
        int timeout_ms = -1
    );
    
    /**
     * @brief Close the producer (send EOS)
     * 
    * Decrements active_producers counter and notifies consumers.
     * Does NOT unlink shared memory (consumers own cleanup).
     */
    void close() override;
    
    /**
     * @brief Unlink shared memory (producer-side, no-op with warning)
     * 
     * Producers do not own the SHM lifecycle; consumers do.
     * This is provided for API compatibility but does nothing.
     */
    void unlink();
    
    /**
     * @brief Get available space in buffer
     */
    size_t available_space() const;
    
    /**
     * @brief Check if buffer is initialized
     */
    bool is_initialized() const { return shm_ != nullptr; }
    
    /**
     * @brief Get shared memory name
     */
    const std::string& shm_name() const { return shm_name_; }

private:
    /**
     * @brief Create new shared memory and initialize header
     */
    void create_shm();
    
    /**
     * @brief Attach to existing shared memory as additional producer
     */
    void attach_shm();
    
    bool create_new_;
    IPCError last_error_ = IPCError::NotInitialized;
    bool registered_ = false;
};

} // namespace ipc0cp

#include "ring_buffer.hpp"
#include "logger.hpp"
#include <cstring>
#include <thread>
#include <chrono>
#include <stdexcept>
#include <cerrno>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace ipc0cp {

namespace {

class InitSemaphoreGuard {
public:
    explicit InitSemaphoreGuard(const std::string& buffer_name)
        : name_(posix_name(normalize_ipc_base_name(buffer_name) + "_init"))
    {
        sem_ = sem_open(name_.c_str(), O_CREAT, 0600, 1);
        if (sem_ == SEM_FAILED) {
            throw std::runtime_error(std::string("sem_open(init) failed: ") + strerror(errno));
        }
        if (sem_wait(sem_) < 0) {
            int err = errno;
            sem_close(sem_);
            sem_ = nullptr;
            throw std::runtime_error(std::string("sem_wait(init) failed: ") + strerror(err));
        }
        locked_ = true;
    }

    ~InitSemaphoreGuard() {
        if (sem_ && sem_ != SEM_FAILED) {
            if (locked_) {
                sem_post(sem_);
            }
            sem_close(sem_);
        }
    }

    InitSemaphoreGuard(const InitSemaphoreGuard&) = delete;
    InitSemaphoreGuard& operator=(const InitSemaphoreGuard&) = delete;

private:
    std::string name_;
    sem_t* sem_ = nullptr;
    bool locked_ = false;
};

// Helper to get data region boundaries
inline uint64_t data_begin_abs() {
    return HEADER_SIZE;
}

inline uint64_t data_end_abs(size_t total_data_bytes) {
    return HEADER_SIZE + total_data_bytes;
}

inline uint64_t normalize_abs(uint64_t abs_pos, size_t total_data_bytes) {
    const uint64_t begin = data_begin_abs();
    const uint64_t end = data_end_abs(total_data_bytes);
    if (abs_pos >= end) {
        return begin + (abs_pos - begin) % total_data_bytes;
    }
    return abs_pos;
}

// Convenience constant for producer-wait-for-consumer timeout
constexpr int PRODUCER_WAIT_FOR_CONSUMER_TIMEOUT_MS = 60000;  // 60 seconds

} // anonymous namespace

// ============================================================================
// SharedRingBufferBase
// ============================================================================

SharedRingBufferBase::SharedRingBufferBase(
    std::string shm_name,
    size_t total_data_bytes,
    bool blocking
)
    : shm_name_(std::move(shm_name))
    , total_data_bytes_(total_data_bytes)
    , blocking_(blocking)
    , shm_size_(HEADER_SIZE + total_data_bytes)
{}

SharedRingBufferBase::~SharedRingBufferBase() {
    close();
}

void SharedRingBufferBase::close() {
    if (shm_) {
        shm_->close();
        shm_.reset();
    }
    if (condition_) {
        condition_.reset();
    }
}

void SharedRingBufferBase::unlink() {
    // Unlink by name so this works even after close().
    ::shm_unlink(posix_name(shm_name_).c_str());
    cleanup_buffer_semaphores(shm_name_);
}

uint64_t SharedRingBufferBase::get_write_pos() const {
    if (!shm_) return 0;
    return read_le64(static_cast<const uint8_t*>(shm_->data()));
}

uint64_t SharedRingBufferBase::get_read_pos() const {
    if (!shm_) return 0;
    return read_le64(static_cast<const uint8_t*>(shm_->data()) + 8);
}

void SharedRingBufferBase::set_write_pos(uint64_t pos) {
    if (shm_) {
        write_le64(static_cast<uint8_t*>(shm_->data()), pos);
    }
}

void SharedRingBufferBase::set_read_pos(uint64_t pos) {
    if (shm_) {
        write_le64(static_cast<uint8_t*>(shm_->data()) + 8, pos);
    }
}

uint32_t SharedRingBufferBase::get_active_producers() const {
    if (!shm_) return 0;
    return read_le32(static_cast<const uint8_t*>(shm_->data()) + 24);
}

void SharedRingBufferBase::set_active_producers(uint32_t count) {
    if (shm_) {
        write_le32(static_cast<uint8_t*>(shm_->data()) + 24, count);
    }
}

uint32_t SharedRingBufferBase::increment_active_producers() {
    uint32_t count = get_active_producers() + 1;
    set_active_producers(count);
    return count;
}

uint32_t SharedRingBufferBase::decrement_active_producers() {
    uint32_t count = get_active_producers();
    if (count > 0) count--;
    set_active_producers(count);
    return count;
}

uint32_t SharedRingBufferBase::get_active_consumers() const {
    if (!shm_) return 0;
    return read_le32(static_cast<const uint8_t*>(shm_->data()) + 28);
}

void SharedRingBufferBase::set_active_consumers(uint32_t count) {
    if (shm_) {
        write_le32(static_cast<uint8_t*>(shm_->data()) + 28, count);
    }
}

uint32_t SharedRingBufferBase::increment_active_consumers() {
    uint32_t count = get_active_consumers() + 1;
    set_active_consumers(count);
    return count;
}

uint32_t SharedRingBufferBase::decrement_active_consumers() {
    uint32_t count = get_active_consumers();
    if (count > 0) count--;
    set_active_consumers(count);
    return count;
}

size_t SharedRingBufferBase::available_space(uint64_t write_pos, uint64_t read_pos) const {
    if (write_pos >= read_pos) {
        size_t space_to_end = data_end_abs(total_data_bytes_) - write_pos;
        size_t space_from_start = read_pos - data_begin_abs();
        return space_to_end + space_from_start;
    } else {
        return read_pos - write_pos;
    }
}

uint64_t SharedRingBufferBase::normalize_pos(uint64_t pos) const {
    return normalize_abs(pos, total_data_bytes_);
}

bool SharedRingBufferBase::is_empty() const {
    return get_write_pos() == get_read_pos();
}

void SharedRingBufferBase::lock_buffer() {
    if (condition_) {
        condition_->lock();
    }
}

void SharedRingBufferBase::unlock_buffer() {
    if (condition_) {
        condition_->unlock();
    }
}

bool SharedRingBufferBase::wait_for_signal(int timeout_ms) {
    if (condition_) {
        return condition_->wait(timeout_ms);
    }
    return false;
}

void SharedRingBufferBase::notify_all() {
    if (condition_) {
        condition_->notify_all();
    }
}

uint64_t SharedRingBufferBase::read_uint64(uint64_t pos) const {
    auto bytes = read_bytes(pos, 8);
    return read_le64(bytes.data());
}

uint32_t SharedRingBufferBase::read_uint32(uint64_t pos) const {
    auto bytes = read_bytes(pos, 4);
    return read_le32(bytes.data());
}

void SharedRingBufferBase::write_uint64(uint64_t pos, uint64_t value) {
    uint8_t buf[8];
    write_le64(buf, value);
    write_bytes(pos, buf, 8);
}

void SharedRingBufferBase::write_uint32(uint64_t pos, uint32_t value) {
    uint8_t buf[4];
    write_le32(buf, value);
    write_bytes(pos, buf, 4);
}

void SharedRingBufferBase::write_bytes(uint64_t pos, const void* data, size_t size) {
    if (!shm_) return;
    
    uint64_t abs_pos = normalize_abs(pos, total_data_bytes_);
    const uint64_t end_of_region = data_end_abs(total_data_bytes_);
    
    if (abs_pos + size <= end_of_region) {
        std::memcpy(static_cast<uint8_t*>(shm_->data()) + abs_pos, data, size);
    } else {
        // Wraparound needed
        size_t first_part = end_of_region - abs_pos;
        std::memcpy(static_cast<uint8_t*>(shm_->data()) + abs_pos, data, first_part);
        size_t second_part = size - first_part;
        std::memcpy(static_cast<uint8_t*>(shm_->data()) + data_begin_abs(),
                   static_cast<const uint8_t*>(data) + first_part, second_part);
    }
}

std::vector<uint8_t> SharedRingBufferBase::read_bytes(uint64_t pos, size_t length) const {
    if (!shm_) return {};
    
    std::vector<uint8_t> result(length);
    uint64_t abs_pos = normalize_abs(pos, total_data_bytes_);
    const uint64_t end_of_region = data_end_abs(total_data_bytes_);
    
    if (abs_pos + length <= end_of_region) {
        std::memcpy(result.data(), static_cast<const uint8_t*>(shm_->data()) + abs_pos, length);
    } else {
        // Wraparound needed
        size_t first_part = end_of_region - abs_pos;
        std::memcpy(result.data(), static_cast<const uint8_t*>(shm_->data()) + abs_pos, first_part);
        size_t second_part = length - first_part;
        std::memcpy(result.data() + first_part,
                   static_cast<const uint8_t*>(shm_->data()) + data_begin_abs(), second_part);
    }
    
    return result;
}

SharedRingBufferBase::Stats SharedRingBufferBase::get_stats() const {
    uint64_t write_pos = get_write_pos();
    uint64_t read_pos = get_read_pos();
    size_t available = available_space(write_pos, read_pos);
    
    return Stats{
        .write_pos = write_pos,
        .read_pos = read_pos,
        .available_bytes = available,
        .used_bytes = total_data_bytes_ - available,
        .total_data_bytes = total_data_bytes_,
        .is_empty = (write_pos == read_pos),
        .active_producers = get_active_producers(),
        .active_consumers = get_active_consumers()
    };
}

// ============================================================================
// SharedRingBufferConsumer
// ============================================================================

SharedRingBufferConsumer::SharedRingBufferConsumer(
    std::string shm_name,
    bool blocking,
    bool auto_unlink
)
    : SharedRingBufferBase(std::move(shm_name), /*total_data_bytes=*/0, blocking)
    , auto_unlink_(auto_unlink)
{
    is_producer_ = false;
}

SharedRingBufferConsumer::~SharedRingBufferConsumer() {
    try {
        close();
    } catch (...) {}
}

SharedRingBufferConsumer::SharedRingBufferConsumer(SharedRingBufferConsumer&& other) noexcept
    : SharedRingBufferBase(std::move(other.shm_name_), other.total_data_bytes_, other.blocking_)
    , last_error_(other.last_error_)
    , eos_received_(other.eos_received_)
    , auto_unlink_(other.auto_unlink_)
{
    shm_ = std::move(other.shm_);
    condition_ = std::move(other.condition_);
    is_producer_ = other.is_producer_;
    other.eos_received_ = false;
}

SharedRingBufferConsumer& SharedRingBufferConsumer::operator=(SharedRingBufferConsumer&& other) noexcept {
    if (this != &other) {
        close();
        shm_ = std::move(other.shm_);
        condition_ = std::move(other.condition_);
        shm_name_ = std::move(other.shm_name_);
        total_data_bytes_ = other.total_data_bytes_;
        blocking_ = other.blocking_;
        last_error_ = other.last_error_;
        eos_received_ = other.eos_received_;
        auto_unlink_ = other.auto_unlink_;
        is_producer_ = other.is_producer_;
        other.eos_received_ = false;
    }
    return *this;
}

bool SharedRingBufferConsumer::attach() {
    try {
        if (registered_) {
            return true;
        }

        // Create POSIX SHM wrapper (attach to existing)
        shm_ = std::make_unique<PosixSharedMemory>(shm_name_, false, 0);
        
        // Attach to existing POSIX semaphores
        condition_ = get_buffer_semaphores(shm_name_, false);

        const size_t actual_shm_size = shm_->size();
        if (actual_shm_size < HEADER_SIZE) {
            throw std::runtime_error("Shared memory too small");
        }

        // Read total_data_bytes from header and validate it against the actual SHM size.
        const uint64_t stored_total = read_le64(
            static_cast<const uint8_t*>(shm_->data()) + 16
        );
        if (stored_total == 0) {
            throw std::runtime_error("Invalid total_data_bytes in header");
        }

        const size_t expected_shm_size = HEADER_SIZE + static_cast<size_t>(stored_total);
        if (expected_shm_size != actual_shm_size) {
            throw std::runtime_error("Shared memory size mismatch");
        }

        if (total_data_bytes_ != 0 && stored_total != total_data_bytes_) {
            IPC_LOG_WARNING(
                "total_data_bytes mismatch: stored=" << stored_total
                                                    << " expected=" << total_data_bytes_
            );
        }
        total_data_bytes_ = static_cast<size_t>(stored_total);
        shm_size_ = expected_shm_size;
        
        // Register this consumer
        lock_buffer();
        increment_active_consumers();
        registered_ = true;
        notify_all();
        unlock_buffer();
        
        IPC_LOG_INFO("Consumer attached to '" << shm_name_ << "'");
        return true;
    } catch (const std::exception& e) {
        last_error_ = IPCError::ShmNotFound;
        return false;
    }
}

void SharedRingBufferConsumer::close() {
    if (!shm_ && !condition_) {
        return;
    }

    bool should_unlink = false;
    if (registered_ && shm_ && condition_) {
        lock_buffer();
        decrement_active_consumers();
        registered_ = false;
        uint32_t remaining_consumers = get_active_consumers();
        uint32_t remaining_producers = get_active_producers();
        notify_all();
        unlock_buffer();

        should_unlink = auto_unlink_ && eos_received_ && remaining_consumers == 0 && remaining_producers == 0;
    }

    if (should_unlink) {
        try {
            SharedRingBufferBase::unlink();
        } catch (...) {
            // best-effort
        }
    }

    SharedRingBufferBase::close();
}

std::optional<std::pair<std::string, std::vector<uint8_t>>> SharedRingBufferConsumer::pop_raw(
    int timeout_ms
) {
    if (!shm_) {
        throw IPCException(IPCError::NotInitialized);
    }
    
    auto start_time = std::chrono::steady_clock::now();
    
    while (true) {
        lock_buffer();
        
        uint64_t write_pos = get_write_pos();
        uint64_t read_pos = get_read_pos();
        uint32_t active_producers = get_active_producers();
        
        // Check if we have data
        if (write_pos != read_pos) {
            // We have data, read it
            uint64_t current_pos = read_pos;
            
            uint64_t next_pos = read_uint64(current_pos);
            current_pos = normalize_abs(current_pos + 8, total_data_bytes_);
            
            uint32_t metadata_size = read_uint32(current_pos);
            current_pos = normalize_abs(current_pos + 4, total_data_bytes_);
            
            uint64_t payload_size = read_uint64(current_pos);
            current_pos = normalize_abs(current_pos + 8, total_data_bytes_);
            
            if (metadata_size > MAX_METADATA_SIZE) {
                unlock_buffer();
                throw IPCException(IPCError::InvalidMetadata, 
                                  "Invalid metadata size");
            }
            
            // Read metadata
            auto metadata_bytes = read_bytes(current_pos, metadata_size);
            current_pos = normalize_abs(current_pos + metadata_size, total_data_bytes_);
            
            // Read start sentinel
            auto start_sentinel = read_bytes(current_pos, 1);
            if (start_sentinel.empty() || start_sentinel[0] != SENTINEL_BYTE) {
                unlock_buffer();
                throw IPCException(IPCError::CorruptPayload, "Invalid start sentinel");
            }
            current_pos = normalize_abs(current_pos + 1, total_data_bytes_);
            
            // Read payload
            auto payload = read_bytes(current_pos, payload_size);
            current_pos = normalize_abs(current_pos + payload_size, total_data_bytes_);
            
            // Read end sentinel
            auto end_sentinel = read_bytes(current_pos, 1);
            if (end_sentinel.empty() || end_sentinel[0] != SENTINEL_BYTE) {
                unlock_buffer();
                throw IPCException(IPCError::CorruptPayload, "Invalid end sentinel");
            }
            
            // Update read position
            set_read_pos(next_pos);
            notify_all();  // Wake producers waiting for space
            unlock_buffer();
            
            std::string metadata_json(metadata_bytes.begin(), metadata_bytes.end());
            return std::make_pair(metadata_json, payload);
        }
        
        // No data available
        if (active_producers == 0 && write_pos == read_pos) {
            // EOS: no more producers and buffer is empty
            eos_received_ = true;
            unlock_buffer();
            return std::nullopt;
        }
        
        // Still waiting for data
        if (!blocking_) {
            unlock_buffer();
            throw IPCException(IPCError::BufferEmpty, "Buffer is empty");
        }
        
        // Check timeout
        if (timeout_ms >= 0) {
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
            if (elapsed_ms >= timeout_ms) {
                unlock_buffer();
                throw IPCException(IPCError::Timeout, "pop_raw timed out");
            }
        }
        
        // Wait for signal
        int remaining_ms = -1;
        if (timeout_ms >= 0) {
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
            remaining_ms = timeout_ms - elapsed_ms;
            if (remaining_ms <= 0) {
                unlock_buffer();
                throw IPCException(IPCError::Timeout, "pop_raw timed out");
            }
        }
        
        wait_for_signal(remaining_ms);
        unlock_buffer();
    }
}

std::unique_ptr<IPCObject> SharedRingBufferConsumer::pop(int timeout_ms) {
    auto raw_pair = pop_raw(timeout_ms);
    if (!raw_pair) {
        return nullptr;  // EOS
    }
    
    auto metadata_json = raw_pair->first;
    auto payload = raw_pair->second;
    
    // Deserialize
    auto deserialized = deserialize(metadata_json, payload);
    if (!deserialized) {
        throw IPCException(IPCError::DeserializationFailed);
    }
    
    // Parse metadata
    try {
        auto j = json::parse(metadata_json);
        std::map<std::string, std::string> metadata_map;
        for (auto& [key, value] : j.items()) {
            metadata_map[key] = value.dump();
        }
        
        auto ipc_obj = std::make_unique<IPCObject>(std::move(deserialized));
        ipc_obj->raw_metadata = std::move(metadata_map);
        return ipc_obj;
    } catch (...) {
        throw IPCException(IPCError::InvalidMetadata, "Failed to parse metadata");
    }
}

// ============================================================================
// SharedRingBufferProducer
// ============================================================================

SharedRingBufferProducer::SharedRingBufferProducer(
    const std::string& shm_name,
    size_t total_data_bytes,
    bool create_new,
    bool blocking
)
    : SharedRingBufferBase(shm_name, total_data_bytes, blocking)
    , create_new_(create_new)
{
    is_producer_ = true;

    // Serialize initialization/attachment to avoid races between concurrent creators.
    // This mutex exists independently of the ring buffer's own semaphores.
    InitSemaphoreGuard init_guard(shm_name_);

    if (create_new) {
        // create_new=true means: create-or-attach.
        try {
            create_shm();
        } catch (const IPCException& create_err) {
            try {
                attach_shm();
            } catch (...) {
                throw create_err;
            }
        }
    } else {
        // create_new=false means: attach-only.
        attach_shm();
    }
}

SharedRingBufferProducer::~SharedRingBufferProducer() {
    try {
        close();
    } catch (...) {}
}

SharedRingBufferProducer::SharedRingBufferProducer(SharedRingBufferProducer&& other) noexcept
    : SharedRingBufferBase(std::move(other.shm_name_), other.total_data_bytes_, other.blocking_)
    , create_new_(other.create_new_)
    , last_error_(other.last_error_)
{
    shm_ = std::move(other.shm_);
    condition_ = std::move(other.condition_);
    is_producer_ = true;
}

SharedRingBufferProducer& SharedRingBufferProducer::operator=(SharedRingBufferProducer&& other) noexcept {
    if (this != &other) {
        shm_ = std::move(other.shm_);
        condition_ = std::move(other.condition_);
        shm_name_ = std::move(other.shm_name_);
        total_data_bytes_ = other.total_data_bytes_;
        blocking_ = other.blocking_;
        create_new_ = other.create_new_;
        last_error_ = other.last_error_;
        is_producer_ = true;
    }
    return *this;
}

void SharedRingBufferProducer::create_shm() {
    try {
        // Create POSIX SHM
        shm_ = std::make_unique<PosixSharedMemory>(shm_name_, true, shm_size_);
        
        // Create POSIX semaphores (create-or-attach to survive leftover semaphores after crashes)
        try {
            condition_ = get_buffer_semaphores(shm_name_, true);
        } catch (...) {
            condition_ = get_buffer_semaphores(shm_name_, false);
        }
        
        // Initialize header
        auto* header = static_cast<uint8_t*>(shm_->data());
        write_le64(header + 0, data_begin_abs());   // write_pos
        write_le64(header + 8, data_begin_abs());   // read_pos
        write_le64(header + 16, total_data_bytes_); // total_data_bytes
        write_le32(header + 24, 0);                 // active_producers (will increment below)
        write_le32(header + 28, 0);                 // active_consumers
        write_le32(header + 32, 0);                 // total_producers_joined
        write_le32(header + 36, 0);                 // total_consumers_joined
        std::memset(header + 40, 0, 24);            // reserved
        
        // Register this producer
        lock_buffer();
        increment_active_producers();
        registered_ = true;
        unlock_buffer();
        
        IPC_LOG_INFO("Created shared memory '" << shm_name_ << "' (" << shm_size_ << " bytes)");
    } catch (const std::exception& e) {
        last_error_ = IPCError::NotInitialized;
        throw IPCException(IPCError::NotInitialized,
                          std::string("Failed to create shared memory: ") + e.what());
    }
}

void SharedRingBufferProducer::attach_shm() {
    try {
        // Attach to existing POSIX SHM
        shm_ = std::make_unique<PosixSharedMemory>(shm_name_, false, 0);
        
        // Attach to existing POSIX semaphores (or create if missing)
        try {
            condition_ = get_buffer_semaphores(shm_name_, false);
        } catch (...) {
            condition_ = get_buffer_semaphores(shm_name_, true);
        }
        
        // Verify size
        if (shm_->size() != shm_size_) {
            throw std::runtime_error("Shared memory size mismatch");
        }
        
        // Read total_data_bytes from header
        uint64_t stored_total = read_le64(
            static_cast<const uint8_t*>(shm_->data()) + 16
        );
        if (stored_total != total_data_bytes_) {
            IPC_LOG_WARNING("total_data_bytes mismatch: stored=" << stored_total 
                          << " expected=" << total_data_bytes_);
            total_data_bytes_ = stored_total;
            shm_size_ = HEADER_SIZE + stored_total;
        }
        
        // Register this producer
        lock_buffer();
        increment_active_producers();
        registered_ = true;
        unlock_buffer();
        
        IPC_LOG_INFO("Producer attached to '" << shm_name_ << "'");
    } catch (const std::exception& e) {
        last_error_ = IPCError::ShmNotFound;
        throw IPCException(IPCError::ShmNotFound,
                          std::string("Failed to attach producer: ") + e.what());
    }
}

bool SharedRingBufferProducer::push_raw(
    const std::string& metadata_json,
    const std::vector<uint8_t>& payload,
    int timeout_ms
) {
    if (!shm_) {
        throw IPCException(IPCError::NotInitialized, "Shared memory not initialized");
    }
    
    if (metadata_json.size() > MAX_METADATA_SIZE) {
        throw IPCException(IPCError::InvalidMetadata, "Metadata too large");
    }
    
    uint64_t slot_size = SLOT_HEADER_SIZE + metadata_json.size() + 2 + payload.size();
    if (slot_size > MAX_SLOT_SIZE) {
        throw IPCException(IPCError::BufferFull, "Slot too large");
    }
    
    auto start_time = std::chrono::steady_clock::now();
    
    // Wait for at least one consumer before pushing (unless EOS marker)
    if (metadata_json.size() > 0 || payload.size() > 0) {
        while (true) {
            lock_buffer();
            uint32_t active_consumers = get_active_consumers();
            unlock_buffer();
            
            if (active_consumers >= 1) {
                break;
            }
            
            if (!blocking_) {
                throw IPCException(IPCError::NoConsumers, "No active consumers");
            }
            
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
            if (elapsed_ms >= PRODUCER_WAIT_FOR_CONSUMER_TIMEOUT_MS) {
                throw IPCException(IPCError::NoConsumers, 
                                  "Timed out waiting for a consumer to attach");
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    
    // Wait for buffer space
    while (true) {
        lock_buffer();
        
        uint64_t write_pos = get_write_pos();
        uint64_t read_pos = get_read_pos();
        size_t available = SharedRingBufferBase::available_space(write_pos, read_pos);
        
        if (available >= slot_size) {
            // Write slot
            uint64_t current_pos = write_pos;
            uint64_t next_pos = normalize_abs(write_pos + slot_size, total_data_bytes_);
            
            // Write slot header
            write_uint64(current_pos, next_pos);
            current_pos = normalize_abs(current_pos + 8, total_data_bytes_);
            
            write_uint32(current_pos, static_cast<uint32_t>(metadata_json.size()));
            current_pos = normalize_abs(current_pos + 4, total_data_bytes_);
            
            write_uint64(current_pos, payload.size());
            current_pos = normalize_abs(current_pos + 8, total_data_bytes_);
            
            // Write metadata
            if (metadata_json.size() > 0) {
                write_bytes(current_pos, metadata_json.data(), metadata_json.size());
                current_pos = normalize_abs(current_pos + metadata_json.size(), total_data_bytes_);
            }
            
            // Write start sentinel
            uint8_t sentinel = SENTINEL_BYTE;
            write_bytes(current_pos, &sentinel, 1);
            current_pos = normalize_abs(current_pos + 1, total_data_bytes_);
            
            // Write payload
            if (payload.size() > 0) {
                write_bytes(current_pos, payload.data(), payload.size());
                current_pos = normalize_abs(current_pos + payload.size(), total_data_bytes_);
            }
            
            // Write end sentinel
            write_bytes(current_pos, &sentinel, 1);
            
            // Update write position
            set_write_pos(next_pos);
            notify_all();  // Wake consumers
            unlock_buffer();
            
            return true;
        }
        
        // No space
        if (timeout_ms == 0) {
            unlock_buffer();
            throw IPCException(IPCError::BufferFull, "Buffer is full (no wait)");
        }
        
        if (timeout_ms > 0) {
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
            if (elapsed_ms >= timeout_ms) {
                unlock_buffer();
                throw IPCException(IPCError::Timeout, "push_raw timed out");
            }
        }
        
        // Wait for signal
        int remaining_ms = -1;
        if (timeout_ms > 0) {
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
            remaining_ms = timeout_ms - elapsed_ms;
        }
        
        wait_for_signal(remaining_ms);
        unlock_buffer();
    }
}

void SharedRingBufferProducer::push(const SerializableObject& obj, int timeout_ms) {
    auto serialized = obj.serialize();
    push_raw(serialized.metadata_json, serialized.payload, timeout_ms);
}

void SharedRingBufferProducer::close() {
    if (!shm_ || !condition_) {
        return;
    }
    if (!registered_) {
        SharedRingBufferBase::close();
        return;
    }

    lock_buffer();
    decrement_active_producers();
    registered_ = false;
    notify_all();
    unlock_buffer();

    SharedRingBufferBase::close();
}

void SharedRingBufferProducer::unlink() {
    // Producers don't own SHM cleanup; log a warning and no-op
    IPC_LOG_WARNING("Producer.unlink() called but producers do not own cleanup; "
                   "last consumer will clean up instead");
}

size_t SharedRingBufferProducer::available_space() const {
    return SharedRingBufferBase::available_space(get_write_pos(), get_read_pos());
}

} // namespace ipc0cp

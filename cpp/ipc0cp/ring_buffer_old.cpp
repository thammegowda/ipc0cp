#include "ring_buffer.hpp"
#include "logger.hpp"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <thread>
#include <stdexcept>
#include <iostream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace ipc0cp {

namespace {

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

// Accept either an absolute position (>= HEADER_SIZE) or a data-relative position
// (< HEADER_SIZE) and return an absolute position in the data region.
inline uint64_t to_abs_pos(uint64_t pos_or_rel, size_t total_data_bytes) {
    uint64_t abs_pos = (pos_or_rel < HEADER_SIZE) ? (data_begin_abs() + pos_or_rel) : pos_or_rel;
    return normalize_abs(abs_pos, total_data_bytes);
}

} // anonymous namespace

// SharedRingBufferBase implementation

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

uint64_t SharedRingBufferBase::get_write_pos() const {
    if (!shm_ptr_) return 0;
    return read_le64(shm_ptr_);
}

uint64_t SharedRingBufferBase::get_read_pos() const {
    if (!shm_ptr_) return 0;
    return read_le64(static_cast<const uint8_t*>(shm_ptr_) + 8);
}

size_t SharedRingBufferBase::available_space(uint64_t write_pos, uint64_t read_pos) const {
    if (write_pos >= read_pos) {
        // Case 1: write is ahead of read
        size_t space_to_end = (HEADER_SIZE + total_data_bytes_) - write_pos;
        size_t space_from_start = read_pos - HEADER_SIZE;
        return space_to_end + space_from_start;
    } else {
        // Case 2: write has wrapped around
        return read_pos - write_pos;
    }
}

uint64_t SharedRingBufferBase::normalize_pos(uint64_t pos) const {
    if (pos >= HEADER_SIZE + total_data_bytes_) {
        return HEADER_SIZE + (pos - HEADER_SIZE) % total_data_bytes_;
    }
    return pos;
}

void SharedRingBufferBase::close() {
    if (shm_ptr_ != nullptr) {
        munmap(shm_ptr_, shm_size_);
        shm_ptr_ = nullptr;
    }
    if (shm_fd_ >= 0) {
        ::close(shm_fd_);
        shm_fd_ = -1;
    }
}

void SharedRingBufferBase::unlink() {
    if (!shm_name_.empty()) {
        shm_unlink(shm_name_.c_str());
        IPC_LOG_INFO("Unlinked shared memory '" << shm_name_ << "'");
    }
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
        .is_empty = (write_pos == read_pos)
    };
}

bool SharedRingBufferBase::is_empty() const {
    return get_write_pos() == get_read_pos();
}

// SharedRingBufferConsumer implementation

SharedRingBufferConsumer::SharedRingBufferConsumer(
    std::string shm_name,
    size_t total_data_bytes,
    bool blocking,
    bool auto_attach,
    bool auto_unlink
)
    : SharedRingBufferBase(std::move(shm_name), total_data_bytes, blocking),
      auto_unlink_(auto_unlink)
{
    if (auto_attach) {
        if (!attach()) {
            last_error_ = RingBufferError::ShmNotFound;
            throw std::runtime_error("Failed to attach to shared memory: " + shm_name_);
        }
    }
}

bool SharedRingBufferConsumer::attach() {
    // Open existing shared memory
    shm_fd_ = shm_open(shm_name_.c_str(), O_RDWR, 0666);
    if (shm_fd_ < 0) {
        last_error_ = RingBufferError::ShmNotFound;
        return false;
    }
    
    // Get size and verify
    struct stat stat_buf;
    if (fstat(shm_fd_, &stat_buf) < 0) {
        ::close(shm_fd_);
        shm_fd_ = -1;
        last_error_ = RingBufferError::ShmNotFound;
        return false;
    }
    
    if (static_cast<size_t>(stat_buf.st_size) != shm_size_) {
        ::close(shm_fd_);
        shm_fd_ = -1;
        last_error_ = RingBufferError::SizeMismatch;
        return false;
    }
    
    // Map shared memory
    shm_ptr_ = mmap(nullptr, shm_size_, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
    if (shm_ptr_ == MAP_FAILED) {
        ::close(shm_fd_);
        shm_fd_ = -1;
        shm_ptr_ = nullptr;
        last_error_ = RingBufferError::ShmNotFound;
        return false;
    }
    
    // Read and verify total_data_bytes from header
    uint64_t stored_total = read_le64(static_cast<const uint8_t*>(shm_ptr_) + 16);
    if (stored_total != total_data_bytes_) {
        IPC_LOG_WARNING("total_data_bytes mismatch: using stored value " << stored_total);
        total_data_bytes_ = stored_total;
        shm_size_ = HEADER_SIZE + stored_total;
    }
    
    IPC_LOG_INFO("Attached to shared memory '" << shm_name_ << "'");
    return true;
}

void SharedRingBufferConsumer::set_read_pos(uint64_t pos) {
    if (shm_ptr_) {
        write_le64(static_cast<uint8_t*>(shm_ptr_) + 8, pos);
    }
}

uint64_t SharedRingBufferConsumer::read_uint64(uint64_t pos) {
    auto bytes = read_bytes(pos, 8);
    return read_le64(bytes.data());
}

uint32_t SharedRingBufferConsumer::read_uint32(uint64_t pos) {
    auto bytes = read_bytes(pos, 4);
    return read_le32(bytes.data());
}

std::vector<uint8_t> SharedRingBufferConsumer::read_bytes(uint64_t pos, size_t length) {
    std::vector<uint8_t> result(length);

    // Stored values are absolute positions in the mapped region.
    uint64_t abs_pos = to_abs_pos(pos, total_data_bytes_);
    const uint64_t end_of_region = data_end_abs(total_data_bytes_);
    
    if (abs_pos + length <= end_of_region) {
        // No wraparound
        std::memcpy(result.data(), static_cast<const uint8_t*>(shm_ptr_) + abs_pos, length);
    } else {
        // Wraparound needed
        size_t first_part_len = end_of_region - abs_pos;
        std::memcpy(result.data(), static_cast<const uint8_t*>(shm_ptr_) + abs_pos, first_part_len);
        
        size_t second_part_len = length - first_part_len;
        std::memcpy(result.data() + first_part_len,
                   static_cast<const uint8_t*>(shm_ptr_) + data_begin_abs(),
                   second_part_len);
    }
    
    return result;
}

uint64_t SharedRingBufferConsumer::advance_pos(uint64_t pos, size_t delta) {
    return normalize_pos(pos + delta);
}

std::optional<std::map<std::string, std::string>> SharedRingBufferConsumer::parse_metadata(
    const std::string& metadata_json
) {
    try {
        auto j = json::parse(metadata_json);
        
        std::map<std::string, std::string> metadata_map;
        
        // Convert all JSON values to strings for the metadata map
        for (auto& [key, value] : j.items()) {
            if (value.is_string()) {
                metadata_map[key] = value.get<std::string>();
            } else if (value.is_number()) {
                metadata_map[key] = value.dump();
            } else if (value.is_array() || value.is_object()) {
                metadata_map[key] = value.dump();
            } else if (value.is_boolean()) {
                metadata_map[key] = value.get<bool>() ? "true" : "false";
            } else {
                metadata_map[key] = value.dump();
            }
        }
        
        return metadata_map;
    } catch (const json::exception& e) {
        IPC_LOG_ERROR("JSON parse error: " << e.what());
        last_error_ = RingBufferError::InvalidMetadata;
        return std::nullopt;
    } catch (...) {
        last_error_ = RingBufferError::InvalidMetadata;
        return std::nullopt;
    }
}

std::optional<std::pair<std::string, std::vector<uint8_t>>> SharedRingBufferConsumer::pop_raw(
    int timeout_ms) {
    std::optional<std::chrono::milliseconds> timeout;
    if (timeout_ms >= 0) {
        timeout = std::chrono::milliseconds(timeout_ms);
    }

    if (!shm_ptr_) {
        throw IPCException(IPCError::NotInitialized);
    }

    auto start_time = std::chrono::steady_clock::now();
    while (true) {
        uint64_t write_pos = get_write_pos();
        uint64_t read_pos = get_read_pos();
        
        if (write_pos != read_pos) {
            break;
        }
        
        if (!blocking_) {
            throw IPCException(IPCError::BufferEmpty);
        }
        
        if (timeout.has_value()) {
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            if (elapsed >= *timeout) {
                throw IPCException(IPCError::Timeout);
            }
        }
        
        std::this_thread::sleep_for(std::chrono::microseconds(500));
    }
    
    uint64_t read_pos = get_read_pos();
    uint64_t current_pos = read_pos;
    
    uint64_t next_pos = read_uint64(current_pos);
    current_pos = advance_pos(current_pos, 8);

    uint32_t metadata_size = read_uint32(current_pos);
    current_pos = advance_pos(current_pos, 4);
    
    if (metadata_size > MAX_METADATA_SIZE) {
        throw IPCException(IPCError::InvalidMetadata, 
            "Invalid metadata_size: " + std::to_string(metadata_size));
    }
    
    uint64_t payload_size = read_uint64(current_pos);
    current_pos = advance_pos(current_pos, 8);
    
    if (metadata_size == 0 && payload_size == 0) {
        eos_received_ = true;
        set_read_pos(next_pos);
        
        if (auto_unlink_) {
            close();
            unlink();
        }
        
        return std::nullopt;
    }
    
    auto metadata_bytes = read_bytes(current_pos, metadata_size);
    current_pos = advance_pos(current_pos, metadata_size);
    
    std::string metadata_json(metadata_bytes.begin(), metadata_bytes.end());
    
    auto start_sentinel = read_bytes(current_pos, 1);
    if (start_sentinel.empty() || start_sentinel[0] != SENTINEL_BYTE) {
        last_error_ = IPCError::CorruptPayload;
        throw IPCException(IPCError::CorruptPayload, "Invalid start sentinel");
    }
    current_pos = advance_pos(current_pos, 1);
    
    auto payload = read_bytes(current_pos, payload_size);
    current_pos = advance_pos(current_pos, payload_size);
    
    auto end_sentinel = read_bytes(current_pos, 1);
    if (end_sentinel.empty() || end_sentinel[0] != SENTINEL_BYTE) {
        last_error_ = IPCError::CorruptPayload;
        throw IPCException(IPCError::CorruptPayload, "Invalid end sentinel");
    }
    
    set_read_pos(next_pos);
    
    return std::make_pair(std::move(metadata_json), std::move(payload));
}

std::unique_ptr<IPCObject> SharedRingBufferConsumer::pop(
    int timeout_ms) {
    auto raw_pair = pop_raw(timeout_ms);
    if (!raw_pair) {
        return nullptr;
    }

    auto metadata_json = std::move(raw_pair->first);
    auto payload = std::move(raw_pair->second);

    auto deserialized = deserialize(metadata_json, payload);
    if (!deserialized) {
        throw IPCException(IPCError::DeserializationFailed);
    }

    auto metadata_result = parse_metadata(metadata_json);
    if (!metadata_result) {
        throw IPCException(IPCError::InvalidMetadata, "Failed to parse metadata JSON");
    }

    auto ipc_obj = std::make_unique<IPCObject>(std::move(deserialized));
    ipc_obj->raw_metadata = std::move(*metadata_result);
    return ipc_obj;
}

// SharedRingBufferProducer implementation

SharedRingBufferProducer::SharedRingBufferProducer(
    const std::string& shm_name,
    size_t total_data_bytes,
    bool create_new
)
    : shm_name_(shm_name)
    , total_data_bytes_(total_data_bytes)
    , shm_size_(HEADER_SIZE + total_data_bytes)
    , shm_fd_(-1)
    , shm_ptr_(nullptr)
{
    if (!init_shm(create_new)) {
        throw std::runtime_error("Failed to initialize shared memory: " + shm_name_);
    }
}

SharedRingBufferProducer::~SharedRingBufferProducer() {
    if (shm_ptr_) {
        munmap(shm_ptr_, shm_size_);
        shm_ptr_ = nullptr;
    }
    if (shm_fd_ >= 0) {
        ::close(shm_fd_);
        shm_fd_ = -1;
    }
}

SharedRingBufferProducer::SharedRingBufferProducer(SharedRingBufferProducer&& other) noexcept
    : shm_name_(std::move(other.shm_name_))
    , total_data_bytes_(other.total_data_bytes_)
    , shm_size_(other.shm_size_)
    , shm_fd_(other.shm_fd_)
    , shm_ptr_(other.shm_ptr_)
{
    other.shm_fd_ = -1;
    other.shm_ptr_ = nullptr;
}

SharedRingBufferProducer& SharedRingBufferProducer::operator=(SharedRingBufferProducer&& other) noexcept {
    if (this != &other) {
        if (shm_ptr_) munmap(shm_ptr_, shm_size_);
        if (shm_fd_ >= 0) ::close(shm_fd_);
        
        shm_name_ = std::move(other.shm_name_);
        total_data_bytes_ = other.total_data_bytes_;
        shm_size_ = other.shm_size_;
        shm_fd_ = other.shm_fd_;
        shm_ptr_ = other.shm_ptr_;
        
        other.shm_fd_ = -1;
        other.shm_ptr_ = nullptr;
    }
    return *this;
}

bool SharedRingBufferProducer::init_shm(bool create_new) {
    int flags = O_RDWR;
    if (create_new) {
        flags |= O_CREAT | O_EXCL;
    }
    
    // Open/create shared memory
    shm_fd_ = shm_open(shm_name_.c_str(), flags, 0666);
    if (shm_fd_ < 0) {
        IPC_LOG_ERROR("Failed to open shared memory '" << shm_name_ << "': " 
                  << strerror(errno));
        return false;
    }
    
    if (create_new) {
        // Set size for new shared memory
        if (ftruncate(shm_fd_, shm_size_) < 0) {
            IPC_LOG_ERROR("Failed to set shared memory size: " << strerror(errno));
            ::close(shm_fd_);
            shm_fd_ = -1;
            shm_unlink(shm_name_.c_str());
            return false;
        }
    } else {
        // Verify size for existing shared memory
        struct stat stat_buf;
        if (fstat(shm_fd_, &stat_buf) < 0) {
            IPC_LOG_ERROR("Failed to stat shared memory: " << strerror(errno));
            ::close(shm_fd_);
            shm_fd_ = -1;
            return false;
        }
        
        if (static_cast<size_t>(stat_buf.st_size) != shm_size_) {
            IPC_LOG_ERROR("Shared memory size mismatch");
            ::close(shm_fd_);
            shm_fd_ = -1;
            return false;
        }
    }
    
    // Map shared memory
    shm_ptr_ = mmap(nullptr, shm_size_, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
    if (shm_ptr_ == MAP_FAILED) {
        IPC_LOG_ERROR("Failed to map shared memory: " << strerror(errno));
        ::close(shm_fd_);
        shm_fd_ = -1;
        shm_ptr_ = nullptr;
        if (create_new) {
            shm_unlink(shm_name_.c_str());
        }
        return false;
    }
    
    if (create_new) {
        // Initialize header
        auto* header = static_cast<uint8_t*>(shm_ptr_);
        // Positions are absolute within the mapped shared memory region.
        // Match Python: initial positions start at the beginning of the data region.
        write_le64(header + 0, data_begin_abs());  // write_pos
        write_le64(header + 8, data_begin_abs());  // read_pos
        write_le64(header + 16, total_data_bytes_);  // total_data_bytes
        
        IPC_LOG_INFO("Created shared memory '" << shm_name_ << "' (" 
                  << shm_size_ << " bytes)");
    } else {
        IPC_LOG_INFO("Attached to shared memory '" << shm_name_ << "'");
    }
    
    return true;
}

uint64_t SharedRingBufferProducer::get_write_pos() const {
    if (!shm_ptr_) return 0;
    return read_le64(static_cast<const uint8_t*>(shm_ptr_));
}

uint64_t SharedRingBufferProducer::get_read_pos() const {
    if (!shm_ptr_) return 0;
    return read_le64(static_cast<const uint8_t*>(shm_ptr_) + 8);
}

void SharedRingBufferProducer::set_write_pos(uint64_t pos) {
    if (shm_ptr_) {
        write_le64(static_cast<uint8_t*>(shm_ptr_), pos);
    }
}

size_t SharedRingBufferProducer::available_space(uint64_t write_pos, uint64_t read_pos) const {
    // Positions are absolute in [HEADER_SIZE, HEADER_SIZE + total_data_bytes_)
    // Compute free space exactly like the Python implementation.
    if (write_pos >= read_pos) {
        size_t space_to_end = data_end_abs(total_data_bytes_) - write_pos;
        size_t space_from_start = read_pos - data_begin_abs();
        return space_to_end + space_from_start;
    }
    return read_pos - write_pos;
}

size_t SharedRingBufferProducer::available_space() const {
    return available_space(get_write_pos(), get_read_pos());
}

void SharedRingBufferProducer::write_bytes(size_t pos, const void* data, size_t size) {
    // Write at an absolute position within the mapped region, handling wraparound.
    const uint64_t end_of_region = data_end_abs(total_data_bytes_);
    uint64_t abs_pos = to_abs_pos(static_cast<uint64_t>(pos), total_data_bytes_);

    if (abs_pos + size <= end_of_region) {
        std::memcpy(static_cast<uint8_t*>(shm_ptr_) + abs_pos, data, size);
        return;
    }

    // Wraparound needed
    size_t first_part = end_of_region - abs_pos;
    std::memcpy(static_cast<uint8_t*>(shm_ptr_) + abs_pos, data, first_part);
    size_t second_part = size - first_part;
    std::memcpy(static_cast<uint8_t*>(shm_ptr_) + data_begin_abs(),
                static_cast<const uint8_t*>(data) + first_part,
                second_part);
}

bool SharedRingBufferProducer::write_slot(
    const std::string& metadata_json,
    const std::vector<uint8_t>& payload,
    uint64_t write_pos
) {
    uint32_t metadata_size = metadata_json.size();
    uint64_t payload_size = payload.size();
    // Include sentinels in slot size calculation
    uint64_t slot_size = SLOT_HEADER_SIZE + metadata_size + 1 + payload_size + 1;
    uint64_t next_pos = write_pos + slot_size;
    next_pos = normalize_abs(next_pos, total_data_bytes_);
    
    // Write slot header
    uint8_t slot_header[SLOT_HEADER_SIZE];
    write_le64(slot_header, next_pos);
    write_le32(slot_header + 8, metadata_size);
    write_le64(slot_header + 12, payload_size);
    
    auto advance_abs = [&](uint64_t pos, uint64_t delta) -> uint64_t {
        return normalize_abs(pos + delta, total_data_bytes_);
    };

    uint64_t header_pos = write_pos;
    uint64_t metadata_pos = advance_abs(header_pos, SLOT_HEADER_SIZE);
    uint64_t start_sentinel_pos = advance_abs(metadata_pos, metadata_size);
    uint64_t payload_pos = advance_abs(start_sentinel_pos, 1);
    uint64_t end_sentinel_pos = advance_abs(payload_pos, payload_size);

    // Write slot header
    write_bytes(header_pos, slot_header, SLOT_HEADER_SIZE);

    // Write metadata
    if (metadata_size > 0) {
        write_bytes(metadata_pos, metadata_json.data(), metadata_size);
    }

    // Write start sentinel
    uint8_t sentinel = SENTINEL_BYTE;
    write_bytes(start_sentinel_pos, &sentinel, 1);

    // Write payload
    if (payload_size > 0) {
        write_bytes(payload_pos, payload.data(), payload_size);
    }

    // Write end sentinel
    write_bytes(end_sentinel_pos, &sentinel, 1);
    
    return true;
}
void SharedRingBufferProducer::close() {
    if (shm_ptr_ != nullptr) {
        // Push end-of-stream marker (empty metadata + empty payload)
        try {
            std::vector<uint8_t> empty_payload;
            push_raw("", empty_payload, 5000);  // 5 second timeout
        } catch (...) {
            // Ignore errors when sending EOS marker
        }
    }
}
bool SharedRingBufferProducer::push_raw(
    const std::string& metadata_json,
    const std::vector<uint8_t>& payload,
    int timeout_ms
) {
    if (!shm_ptr_) {
        IPC_LOG_ERROR("Shared memory not initialized");
        return false;
    }
    
    if (metadata_json.size() > MAX_METADATA_SIZE) {
        IPC_LOG_ERROR("Metadata too large: " << metadata_json.size() << " > " << MAX_METADATA_SIZE);
        return false;
    }
    
    size_t slot_size = SLOT_HEADER_SIZE + metadata_json.size() + 2 + payload.size();
    if (slot_size > MAX_SLOT_SIZE) {
        IPC_LOG_ERROR("Slot too large: " << slot_size << " > " << MAX_SLOT_SIZE);
        return false;
    }
    
    // Wait for space
    auto start_time = std::chrono::steady_clock::now();
    while (true) {
        uint64_t write_pos = get_write_pos();
        uint64_t read_pos = get_read_pos();
        size_t available = available_space(write_pos, read_pos);
        
        if (available >= slot_size) {
            // Write slot
            if (!write_slot(metadata_json, payload, write_pos)) {
                return false;
            }
            
            // Update write position
            uint64_t next_pos = normalize_abs(write_pos + slot_size, total_data_bytes_);
            set_write_pos(next_pos);
            return true;
        }
        
        // Check timeout
        if (timeout_ms == 0) {
            IPC_LOG_ERROR("Buffer full (no wait)");
            return false;
        }
        
        if (timeout_ms > 0) {
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
            if (elapsed_ms >= timeout_ms) {
                IPC_LOG_ERROR("Timeout waiting for space");
                return false;
            }
        }
        
        // Sleep briefly before retry
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void SharedRingBufferProducer::push(const SerializableObject& obj, 
                                  int timeout_ms) {
    auto serialized = obj.serialize();
    if (!push_raw(serialized.metadata_json, serialized.payload, timeout_ms)) {
        throw IPCException(
            IPCError::DeserializationFailed,
            "Failed to push object to ring buffer"
        );
    }
}

} // namespace ipc0cp

#include "ring_buffer.hpp"
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

// Read little-endian uint64 from memory
uint64_t readUint64LE(const void* ptr) {
    const uint8_t* bytes = static_cast<const uint8_t*>(ptr);
    return static_cast<uint64_t>(bytes[0]) |
           (static_cast<uint64_t>(bytes[1]) << 8) |
           (static_cast<uint64_t>(bytes[2]) << 16) |
           (static_cast<uint64_t>(bytes[3]) << 24) |
           (static_cast<uint64_t>(bytes[4]) << 32) |
           (static_cast<uint64_t>(bytes[5]) << 40) |
           (static_cast<uint64_t>(bytes[6]) << 48) |
           (static_cast<uint64_t>(bytes[7]) << 56);
}

// Read little-endian uint32 from memory
uint32_t readUint32LE(const void* ptr) {
    const uint8_t* bytes = static_cast<const uint8_t*>(ptr);
    return static_cast<uint32_t>(bytes[0]) |
           (static_cast<uint32_t>(bytes[1]) << 8) |
           (static_cast<uint32_t>(bytes[2]) << 16) |
           (static_cast<uint32_t>(bytes[3]) << 24);
}

// Write little-endian uint64 to memory
void writeUint64LE(void* ptr, uint64_t value) {
    uint8_t* bytes = static_cast<uint8_t*>(ptr);
    bytes[0] = value & 0xFF;
    bytes[1] = (value >> 8) & 0xFF;
    bytes[2] = (value >> 16) & 0xFF;
    bytes[3] = (value >> 24) & 0xFF;
    bytes[4] = (value >> 32) & 0xFF;
    bytes[5] = (value >> 40) & 0xFF;
    bytes[6] = (value >> 48) & 0xFF;
    bytes[7] = (value >> 56) & 0xFF;
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

uint64_t SharedRingBufferBase::get_write_offset() const {
    if (!shm_ptr_) return 0;
    return readUint64LE(shm_ptr_);
}

uint64_t SharedRingBufferBase::get_read_offset() const {
    if (!shm_ptr_) return 0;
    return readUint64LE(static_cast<const uint8_t*>(shm_ptr_) + 8);
}

size_t SharedRingBufferBase::available_space(uint64_t write_offset, uint64_t read_offset) const {
    if (write_offset >= read_offset) {
        // Case 1: write is ahead of read
        size_t space_to_end = (HEADER_SIZE + total_data_bytes_) - write_offset;
        size_t space_from_start = read_offset - HEADER_SIZE;
        return space_to_end + space_from_start;
    } else {
        // Case 2: write has wrapped around
        return read_offset - write_offset;
    }
}

uint64_t SharedRingBufferBase::normalize_offset(uint64_t offset) const {
    if (offset >= HEADER_SIZE + total_data_bytes_) {
        return HEADER_SIZE + (offset - HEADER_SIZE) % total_data_bytes_;
    }
    return offset;
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
        std::cout << "Unlinked shared memory '" << shm_name_ << "'" << std::endl;
    }
}

SharedRingBufferBase::Stats SharedRingBufferBase::get_stats() const {
    uint64_t write_offset = get_write_offset();
    uint64_t read_offset = get_read_offset();
    size_t available = available_space(write_offset, read_offset);
    
    return Stats{
        .write_offset = write_offset,
        .read_offset = read_offset,
        .available_bytes = available,
        .used_bytes = total_data_bytes_ - available,
        .total_data_bytes = total_data_bytes_,
        .is_empty = (write_offset == read_offset)
    };
}

bool SharedRingBufferBase::is_empty() const {
    return get_write_offset() == get_read_offset();
}

// SharedRingBufferConsumer implementation

SharedRingBufferConsumer::SharedRingBufferConsumer(
    std::string shm_name,
    size_t total_data_bytes,
    bool blocking,
    bool auto_attach
)
    : SharedRingBufferBase(std::move(shm_name), total_data_bytes, blocking)
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
    uint64_t stored_total = readUint64LE(static_cast<const uint8_t*>(shm_ptr_) + 16);
    if (stored_total != total_data_bytes_) {
        std::cerr << "Warning: total_data_bytes mismatch: using stored value " << stored_total << std::endl;
        total_data_bytes_ = stored_total;
        shm_size_ = HEADER_SIZE + stored_total;
    }
    
    std::cout << "Attached to shared memory '" << shm_name_ << "'" << std::endl;
    return true;
}

void SharedRingBufferConsumer::set_read_offset(uint64_t offset) {
    if (shm_ptr_) {
        writeUint64LE(static_cast<uint8_t*>(shm_ptr_) + 8, offset);
    }
}

uint64_t SharedRingBufferConsumer::read_uint64(uint64_t pos) {
    auto bytes = read_bytes(pos, 8);
    return readUint64LE(bytes.data());
}

uint32_t SharedRingBufferConsumer::read_uint32(uint64_t pos) {
    auto bytes = read_bytes(pos, 4);
    return readUint32LE(bytes.data());
}

std::vector<uint8_t> SharedRingBufferConsumer::read_bytes(uint64_t pos, size_t length) {
    std::vector<uint8_t> result(length);
    
    // Convert ring buffer position to absolute shared memory position
    uint64_t abs_pos = HEADER_SIZE + (pos % total_data_bytes_);
    const uint64_t end_of_region = HEADER_SIZE + total_data_bytes_;
    
    if (abs_pos + length <= end_of_region) {
        // No wraparound
        std::memcpy(result.data(), static_cast<const uint8_t*>(shm_ptr_) + abs_pos, length);
    } else {
        // Wraparound needed
        size_t first_part_len = end_of_region - abs_pos;
        std::memcpy(result.data(), static_cast<const uint8_t*>(shm_ptr_) + abs_pos, first_part_len);
        
        size_t second_part_len = length - first_part_len;
        std::memcpy(result.data() + first_part_len,
                   static_cast<const uint8_t*>(shm_ptr_) + HEADER_SIZE,
                   second_part_len);
    }
    
    return result;
}

uint64_t SharedRingBufferConsumer::advance_pos(uint64_t pos, size_t offset) {
    return normalize_offset(pos + offset);
}

std::optional<std::map<std::string, std::string>> SharedRingBufferConsumer::parse_metadata(
    const std::vector<uint8_t>& metadata_bytes
) {
    try {
        std::string json_str(metadata_bytes.begin(), metadata_bytes.end());
        auto j = json::parse(json_str);
        
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
        std::cerr << "JSON parse error: " << e.what() << std::endl;
        last_error_ = RingBufferError::InvalidMetadata;
        return std::nullopt;
    } catch (...) {
        last_error_ = RingBufferError::InvalidMetadata;
        return std::nullopt;
    }
}

std::optional<RingBufferObject> SharedRingBufferConsumer::pop(
    std::optional<std::chrono::milliseconds> timeout
) {
    if (!shm_ptr_) {
        throw RingBufferException(RingBufferError::NotInitialized);
    }
    
    // Wait for data if blocking
    auto start_time = std::chrono::steady_clock::now();
    
    while (true) {
        uint64_t write_offset = get_write_offset();
        uint64_t read_offset = get_read_offset();
        
        if (write_offset != read_offset) {
            break;  // Data available
        }
        
        if (!blocking_) {
            throw RingBufferException(RingBufferError::BufferEmpty);
        }
        
        if (timeout.has_value()) {
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            if (elapsed >= *timeout) {
                throw RingBufferException(RingBufferError::Timeout);
            }
        }
        
        std::this_thread::sleep_for(std::chrono::microseconds(500));
    }
    
    // Read slot at read_offset
    uint64_t read_offset = get_read_offset();
    uint64_t current_pos = read_offset;
    
    // Read next_offset (8 bytes)
    uint64_t next_offset = read_uint64(current_pos);
    current_pos = advance_pos(current_pos, 8);
    
    // Read metadata_size (4 bytes)
    uint32_t metadata_size = read_uint32(current_pos);
    current_pos = advance_pos(current_pos, 4);
    
    // Validate metadata size
    if (metadata_size > MAX_METADATA_SIZE) {
        throw RingBufferException(RingBufferError::InvalidMetadata, 
            "Invalid metadata_size: " + std::to_string(metadata_size) + " > " + std::to_string(MAX_METADATA_SIZE));
    }
    
    // Read payload_size (8 bytes)
    uint64_t payload_size = read_uint64(current_pos);
    current_pos = advance_pos(current_pos, 8);
    
    // Check for end-of-stream marker (payload_size == 0)
    if (payload_size == 0) {
        eos_received_ = true;
        std::cout << "Received end-of-stream marker" << std::endl;
        // Update read_offset to consume the EOS slot
        set_read_offset(next_offset);
        return std::nullopt;
    }
    
    // Read metadata JSON
    auto metadata_bytes = read_bytes(current_pos, metadata_size);
    current_pos = advance_pos(current_pos, metadata_size);
    
    // Parse metadata
    auto metadata_result = parse_metadata(metadata_bytes);
    if (!metadata_result) {
        throw RingBufferException(RingBufferError::InvalidMetadata, "Failed to parse metadata JSON");
    }
    
    // Read and verify start sentinel
    auto start_sentinel = read_bytes(current_pos, 1);
    if (start_sentinel.empty() || start_sentinel[0] != SENTINEL_BYTE) {
        last_error_ = RingBufferError::CorruptPayload;
        throw RingBufferException(RingBufferError::CorruptPayload, 
            "Invalid start sentinel (expected " + std::to_string(static_cast<int>(SENTINEL_BYTE)) + 
            ", got " + (start_sentinel.empty() ? "empty" : std::to_string(start_sentinel[0])) + ")");
    }
    current_pos = advance_pos(current_pos, 1);
    
    // Read payload
    auto payload = read_bytes(current_pos, payload_size);
    current_pos = advance_pos(current_pos, payload_size);
    
    // Read and verify end sentinel
    auto end_sentinel = read_bytes(current_pos, 1);
    if (end_sentinel.empty() || end_sentinel[0] != SENTINEL_BYTE) {
        last_error_ = RingBufferError::CorruptPayload;
        throw RingBufferException(RingBufferError::CorruptPayload,
            "Invalid end sentinel (expected " + std::to_string(static_cast<int>(SENTINEL_BYTE)) + 
            ", got " + (end_sentinel.empty() ? "empty" : std::to_string(end_sentinel[0])) + ")");
    }
    
    // Deserialize using the SerializableObject factory
    auto deserialized = SerializableObject::deserialize(*metadata_result, payload);
    
    // Check if deserialization succeeded
    if (!deserialized) {
        throw RingBufferException(RingBufferError::DeserializationFailed);
    }
    
    // Update read_offset
    set_read_offset(next_offset);
    
    // Create and return RingBufferObject
    RingBufferObject obj(std::move(deserialized));
    obj.raw_metadata = std::move(*metadata_result);
    
    return obj;
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

void SharedRingBufferProducer::write_uint64_le(uint8_t* ptr, uint64_t value) {
    ptr[0] = value & 0xFF;
    ptr[1] = (value >> 8) & 0xFF;
    ptr[2] = (value >> 16) & 0xFF;
    ptr[3] = (value >> 24) & 0xFF;
    ptr[4] = (value >> 32) & 0xFF;
    ptr[5] = (value >> 40) & 0xFF;
    ptr[6] = (value >> 48) & 0xFF;
    ptr[7] = (value >> 56) & 0xFF;
}

void SharedRingBufferProducer::write_uint32_le(uint8_t* ptr, uint32_t value) {
    ptr[0] = value & 0xFF;
    ptr[1] = (value >> 8) & 0xFF;
    ptr[2] = (value >> 16) & 0xFF;
    ptr[3] = (value >> 24) & 0xFF;
}

bool SharedRingBufferProducer::init_shm(bool create_new) {
    int flags = O_RDWR;
    if (create_new) {
        flags |= O_CREAT | O_EXCL;
    }
    
    // Open/create shared memory
    shm_fd_ = shm_open(shm_name_.c_str(), flags, 0666);
    if (shm_fd_ < 0) {
        std::cerr << "Failed to open shared memory '" << shm_name_ << "': " 
                  << strerror(errno) << std::endl;
        return false;
    }
    
    if (create_new) {
        // Set size for new shared memory
        if (ftruncate(shm_fd_, shm_size_) < 0) {
            std::cerr << "Failed to set shared memory size: " << strerror(errno) << std::endl;
            ::close(shm_fd_);
            shm_fd_ = -1;
            shm_unlink(shm_name_.c_str());
            return false;
        }
    } else {
        // Verify size for existing shared memory
        struct stat stat_buf;
        if (fstat(shm_fd_, &stat_buf) < 0) {
            std::cerr << "Failed to stat shared memory: " << strerror(errno) << std::endl;
            ::close(shm_fd_);
            shm_fd_ = -1;
            return false;
        }
        
        if (static_cast<size_t>(stat_buf.st_size) != shm_size_) {
            std::cerr << "Shared memory size mismatch" << std::endl;
            ::close(shm_fd_);
            shm_fd_ = -1;
            return false;
        }
    }
    
    // Map shared memory
    shm_ptr_ = mmap(nullptr, shm_size_, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
    if (shm_ptr_ == MAP_FAILED) {
        std::cerr << "Failed to map shared memory: " << strerror(errno) << std::endl;
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
        write_uint64_le(header + 0, 0);  // write_offset
        write_uint64_le(header + 8, 0);  // read_offset
        write_uint64_le(header + 16, total_data_bytes_);  // total_data_bytes
        
        std::cout << "Created shared memory '" << shm_name_ << "' (" 
                  << shm_size_ << " bytes)" << std::endl;
    } else {
        std::cout << "Attached to shared memory '" << shm_name_ << "'" << std::endl;
    }
    
    return true;
}

uint64_t SharedRingBufferProducer::get_write_offset() const {
    if (!shm_ptr_) return 0;
    auto* ptr = static_cast<const uint8_t*>(shm_ptr_);
    uint64_t value = 0;
    for (int i = 7; i >= 0; --i) {
        value = (value << 8) | ptr[i];
    }
    return value;
}

uint64_t SharedRingBufferProducer::get_read_offset() const {
    if (!shm_ptr_) return 0;
    auto* ptr = static_cast<const uint8_t*>(shm_ptr_) + 8;
    uint64_t value = 0;
    for (int i = 7; i >= 0; --i) {
        value = (value << 8) | ptr[i];
    }
    return value;
}

void SharedRingBufferProducer::set_write_offset(uint64_t offset) {
    if (shm_ptr_) {
        write_uint64_le(static_cast<uint8_t*>(shm_ptr_), offset);
    }
}

size_t SharedRingBufferProducer::available_space(uint64_t write_offset, uint64_t read_offset) const {
    if (write_offset >= read_offset) {
        return total_data_bytes_ - (write_offset - read_offset);
    } else {
        return read_offset - write_offset;
    }
}

size_t SharedRingBufferProducer::available_space() const {
    return available_space(get_write_offset(), get_read_offset());
}

void SharedRingBufferProducer::write_bytes(size_t pos, const void* data, size_t size) {
    auto* dest = static_cast<uint8_t*>(shm_ptr_) + HEADER_SIZE + pos;
    std::memcpy(dest, data, size);
}

bool SharedRingBufferProducer::write_slot(
    const std::string& metadata_json,
    const std::vector<uint8_t>& payload,
    uint64_t write_offset
) {
    uint32_t metadata_size = metadata_json.size();
    uint64_t payload_size = payload.size();
    // Include sentinels in slot size calculation
    uint64_t slot_size = SLOT_HEADER_SIZE + metadata_size + 1 + payload_size + 1;
    uint64_t next_offset = (write_offset + slot_size) % total_data_bytes_;
    
    // Write slot header
    uint8_t slot_header[SLOT_HEADER_SIZE];
    write_uint64_le(slot_header, next_offset);
    write_uint32_le(slot_header + 8, metadata_size);
    write_uint64_le(slot_header + 12, payload_size);
    
    // Calculate positions
    size_t header_pos = write_offset % total_data_bytes_;
    size_t metadata_pos = (header_pos + SLOT_HEADER_SIZE) % total_data_bytes_;
    size_t start_sentinel_pos = (metadata_pos + metadata_size) % total_data_bytes_;
    size_t payload_pos = (start_sentinel_pos + 1) % total_data_bytes_;
    size_t end_sentinel_pos = (payload_pos + payload_size) % total_data_bytes_;
    
    // Write slot header
    if (header_pos + SLOT_HEADER_SIZE <= total_data_bytes_) {
        write_bytes(header_pos, slot_header, SLOT_HEADER_SIZE);
    } else {
        // Wrap around
        size_t first_part = total_data_bytes_ - header_pos;
        write_bytes(header_pos, slot_header, first_part);
        write_bytes(0, slot_header + first_part, SLOT_HEADER_SIZE - first_part);
    }
    
    // Write metadata
    if (metadata_pos + metadata_size <= total_data_bytes_) {
        write_bytes(metadata_pos, metadata_json.data(), metadata_size);
    } else {
        // Wrap around
        size_t first_part = total_data_bytes_ - metadata_pos;
        write_bytes(metadata_pos, metadata_json.data(), first_part);
        write_bytes(0, metadata_json.data() + first_part, metadata_size - first_part);
    }
    
    // Write start sentinel
    uint8_t sentinel = SENTINEL_BYTE;
    write_bytes(start_sentinel_pos, &sentinel, 1);
    
    // Write payload
    if (!payload.empty()) {
        if (payload_pos + payload_size <= total_data_bytes_) {
            write_bytes(payload_pos, payload.data(), payload_size);
        } else {
            // Wrap around
            size_t first_part = total_data_bytes_ - payload_pos;
            write_bytes(payload_pos, payload.data(), first_part);
            write_bytes(0, payload.data() + first_part, payload_size - first_part);
        }
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
            push_raw("{}", empty_payload, 5000);  // 5 second timeout
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
        std::cerr << "Shared memory not initialized" << std::endl;
        return false;
    }
    
    if (metadata_json.size() > MAX_METADATA_SIZE) {
        std::cerr << "Metadata too large: " << metadata_json.size() << " > " << MAX_METADATA_SIZE << std::endl;
        return false;
    }
    
    uint64_t slot_size = SLOT_HEADER_SIZE + metadata_json.size() + 1 + payload.size() + 1;
    if (slot_size > MAX_SLOT_SIZE) {
        std::cerr << "Slot too large: " << slot_size << " > " << MAX_SLOT_SIZE << std::endl;
        return false;
    }
    
    // Wait for space
    auto start_time = std::chrono::steady_clock::now();
    while (true) {
        uint64_t write_offset = get_write_offset();
        uint64_t read_offset = get_read_offset();
        size_t available = available_space(write_offset, read_offset);
        
        if (available >= slot_size) {
            // Write slot
            if (!write_slot(metadata_json, payload, write_offset)) {
                return false;
            }
            
            // Update write offset
            uint64_t next_offset = (write_offset + slot_size) % total_data_bytes_;
            set_write_offset(next_offset);
            return true;
        }
        
        // Check timeout
        if (timeout_ms == 0) {
            std::cerr << "Buffer full (no wait)" << std::endl;
            return false;
        }
        
        if (timeout_ms > 0) {
            auto elapsed = std::chrono::steady_clock::now() - start_time;
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
            if (elapsed_ms >= timeout_ms) {
                std::cerr << "Timeout waiting for space" << std::endl;
                return false;
            }
        }
        
        // Sleep briefly before retry
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

bool SharedRingBufferProducer::push(const SerializableObject& obj, int timeout_ms) {
    // Serialize object
    auto serialized = obj.serialize();
    return push_raw(serialized.metadata_json, serialized.payload, timeout_ms);
}

} // namespace ipc0cp

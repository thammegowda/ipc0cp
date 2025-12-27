/**
 * @file ipc.hpp
 * @brief Common types and utilities for IPC operations
 */

#pragma once

#include "serialize.hpp"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <endian.h>
#include <map>
#include <stdexcept>

namespace ipc0cp {

/**
 * @brief Error types for IPC operations
 */
enum class IPCError {
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
inline std::string errorToString(IPCError error) {
    switch (error) {
        case IPCError::None: return "No error";
        case IPCError::NotInitialized: return "IPC not initialized";
        case IPCError::ShmNotFound: return "Shared memory segment not found";
        case IPCError::SizeMismatch: return "Size mismatch";
        case IPCError::InvalidMetadata: return "Invalid metadata";
        case IPCError::InvalidSlot: return "Invalid slot data";
        case IPCError::Timeout: return "Operation timed out";
        case IPCError::BufferEmpty: return "Buffer is empty";
        case IPCError::DeserializationFailed: return "Deserialization failed";
        case IPCError::CorruptPayload: return "Corrupt payload";
        default: return "Unknown error";
    }
}

/**
 * @brief Exception class for IPC errors
 */
class IPCException : public std::runtime_error {
public:
    IPCError error_type;
    
    explicit IPCException(IPCError error) 
        : std::runtime_error(errorToString(error)), error_type(error) {}
    
    IPCException(IPCError error, const std::string& msg) 
        : std::runtime_error(msg), error_type(error) {}
};

/**
 * @brief A generic object from IPC with deserialized data
 */
struct IPCObject {
    std::unique_ptr<SerializableObject> data;
    std::map<std::string, std::string> raw_metadata;  // Original metadata
    
    IPCObject() = default;
    explicit IPCObject(std::unique_ptr<SerializableObject> d) 
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

// Shared constants used by multiple IPC transports
constexpr size_t HEADER_SIZE = 24;  // 3 * uint64: write_pos, read_pos, total_data_bytes
constexpr size_t SLOT_HEADER_SIZE = 20;  // next_pos(8) + metadata_size(4) + payload_size(8)
constexpr uint8_t SENTINEL_BYTE = 0x00;
constexpr size_t MAX_METADATA_SIZE = 1024;
constexpr size_t MAX_SLOT_SIZE = 10 * 1024 * 1024;  // 10 MB
constexpr size_t DEFAULT_TOTAL_DATA_BYTES = 1024ULL * 1024 * 1024;  // 1 GB

inline uint64_t read_le64(const void* ptr) {
    uint64_t value;
    std::memcpy(&value, ptr, sizeof(value));
    return static_cast<uint64_t>(le64toh(value));
}

inline uint32_t read_le32(const void* ptr) {
    uint32_t value;
    std::memcpy(&value, ptr, sizeof(value));
    return static_cast<uint32_t>(le32toh(value));
}

inline void write_le64(void* ptr, uint64_t value) {
    uint64_t converted = htole64(value);
    std::memcpy(ptr, &converted, sizeof(converted));
}

inline void write_le32(void* ptr, uint32_t value) {
    uint32_t converted = htole32(value);
    std::memcpy(ptr, &converted, sizeof(converted));
}

} // namespace ipc0cp

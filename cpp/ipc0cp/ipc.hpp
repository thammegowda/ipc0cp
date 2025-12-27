/**
 * @file ipc.hpp
 * @brief Common types and utilities for IPC operations
 */

#pragma once

#include "serialize.hpp"
#include <string>
#include <memory>
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

} // namespace ipc0cp

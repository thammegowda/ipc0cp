#pragma once

#include <string>
#include <functional>
#include <map>
#include <memory>
#include <vector>

namespace ipc0cp {

// Forward declaration
class SerializableObject;

/**
 * @brief Function signature for custom deserializers
 * 
 * Deserializer function that reconstructs an object from metadata and payload.
 * Should throw or return nullptr on failure.
 */
using CustomDeserializer = std::function<std::unique_ptr<SerializableObject>(
    const std::map<std::string, std::string>& metadata,
    const std::vector<uint8_t>& payload
)>;

/**
 * @brief Type registry for custom serializable types
 * 
 * Provides a singleton registry for registering and retrieving custom object types.
 * Enables extensibility beyond built-in types (bytes, text, json, ndarray, image).
 * 
 * Usage:
 * @code
 * // Register a custom type
 * auto deserializer = [](const auto& meta, const auto& payload) {
 *     return std::make_unique<MyCustomType>(meta, payload);
 * };
 * TypeRegistry::instance().register_type("MyType", deserializer, "1.0");
 * 
 * // Get deserializer for a type
 * auto deser = TypeRegistry::instance().get_deserializer("MyType", "1.0");
 * if (deser) {
 *     auto obj = deser(metadata, payload);
 * }
 * @endcode
 */
class TypeRegistry {
public:
    /**
     * @brief Create or retrieve singleton instance
     */
    static TypeRegistry& instance();
    
    /**
     * @brief Register a type
     *
     * Logs and overwrites if the same key already exists.
     */
    void register_type(
        const std::string& type_name,
        CustomDeserializer deserializer,
        const std::string& version = ""
    );
    
    /**
     * @brief Get deserializer for a type
     */
    CustomDeserializer get_deserializer(
        const std::string& type_name,
        const std::string& version = ""
    );
    
    /**
     * @brief Get all registered type keys
     */
    std::vector<std::string> get_registered_types() const;

private:
    TypeRegistry();
    ~TypeRegistry() = default;
    
    TypeRegistry(const TypeRegistry&) = delete;
    TypeRegistry& operator=(const TypeRegistry&) = delete;
    TypeRegistry(TypeRegistry&&) = delete;
    TypeRegistry& operator=(TypeRegistry&&) = delete;
    
    // Helper to create registry key: "typename" or "typename@version"
    std::string make_key(const std::string& type_name, const std::string& version) const;
    
    std::map<std::string, CustomDeserializer> registry_;
};

} // namespace ipc0cp

#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <variant>
#include <concepts>
#include <nlohmann/json.hpp>

namespace ipc0cp {

/**
 * @brief Object types supported by serialization
 */
enum class ObjectType {
    NumpyArray,
    Image,
    Text,
    Json,
    Bytes,
    Unknown
};

/**
 * @brief Convert string to ObjectType
 */
ObjectType stringToObjectType(const std::string& type_str);

/**
 * @brief Convert ObjectType to string
 */
std::string objectTypeToString(ObjectType type);

/**
 * @brief Metadata for serialized objects
 */
struct SerializedMetadata {
    ObjectType type;
    std::map<std::string, std::string> attributes;
    
    SerializedMetadata() : type(ObjectType::Unknown) {}
    explicit SerializedMetadata(ObjectType t) : type(t) {}
};

/**
 * @brief Result of serialization
 */
struct SerializedData {
    std::string metadata_json;  // JSON metadata string
    std::vector<uint8_t> payload;  // Binary payload
};

/**
 * @brief Base class for serializable objects with polymorphic serialization
 */
class SerializableObject {
public:
    virtual ~SerializableObject() = default;
    
    /**
     * @brief Get the object type
     */
    virtual ObjectType get_type() const = 0;
    
    /**
     * @brief Serialize this object to metadata JSON and payload
     * @return SerializedData containing metadata and payload
     */
    virtual SerializedData serialize() const = 0;
    
    /**
     * @brief Deserialize from metadata and payload (factory method)
     * @param metadata Parsed metadata map
     * @param payload Raw payload bytes
     * @return Unique pointer to deserialized object, or nullptr on failure
     */
    static std::unique_ptr<SerializableObject> deserialize(
        const std::map<std::string, std::string>& metadata,
        const std::vector<uint8_t>& payload
    );
};

/**
 * @brief Raw bytes - base class for all data types
 * All serializable objects are fundamentally bytes with interpretation
 */
class BytesData : public SerializableObject {
public:
    std::vector<uint8_t> bytes;
    
    BytesData() = default;
    explicit BytesData(std::vector<uint8_t> data) : bytes(std::move(data)) {}
    
    ObjectType get_type() const override { return ObjectType::Bytes; }
    
    SerializedData serialize() const override;
    
    // Static factory for deserialization
    static std::unique_ptr<BytesData> deserialize(
        const std::map<std::string, std::string>& metadata,
        const std::vector<uint8_t>& payload
    );
};

/**
 * @brief Text string - bytes with text encoding
 */
class TextData : public BytesData {
public:
    std::string text;
    std::string encoding = "utf-8";
    
    TextData() = default;
    explicit TextData(std::string str);
    
    ObjectType get_type() const override { return ObjectType::Text; }
    
    SerializedData serialize() const override;
    
    // Static factory for deserialization
    static std::unique_ptr<TextData> deserialize(
        const std::map<std::string, std::string>& metadata,
        const std::vector<uint8_t>& payload
    );
};

/**
 * @brief JSON data - text with JSON structure
 * Extends TextData, provides JSON parsing capability
 */
class JsonData : public TextData {
public:
    JsonData() = default;
    explicit JsonData(std::string json_str);
    
    ObjectType get_type() const override { return ObjectType::Json; }
    
    SerializedData serialize() const override;
    
    /**
     * @brief Parse and return JSON object
     * @return nlohmann::json object
     * @throws nlohmann::json::parse_error if parsing fails
     */
    nlohmann::json json() const;
    
    // Static factory for deserialization
    static std::unique_ptr<JsonData> deserialize(
        const std::map<std::string, std::string>& metadata,
        const std::vector<uint8_t>& payload
    );
};

/**
 * @brief Image representation (PNG-encoded)
 * bytes contains PNG-encoded image data
 */
class ImageData : public BytesData {
public:
    std::string mode;  // RGB, RGBA, L, etc.
    size_t width;
    size_t height;
    // Note: bytes member (from BytesData) contains PNG-encoded data
    
    ImageData() = default;
    
    ObjectType get_type() const override { return ObjectType::Image; }
    
    SerializedData serialize() const override;
    
    // Static factory for deserialization
    static std::unique_ptr<ImageData> deserialize(
        const std::map<std::string, std::string>& metadata,
        const std::vector<uint8_t>& payload
    );
};

/**
 * @brief NumPy array representation
 * bytes contains raw array data, array provides future typed access
 */
class NumpyArray : public BytesData {
public:
    std::vector<size_t> shape;
    std::string dtype;
    // Note: bytes member (from BytesData) contains raw array data
    
    // Placeholder for future numpy integration
    // TODO: Implement proper numpy array wrapper
    void* array = nullptr;
    
    NumpyArray() = default;
    
    ObjectType get_type() const override { return ObjectType::NumpyArray; }
    
    SerializedData serialize() const override;
    
    // Helper to get element count
    size_t element_count() const {
        size_t count = 1;
        for (size_t dim : shape) count *= dim;
        return count;
    }
    
    // Helper to get element size in bytes
    size_t element_size() const {
        return bytes.size() / element_count();
    }
    
    // Template method to get typed data pointer
    template<typename T>
    const T* data_as() const {
        return reinterpret_cast<const T*>(bytes.data());
    }
    
    template<typename T>
    T* data_as() {
        return reinterpret_cast<T*>(bytes.data());
    }
    
    // Static factory for deserialization
    static std::unique_ptr<NumpyArray> deserialize(
        const std::map<std::string, std::string>& metadata,
        const std::vector<uint8_t>& payload
    );
};

/**
 * @brief Helper functions for serialization
 */
class SerializerUtils {
public:
    // Helper to parse shape string like "[100,100,3]"
    static std::vector<size_t> parse_shape(const std::string& shape_str);
    
    // Helper to parse size string like "[200,150]" to (width, height)
    static std::pair<size_t, size_t> parse_size(const std::string& size_str);
    
    // Helper to format shape as string "[dim1,dim2,...]"
    static std::string format_shape(const std::vector<size_t>& shape);
    
    // Helper to format size as string "[width,height]"
    static std::string format_size(size_t width, size_t height);
};

/**
 * @brief Global deserialize function
 * @param metadata_json JSON metadata string
 * @param payload Binary payload
 * @return Unique pointer to deserialized object
 */
std::unique_ptr<SerializableObject> deserialize(
    const std::string& metadata_json,
    const std::vector<uint8_t>& payload
);

} // namespace ipc0cp

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
    List,
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
 * @brief Raw bytes - base class for all data types.
 *
 * Supports two modes:
 * - **Owning**: `bytes` vector holds the data (default, from construction/copy).
 * - **View**: data points into a shared payload buffer (zero-copy deserialization).
 *   The payload is kept alive via `payload_ref`.
 *
 * Use `data()` and `size()` for mode-independent access.
 */
class BytesData : public SerializableObject {
public:
    std::vector<uint8_t> bytes;  ///< Owned data (empty in view mode)

    // ── View mode (zero-copy) ────────────────────────────────────────────
    std::shared_ptr<const std::vector<uint8_t>> payload_ref;  ///< Shared payload (keeps data alive)
    const uint8_t* view_ptr = nullptr;   ///< Pointer into payload_ref (null = owning mode)
    size_t view_size = 0;                ///< Size of view slice

    BytesData() = default;
    explicit BytesData(std::vector<uint8_t> data) : bytes(std::move(data)) {}

    /// Construct a zero-copy view into a shared payload.
    BytesData(std::shared_ptr<const std::vector<uint8_t>> payload,
              const uint8_t* ptr, size_t size)
        : payload_ref(std::move(payload)), view_ptr(ptr), view_size(size) {}

    /// Mode-independent data pointer.
    const uint8_t* data() const { return view_ptr ? view_ptr : bytes.data(); }

    /// Mode-independent size.
    size_t size() const { return view_ptr ? view_size : bytes.size(); }

    /// True if this object borrows data from a shared payload (zero-copy mode).
    bool is_view() const { return view_ptr != nullptr; }

    ObjectType get_type() const override { return ObjectType::Bytes; }
    
    SerializedData serialize() const override;
    
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
 * @brief List of serializable objects - supports mixed types
 * 
 * Serializes a list of objects with individual metadata for each item.
 * Enables collections like [image, text, image] or [array1, array2].
 * 
 * Metadata:
 *   {
 *     "type": "list",
 *     "version": "1.0",
 *     "count": 3,
 *     "items": [
 *       {
 *         "metadata": {"type": "image", ...},
 *         "payload_size": 12345
 *       },
 *       { ... }
 *     ]
 *   }
 * 
 * Payload format: concatenated item payloads (metadata lives entirely in the slot metadata)
 * 
 * Constraints:
 * - Minimum 1 item (empty lists not supported)
 * - Maximum 10 items and 10 nesting depth
 * - Items can be any serializable type (including nested lists)
 */
class ListData : public SerializableObject {
public:
    std::vector<std::unique_ptr<SerializableObject>> items;
    static constexpr size_t MAX_ITEMS = 10;
    static constexpr size_t MAX_DEPTH = 10;
    
    ListData() = default;
    
    /**
     * @brief Create from vector of items
     * @param items Vector of serializable objects
     * @throws std::invalid_argument if empty or exceeds max depth
     */
    explicit ListData(std::vector<std::unique_ptr<SerializableObject>> items);
    
    ObjectType get_type() const override { return ObjectType::List; }
    
    SerializedData serialize() const override;
    
    /**
     * @brief Get item count
     */
    size_t size() const { return items.size(); }
    
    /**
     * @brief Check if list is empty
     */
    bool empty() const { return items.empty(); }
    
    /**
     * @brief Get item at index
     */
    const SerializableObject* at(size_t index) const;
    
    /**
     * @brief Get item at index (mutable)
     */
    SerializableObject* at_mut(size_t index);
    
    // Static factory for deserialization
    static std::unique_ptr<ListData> deserialize(
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
 * @brief Global deserialize function (owning — copies payload into each object).
 */
std::unique_ptr<SerializableObject> deserialize(
    const std::string& metadata_json,
    const std::vector<uint8_t>& payload
);

/**
 * @brief Zero-copy deserialize — objects borrow data from the shared payload.
 *
 * Returns the same types (ListData, NumpyArray, etc.) but BytesData-derived
 * objects have their view_ptr set to point into the payload.  The shared_ptr
 * keeps the payload alive as long as any deserialized object references it.
 *
 * Usage:
 *   auto payload_ptr = std::make_shared<std::vector<uint8_t>>(std::move(raw_bytes));
 *   auto obj = ipc0cp::deserialize(metadata_json, payload_ptr);
 *   auto* list = dynamic_cast<ListData*>(obj.get());
 *   auto* arr = dynamic_cast<NumpyArray*>(list->items[0].get());
 *   // arr->data() points into payload_ptr — zero copy
 */
std::unique_ptr<SerializableObject> deserialize(
    const std::string& metadata_json,
    std::shared_ptr<const std::vector<uint8_t>> payload
);

} // namespace ipc0cp

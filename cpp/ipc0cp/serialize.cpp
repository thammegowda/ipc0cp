// c++ standard includes
// c++ standard includes
#include "serialize.hpp"
#include "logger.hpp"
#include "type_registry.hpp"
#include <sstream>
#include <algorithm>
#include <cctype>
#include <iostream>
#include <stdexcept>
#include <nlohmann/json.hpp>

namespace ipc0cp {

using json = nlohmann::json;

ObjectType stringToObjectType(const std::string& type_str) {
    static const std::map<std::string, ObjectType> type_map = {
        {"ndarray", ObjectType::NumpyArray},
        {"image", ObjectType::Image},
        {"text", ObjectType::Text},
        {"json", ObjectType::Json},
        {"bytes", ObjectType::Bytes},
        {"list", ObjectType::List}
    };
    
    auto it = type_map.find(type_str);
    return it != type_map.end() ? it->second : ObjectType::Unknown;
}

std::string objectTypeToString(ObjectType type) {
    switch (type) {
        case ObjectType::NumpyArray: return "ndarray";
        case ObjectType::Image: return "image";
        case ObjectType::Text: return "text";
        case ObjectType::Json: return "json";
        case ObjectType::Bytes: return "bytes";
        case ObjectType::List: return "list";
        default: return "unknown";
    }
}

namespace {

bool register_builtin_types() {
    static const bool initialized = [] {
        auto& registry = TypeRegistry::instance();
        registry.register_type("bytes", [](const auto& metadata, const auto& payload) {
            return BytesData::deserialize(metadata, payload);
        });
        registry.register_type("text", [](const auto& metadata, const auto& payload) {
            return TextData::deserialize(metadata, payload);
        });
        registry.register_type("json", [](const auto& metadata, const auto& payload) {
            return JsonData::deserialize(metadata, payload);
        });
        registry.register_type("image", [](const auto& metadata, const auto& payload) {
            return ImageData::deserialize(metadata, payload);
        });
        registry.register_type("ndarray", [](const auto& metadata, const auto& payload) {
            return NumpyArray::deserialize(metadata, payload);
        });
        registry.register_type("list", [](const auto& metadata, const auto& payload) {
            return ListData::deserialize(metadata, payload);
        });
        return true;
    }();
    return initialized;
}

} // namespace

// ==================== SerializerUtils Helper Functions ====================

std::vector<size_t> SerializerUtils::parse_shape(const std::string& shape_str) {
    std::vector<size_t> shape;
    
    // Remove brackets and whitespace
    std::string cleaned;
    for (char c : shape_str) {
        if (std::isdigit(c) || c == ',') {
            cleaned += c;
        }
    }
    
    // Parse comma-separated numbers
    std::istringstream ss(cleaned);
    std::string token;
    while (std::getline(ss, token, ',')) {
        if (!token.empty()) {
            shape.push_back(std::stoull(token));
        }
    }
    
    return shape;
}

std::pair<size_t, size_t> SerializerUtils::parse_size(const std::string& size_str) {
    auto dims = parse_shape(size_str);
    if (dims.size() >= 2) {
        return {dims[0], dims[1]};  // width, height
    }
    return {0, 0};
}

std::string SerializerUtils::format_shape(const std::vector<size_t>& shape) {
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < shape.size(); ++i) {
        if (i > 0) oss << ",";
        oss << shape[i];
    }
    oss << "]";
    return oss.str();
}

std::string SerializerUtils::format_size(size_t width, size_t height) {
    std::ostringstream oss;
    oss << "[" << width << "," << height << "]";
    return oss.str();
}

// ==================== SerializableObject Base ====================

std::unique_ptr<SerializableObject> SerializableObject::deserialize(
    const std::map<std::string, std::string>& metadata,
    const std::vector<uint8_t>& payload
) {
    register_builtin_types();

    auto type_it = metadata.find("type");
    if (type_it == metadata.end()) {
        return nullptr;
    }

    const std::string& type_name = type_it->second;
    const auto version_it = metadata.find("version");
    const std::string version = version_it != metadata.end() ? version_it->second : "";

    auto& registry = TypeRegistry::instance();
    CustomDeserializer deserializer;
    if (!version.empty()) {
        deserializer = registry.get_deserializer(type_name, version);
    }
    if (!deserializer) {
        deserializer = registry.get_deserializer(type_name);
    }

    if (deserializer) {
        return deserializer(metadata, payload);
    }

    if (version.empty()) {
        IPC_LOG_WARNING("Unknown serialized type '" << type_name << "' - falling back to BytesData");
    } else {
        IPC_LOG_WARNING(
            "Unknown serialized type '" << type_name << "'@'" << version << "' - falling back to BytesData"
        );
    }

    return BytesData::deserialize(metadata, payload);
}

// ==================== BytesData ====================

std::unique_ptr<BytesData> BytesData::deserialize(
    const std::map<std::string, std::string>& metadata,
    const std::vector<uint8_t>& payload
) {
    (void)metadata;  // unused parameter
    auto bytes_obj = std::make_unique<BytesData>();
    bytes_obj->bytes = payload;
    return bytes_obj;
}

SerializedData BytesData::serialize() const {
    SerializedData result;
    
    // Build metadata JSON
    json metadata;
    metadata["type"] = "bytes";
    
    result.metadata_json = metadata.dump();
    result.payload = bytes;
    
    return result;
}

// ==================== TextData ====================

TextData::TextData(std::string str) : text(std::move(str)) {
    // Sync text to bytes
    bytes.assign(text.begin(), text.end());
}

std::unique_ptr<TextData> TextData::deserialize(
    const std::map<std::string, std::string>& metadata,
    const std::vector<uint8_t>& payload
) {
    auto text_obj = std::make_unique<TextData>();
    
    // Get encoding (default utf-8)
    auto encoding_it = metadata.find("encoding");
    if (encoding_it != metadata.end()) {
        text_obj->encoding = encoding_it->second;
    }
    
    // Store bytes and text
    text_obj->bytes = payload;
    text_obj->text = std::string(payload.begin(), payload.end());
    
    return text_obj;
}

SerializedData TextData::serialize() const {
    SerializedData result;
    
    // Build metadata JSON
    json metadata;
    metadata["type"] = "text";
    metadata["encoding"] = encoding;
    
    result.metadata_json = metadata.dump();
    result.payload.assign(text.begin(), text.end());
    
    return result;
}

// ==================== JsonData ====================

JsonData::JsonData(std::string json_str) {
    text = std::move(json_str);
    bytes.assign(text.begin(), text.end());
}

nlohmann::json JsonData::json() const {
    return json::parse(text);
}

std::unique_ptr<JsonData> JsonData::deserialize(
    const std::map<std::string, std::string>& metadata,
    const std::vector<uint8_t>& payload
) {
    auto json_obj = std::make_unique<JsonData>();
    
    // Get encoding (default utf-8)
    auto encoding_it = metadata.find("encoding");
    if (encoding_it != metadata.end()) {
        json_obj->encoding = encoding_it->second;
    }
    
    // Store bytes and text
    json_obj->bytes = payload;
    json_obj->text = std::string(payload.begin(), payload.end());
    
    return json_obj;
}

SerializedData JsonData::serialize() const {
    SerializedData result;
    
    // Build metadata JSON
    nlohmann::json metadata;
    metadata["type"] = "json";
    metadata["encoding"] = encoding;
    
    result.metadata_json = metadata.dump();
    result.payload.assign(text.begin(), text.end());
    
    return result;
}

// ==================== ImageData ====================

std::unique_ptr<ImageData> ImageData::deserialize(
    const std::map<std::string, std::string>& metadata,
    const std::vector<uint8_t>& payload
) {
    auto image = std::make_unique<ImageData>();
    
    // Get mode
    auto mode_it = metadata.find("mode");
    if (mode_it != metadata.end()) {
        image->mode = mode_it->second;
    } else {
        image->mode = "RGB";  // default
    }
    
    // Get size (width, height)
    auto size_it = metadata.find("size");
    if (size_it != metadata.end()) {
        auto [width, height] = SerializerUtils::parse_size(size_it->second);
        image->width = width;
        image->height = height;
    } else {
        IPC_LOG_ERROR("Image missing size");
        return nullptr;
    }
    
    // Store PNG-encoded data in bytes
    image->bytes = payload;
    
    return image;
}

SerializedData ImageData::serialize() const {
    SerializedData result;
    
    // Build metadata JSON
    json metadata;
    metadata["type"] = "image";
    metadata["mode"] = mode;
    metadata["size"] = SerializerUtils::format_size(width, height);
    
    result.metadata_json = metadata.dump();
    result.payload = bytes;  // bytes contains PNG data
    
    return result;
}

// ==================== NumpyArray ====================

std::unique_ptr<NumpyArray> NumpyArray::deserialize(
    const std::map<std::string, std::string>& metadata,
    const std::vector<uint8_t>& payload
) {
    auto array = std::make_unique<NumpyArray>();
    
    // Parse shape
    auto shape_it = metadata.find("shape");
    if (shape_it != metadata.end()) {
        array->shape = SerializerUtils::parse_shape(shape_it->second);
    } else {
        IPC_LOG_ERROR("NumPy array missing shape");
        return nullptr;
    }
    
    // Get dtype
    auto dtype_it = metadata.find("dtype");
    if (dtype_it != metadata.end()) {
        array->dtype = dtype_it->second;
    } else {
        IPC_LOG_ERROR("NumPy array missing dtype");
        return nullptr;
    }
    
    // Copy payload data to bytes
    array->bytes = payload;
    
    return array;
}

SerializedData NumpyArray::serialize() const {
    SerializedData result;
    
    // Build metadata JSON
    json metadata;
    metadata["type"] = "ndarray";
    metadata["shape"] = SerializerUtils::format_shape(shape);
    metadata["dtype"] = dtype;
    
    result.metadata_json = metadata.dump();
    result.payload = bytes;  // bytes contains array data
    
    return result;
}

// ==================== ListData ====================

namespace {

size_t compute_list_depth(const ListData* list);

size_t compute_list_depth(const ListData* list) {
    size_t max_child_depth = 0;
    for (const auto& child : list->items) {
        if (const ListData* nested = dynamic_cast<const ListData*>(child.get())) {
            max_child_depth = std::max(max_child_depth, compute_list_depth(nested));
        }
    }
    return 1 + max_child_depth;
}

}

ListData::ListData(std::vector<std::unique_ptr<SerializableObject>> items_in)
    : items(std::move(items_in)) {
    if (items.empty()) {
        throw std::invalid_argument("ListData cannot be empty");
    }
    if (items.size() > MAX_ITEMS) {
        throw std::invalid_argument("ListData exceeds maximum items (" + std::to_string(MAX_ITEMS) + ")");
    }
    if (compute_list_depth(this) > MAX_DEPTH) {
        throw std::invalid_argument("ListData exceeds maximum nesting depth (" + std::to_string(MAX_DEPTH) + ")");
    }
}

const SerializableObject* ListData::at(size_t index) const {
    if (index >= items.size()) {
        return nullptr;
    }
    return items[index].get();
}

SerializableObject* ListData::at_mut(size_t index) {
    if (index >= items.size()) {
        return nullptr;
    }
    return items[index].get();
}

SerializedData ListData::serialize() const {
    SerializedData result;

    json metadata;
    metadata["type"] = "list";
    metadata["version"] = "1.0";
    metadata["count"] = items.size();

    json item_array = json::array();
    std::vector<uint8_t> concatenated_payload;
    concatenated_payload.reserve(1024);

    for (const auto& item : items) {
        if (!item) {
            throw std::runtime_error("Null item in ListData");
        }

        auto item_data = item->serialize();
        json item_entry;
        item_entry["metadata"] = json::parse(item_data.metadata_json);
        item_entry["payload_size"] = item_data.payload.size();
        item_array.push_back(item_entry);

        concatenated_payload.insert(
            concatenated_payload.end(),
            item_data.payload.begin(),
            item_data.payload.end()
        );
    }

    metadata["items"] = item_array;
    result.metadata_json = metadata.dump();
    result.payload = std::move(concatenated_payload);

    return result;
}

std::unique_ptr<ListData> ListData::deserialize(
    const std::map<std::string, std::string>& metadata,
    const std::vector<uint8_t>& payload
) {
    auto count_it = metadata.find("count");
    if (count_it == metadata.end()) {
        IPC_LOG_ERROR("ListData missing count");
        return nullptr;
    }

    size_t count = 0;
    try {
        count = std::stoull(count_it->second);
    } catch (...) {
        IPC_LOG_ERROR("ListData invalid count: " << count_it->second);
        return nullptr;
    }

    if (count == 0 || count > MAX_ITEMS) {
        IPC_LOG_ERROR("ListData invalid count: " << count);
        return nullptr;
    }

    auto items_it = metadata.find("items");
    if (items_it == metadata.end()) {
        IPC_LOG_ERROR("ListData missing items array");
        return nullptr;
    }

    json items_json;
    try {
        items_json = json::parse(items_it->second);
    } catch (const std::exception& e) {
        IPC_LOG_ERROR("ListData items JSON parse failed: " << e.what());
        return nullptr;
    }

    if (!items_json.is_array() || items_json.size() != count) {
        IPC_LOG_ERROR("ListData items count mismatch");
        return nullptr;
    }

    std::vector<std::unique_ptr<SerializableObject>> items_vec;
    items_vec.reserve(count);
    size_t offset = 0;

    for (size_t i = 0; i < count; ++i) {
        const auto& entry = items_json[i];
        if (!entry.is_object()) {
            IPC_LOG_ERROR("ListData entry " << i << " is not an object");
            return nullptr;
        }

        if (!entry.contains("payload_size") || !entry["payload_size"].is_number_unsigned()) {
            IPC_LOG_ERROR("ListData entry " << i << " missing payload_size");
            return nullptr;
        }

        auto payload_size = entry["payload_size"].get<size_t>();
        if (offset + payload_size > payload.size()) {
            IPC_LOG_ERROR("ListData item " << i << " incomplete payload");
            return nullptr;
        }

        if (!entry.contains("metadata") || !entry["metadata"].is_object()) {
            IPC_LOG_ERROR("ListData entry " << i << " missing metadata");
            return nullptr;
        }

        std::string item_meta_json = entry["metadata"].dump();
        std::vector<uint8_t> item_payload(
            payload.begin() + offset,
            payload.begin() + offset + payload_size
        );
        offset += payload_size;

        auto item = ::ipc0cp::deserialize(item_meta_json, item_payload);
        if (!item) {
            IPC_LOG_ERROR("ListData item " << i << " deserialization failed");
            return nullptr;
        }

        items_vec.push_back(std::move(item));
    }

    if (offset != payload.size()) {
        IPC_LOG_ERROR("ListData payload has trailing bytes");
        return nullptr;
    }

    return std::make_unique<ListData>(std::move(items_vec));
}

// Global deserialize function
std::unique_ptr<SerializableObject> deserialize(
    const std::string& metadata_json,
    const std::vector<uint8_t>& payload
) {
    // Parse metadata JSON
    json metadata = json::parse(metadata_json);
    
    // Convert JSON to map<string, string>
    std::map<std::string, std::string> metadata_map;
    for (auto& [key, value] : metadata.items()) {
        if (value.is_string()) {
            metadata_map[key] = value.get<std::string>();
        } else {
            metadata_map[key] = value.dump();
        }
    }
    
    // Use SerializableObject::deserialize
    return SerializableObject::deserialize(metadata_map, payload);
}

// ─────────────────────────────────────────────────────────────────────────────
// Zero-copy deserialize: objects borrow data from shared payload
// ─────────────────────────────────────────────────────────────────────────────

namespace {

// Recursive zero-copy deserialize helper.
// For BytesData-derived types, creates view-mode objects pointing into payload.
// For ListData, recursively deserializes items with views.
std::unique_ptr<SerializableObject> deserialize_zero_copy(
    const std::string& metadata_json,
    std::shared_ptr<const std::vector<uint8_t>> payload,
    size_t offset, size_t length
) {
    using json = nlohmann::json;
    auto metadata = json::parse(metadata_json);
    auto type_str = metadata.value("type", "unknown");

    if (type_str == "list") {
        size_t count = metadata.value("count", size_t(0));
        auto items_array = metadata.value("items", json::array());
        if (items_array.size() != count) {
            throw std::runtime_error("Zero-copy deserialize: list count mismatch");
        }

        std::vector<std::unique_ptr<SerializableObject>> items_vec;
        items_vec.reserve(count);
        size_t item_offset = offset;

        for (size_t i = 0; i < count; ++i) {
            auto& entry = items_array[i];
            size_t payload_size = entry.value("payload_size", size_t(0));
            auto item_meta = entry.value("metadata", json::object());
            std::string item_meta_json = item_meta.dump();

            auto item = deserialize_zero_copy(item_meta_json, payload, item_offset, payload_size);
            items_vec.push_back(std::move(item));
            item_offset += payload_size;
        }

        return std::make_unique<ListData>(std::move(items_vec));
    }

    // BytesData-derived types: create view into payload
    const uint8_t* ptr = payload->data() + offset;

    if (type_str == "ndarray" || type_str == "numpy") {
        auto arr = std::make_unique<NumpyArray>();
        arr->payload_ref = payload;
        arr->view_ptr = ptr;
        arr->view_size = length;
        arr->dtype = metadata.value("dtype", "uint8");
        auto shape_str = metadata.value("shape", "[]");
        arr->shape = SerializerUtils::parse_shape(shape_str);
        return arr;
    }

    if (type_str == "json") {
        auto obj = std::make_unique<JsonData>();
        obj->payload_ref = payload;
        obj->view_ptr = ptr;
        obj->view_size = length;
        if (length > 0) {
            obj->text = std::string(reinterpret_cast<const char*>(ptr), length);
        }
        return obj;
    }

    if (type_str == "text") {
        auto obj = std::make_unique<TextData>();
        obj->payload_ref = payload;
        obj->view_ptr = ptr;
        obj->view_size = length;
        obj->encoding = metadata.value("encoding", "utf-8");
        if (length > 0) {
            obj->text = std::string(reinterpret_cast<const char*>(ptr), length);
        }
        return obj;
    }

    if (type_str == "image") {
        auto obj = std::make_unique<ImageData>();
        obj->payload_ref = payload;
        obj->view_ptr = ptr;
        obj->view_size = length;
        obj->mode = metadata.value("mode", "RGB");
        auto size_str = metadata.value("size", "[0,0]");
        auto [w, h] = SerializerUtils::parse_size(size_str);
        obj->width = w;
        obj->height = h;
        return obj;
    }

    // Fallback: generic BytesData view
    auto obj = std::make_unique<BytesData>(payload, ptr, length);
    return obj;
}

} // anonymous namespace

std::unique_ptr<SerializableObject> deserialize(
    const std::string& metadata_json,
    std::shared_ptr<const std::vector<uint8_t>> payload
) {
    return deserialize_zero_copy(metadata_json, std::move(payload), 0, payload->size());
}

} // namespace ipc0cp

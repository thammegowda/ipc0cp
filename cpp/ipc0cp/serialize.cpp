#include "serialize.hpp"
#include "logger.hpp"
#include <sstream>
#include <algorithm>
#include <cctype>
#include <iostream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace ipc0cp {

ObjectType stringToObjectType(const std::string& type_str) {
    static const std::map<std::string, ObjectType> type_map = {
        {"ndarray", ObjectType::NumpyArray},
        {"image", ObjectType::Image},
        {"text", ObjectType::Text},
        {"json", ObjectType::Json},
        {"bytes", ObjectType::Bytes}
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
        default: return "unknown";
    }
}

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
    auto type_it = metadata.find("type");
    if (type_it == metadata.end()) {
        return nullptr;
    }
    
    ObjectType obj_type = stringToObjectType(type_it->second);
    
    switch (obj_type) {
        case ObjectType::NumpyArray:
            return NumpyArray::deserialize(metadata, payload);
        case ObjectType::Image:
            return ImageData::deserialize(metadata, payload);
        case ObjectType::Text:
            return TextData::deserialize(metadata, payload);
        case ObjectType::Json:
            return JsonData::deserialize(metadata, payload);
        case ObjectType::Bytes:
            return BytesData::deserialize(metadata, payload);
        default:
            return nullptr;
    }
}

// ==================== BytesData ====================

std::unique_ptr<BytesData> BytesData::deserialize(
    const std::map<std::string, std::string>& metadata,
    const std::vector<uint8_t>& payload
) {
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

} // namespace ipc0cp

#include <nlohmann/json.hpp>
#include <gtest/gtest.h>
#include "ipc0cp/serialize.hpp"
#include <cstring>

using namespace ipc0cp;

static std::map<std::string, std::string> parse_metadata(const std::string& json_str) {
    auto parsed_json = nlohmann::json::parse(json_str);
    std::map<std::string, std::string> metadata;
    for (auto& [key, value] : parsed_json.items()) {
        metadata[key] = value.dump();
        if (!metadata[key].empty() && metadata[key].front() == '"' && metadata[key].back() == '"') {
            metadata[key] = metadata[key].substr(1, metadata[key].size() - 2);
        }
    }
    return metadata;
}

class SerializationTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// Test NumpyArray serialization/deserialization
TEST_F(SerializationTest, NumpyArrayRoundTrip) {
    // Create a NumPy array
    NumpyArray arr;
    arr.shape = {10, 20};
    arr.dtype = "<f4";
    arr.bytes.resize(10 * 20 * sizeof(float));
    
    // Fill with test data
    float* data = arr.data_as<float>();
    for (size_t i = 0; i < 200; ++i) {
        data[i] = static_cast<float>(i) * 0.5f;
    }
    
    // Serialize
    auto serialized = arr.serialize();
    EXPECT_FALSE(serialized.metadata_json.empty());
    EXPECT_EQ(serialized.payload.size(), arr.bytes.size());
    
    // Parse metadata back
    auto parsed_json = nlohmann::json::parse(serialized.metadata_json);
    std::map<std::string, std::string> metadata;
    for (auto& [key, value] : parsed_json.items()) {
        metadata[key] = value.dump();
        // Remove quotes from string values
        if (metadata[key].front() == '"' && metadata[key].back() == '"') {
            metadata[key] = metadata[key].substr(1, metadata[key].size() - 2);
        }
    }
    
    // Deserialize
    auto deserialized = NumpyArray::deserialize(metadata, serialized.payload);
    ASSERT_NE(deserialized, nullptr);
    
    // Verify
    EXPECT_EQ(deserialized->shape, arr.shape);
    EXPECT_EQ(deserialized->dtype, arr.dtype);
    EXPECT_EQ(deserialized->bytes.size(), arr.bytes.size());
    EXPECT_EQ(deserialized->element_count(), 200);
    
    const float* deserialized_data = deserialized->data_as<float>();
    for (size_t i = 0; i < 200; ++i) {
        EXPECT_FLOAT_EQ(deserialized_data[i], static_cast<float>(i) * 0.5f);
    }
}

// Test TextData serialization/deserialization
TEST_F(SerializationTest, TextDataRoundTrip) {
    TextData text("Hello, World! 🌍");
    
    // Serialize
    auto serialized = text.serialize();
    EXPECT_FALSE(serialized.metadata_json.empty());
    
    // Parse metadata
    auto parsed_json = nlohmann::json::parse(serialized.metadata_json);
    std::map<std::string, std::string> metadata;
    for (auto& [key, value] : parsed_json.items()) {
        metadata[key] = value.dump();
        if (metadata[key].front() == '"' && metadata[key].back() == '"') {
            metadata[key] = metadata[key].substr(1, metadata[key].size() - 2);
        }
    }
    
    // Deserialize
    auto deserialized = TextData::deserialize(metadata, serialized.payload);
    ASSERT_NE(deserialized, nullptr);
    
    // Verify
    EXPECT_EQ(deserialized->text, text.text);
    EXPECT_EQ(deserialized->encoding, text.encoding);
}

// Test JsonData serialization/deserialization
TEST_F(SerializationTest, JsonDataRoundTrip) {
    JsonData json_obj(R"({"name": "test", "value": 42, "array": [1, 2, 3]})");
    
    // Serialize
    auto serialized = json_obj.serialize();
    EXPECT_FALSE(serialized.metadata_json.empty());
    
    // Parse metadata
    auto parsed_json = nlohmann::json::parse(serialized.metadata_json);
    std::map<std::string, std::string> metadata;
    for (auto& [key, value] : parsed_json.items()) {
        metadata[key] = value.dump();
        if (metadata[key].front() == '"' && metadata[key].back() == '"') {
            metadata[key] = metadata[key].substr(1, metadata[key].size() - 2);
        }
    }
    
    // Deserialize
    auto deserialized = JsonData::deserialize(metadata, serialized.payload);
    ASSERT_NE(deserialized, nullptr);
    
    // Verify
    EXPECT_EQ(deserialized->text, json_obj.text);
    
    // Verify it's valid JSON
    auto parsed = deserialized->json();
    EXPECT_EQ(parsed["name"], "test");
    EXPECT_EQ(parsed["value"], 42);
    EXPECT_EQ(parsed["array"].size(), 3);
}

// Test BytesData serialization/deserialization
TEST_F(SerializationTest, BytesDataRoundTrip) {
    std::vector<uint8_t> test_bytes = {0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD};
    BytesData bytes_obj(test_bytes);
    
    // Serialize
    auto serialized = bytes_obj.serialize();
    EXPECT_FALSE(serialized.metadata_json.empty());
    
    // Parse metadata
    auto parsed_json = nlohmann::json::parse(serialized.metadata_json);
    std::map<std::string, std::string> metadata;
    for (auto& [key, value] : parsed_json.items()) {
        metadata[key] = value.dump();
        if (metadata[key].front() == '"' && metadata[key].back() == '"') {
            metadata[key] = metadata[key].substr(1, metadata[key].size() - 2);
        }
    }
    
    // Deserialize
    auto deserialized = BytesData::deserialize(metadata, serialized.payload);
    ASSERT_NE(deserialized, nullptr);
    
    // Verify
    EXPECT_EQ(deserialized->bytes, bytes_obj.bytes);
}

// Test ImageData serialization/deserialization
TEST_F(SerializationTest, ImageDataRoundTrip) {
    ImageData img;
    img.mode = "RGB";
    img.width = 100;
    img.height = 50;
    img.bytes = {0x89, 0x50, 0x4E, 0x47};  // PNG magic bytes
    
    // Serialize
    auto serialized = img.serialize();
    EXPECT_FALSE(serialized.metadata_json.empty());
    
    // Parse metadata
    auto parsed_json = nlohmann::json::parse(serialized.metadata_json);
    std::map<std::string, std::string> metadata;
    for (auto& [key, value] : parsed_json.items()) {
        metadata[key] = value.dump();
        if (metadata[key].front() == '"' && metadata[key].back() == '"') {
            metadata[key] = metadata[key].substr(1, metadata[key].size() - 2);
        }
    }
    
    // Deserialize
    auto deserialized = ImageData::deserialize(metadata, serialized.payload);
    ASSERT_NE(deserialized, nullptr);
    
    // Verify
    EXPECT_EQ(deserialized->mode, img.mode);
    EXPECT_EQ(deserialized->width, img.width);
    EXPECT_EQ(deserialized->height, img.height);
    EXPECT_EQ(deserialized->bytes, img.bytes);
}

// Test polymorphic deserialization
TEST_F(SerializationTest, PolymorphicDeserialization) {
    TextData text("Test message");
    auto serialized = text.serialize();
    
    // Parse metadata
    auto parsed_json = nlohmann::json::parse(serialized.metadata_json);
    std::map<std::string, std::string> metadata;
    for (auto& [key, value] : parsed_json.items()) {
        metadata[key] = value.dump();
        if (metadata[key].front() == '"' && metadata[key].back() == '"') {
            metadata[key] = metadata[key].substr(1, metadata[key].size() - 2);
        }
    }
    
    // Deserialize using base class factory
    auto obj = SerializableObject::deserialize(metadata, serialized.payload);
    ASSERT_NE(obj, nullptr);
    EXPECT_EQ(obj->get_type(), ObjectType::Text);
    
    // Dynamic cast to specific type
    auto* text_ptr = dynamic_cast<TextData*>(obj.get());
    ASSERT_NE(text_ptr, nullptr);
    EXPECT_EQ(text_ptr->text, "Test message");
}

TEST_F(SerializationTest, ListDataMixedPayloads) {
    std::vector<std::unique_ptr<SerializableObject>> items;
    items.push_back(std::make_unique<BytesData>(std::vector<uint8_t>{0xDE, 0xAD, 0xBE, 0xEF}));
    items.push_back(std::make_unique<JsonData>(R"({"name":"list","value":123})"));
    items.push_back(std::make_unique<TextData>("list text"));

    ListData list(std::move(items));

    auto serialized = list.serialize();
    EXPECT_EQ(list.size(), 3u);

    auto metadata = parse_metadata(serialized.metadata_json);
    auto deserialized = ListData::deserialize(metadata, serialized.payload);
    ASSERT_NE(deserialized, nullptr);
    EXPECT_EQ(deserialized->size(), 3u);

    const auto& deserialized_items = deserialized->items;
    ASSERT_EQ(deserialized_items.size(), 3u);

    auto* bytes_item = dynamic_cast<BytesData*>(deserialized_items[0].get());
    ASSERT_NE(bytes_item, nullptr);
    EXPECT_EQ(bytes_item->bytes, std::vector<uint8_t>({0xDE, 0xAD, 0xBE, 0xEF}));

    auto* json_item = dynamic_cast<JsonData*>(deserialized_items[1].get());
    ASSERT_NE(json_item, nullptr);
    auto parsed = json_item->json();
    EXPECT_EQ(parsed["name"], "list");
    EXPECT_EQ(parsed["value"], 123);

    auto* text_item = dynamic_cast<TextData*>(deserialized_items[2].get());
    ASSERT_NE(text_item, nullptr);
    EXPECT_EQ(text_item->text, "list text");
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

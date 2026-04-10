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

// ============================================================================
// Zero-copy deserialize tests
// ============================================================================

TEST_F(SerializationTest, BytesDataViewMode) {
    // Create owned BytesData
    auto owned = std::make_shared<const std::vector<uint8_t>>(
        std::vector<uint8_t>{10, 20, 30, 40, 50});

    BytesData view(owned, owned->data() + 1, 3);

    EXPECT_TRUE(view.is_view());
    EXPECT_EQ(view.size(), 3);
    EXPECT_EQ(view.data()[0], 20);
    EXPECT_EQ(view.data()[1], 30);
    EXPECT_EQ(view.data()[2], 40);

    // Owned mode
    BytesData owned_obj(std::vector<uint8_t>{1, 2, 3});
    EXPECT_FALSE(owned_obj.is_view());
    EXPECT_EQ(owned_obj.size(), 3);
    EXPECT_EQ(owned_obj.data()[0], 1);
}

TEST_F(SerializationTest, ZeroCopyNumpyArray) {
    // Create a NumpyArray, serialize it, then zero-copy deserialize
    NumpyArray arr;
    arr.shape = {4, 3};
    arr.dtype = "|u1";  // uint8
    arr.bytes = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};

    auto serialized = arr.serialize();

    // Zero-copy deserialize
    auto payload_ptr = std::make_shared<const std::vector<uint8_t>>(serialized.payload);
    auto result = ipc0cp::deserialize(serialized.metadata_json, payload_ptr);
    ASSERT_NE(result, nullptr);

    auto* np = dynamic_cast<NumpyArray*>(result.get());
    ASSERT_NE(np, nullptr);
    EXPECT_TRUE(np->is_view());
    EXPECT_EQ(np->size(), 12);
    EXPECT_EQ(np->shape, (std::vector<size_t>{4, 3}));
    EXPECT_EQ(np->dtype, "|u1");

    // Data should point into payload_ptr (same address)
    EXPECT_EQ(np->data(), payload_ptr->data());
    EXPECT_EQ(np->data()[0], 1);
    EXPECT_EQ(np->data()[11], 12);
}

TEST_F(SerializationTest, ZeroCopyListRoundTrip) {
    // Build a list: [NumpyArray, NumpyArray, JsonData] — mimics streaming_inpaint protocol
    std::vector<std::unique_ptr<SerializableObject>> items;

    auto img = std::make_unique<NumpyArray>();
    img->shape = {2, 2, 3};
    img->dtype = "|u1";
    img->bytes = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120};
    items.push_back(std::move(img));

    auto mask = std::make_unique<NumpyArray>();
    mask->shape = {2, 2};
    mask->dtype = "|u1";
    mask->bytes = {0, 255, 255, 0};
    items.push_back(std::move(mask));

    auto meta = std::make_unique<JsonData>(R"({"filename": "test.png"})");
    items.push_back(std::move(meta));

    ListData list(std::move(items));
    auto serialized = list.serialize();

    // Zero-copy deserialize
    auto payload_ptr = std::make_shared<const std::vector<uint8_t>>(serialized.payload);
    auto result = ipc0cp::deserialize(serialized.metadata_json, payload_ptr);
    ASSERT_NE(result, nullptr);

    auto* rlist = dynamic_cast<ListData*>(result.get());
    ASSERT_NE(rlist, nullptr);
    ASSERT_EQ(rlist->size(), 3);

    // Check image (NumpyArray, view mode)
    auto* rimg = dynamic_cast<NumpyArray*>(rlist->items[0].get());
    ASSERT_NE(rimg, nullptr);
    EXPECT_TRUE(rimg->is_view());
    EXPECT_EQ(rimg->shape, (std::vector<size_t>{2, 2, 3}));
    EXPECT_EQ(rimg->size(), 12);
    EXPECT_EQ(rimg->data()[0], 10);
    EXPECT_EQ(rimg->data()[11], 120);

    // Check mask (NumpyArray, view mode)
    auto* rmask = dynamic_cast<NumpyArray*>(rlist->items[1].get());
    ASSERT_NE(rmask, nullptr);
    EXPECT_TRUE(rmask->is_view());
    EXPECT_EQ(rmask->shape, (std::vector<size_t>{2, 2}));
    EXPECT_EQ(rmask->data()[0], 0);
    EXPECT_EQ(rmask->data()[1], 255);

    // Check json
    auto* rjson = dynamic_cast<JsonData*>(rlist->items[2].get());
    ASSERT_NE(rjson, nullptr);
    auto j = rjson->json();
    EXPECT_EQ(j["filename"], "test.png");
}

TEST_F(SerializationTest, ZeroCopyPayloadLifetime) {
    // Verify that the payload stays alive as long as any view references it
    std::weak_ptr<const std::vector<uint8_t>> weak_ref;
    NumpyArray* raw_ptr = nullptr;

    {
        NumpyArray arr;
        arr.shape = {3};
        arr.dtype = "|u1";
        arr.bytes = {42, 43, 44};
        auto serialized = arr.serialize();

        auto payload_ptr = std::make_shared<const std::vector<uint8_t>>(serialized.payload);
        weak_ref = payload_ptr;

        auto result = ipc0cp::deserialize(serialized.metadata_json, payload_ptr);
        raw_ptr = dynamic_cast<NumpyArray*>(result.get());
        ASSERT_NE(raw_ptr, nullptr);

        // payload_ptr goes out of scope here, but result still holds a ref
        EXPECT_FALSE(weak_ref.expired());  // still alive via result->payload_ref
    }
    // result also went out of scope — payload should be freed
    EXPECT_TRUE(weak_ref.expired());
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

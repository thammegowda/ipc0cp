#include <gtest/gtest.h>
#include "ipc0cp/ring_buffer.hpp"
#include <sys/mman.h>
#include <thread>
#include <chrono>

using namespace ipc0cp;

class RingBufferTest : public ::testing::Test {
protected:
    std::string shm_name_;
    
    void SetUp() override {
        // Use unique name for each test
        shm_name_ = "/test_ipc0cp_" + std::to_string(time(nullptr)) + "_" + std::to_string(rand());
    }
    
    void TearDown() override {
        // Clean up shared memory
        shm_unlink(shm_name_.c_str());
    }
};

// Test basic producer-consumer communication
TEST_F(RingBufferTest, BasicProducerConsumer) {
    const size_t buffer_size = 1024 * 1024;  // 1MB
    
    // Create producer
    SharedRingBufferProducer producer(shm_name_, buffer_size, true);
    ASSERT_TRUE(producer.is_initialized());
    
    // Create consumer
    SharedRingBufferConsumer consumer(shm_name_, buffer_size);
    // Consumer created successfully
    
    // Push some text
    TextData text_obj("Hello from C++!");
    ASSERT_TRUE(producer.push(text_obj));
    
    // Pop and verify
    auto obj = consumer.pop(std::chrono::milliseconds(1000));
    ASSERT_TRUE(obj.has_value());
    EXPECT_EQ(obj->get_type(), ObjectType::Text);
    
    auto& text = obj->as<TextData>();
    EXPECT_EQ(text.text, "Hello from C++!");
}

// Test multiple objects
TEST_F(RingBufferTest, MultipleObjects) {
    const size_t buffer_size = 1024 * 1024;
    
    SharedRingBufferProducer producer(shm_name_, buffer_size, true);
    SharedRingBufferConsumer consumer(shm_name_, buffer_size);
    
    // Push different types of objects
    TextData text("Test text");
    JsonData json_obj(R"({"key": "value"})");
    BytesData bytes({0x01, 0x02, 0x03});
    
    ASSERT_TRUE(producer.push(text));
    ASSERT_TRUE(producer.push(json_obj));
    ASSERT_TRUE(producer.push(bytes));
    
    // Pop and verify order
    auto obj1 = consumer.pop(std::chrono::milliseconds(1000));
    ASSERT_TRUE(obj1.has_value());
    EXPECT_EQ(obj1->get_type(), ObjectType::Text);
    EXPECT_EQ(obj1->as<TextData>().text, "Test text");
    
    auto obj2 = consumer.pop(std::chrono::milliseconds(1000));
    ASSERT_TRUE(obj2.has_value());
    EXPECT_EQ(obj2->get_type(), ObjectType::Json);
    
    auto obj3 = consumer.pop(std::chrono::milliseconds(1000));
    ASSERT_TRUE(obj3.has_value());
    EXPECT_EQ(obj3->get_type(), ObjectType::Bytes);
    EXPECT_EQ(obj3->as<BytesData>().bytes.size(), 3);
}

// Test NumPy array transfer
TEST_F(RingBufferTest, NumpyArrayTransfer) {
    const size_t buffer_size = 10 * 1024 * 1024;  // 10MB
    
    SharedRingBufferProducer producer(shm_name_, buffer_size, true);
    SharedRingBufferConsumer consumer(shm_name_, buffer_size);
    
    // Create NumPy array
    NumpyArray arr;
    arr.shape = {100, 100, 3};
    arr.dtype = "<f4";
    arr.bytes.resize(100 * 100 * 3 * sizeof(float));
    
    float* data = arr.data_as<float>();
    for (size_t i = 0; i < 100 * 100 * 3; ++i) {
        data[i] = static_cast<float>(i);
    }
    
    ASSERT_TRUE(producer.push(arr));
    
    auto obj = consumer.pop(std::chrono::milliseconds(1000));
    ASSERT_TRUE(obj.has_value());
    EXPECT_EQ(obj->get_type(), ObjectType::NumpyArray);
    
    auto& arr_received = obj->as<NumpyArray>();
    EXPECT_EQ(arr_received.shape, arr.shape);
    EXPECT_EQ(arr_received.dtype, arr.dtype);
    EXPECT_EQ(arr_received.element_count(), 100 * 100 * 3);
    
    const float* received_data = arr_received.data_as<float>();
    for (size_t i = 0; i < 100; ++i) {  // Check first 100 elements
        EXPECT_FLOAT_EQ(received_data[i], static_cast<float>(i));
    }
}

// Test buffer wrap-around
TEST_F(RingBufferTest, BufferWrapAround) {
    const size_t buffer_size = 1024;  // Small buffer to force wrap
    
    SharedRingBufferProducer producer(shm_name_, buffer_size, true);
    SharedRingBufferConsumer consumer(shm_name_, buffer_size);
    
    // Push and pop multiple times to wrap around
    for (int i = 0; i < 10; ++i) {
        TextData text("Message " + std::to_string(i));
        ASSERT_TRUE(producer.push(text));
        
        auto obj = consumer.pop(std::chrono::milliseconds(1000));
        ASSERT_TRUE(obj.has_value());
        EXPECT_EQ(obj->as<TextData>().text, "Message " + std::to_string(i));
    }
}

// Test empty buffer
TEST_F(RingBufferTest, EmptyBuffer) {
    const size_t buffer_size = 1024 * 1024;
    
    SharedRingBufferProducer producer(shm_name_, buffer_size, true);
    SharedRingBufferConsumer consumer(shm_name_, buffer_size);
    
    // Try to pop from empty buffer with short timeout
    auto obj = consumer.pop(std::chrono::milliseconds(100));  // 100ms timeout
    EXPECT_FALSE(obj.has_value());
}

// Test concurrent producer-consumer
TEST_F(RingBufferTest, ConcurrentProducerConsumer) {
    const size_t buffer_size = 10 * 1024 * 1024;
    const int num_messages = 100;
    
    SharedRingBufferProducer producer(shm_name_, buffer_size, true);
    
    // Producer thread
    std::thread producer_thread([&]() {
        for (int i = 0; i < num_messages; ++i) {
            TextData text("Message " + std::to_string(i));
            while (!producer.push(text, 0)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    });
    
    // Consumer thread
    std::atomic<int> received_count{0};
    std::thread consumer_thread([&]() {
        SharedRingBufferConsumer consumer(shm_name_, buffer_size);
        
        while (received_count < num_messages) {
            auto obj = consumer.pop(std::chrono::milliseconds(100));
            if (obj.has_value()) {
                EXPECT_EQ(obj->get_type(), ObjectType::Text);
                received_count++;
            }
        }
    });
    
    producer_thread.join();
    consumer_thread.join();
    
    EXPECT_EQ(received_count, num_messages);
}

// Test buffer stats
TEST_F(RingBufferTest, BufferStats) {
    const size_t buffer_size = 1024 * 1024;
    
    SharedRingBufferProducer producer(shm_name_, buffer_size, true);
    SharedRingBufferConsumer consumer(shm_name_, buffer_size);
    
    auto stats = consumer.get_stats();
    EXPECT_EQ(stats.total_data_bytes, buffer_size);
    EXPECT_TRUE(stats.is_empty);
    EXPECT_EQ(stats.used_bytes, 0);
    
    // Push an object
    TextData text("Test message");
    ASSERT_TRUE(producer.push(text));
    
    stats = consumer.get_stats();
    EXPECT_FALSE(stats.is_empty);
    EXPECT_GT(stats.used_bytes, 0);
    
    // Pop the object
    auto obj = consumer.pop(std::chrono::milliseconds(1000));
    ASSERT_TRUE(obj.has_value());
    
    stats = consumer.get_stats();
    EXPECT_TRUE(stats.is_empty);
    EXPECT_EQ(stats.used_bytes, 0);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

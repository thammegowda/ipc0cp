#include <gtest/gtest.h>
#include "ipc0cp/ring_buffer.hpp"
#include <sys/mman.h>
#include <thread>
#include <chrono>
#include <atomic>

using namespace ipc0cp;

static void attach_with_retry(SharedRingBufferConsumer& consumer, int timeout_ms = 2000) {
    auto start = std::chrono::steady_clock::now();
    while (!consumer.attach()) {
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() >= timeout_ms) {
            throw std::runtime_error("Timed out waiting to attach consumer");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

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

        // Clean up semaphores
        try {
            cleanup_buffer_semaphores(shm_name_);
        } catch (...) {
        }
    }
};

// Test basic producer-consumer communication
TEST_F(RingBufferTest, BasicProducerConsumer) {
    const size_t buffer_size = 1024 * 1024;  // 1MB
    
    // Create producer
    SharedRingBufferProducer producer(shm_name_, buffer_size, true);
    ASSERT_TRUE(producer.is_initialized());
    
    // Create consumer
    SharedRingBufferConsumer consumer(shm_name_);
    attach_with_retry(consumer);
    
    // Push some text
    TextData text_obj("Hello from C++!");
    producer.push(text_obj);
    
    // Pop and verify
    auto obj = consumer.pop(1000);
    ASSERT_TRUE(obj);
    EXPECT_EQ(obj->get_type(), ObjectType::Text);
    
    auto& text = obj->as<TextData>();
    EXPECT_EQ(text.text, "Hello from C++!");
}

// Test multiple objects
TEST_F(RingBufferTest, MultipleObjects) {
    const size_t buffer_size = 1024 * 1024;
    
    SharedRingBufferProducer producer(shm_name_, buffer_size, true);
    SharedRingBufferConsumer consumer(shm_name_);
    attach_with_retry(consumer);
    
    // Push different types of objects
    TextData text("Test text");
    JsonData json_obj(R"({"key": "value"})");
    BytesData bytes({0x01, 0x02, 0x03});
    
    producer.push(text);
    producer.push(json_obj);
    producer.push(bytes);
    
    // Pop and verify order
    auto obj1 = consumer.pop(1000);
    ASSERT_TRUE(obj1);
    EXPECT_EQ(obj1->get_type(), ObjectType::Text);
    EXPECT_EQ(obj1->as<TextData>().text, "Test text");
    
    auto obj2 = consumer.pop(1000);
    ASSERT_TRUE(obj2);
    EXPECT_EQ(obj2->get_type(), ObjectType::Json);
    
    auto obj3 = consumer.pop(1000);
    ASSERT_TRUE(obj3);
    EXPECT_EQ(obj3->get_type(), ObjectType::Bytes);
    EXPECT_EQ(obj3->as<BytesData>().bytes.size(), 3);
}

// Test NumPy array transfer
TEST_F(RingBufferTest, NumpyArrayTransfer) {
    const size_t buffer_size = 10 * 1024 * 1024;  // 10MB
    
    SharedRingBufferProducer producer(shm_name_, buffer_size, true);
    SharedRingBufferConsumer consumer(shm_name_);
    attach_with_retry(consumer);
    
    // Create NumPy array
    NumpyArray arr;
    arr.shape = {100, 100, 3};
    arr.dtype = "<f4";
    arr.bytes.resize(100 * 100 * 3 * sizeof(float));
    
    float* data = arr.data_as<float>();
    for (size_t i = 0; i < 100 * 100 * 3; ++i) {
        data[i] = static_cast<float>(i);
    }
    
    producer.push(arr);
    
    auto obj = consumer.pop(1000);
    ASSERT_TRUE(obj);
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
    SharedRingBufferConsumer consumer(shm_name_);
    attach_with_retry(consumer);
    
    // Push and pop multiple times to wrap around
    for (int i = 0; i < 10; ++i) {
        TextData text("Message " + std::to_string(i));
        producer.push(text);
        
        auto obj = consumer.pop(1000);
        ASSERT_TRUE(obj);
        EXPECT_EQ(obj->as<TextData>().text, "Message " + std::to_string(i));
    }
}

// Test empty buffer
TEST_F(RingBufferTest, EmptyBuffer) {
    const size_t buffer_size = 1024 * 1024;
    
    SharedRingBufferProducer producer(shm_name_, buffer_size, true);
    SharedRingBufferConsumer consumer(shm_name_);
    attach_with_retry(consumer);
    
    // Try to pop from empty buffer with short timeout - should throw IPCException
    EXPECT_THROW({
        consumer.pop(100);  // 100ms timeout
    }, IPCException);
}

// Test concurrent producer-consumer
TEST_F(RingBufferTest, ConcurrentProducerConsumer) {
    const size_t buffer_size = 10 * 1024 * 1024;
    const int num_messages = 50;
    
    SharedRingBufferProducer producer(shm_name_, buffer_size, true);

    std::atomic<bool> consumer_ready{false};
    
    // Producer thread
    std::thread producer_thread([&]() {
        // Ensure at least one consumer is attached before producing.
        while (!consumer_ready.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        for (int i = 0; i < num_messages; ++i) {
            TextData text("Message " + std::to_string(i));
            try {
                producer.push(text, 0);
            } catch (...) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }

        producer.close();
    });
    
    // Consumer thread
    std::atomic<int> received_count{0};
    std::thread consumer_thread([&]() {
        SharedRingBufferConsumer consumer(shm_name_);
        attach_with_retry(consumer);
        consumer_ready = true;
        
        while (received_count < num_messages) {
            auto obj = consumer.pop(200);
            if (obj) {
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
    SharedRingBufferConsumer consumer(shm_name_);
    attach_with_retry(consumer);
    
    auto stats = consumer.get_stats();
    EXPECT_EQ(stats.total_data_bytes, buffer_size);
    EXPECT_TRUE(stats.is_empty);
    EXPECT_EQ(stats.used_bytes, 0);
    
    // Push an object
    TextData text("Test message");
    producer.push(text);
    
    stats = consumer.get_stats();
    EXPECT_FALSE(stats.is_empty);
    EXPECT_GT(stats.used_bytes, 0);
    
    // Pop the object
    auto obj = consumer.pop(1000);
    ASSERT_TRUE(obj);
    
    stats = consumer.get_stats();
    EXPECT_TRUE(stats.is_empty);
    // With MPMC semantics + possible contention, allow used_bytes to be 0 after pop.
    EXPECT_EQ(stats.used_bytes, 0);
}

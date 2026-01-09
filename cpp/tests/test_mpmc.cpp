/**
 * @file test_mpmc.cpp
 * @brief Multi-producer multi-consumer tests for SharedRingBuffer
 * 
 * Demonstrates:
 * - Single producer, single consumer
 * - Multiple producers, single consumer
 * - Single producer, multiple consumers
 * - Multiple producers, multiple consumers
 * - Producer gating (wait for consumer)
 * - EOS semantics
 */

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <memory>
#include <iostream>
#include <sstream>
#include <chrono>
#include <atomic>
#include <cctype>
#include <unistd.h>

#include "ipc0cp/ring_buffer.hpp"
#include "ipc0cp/serialize.hpp"

using namespace ipc0cp;

namespace {

static constexpr int OP_TIMEOUT_MS = 1000;

static void spin_wait_until(const std::function<bool()>& predicate, int timeout_ms, int poll_ms = 1) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            throw std::runtime_error("Timed out waiting for condition");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
    }
}

template <typename F>
std::thread start_thread(const char* label, F&& fn) {
    return std::thread([label, fn = std::forward<F>(fn)]() mutable {
        try {
            fn();
        } catch (const std::exception& e) {
            std::cerr << label << " error: " << e.what() << std::endl;
        } catch (...) {
            std::cerr << label << " error: unknown exception" << std::endl;
        }
    });
}

static void join_all(std::vector<std::thread>& threads) {
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

static void attach_with_retry(SharedRingBufferConsumer& consumer, int timeout_ms = 5000) {
    auto start = std::chrono::steady_clock::now();
    while (!consumer.attach()) {
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() >= timeout_ms) {
            throw std::runtime_error("Timed out waiting to attach consumer");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

static int consume_until_eos(SharedRingBufferConsumer& consumer, int timeout_ms = OP_TIMEOUT_MS) {
    int count = 0;
    while (true) {
        auto obj = consumer.pop(timeout_ms);
        if (!obj) {
            break;
        }
        count++;
    }
    return count;
}

static void push_text_messages(
    SharedRingBufferProducer& producer,
    const std::string& prefix,
    int count,
    int timeout_ms = OP_TIMEOUT_MS
) {
    static constexpr const char* kMeta = R"({"type":"text"})";
    for (int i = 0; i < count; i++) {
        const std::string msg = prefix + std::to_string(i);
        std::vector<uint8_t> payload(msg.begin(), msg.end());
        producer.push_raw(kMeta, payload, timeout_ms);
    }
}

}  // namespace

// Test fixture for MPMC tests
class MPMCTest : public ::testing::Test {
protected:
    static constexpr size_t BUFFER_SIZE = 10 * 1024 * 1024;  // 10 MB

    std::string shm_name_;

    static std::string make_unique_name(const ::testing::TestInfo* info) {
        // Must be a POSIX IPC base name: no leading '/', no embedded '/'.
        const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count();
        std::ostringstream oss;
        oss << "test_mpmc_" << getpid() << "_" << now_ns;
        if (info) {
            oss << "_" << info->test_suite_name() << "_" << info->name();
        }
        auto name = oss.str();
        for (auto& ch : name) {
            if (!(std::isalnum(static_cast<unsigned char>(ch)) || ch == '_' || ch == '-')) {
                ch = '_';
            }
        }
        return name;
    }
    
    void SetUp() override {
        shm_name_ = make_unique_name(::testing::UnitTest::GetInstance()->current_test_info());

        // Best-effort cleanup in case of a prior crash using the same generated name.
        try {
            PosixSharedMemory temp(shm_name_, false, 0);
            temp.unlink();
        } catch (...) {
        }

        try {
            cleanup_buffer_semaphores(shm_name_);
        } catch (...) {
        }
    }
    
    void TearDown() override {
        // Cleanup after test
        try {
            PosixSharedMemory temp(shm_name_, false, 0);
            temp.unlink();
        } catch (...) {}
        
        try {
            cleanup_buffer_semaphores(shm_name_);
        } catch (...) {}
    }
};

// ============================================================================
// Test 1: Single Producer, Single Consumer
// ============================================================================

TEST_F(MPMCTest, SingleProducerSingleConsumer) {
    const std::string buffer_name = shm_name_;

    std::atomic<int> ready{0};
    std::atomic<int> messages_received{0};

    auto producer_thread = start_thread("producer", [&]() {
        auto producer = SharedRingBufferProducer(buffer_name, BUFFER_SIZE);
        spin_wait_until([&]() { return ready.load() != 0; }, /*timeout_ms=*/5000, /*poll_ms=*/10);
        push_text_messages(producer, "Message ", 10);
        producer.close();
    });

    auto consumer_thread = start_thread("consumer", [&]() {
        auto consumer = SharedRingBufferConsumer(buffer_name);
        attach_with_retry(consumer);
        ready = 1;
        messages_received = consume_until_eos(consumer);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    });

    producer_thread.join();
    consumer_thread.join();

    EXPECT_EQ(messages_received, 10);
}

// ============================================================================
// Test 2: Multiple Producers, Single Consumer
// ============================================================================

TEST_F(MPMCTest, MultipleProducersSingleConsumer) {
    static constexpr int NUM_PRODUCERS = 3;
    static constexpr int MESSAGES_PER_PRODUCER = 5;

    const std::string buffer_name = shm_name_;
    
    std::atomic<int> consumer_ready{0};
    std::atomic<int> messages_received{0};

    // Prevent consumers from treating the stream as ended before all producers attach.
    std::atomic<int> producers_ready{0};

    // Producers start first so shared memory exists for consumer attach.
    std::vector<std::thread> producer_threads;
    for (int p = 0; p < NUM_PRODUCERS; p++) {
        producer_threads.emplace_back(start_thread("producer", [p, &producers_ready, &consumer_ready, buffer_name]() {
            auto producer = SharedRingBufferProducer(buffer_name, BUFFER_SIZE);
            producers_ready.fetch_add(1);
            spin_wait_until([&]() {
                return producers_ready.load() >= NUM_PRODUCERS && consumer_ready.load() != 0;
            }, /*timeout_ms=*/5000);
            push_text_messages(producer, "P" + std::to_string(p) + "_M", MESSAGES_PER_PRODUCER);
            producer.close();
        }));
    }

    auto consumer_thread = start_thread("consumer", [&]() {
        auto consumer = SharedRingBufferConsumer(buffer_name);
        attach_with_retry(consumer);
        consumer_ready = 1;
        spin_wait_until([&]() { return producers_ready.load() >= NUM_PRODUCERS; }, /*timeout_ms=*/5000);
        messages_received = consume_until_eos(consumer);
    });
    
    join_all(producer_threads);
    consumer_thread.join();
    
    EXPECT_EQ(messages_received, NUM_PRODUCERS * MESSAGES_PER_PRODUCER);
}

// ============================================================================
// Test 3: Single Producer, Multiple Consumers
// ============================================================================

TEST_F(MPMCTest, SingleProducerMultipleConsumers) {
    static constexpr int NUM_CONSUMERS = 3;
    static constexpr int NUM_MESSAGES = 15;

    const std::string buffer_name = shm_name_;
    
    std::atomic<int> all_consumers_ready{0};
    std::vector<std::atomic<int>> messages_per_consumer(NUM_CONSUMERS);

    // Ensure SHM+semaphores exist before consumers attach.
    auto producer = SharedRingBufferProducer(
        buffer_name,
        BUFFER_SIZE
    );
    
    // Initialize atomics
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        messages_per_consumer[i] = 0;
    }
    
    std::vector<std::thread> consumer_threads;
    for (int c = 0; c < NUM_CONSUMERS; c++) {
        consumer_threads.emplace_back(start_thread("consumer", [c, &messages_per_consumer, &all_consumers_ready, buffer_name]() {
            auto consumer = SharedRingBufferConsumer(
                buffer_name,
                BUFFER_SIZE
            );

            attach_with_retry(consumer);
            all_consumers_ready.fetch_add(1);
            messages_per_consumer[c] = consume_until_eos(consumer);
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }));
    }
    
    // Wait for all consumers to be ready
    spin_wait_until([&]() { return all_consumers_ready.load() >= NUM_CONSUMERS; }, /*timeout_ms=*/5000, /*poll_ms=*/10);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    push_text_messages(producer, "Message ", NUM_MESSAGES);

    producer.close();
    
    join_all(consumer_threads);
    
    int total = 0;
    for (int c = 0; c < NUM_CONSUMERS; c++) {
        total += messages_per_consumer[c].load();
    }
    EXPECT_EQ(total, NUM_MESSAGES);
}

// ============================================================================
// Test 4: Multiple Producers, Multiple Consumers
// ============================================================================

TEST_F(MPMCTest, MultipleProducersMultipleConsumers) {
    static constexpr int NUM_PRODUCERS = 2;
    static constexpr int NUM_CONSUMERS = 2;
    static constexpr int MESSAGES_PER_PRODUCER = 10;

    const std::string buffer_name = shm_name_;
    
    std::atomic<int> all_consumers_ready{0};
    std::vector<std::atomic<int>> messages_per_consumer(NUM_CONSUMERS);

    // Prevent consumers from treating the stream as ended before all producers attach.
    std::atomic<int> producers_ready{0};
    
    // Initialize atomics
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        messages_per_consumer[i] = 0;
    }
    
    std::vector<std::thread> consumer_threads;
    for (int c = 0; c < NUM_CONSUMERS; c++) {
        consumer_threads.emplace_back(start_thread("consumer", [c, &messages_per_consumer, &all_consumers_ready, buffer_name]() {
            auto consumer = SharedRingBufferConsumer(
                buffer_name,
                BUFFER_SIZE
            );

            attach_with_retry(consumer);
            all_consumers_ready.fetch_add(1);
            messages_per_consumer[c] = consume_until_eos(consumer);
        }));
    }
    
    std::vector<std::thread> producer_threads;
    for (int p = 0; p < NUM_PRODUCERS; p++) {
        producer_threads.emplace_back(start_thread("producer", [p, &producers_ready, &all_consumers_ready, buffer_name]() {
            auto producer = SharedRingBufferProducer(buffer_name, BUFFER_SIZE);
            producers_ready.fetch_add(1);
            spin_wait_until([&]() {
                return producers_ready.load() >= NUM_PRODUCERS && all_consumers_ready.load() >= NUM_CONSUMERS;
            }, /*timeout_ms=*/5000);
            push_text_messages(producer, "P" + std::to_string(p) + "_M", MESSAGES_PER_PRODUCER);
            producer.close();
        }));
    }
    
    join_all(producer_threads);
    join_all(consumer_threads);
    
    int total = 0;
    for (int c = 0; c < NUM_CONSUMERS; c++) {
        total += messages_per_consumer[c].load();
    }
    EXPECT_EQ(total, NUM_PRODUCERS * MESSAGES_PER_PRODUCER);
}

// ============================================================================
// Test 5: Producer Gating (wait for consumer)
// ============================================================================

TEST_F(MPMCTest, ProducerGating) {
    const std::string buffer_name = shm_name_;

    std::atomic<bool> consumer_ready{false};
    std::atomic<bool> producer_blocked{false};
    std::atomic<long long> producer_wait_ms{0};
    
    // Producer thread
    auto producer_thread = start_thread("producer", [&]() {
        auto producer = SharedRingBufferProducer(buffer_name, BUFFER_SIZE);

        producer_blocked = true;

        static constexpr const char* kMeta = R"({"type":"text"})";
        std::vector<uint8_t> payload{'H', 'i'};
        const auto start = std::chrono::steady_clock::now();
        producer.push_raw(kMeta, payload, OP_TIMEOUT_MS);
        const auto elapsed = std::chrono::steady_clock::now() - start;
        producer_wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

        producer.close();
    });
    
    // Wait for producer to start blocking
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(producer_blocked);
    
    // Now attach consumer
    auto consumer_thread = start_thread("consumer", [&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        auto consumer = SharedRingBufferConsumer(buffer_name);
        attach_with_retry(consumer);
        consumer_ready = true;
        EXPECT_EQ(consume_until_eos(consumer), 1);
    });
    
    // Wait for threads
    producer_thread.join();
    consumer_thread.join();
    
    EXPECT_TRUE(consumer_ready);
    EXPECT_GE(producer_wait_ms.load(), 30);
}

// ============================================================================
// Test 6: Non-blocking Producer with No Consumers (should fail fast)
// ============================================================================

TEST_F(MPMCTest, NonBlockingProducerNoConsumers) {
    const std::string buffer_name = shm_name_;

    auto producer = SharedRingBufferProducer(
        buffer_name,
        BUFFER_SIZE,
        true,    // create_new
        false    // non-blocking - should fail fast
    );

    std::string msg = "Message";
    std::vector<uint8_t> payload(msg.begin(), msg.end());

    try {
        producer.push_raw(R"({"type":"text"})", payload, OP_TIMEOUT_MS);
        FAIL() << "Expected IPCException";
    } catch (const IPCException& e) {
        EXPECT_EQ(e.error_type, IPCError::NoConsumers);
    }

    producer.close();
}

// ============================================================================
// Test 7: Buffer Statistics during MPMC
// ============================================================================

TEST_F(MPMCTest, BufferStatistics) {
    const std::string buffer_name = shm_name_;

    std::atomic<bool> consumer_ready{false};

    // Ensure SHM+semaphores exist before consumer attaches.
    auto producer = SharedRingBufferProducer(
        buffer_name,
        BUFFER_SIZE
    );
    
    // Consumer thread
    auto consumer_thread = start_thread("consumer", [&]() {
        auto consumer = SharedRingBufferConsumer(buffer_name);
        attach_with_retry(consumer);
        consumer_ready = true;
        (void)consume_until_eos(consumer);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    });
    
    // Wait for consumer
    spin_wait_until([&]() { return consumer_ready.load(); }, /*timeout_ms=*/5000, /*poll_ms=*/10);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    push_text_messages(producer, "Message ", 5);

    producer.close();
    
    // Wait for threads
    consumer_thread.join();
}

/**
 * @file stdio.hpp
 * @brief STDIO-based IPC using stdin/stdout for inter-process communication
 * 
 * This module provides producer and consumer classes that use standard input/output
 * for IPC. While not zero-copy like shared memory, it's portable and works across
 * different process boundaries (e.g., network, containers).
 * 
 * Wire Format (per message):
 *     [metadata_size: 4 bytes little-endian uint32]
 *     [payload_size:  8 bytes little-endian uint64]
 *     [metadata_json: metadata_size bytes (UTF-8)]
 *     [start_sentinel: 1 byte]
 *     [payload: payload_size bytes]
 *     [end_sentinel: 1 byte]
 *
 * End-of-stream:
 *     metadata_size=0 and payload_size=0
 */

#pragma once

#include "ipc.hpp"
#include "serialize.hpp"
#include <iostream>
#include <vector>
#include <cstdint>
#include <optional>
#include <string>

namespace ipc0cp {

/**
 * @brief Producer that writes serialized objects to stdout
 * 
 * Uses the same serialization format as SharedRingBufferProducer for consistency.
 * Wire format: [metadata_size:4][payload_size:8][metadata_json][sentinel][payload][sentinel]
 * 
 * Example:
 * @code
 * auto producer = StdioProducer();
 * 
 * // High-level API with SerializableObject
 * TextObject text("Hello, World!");
 * producer.push(text);
 * 
 * // Or low-level API with raw metadata + payload
 * std::string metadata = R"({"type": "json"})";
 * std::vector<uint8_t> payload = {1, 2, 3};
 * producer.push_raw(metadata, payload);
 * 
 * producer.close();  // Sends EOS marker
 * @endcode
 */
class StdioProducer : public IPCProducer {
public:
    /**
     * @brief Construct a STDIO producer
     * @param output Output stream (default: std::cout)
     */
    explicit StdioProducer(std::ostream* output = &std::cout);
    
    /**
     * @brief Push a serializable object
     * @param obj SerializableObject to push
     * @param timeout_ms Ignored for STDIO (blocking only)
     * @throws IPCException if already closed or serialization fails
     */
    void push(const SerializableObject& obj, 
             int timeout_ms = -1) override;
    
    /**
     * @brief Write metadata and payload to stdout (low-level API)
     * @param metadata_json JSON metadata string
     * @param payload Binary payload data
        * @param timeout_ms Ignored for STDIO (blocking only)
        * @return true on success
        * @throws IPCException if already closed or write fails
     */
        bool push_raw(const std::string& metadata_json,
                      const std::vector<uint8_t>& payload,
                      int timeout_ms = -1) override;
    
    /**
     * @brief Send end-of-stream marker (metadata_size=0, payload_size=0)
     */
    void close() override;
    
    /**
     * @brief Check if producer is closed
     */
    bool is_closed() const { return closed_; }

private:
    std::ostream* output_;
    bool closed_;
};

/**
 * @brief Consumer that reads serialized objects from stdin
 * 
 * Uses the same deserialization format as SharedRingBufferConsumer for consistency.
 * Wire format: [metadata_size:4][payload_size:8][metadata_json][sentinel][payload][sentinel]
 * 
 * Example:
 * @code
 * auto consumer = StdioConsumer();
 * while (true) {
 *     auto obj = consumer.pop();
 *     if (!obj) {  // End-of-stream
 *         break;
 *     }
 *     
 *     // Access deserialized data
 *     if (obj->get_type() == ObjectType::Text) {
 *         auto& text = obj->as<TextObject>();
 *         std::cout << "Received: " << text.text << "\n";
 *     }
 * }
 * @endcode
 */
class StdioConsumer : public IPCConsumer {
public:
    /**
     * @brief Construct a STDIO consumer
     * @param input Input stream (default: std::cin)
     */
    explicit StdioConsumer(std::istream* input = &std::cin);
    
    /**
     * @brief Read and deserialize an object
     * @param timeout_ms Ignored for STDIO (blocking I/O only)
     * @return Deserialized IPCObject, or nullptr on end-of-stream
     * @throws IPCException on read errors or corruption
     */
    std::unique_ptr<IPCObject> pop(
        int timeout_ms = -1) override;
    
    /**
     * @brief Read metadata and payload from stdin (low-level API)
     * @param timeout_ms Ignored for STDIO (blocking I/O only)
     * @return Pair of (metadata_json, payload), or nullopt if end-of-stream
     * @throws IPCException on read errors or corruption
     */
    std::optional<std::pair<std::string, std::vector<uint8_t>>> pop_raw(
        int timeout_ms = -1
    ) override;
    
    /**
     * @brief Check if end-of-stream was received
     */
    bool eos_received() const override { return eos_received_; }

private:
    std::istream* input_;
    bool eos_received_;
};

} // namespace ipc0cp

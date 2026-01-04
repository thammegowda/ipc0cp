/**
 * @file stdio.cpp
 * @brief Implementation of STDIO-based IPC
 */

#include "stdio.hpp"
#include <cstring>
#include <stdexcept>

namespace {

bool read_exact(std::istream& in, void* dst, size_t len) {
    in.read(reinterpret_cast<char*>(dst), static_cast<std::streamsize>(len));
    return static_cast<size_t>(in.gcount()) == len;
}

} // namespace

namespace ipc0cp {

// StdioProducer implementation

StdioProducer::StdioProducer(std::ostream* output)
    : output_(output), closed_(false) {
    if (!output_) {
        throw IPCException(
            IPCError::NotInitialized,
            "Output stream is null"
        );
    }
}

void StdioProducer::push(const SerializableObject& obj, 
                        int timeout_ms) {
    auto [metadata, payload] = obj.serialize();
    (void)timeout_ms;
    if (!push_raw(metadata, payload, timeout_ms)) {
        throw IPCException(
            IPCError::DeserializationFailed,
            "Failed to push object"
        );
    }
}

bool StdioProducer::push_raw(const std::string& metadata_json,
                             const std::vector<uint8_t>& payload,
                             int timeout_ms) {
    (void)timeout_ms;  // STDIO ignores timeout
    if (closed_) {
        throw IPCException(
            IPCError::NotInitialized,
            "Producer is closed"
        );
    }
    
    try {
        if (metadata_json.size() > MAX_METADATA_SIZE) {
            throw IPCException(
                IPCError::InvalidMetadata,
                "Metadata too large: " + std::to_string(metadata_json.size()) + " > " + std::to_string(MAX_METADATA_SIZE)
            );
        }

        const uint32_t metadata_size = static_cast<uint32_t>(metadata_json.size());
        const uint64_t payload_size = static_cast<uint64_t>(payload.size());

        uint8_t metadata_buf[4];
        write_le32(metadata_buf, metadata_size);
        output_->write(reinterpret_cast<const char*>(metadata_buf), sizeof(metadata_buf));

        uint8_t payload_buf[8];
        write_le64(payload_buf, payload_size);
        output_->write(reinterpret_cast<const char*>(payload_buf), sizeof(payload_buf));

        if (metadata_size > 0) {
            output_->write(metadata_json.data(), static_cast<std::streamsize>(metadata_size));
        }

        // Sentinels + payload
        const char sentinel = static_cast<char>(SENTINEL_BYTE);
        output_->write(&sentinel, 1);
        if (payload_size > 0) {
            output_->write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload_size));
        }
        output_->write(&sentinel, 1);
        output_->flush();
        
        if (!output_->good()) {
            throw IPCException(
                IPCError::InvalidMetadata,
                "Failed to write to output stream"
            );
        }
        
        return true;
        
    } catch (const IPCException&) {
        throw;
    } catch (const std::exception& e) {
        throw IPCException(
            IPCError::DeserializationFailed,
            std::string("Failed to write data: ") + e.what()
        );
    }
}

void StdioProducer::close() {
    if (!closed_) {
        try {
            // Send EOS marker (metadata_size=0, payload_size=0)
            uint8_t eos[12];
            write_le32(eos, 0);
            write_le64(eos + 4, 0);
            output_->write(reinterpret_cast<const char*>(eos), sizeof(eos));
            output_->flush();
        } catch (...) {
            // Ignore errors when sending EOS
        }
        closed_ = true;
    }
}

// StdioConsumer implementation

StdioConsumer::StdioConsumer(std::istream* input)
    : input_(input), eos_received_(false) {
    if (!input_) {
        throw IPCException(
            IPCError::NotInitialized,
            "Input stream is null"
        );
    }
}

std::unique_ptr<IPCObject> StdioConsumer::pop(
    int timeout_ms) {
    auto result = pop_raw(timeout_ms);
    if (!result) {
        return nullptr;  // End-of-stream
    }
    
    auto& [metadata_json, payload] = *result;
    
    // Deserialize using the serialization system
    try {
        auto obj_ptr = deserialize(metadata_json, payload);
        auto ipc_obj = std::make_unique<IPCObject>(std::move(obj_ptr));
        return ipc_obj;
    } catch (const std::exception& e) {
        throw IPCException(
            IPCError::DeserializationFailed,
            std::string("Failed to deserialize object: ") + e.what()
        );
    }
}


std::optional<std::pair<std::string, std::vector<uint8_t>>> 
StdioConsumer::pop_raw(int timeout_ms) {
    (void)timeout_ms;  // Timeout ignored for STDIO
    
    if (eos_received_) {
        return std::nullopt;
    }
    
    try {
        // Read metadata_size (4 bytes little-endian)
        uint8_t meta_size_b[4];
        if (!read_exact(*input_, meta_size_b, sizeof(meta_size_b))) {
            if (input_->eof() && input_->gcount() == 0) {
                // EOF without EOS marker
                eos_received_ = true;
                return std::nullopt;
            }
            throw IPCException(
                IPCError::CorruptPayload,
                "Incomplete metadata_size field: got " + std::to_string(input_->gcount()) + " bytes"
            );
        }

        // Read payload_size (8 bytes little-endian)
        uint8_t payload_size_b[8];
        if (!read_exact(*input_, payload_size_b, sizeof(payload_size_b))) {
            throw IPCException(
                IPCError::CorruptPayload,
                "Incomplete payload_size field: got " + std::to_string(input_->gcount()) + " bytes"
            );
        }

        const uint32_t metadata_size = read_le32(meta_size_b);
        const uint64_t payload_size = read_le64(payload_size_b);

        // EOS marker
        if (metadata_size == 0 && payload_size == 0) {
            eos_received_ = true;
            return std::nullopt;
        }

        if (metadata_size > MAX_METADATA_SIZE) {
            throw IPCException(
                IPCError::CorruptPayload,
                "Invalid metadata_size: " + std::to_string(metadata_size)
            );
        }

        std::string metadata_json;
        metadata_json.resize(metadata_size);
        if (metadata_size > 0) {
            if (!read_exact(*input_, metadata_json.data(), metadata_size)) {
                throw IPCException(
                    IPCError::CorruptPayload,
                    "Incomplete metadata: expected " + std::to_string(metadata_size) +
                        " bytes, got " + std::to_string(input_->gcount())
                );
            }
        }

        // Start sentinel
        char start_sentinel;
        if (!read_exact(*input_, &start_sentinel, 1)) {
            throw IPCException(
                IPCError::CorruptPayload,
                "Incomplete start sentinel"
            );
        }
        if (static_cast<uint8_t>(start_sentinel) != SENTINEL_BYTE) {
            throw IPCException(
                IPCError::CorruptPayload,
                "Invalid start sentinel"
            );
        }

        std::vector<uint8_t> payload;
        payload.resize(static_cast<size_t>(payload_size));
        if (payload_size > 0) {
            if (!read_exact(*input_, payload.data(), static_cast<size_t>(payload_size))) {
                throw IPCException(
                    IPCError::CorruptPayload,
                    "Incomplete payload: expected " + std::to_string(payload_size) +
                        " bytes, got " + std::to_string(input_->gcount())
                );
            }
        }

        // End sentinel
        char end_sentinel;
        if (!read_exact(*input_, &end_sentinel, 1)) {
            throw IPCException(
                IPCError::CorruptPayload,
                "Incomplete end sentinel"
            );
        }
        if (static_cast<uint8_t>(end_sentinel) != SENTINEL_BYTE) {
            throw IPCException(
                IPCError::CorruptPayload,
                "Invalid end sentinel"
            );
        }

        return std::make_pair(std::move(metadata_json), std::move(payload));
        
    } catch (const IPCException&) {
        throw;
    } catch (const std::exception& e) {
        throw IPCException(
            IPCError::DeserializationFailed,
            std::string("Failed to read data: ") + e.what()
        );
    }
}

} // namespace ipc0cp


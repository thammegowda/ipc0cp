/**
 * @file stdio.cpp
 * @brief Implementation of STDIO-based IPC
 */

#include "stdio.hpp"
#include <cstring>
#include <stdexcept>
#include <nlohmann/json.hpp>

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

bool StdioProducer::push(const SerializableObject& obj) {
    auto [metadata, payload] = obj.serialize();
    return push_raw(metadata, payload);
}

bool StdioProducer::push_raw(const std::string& metadata_json, const std::vector<uint8_t>& payload) {
    if (closed_) {
        throw IPCException(
            IPCError::NotInitialized,
            "Producer is closed"
        );
    }
    
    try {
        // Combine metadata and payload
        std::vector<uint8_t> combined;
        combined.insert(combined.end(), metadata_json.begin(), metadata_json.end());
        combined.insert(combined.end(), payload.begin(), payload.end());
        
        // Write length (8 bytes, little-endian)
        uint64_t length = combined.size();
        output_->write(reinterpret_cast<const char*>(&length), sizeof(length));
        
        // Write combined data
        output_->write(reinterpret_cast<const char*>(combined.data()), combined.size());
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
            // Send EOS marker (length=0)
            uint64_t length = 0;
            output_->write(reinterpret_cast<const char*>(&length), sizeof(length));
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

std::optional<IPCObject> StdioConsumer::pop(int timeout_ms) {
    auto result = pop_raw(timeout_ms);
    if (!result) {
        return std::nullopt;
    }
    
    auto& [metadata_json, payload] = *result;
    
    // Deserialize using the serialization system
    try {
        auto obj_ptr = deserialize(metadata_json, payload);
        IPCObject ipc_obj(std::move(obj_ptr));
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
        // Read length (8 bytes, little-endian)
        uint64_t length;
        input_->read(reinterpret_cast<char*>(&length), sizeof(length));
        
        if (input_->eof() && input_->gcount() == 0) {
            // EOF without EOS marker
            eos_received_ = true;
            return std::nullopt;
        }
        
        if (input_->gcount() != sizeof(length)) {
            throw IPCException(
                IPCError::CorruptPayload,
                "Incomplete length field: got " + std::to_string(input_->gcount()) + " bytes"
            );
        }
        
        // Check for EOS marker
        if (length == 0) {
            eos_received_ = true;
            return std::nullopt;
        }
        
        // Read combined metadata + payload
        std::vector<uint8_t> combined(length);
        input_->read(reinterpret_cast<char*>(combined.data()), length);
        
        if (static_cast<uint64_t>(input_->gcount()) != length) {
            throw IPCException(
                IPCError::CorruptPayload,
                "Incomplete payload: expected " + std::to_string(length) + 
                " bytes, got " + std::to_string(input_->gcount())
            );
        }
        
        // Split metadata and payload
        // The combined format is: metadata_json (UTF-8 text) + payload (binary)
        // Find the end of JSON by parsing
        std::string combined_str(combined.begin(), combined.end());
        
        // Parse JSON to find its end
        try {
            auto json_obj = nlohmann::json::parse(combined_str.begin(), combined_str.end(), nullptr, false);
            if (json_obj.is_discarded()) {
                throw IPCException(
                    IPCError::InvalidMetadata,
                    "Failed to parse metadata JSON"
                );
            }
            
            // Find where JSON ends (simple heuristic: find closing brace/bracket at top level)
            size_t json_end = 0;
            int depth = 0;
            bool in_string = false;
            bool escape = false;
            
            for (size_t i = 0; i < combined.size(); ++i) {
                char c = combined[i];
                
                if (escape) {
                    escape = false;
                    continue;
                }
                
                if (c == '\\') {
                    escape = true;
                    continue;
                }
                
                if (c == '"' && !escape) {
                    in_string = !in_string;
                    continue;
                }
                
                if (in_string) continue;
                
                if (c == '{' || c == '[') depth++;
                if (c == '}' || c == ']') {
                    depth--;
                    if (depth == 0) {
                        json_end = i + 1;
                        break;
                    }
                }
            }
            
            if (json_end == 0) {
                throw IPCException(
                    IPCError::InvalidMetadata,
                    "Could not find end of JSON metadata"
                );
            }
            
            std::string metadata_json(combined.begin(), combined.begin() + json_end);
            std::vector<uint8_t> payload(combined.begin() + json_end, combined.end());
            
            return std::make_pair(metadata_json, payload);
            
        } catch (const IPCException&) {
            throw;
        } catch (const std::exception& e) {
            throw IPCException(
                IPCError::InvalidMetadata,
                std::string("Failed to parse metadata: ") + e.what()
            );
        }
        
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


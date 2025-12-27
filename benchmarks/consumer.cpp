#include "ipc0cp/ring_buffer.hpp"
#include "ipc0cp/stdio.hpp"

#include <chrono>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>

using namespace ipc0cp;

namespace {

struct Config {
    bool use_stdio = false;
    bool use_shm = false;
    std::string shm_name;

    bool quiet = false;
    size_t shm_total_data_bytes = 2ULL * 1024 * 1024 * 1024;

    std::chrono::seconds attach_timeout{5};
};

bool parse_args(int argc, char** argv, Config& config) {
    for (int i = 1; i < argc; ++i) {
        std::string_view arg(argv[i]);
        if (arg == "--stdio") {
            config.use_stdio = true;
        } else if (arg == "--shm" && i + 1 < argc) {
            config.use_shm = true;
            config.shm_name = std::string(argv[++i]);
        } else if (arg == "--quiet") {
            config.quiet = true;
        }
    }

    if (config.use_stdio == config.use_shm) {
        std::cerr << "Exactly one of --stdio or --shm <name> is required" << std::endl;
        return false;
    }

    if (config.use_shm && config.shm_name.empty()) {
        std::cerr << "Missing shm name for --shm" << std::endl;
        return false;
    }

    return true;
}

size_t payload_size_from_obj(const RingBufferObject& obj) {
    if (!obj.data) {
        return 0;
    }
    if (auto* bytes_obj = dynamic_cast<BytesData*>(obj.data.get())) {
        return bytes_obj->bytes.size();
    }
    auto serialized = obj.data->serialize();
    return serialized.payload.size();
}

}  // namespace

int main(int argc, char** argv) {
    Config config;
    if (!parse_args(argc, argv, config)) {
        return 2;
    }

    size_t bytes_received = 0;
    size_t messages = 0;
    auto start = std::chrono::steady_clock::now();

    if (config.use_stdio) {
        StdioConsumer consumer;
        while (true) {
            auto result = consumer.pop_raw();
            if (!result) {
                break;
            }
            bytes_received += result->second.size();
            ++messages;
        }
    } else {
        // Allow producer a short time to create the shared memory
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        SharedRingBufferConsumer consumer(
            config.shm_name,
            config.shm_total_data_bytes,
            true,
            false,
            true
        );

        const auto attach_deadline = std::chrono::steady_clock::now() + config.attach_timeout;
        while (true) {
            if (consumer.attach()) {
                break;
            }

            const auto err = consumer.get_last_error();
            if (err == IPCError::ShmNotFound && std::chrono::steady_clock::now() < attach_deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            std::cerr << "Failed to attach to shared memory: " << errorToString(err) << std::endl;
            return 1;
        }

        try {
            while (true) {
                auto obj = consumer.pop();
                if (!obj) {
                    break;
                }
                bytes_received += payload_size_from_obj(*obj);
                ++messages;
            }
        } catch (...) {
            consumer.close();
            throw;
        }

        consumer.close();
    }

    const auto elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - start
    ).count();

    const double throughput = elapsed > 0
        ? (static_cast<double>(bytes_received) / (1024.0 * 1024.0)) / elapsed
        : 0.0;

    std::cerr << "\nConsumer Stats:\n";
    std::cerr << "  Bytes received (payload only): " << bytes_received << "\n";
    std::cerr << "  Messages received: " << messages << "\n";
    std::cerr << "  Elapsed time: " << elapsed << " seconds\n";
    std::cerr << "  Throughput: " << throughput << " MB/s\n";

    return 0;
}

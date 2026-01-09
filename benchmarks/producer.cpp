#include "ipc0cp/ring_buffer.hpp"
#include "ipc0cp/stdio.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
#include <vector>

using namespace ipc0cp;

namespace {

struct Config {
    bool use_stdio = false;
    bool use_shm = false;
    std::string shm_name;

    double duration = 60.0;
    size_t min_size = 512 * 1024;
    size_t max_size = 5 * 1024 * 1024;
    bool quiet = false;

    size_t shm_total_data_bytes = 2ULL * 1024 * 1024 * 1024;
};

bool parse_args(int argc, char** argv, Config& config) {
    for (int i = 1; i < argc; ++i) {
        std::string_view arg(argv[i]);
        if (arg == "--stdio") {
            config.use_stdio = true;
        } else if (arg == "--shm" && i + 1 < argc) {
            config.use_shm = true;
            config.shm_name = std::string(argv[++i]);
        } else if (arg == "--duration" && i + 1 < argc) {
            config.duration = std::stod(argv[++i]);
        } else if (arg == "--min-size" && i + 1 < argc) {
            config.min_size = std::stoull(argv[++i]);
        } else if (arg == "--max-size" && i + 1 < argc) {
            config.max_size = std::stoull(argv[++i]);
        } else if (arg == "--quiet") {
            config.quiet = true;
        }
    }

    if (config.min_size > config.max_size) {
        std::swap(config.min_size, config.max_size);
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

void fill_random(std::vector<uint8_t>& payload, std::mt19937_64& rng) {
    size_t filled = 0;
    const size_t payload_size = payload.size();
    while (filled < payload_size) {
        uint64_t value = rng();
        size_t copy = std::min(payload_size - filled, sizeof(value));
        std::memcpy(payload.data() + filled, &value, copy);
        filled += copy;
    }
}

}  // namespace

int main(int argc, char** argv) {
    Config config;
    if (!parse_args(argc, argv, config)) {
        return 2;
    }

    std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<size_t> size_dist(config.min_size, config.max_size);

    const std::string metadata = R"({"type":"bytes"})";
    std::vector<uint8_t> payload;
    payload.reserve(config.max_size);

    size_t bytes_sent = 0;
    size_t messages = 0;
    auto start = std::chrono::steady_clock::now();

    if (config.use_stdio) {
        StdioProducer producer;

        while (true) {
            auto elapsed = std::chrono::duration<double>(
                std::chrono::steady_clock::now() - start
            ).count();
            if (elapsed >= config.duration) {
                break;
            }

            const size_t payload_size = size_dist(rng);
            payload.resize(payload_size);
            fill_random(payload, rng);

            producer.push_raw(metadata, payload);
            bytes_sent += payload_size;
            ++messages;
        }

        producer.close();
    } else {
        SharedRingBufferProducer producer(
            config.shm_name,
            config.shm_total_data_bytes,
            true
        );

        while (true) {
            auto elapsed = std::chrono::duration<double>(
                std::chrono::steady_clock::now() - start
            ).count();
            if (elapsed >= config.duration) {
                break;
            }

            const size_t payload_size = size_dist(rng);
            payload.resize(payload_size);
            fill_random(payload, rng);

            if (producer.push_raw(metadata, payload, 5000)) {
                bytes_sent += payload_size;
                ++messages;
            }
        }

        producer.close();
    }

    const auto elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - start
    ).count();

    const double throughput = elapsed > 0
        ? (static_cast<double>(bytes_sent) / (1024.0 * 1024.0)) / elapsed
        : 0.0;

    std::cerr << "\nProducer Stats:\n";
    std::cerr << "  Bytes sent (payload only): " << bytes_sent << "\n";
    std::cerr << "  Messages sent: " << messages << "\n";
    std::cerr << "  Elapsed time: " << elapsed << " seconds\n";
    std::cerr << "  Throughput: " << throughput << " MB/s\n";

    return 0;
}

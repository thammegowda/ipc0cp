#include "ipc0cp/ring_buffer.hpp"
#include <iostream>
#include <string>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <thread>

using namespace ipc0cp;

void printObject(const IPCObject& obj) {
    std::cout << "Received object:" << std::endl;
    std::cout << "  Type: " << objectTypeToString(obj.get_type()) << std::endl;
    
    // Print metadata attributes
    std::cout << "  Metadata:" << std::endl;
    for (const auto& [key, value] : obj.raw_metadata) {
        std::cout << "    " << key << ": " << value << std::endl;
    }
    
    // Print payload based on type
    switch (obj.get_type()) {
        case ObjectType::Text: {
            auto& text = obj.as<TextData>();
            std::cout << "  Content: " << text.text << std::endl;
            break;
        }
        case ObjectType::Json: {
            auto& json = obj.as<JsonData>();
            std::cout << "  JSON: " << json.text << std::endl;
            break;
        }
        case ObjectType::Bytes: {
            auto& bytes = obj.as<BytesData>();
            std::cout << "  Binary data: " << bytes.bytes.size() << " bytes" << std::endl;
            break;
        }
        case ObjectType::NumpyArray: {
            auto& arr = obj.as<NumpyArray>();
            std::cout << "  NumPy array:" << std::endl;
            std::cout << "    dtype: " << arr.dtype << std::endl;
            std::cout << "    shape: [";
            for (size_t i = 0; i < arr.shape.size(); ++i) {
                if (i > 0) std::cout << ", ";
                std::cout << arr.shape[i];
            }
            std::cout << "]" << std::endl;
            std::cout << "    elements: " << arr.element_count() << std::endl;
            std::cout << "    data size: " << arr.bytes.size() << " bytes" << std::endl;
            
            // Print first few elements for float32
            if (arr.dtype == "<f4" && arr.element_count() > 0) {
                const float* data = arr.data_as<float>();
                std::cout << "    first values: [";
                for (size_t i = 0; i < std::min(size_t(5), arr.element_count()); ++i) {
                    if (i > 0) std::cout << ", ";
                    std::cout << std::fixed << std::setprecision(3) << data[i];
                }
                std::cout << " ...]" << std::endl;
            }
            break;
        }
        case ObjectType::Image: {
            auto& img = obj.as<ImageData>();
            std::cout << "  Image:" << std::endl;
            std::cout << "    mode: " << img.mode << std::endl;
            std::cout << "    size: " << img.width << "x" << img.height << std::endl;
            std::cout << "    PNG data: " << img.bytes.size() << " bytes" << std::endl;
            break;
        }
        default:
            std::cout << "  Unknown type" << std::endl;
    }
    std::cout << std::endl;
}

int main(int argc, char* argv[]) {
    std::string shm_name = "test_py_cpp_ipc";
    size_t buffer_size = 50 * 1024 * 1024;  // 50 MB
    
    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-s" || arg == "--shm") {
            if (i + 1 < argc) {
                shm_name = argv[++i];
            }
        } else if (arg == "--buffer-size") {
            if (i + 1 < argc) {
                buffer_size = std::stoull(argv[++i]) * 1024 * 1024;
            }
        }
    }
    
    std::cout << "C++ Consumer starting..." << std::endl;
    std::cout << "Shared memory name: " << shm_name << std::endl;
    std::cout << "Buffer size: " << (buffer_size / (1024 * 1024)) << " MB" << std::endl;
    
    // Create consumer
    auto consumer = SharedRingBufferConsumer(shm_name, buffer_size, true);
    
    // Attach to shared memory
    std::cout << "Waiting for shared memory to be created by producer..." << std::endl;
    
    // Retry attachment for up to 10 seconds
    auto attach_start = std::chrono::steady_clock::now();
    bool attached = false;
    
    while (!attached) {
        if (consumer.attach()) {
            attached = true;
            break;
        }
        
        auto elapsed = std::chrono::steady_clock::now() - attach_start;
        if (elapsed > std::chrono::seconds(10)) {
            std::cerr << "Failed to attach to shared memory after 10 seconds" << std::endl;
            return 1;
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    std::cout << "Successfully attached to shared memory!" << std::endl;
    std::cout << "Waiting for objects...\n" << std::endl;
    
    // Consume objects
    int count = 0;
    auto start_time = std::chrono::steady_clock::now();
    
    while (true) {
        auto obj = consumer.pop(static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::seconds(5)).count()));
        
        if (!obj) {
            std::cout << "No more objects (timeout or empty)" << std::endl;
            break;
        }
        
        count++;
        printObject(*obj);
    }
    
    auto elapsed = std::chrono::steady_clock::now() - start_time;
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    
    std::cout << "Consumed " << count << " objects in " << elapsed_ms << " ms" << std::endl;
    if (count > 0) {
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Average: " << (static_cast<double>(elapsed_ms) / count) << " ms/object" << std::endl;
    }
    
    // Print final stats
    auto stats = consumer.get_stats();
    std::cout << "\nFinal buffer stats:" << std::endl;
    std::cout << "  Write pos: " << stats.write_pos << std::endl;
    std::cout << "  Read pos: " << stats.read_pos << std::endl;
    std::cout << "  Available: " << stats.available_bytes << " bytes" << std::endl;
    std::cout << "  Used: " << stats.used_bytes << " bytes" << std::endl;
    std::cout << "  Empty: " << (stats.is_empty ? "yes" : "no") << std::endl;
    
    return 0;
}

#include "ipc.hpp"

namespace ipc0cp {

SharedMemoryIPC::SharedMemoryIPC(const std::string& name, size_t size)
    : name_(name), size_(size) {
    // Implementation placeholder for shared memory initialization
}

SharedMemoryIPC::~SharedMemoryIPC() {
    // Implementation placeholder for cleanup
}

} // namespace ipc0cp

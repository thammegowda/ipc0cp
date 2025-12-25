#ifndef IPC0CP_IPC_HPP
#define IPC0CP_IPC_HPP

#include <string>
#include <cstddef>

namespace ipc0cp {

/**
 * @brief Base class for shared memory IPC operations
 * 
 * This class provides the foundation for zero-copy inter-process
 * communication using shared memory.
 */
class SharedMemoryIPC {
public:
    /**
     * @brief Constructor
     * @param name Shared memory segment name
     * @param size Size of the shared memory segment in bytes
     */
    SharedMemoryIPC(const std::string& name, size_t size);
    
    /**
     * @brief Destructor
     */
    virtual ~SharedMemoryIPC();
    
    /**
     * @brief Get the name of the shared memory segment
     * @return The segment name
     */
    const std::string& getName() const { return name_; }
    
    /**
     * @brief Get the size of the shared memory segment
     * @return The segment size in bytes
     */
    size_t getSize() const { return size_; }

protected:
    std::string name_;
    size_t size_;
};

} // namespace ipc0cp

#endif // IPC0CP_IPC_HPP

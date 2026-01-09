/**
 * @file posix_sync.hpp
 * @brief POSIX IPC utilities for shared memory and synchronization
 * 
 * Provides:
 * - PosixSharedMemory: wrapper around mmap-backed shared memory
 * - PosixCondition: condition variable emulation using semaphores
 * - Semaphore helpers for buffer synchronization
 * - Name normalization for consistent IPC naming
 */

#pragma once

#include <string>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <semaphore.h>
#include <sys/mman.h>
#include <chrono>

namespace ipc0cp {

/**
 * @brief Normalize an IPC object base name
 * 
 * Removes leading '/' and ensures no embedded '/' characters.
 * Returns the canonical base name.
 * 
 * @param name Input name (may have leading '/')
 * @return Normalized base name without leading '/'
 * @throws std::invalid_argument if name is empty or contains embedded '/'
 */
std::string normalize_ipc_base_name(const std::string& name);

/**
 * @brief Convert a base name to a POSIX IPC name
 * 
 * Adds a leading '/' to create a valid POSIX named object name.
 * 
 * @param base_name Normalized base name
 * @return POSIX IPC name with leading '/'
 */
std::string posix_name(const std::string& base_name);

/**
 * @brief POSIX shared memory segment backed by mmap
 * 
 * Wraps POSIX shm_open() + mmap for cross-process shared memory.
 * Avoids multiprocessing.shared_memory resource_tracker issues.
 */
class PosixSharedMemory {
public:
    /**
     * @brief Construct and map shared memory
     * 
     * @param name Base name (without leading '/')
     * @param create If true, create new segment; if false, attach to existing
     * @param size Size of segment (required if create=true)
     * @param mode Permission mode (default 0o600)
     * @throws std::exception on failure
     */
    PosixSharedMemory(
        const std::string& name,
        bool create,
        size_t size = 0,
        int mode = 0600
    );
    
    ~PosixSharedMemory();
    
    // Disable copy, allow move
    PosixSharedMemory(const PosixSharedMemory&) = delete;
    PosixSharedMemory& operator=(const PosixSharedMemory&) = delete;
    PosixSharedMemory(PosixSharedMemory&& other) noexcept;
    PosixSharedMemory& operator=(PosixSharedMemory&& other) noexcept;
    
    /**
     * @brief Get mutable buffer pointer
     */
    void* data() { return mmap_ptr_; }
    
    /**
     * @brief Get const buffer pointer
     */
    const void* data() const { return mmap_ptr_; }
    
    /**
     * @brief Get size in bytes
     */
    size_t size() const { return size_; }
    
    /**
     * @brief Get normalized name
     */
    const std::string& name() const { return name_; }
    
    /**
     * @brief Close the mapping (called in destructor)
     */
    void close();
    
    /**
     * @brief Unlink the shared memory (remove from system)
     * 
     * Call this from the last process to clean up the resource.
     * Does not close the mapping.
     */
    void unlink();

private:
    std::string name_;         ///< Normalized base name
    std::string posix_name_;   ///< POSIX name with leading '/'
    int shm_fd_ = -1;          ///< Shared memory file descriptor
    void* mmap_ptr_ = nullptr; ///< mmap address
    size_t size_ = 0;          ///< Size in bytes
};

/**
 * @brief Condition variable emulation using POSIX semaphores
 * 
 * Uses two semaphores to emulate a condition variable:
 * - mutex_sem: mutual exclusion (1 = unlocked, 0 = locked)
 * - wait_sem: condition wait (0 = waiting, >0 = signaled)
 * 
 * Only supports one waiter at a time (acceptable for our use case).
 */
class PosixCondition {
public:
    /**
     * @brief Construct condition with base name
     * 
     * Creates two named semaphores: {base_name}_lock and {base_name}_cond
     * 
     * @param base_name Base name for semaphores
     * @param create If true, create new semaphores; if false, attach to existing
     */
    PosixCondition(
        const std::string& base_name,
        bool create
    );
    
    ~PosixCondition();
    
    // Disable copy, allow move
    PosixCondition(const PosixCondition&) = delete;
    PosixCondition& operator=(const PosixCondition&) = delete;
    PosixCondition(PosixCondition&& other) noexcept;
    PosixCondition& operator=(PosixCondition&& other) noexcept;
    
    /**
     * @brief Acquire the lock (blocks until available)
     */
    void lock();
    
    /**
     * @brief Release the lock
     */
    void unlock();
    
    /**
     * @brief Wait for signal (releases lock, waits, reacquires lock)
     * 
     * @param timeout_ms Timeout in milliseconds (negative = infinite)
     * @return true if signaled, false if timeout
     */
    bool wait(int timeout_ms = -1);
    
    /**
     * @brief Signal all waiters (wakes up wait())
     */
    void notify_all();
    
    /**
     * @brief Get the base name
     */
    const std::string& base_name() const { return base_name_; }

private:
    std::string base_name_;
    sem_t* mutex_sem_ = nullptr;  ///< Mutual exclusion semaphore
    sem_t* wait_sem_ = nullptr;   ///< Condition wait semaphore
    bool create_;                 ///< Whether we created these semaphores
};

/**
 * @brief Get or create buffer synchronization semaphores
 * 
 * Returns a PosixCondition for the buffer.
 * 
 * @param buffer_name Base name for buffer
 * @param create If true, create new; if false, attach to existing
 * @return Unique pointer to PosixCondition
 */
std::unique_ptr<PosixCondition> get_buffer_semaphores(
    const std::string& buffer_name,
    bool create
);

/**
 * @brief Clean up (unlink) buffer synchronization semaphores
 * 
 * Called by consumer as part of cleanup when auto_unlink=true.
 * 
 * @param buffer_name Base name for buffer
 */
void cleanup_buffer_semaphores(const std::string& buffer_name);

} // namespace ipc0cp

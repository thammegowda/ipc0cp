#include "posix_sync.hpp"
#include "logger.hpp"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <semaphore.h>
#include <stdexcept>
#include <sstream>
#include <cerrno>
#include <cstring>

namespace ipc0cp {

// Name normalization
std::string normalize_ipc_base_name(const std::string& name) {
    if (name.empty()) {
        throw std::invalid_argument("IPC name must not be empty");
    }
    
    std::string base = name;
    // Remove leading '/'
    if (!base.empty() && base[0] == '/') {
        base = base.substr(1);
    }
    
    if (base.empty()) {
        throw std::invalid_argument("IPC name must not be empty");
    }
    
    // Forbid embedded '/'
    if (base.find('/') != std::string::npos) {
        throw std::invalid_argument("IPC name must not contain '/' characters");
    }
    
    return base;
}

std::string posix_name(const std::string& base_name) {
    const auto& normalized = normalize_ipc_base_name(base_name);
    return "/" + normalized;
}

// PosixSharedMemory implementation

PosixSharedMemory::PosixSharedMemory(
    const std::string& name,
    bool create,
    size_t size,
    int mode
)
    : name_(normalize_ipc_base_name(name))
    , posix_name_(posix_name(name_))
    , size_(size)
{
    int flags = O_RDWR;
    if (create) {
        flags |= (O_CREAT | O_EXCL);
        if (size == 0 || size > 1024UL * 1024UL * 1024UL * 100UL) {
            throw std::invalid_argument("size must be positive and <= 100GB when create=true");
        }
    }
    
    // Open or create shared memory
    shm_fd_ = shm_open(posix_name_.c_str(), flags, mode);
    if (shm_fd_ < 0) {
        int err = errno;
        std::ostringstream oss;
        oss << "shm_open(" << posix_name_ << ") failed: " << strerror(err);
        if (create && err == EEXIST) {
            throw std::runtime_error(oss.str());
        } else if (!create && err == ENOENT) {
            throw std::runtime_error(oss.str());
        } else {
            throw std::runtime_error(oss.str());
        }
    }
    
    // If creating, set size
    if (create) {
        if (ftruncate(shm_fd_, static_cast<off_t>(size_)) < 0) {
            int err = errno;
            std::ostringstream oss;
            oss << "ftruncate failed: " << strerror(err);
            ::close(shm_fd_);
            shm_unlink(posix_name_.c_str());
            throw std::runtime_error(oss.str());
        }
    } else {
        // Get size from existing shared memory
        struct stat sb;
        if (fstat(shm_fd_, &sb) < 0) {
            int err = errno;
            std::ostringstream oss;
            oss << "fstat failed: " << strerror(err);
            ::close(shm_fd_);
            throw std::runtime_error(oss.str());
        }
        size_ = sb.st_size;
    }
    
    // Map into address space
    mmap_ptr_ = mmap(nullptr, size_, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
    if (mmap_ptr_ == MAP_FAILED) {
        int err = errno;
        std::ostringstream oss;
        oss << "mmap failed: " << strerror(err);
        ::close(shm_fd_);
        if (create) {
            shm_unlink(posix_name_.c_str());
        }
        throw std::runtime_error(oss.str());
    }
    
    // Close FD; mmap keeps the mapping alive
    ::close(shm_fd_);
    shm_fd_ = -1;
}

PosixSharedMemory::~PosixSharedMemory() {
    close();
}

PosixSharedMemory::PosixSharedMemory(PosixSharedMemory&& other) noexcept
    : name_(std::move(other.name_))
    , posix_name_(std::move(other.posix_name_))
    , shm_fd_(other.shm_fd_)
    , mmap_ptr_(other.mmap_ptr_)
    , size_(other.size_)
{
    other.shm_fd_ = -1;
    other.mmap_ptr_ = nullptr;
    other.size_ = 0;
}

PosixSharedMemory& PosixSharedMemory::operator=(PosixSharedMemory&& other) noexcept {
    if (this != &other) {
        close();
        name_ = std::move(other.name_);
        posix_name_ = std::move(other.posix_name_);
        shm_fd_ = other.shm_fd_;
        mmap_ptr_ = other.mmap_ptr_;
        size_ = other.size_;
        other.shm_fd_ = -1;
        other.mmap_ptr_ = nullptr;
        other.size_ = 0;
    }
    return *this;
}

void PosixSharedMemory::close() {
    if (mmap_ptr_ != nullptr && mmap_ptr_ != MAP_FAILED) {
        munmap(mmap_ptr_, size_);
        mmap_ptr_ = nullptr;
    }
    if (shm_fd_ >= 0) {
        ::close(shm_fd_);
        shm_fd_ = -1;
    }
}

void PosixSharedMemory::unlink() {
    shm_unlink(posix_name_.c_str());
}

// PosixCondition implementation

PosixCondition::PosixCondition(
    const std::string& base_name,
    bool create
)
    : base_name_(normalize_ipc_base_name(base_name))
    , create_(create)
{
    // Keep naming consistent with Python implementation for cross-language IPC.
    // Python uses: /{base}_mutex and /{base}_wait
    const auto& lock_name = posix_name(base_name_ + "_mutex");
    const auto& cond_name = posix_name(base_name_ + "_wait");
    
    int flags = 0;
    if (create) {
        flags = (O_CREAT | O_EXCL);
    }
    
    try {
        // Open/create mutex semaphore (binary, initial value 1)
        mutex_sem_ = sem_open(lock_name.c_str(), flags, 0600, 1);
        if (mutex_sem_ == SEM_FAILED) {
            throw std::runtime_error(std::string("sem_open(mutex) failed: ") + strerror(errno));
        }
        
        // Open/create condition semaphore (initial value 0)
        wait_sem_ = sem_open(cond_name.c_str(), flags, 0600, 0);
        if (wait_sem_ == SEM_FAILED) {
            sem_close(mutex_sem_);
            if (create) {
                sem_unlink(lock_name.c_str());
            }
            throw std::runtime_error(std::string("sem_open(cond) failed: ") + strerror(errno));
        }
    } catch (...) {
        throw;
    }
}

PosixCondition::~PosixCondition() {
    if (mutex_sem_ != nullptr && mutex_sem_ != SEM_FAILED) {
        sem_close(mutex_sem_);
    }
    if (wait_sem_ != nullptr && wait_sem_ != SEM_FAILED) {
        sem_close(wait_sem_);
    }
}

PosixCondition::PosixCondition(PosixCondition&& other) noexcept
    : base_name_(std::move(other.base_name_))
    , mutex_sem_(other.mutex_sem_)
    , wait_sem_(other.wait_sem_)
    , create_(other.create_)
{
    other.mutex_sem_ = nullptr;
    other.wait_sem_ = nullptr;
}

PosixCondition& PosixCondition::operator=(PosixCondition&& other) noexcept {
    if (this != &other) {
        // Cleanup
        if (mutex_sem_ != nullptr) {
            sem_close(mutex_sem_);
        }
        if (wait_sem_ != nullptr) {
            sem_close(wait_sem_);
        }
        
        base_name_ = std::move(other.base_name_);
        mutex_sem_ = other.mutex_sem_;
        wait_sem_ = other.wait_sem_;
        create_ = other.create_;
        other.mutex_sem_ = nullptr;
        other.wait_sem_ = nullptr;
    }
    return *this;
}

void PosixCondition::lock() {
    if (sem_wait(mutex_sem_) < 0) {
        throw std::runtime_error(std::string("sem_wait(mutex) failed: ") + strerror(errno));
    }
}

void PosixCondition::unlock() {
    if (sem_post(mutex_sem_) < 0) {
        throw std::runtime_error(std::string("sem_post(mutex) failed: ") + strerror(errno));
    }
}

bool PosixCondition::wait(int timeout_ms) {
    // Release lock before waiting
    unlock();
    
    int result;
    if (timeout_ms < 0) {
        // Wait indefinitely
        result = sem_wait(wait_sem_);
    } else {
        // Wait with timeout
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        
        time_t add_sec = timeout_ms / 1000;
        long add_nsec = (timeout_ms % 1000) * 1000000;
        
        ts.tv_sec += add_sec;
        ts.tv_nsec += add_nsec;
        if (ts.tv_nsec >= 1000000000) {
            ts.tv_sec += 1;
            ts.tv_nsec -= 1000000000;
        }
        
        result = sem_timedwait(wait_sem_, &ts);
    }
    
    // Reacquire lock before returning
    lock();
    
    if (result < 0) {
        int err = errno;
        if (err == ETIMEDOUT) {
            return false;  // Timeout
        }
        throw std::runtime_error(std::string("sem_wait(cond) failed: ") + strerror(err));
    }
    
    return true;  // Signaled
}

void PosixCondition::notify_all() {
    if (sem_post(wait_sem_) < 0) {
        throw std::runtime_error(std::string("sem_post(cond) failed: ") + strerror(errno));
    }
}

// Buffer semaphore helpers

std::unique_ptr<PosixCondition> get_buffer_semaphores(
    const std::string& buffer_name,
    bool create
) {
    return std::make_unique<PosixCondition>(buffer_name, create);
}

void cleanup_buffer_semaphores(const std::string& buffer_name) {
    const auto& base = normalize_ipc_base_name(buffer_name);
    const auto& lock_name = posix_name(base + "_mutex");
    const auto& cond_name = posix_name(base + "_wait");
    const auto& init_name = posix_name(base + "_init");
    
    sem_unlink(lock_name.c_str());
    sem_unlink(cond_name.c_str());
    sem_unlink(init_name.c_str());
}

} // namespace ipc0cp

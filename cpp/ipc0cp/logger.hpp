#pragma once

#include <iostream>
#include <sstream>
#include <mutex>

namespace ipc0cp {

/**
 * @brief Simple thread-safe logger that writes to stderr
 * 
 * Usage:
 *   IPC_LOG_INFO("Buffer created: " << buffer_name);
 *   IPC_LOG_WARNING("Size mismatch: " << expected << " vs " << actual);
 *   IPC_LOG_ERROR("Failed to open: " << error_msg);
 */
class Logger {
public:
    enum class Level {
        INFO,
        WARNING,
        ERROR
    };
    
    static Logger& instance() {
        static Logger logger;
        return logger;
    }
    
    /**
     * @brief Enable or disable logging
     * @param enabled If true, log messages are written to stderr; if false, they are suppressed
     */
    void set_enabled(bool enabled) {
        std::lock_guard<std::mutex> lock(mutex_);
        enabled_ = enabled;
    }
    
    bool is_enabled() const {
        return enabled_;
    }
    
    /**
     * @brief Log a message
     * @param level Log level (INFO, WARNING, ERROR)
     * @param message Message to log
     */
    void log(Level level, const std::string& message) {
        if (!enabled_) return;
        
        std::lock_guard<std::mutex> lock(mutex_);
        
        const char* level_str = "";
        switch (level) {
            case Level::INFO:    level_str = "[INFO] "; break;
            case Level::WARNING: level_str = "[WARNING] "; break;
            case Level::ERROR:   level_str = "[ERROR] "; break;
        }
        
        std::cerr << level_str << message << std::endl;
    }

private:
    Logger() : enabled_(false) {}  // Disabled by default
    
    bool enabled_;
    std::mutex mutex_;
};

// Helper class for stream-style logging
class LogStream {
public:
    LogStream(Logger::Level level) : level_(level) {}
    
    ~LogStream() {
        Logger::instance().log(level_, stream_.str());
    }
    
    template<typename T>
    LogStream& operator<<(const T& value) {
        stream_ << value;
        return *this;
    }

private:
    Logger::Level level_;
    std::ostringstream stream_;
};

/**
 * @brief Enable logging to stderr
 */
inline void enable_logging() {
    Logger::instance().set_enabled(true);
}

/**
 * @brief Disable logging
 */
inline void disable_logging() {
    Logger::instance().set_enabled(false);
}

} // namespace ipc0cp

// Convenience macros for logging
#define IPC_LOG_INFO(msg) \
    do { \
        if (ipc0cp::Logger::instance().is_enabled()) { \
            ipc0cp::LogStream(ipc0cp::Logger::Level::INFO) << msg; \
        } \
    } while(0)

#define IPC_LOG_WARNING(msg) \
    do { \
        if (ipc0cp::Logger::instance().is_enabled()) { \
            ipc0cp::LogStream(ipc0cp::Logger::Level::WARNING) << msg; \
        } \
    } while(0)

#define IPC_LOG_ERROR(msg) \
    do { \
        if (ipc0cp::Logger::instance().is_enabled()) { \
            ipc0cp::LogStream(ipc0cp::Logger::Level::ERROR) << msg; \
        } \
    } while(0)

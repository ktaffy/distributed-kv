#include "logger.h"
#include <iostream>
#include <iomanip>
#include <ctime>
#include <algorithm>

namespace raft
{

    std::mutex Logger::global_mutex_;
    LogLevel Logger::global_level_ = LogLevel::INFO;
    std::unordered_map<std::string, std::shared_ptr<Logger>> Logger::loggers_;

    Logger::Logger(const std::string &name, LogLevel level)
        : logger_name_(name), current_level_(level), console_output_(true),
          timestamp_format_("%Y-%m-%d %H:%M:%S") {}

    Logger::Logger(const std::string &name, const std::string &file_path, LogLevel level)
        : logger_name_(name), current_level_(level), console_output_(false),
          timestamp_format_("%Y-%m-%d %H:%M:%S")
    {
        set_file_output(file_path);
    }

    Logger::~Logger()
    {
        if (file_stream_)
        {
            file_stream_->flush();
            file_stream_->close();
        }
    }

    void Logger::set_level(LogLevel level)
    {
        current_level_ = level;
    }

    LogLevel Logger::get_level() const
    {
        return current_level_;
    }

    void Logger::set_file_output(const std::string &file_path)
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        if (file_stream_)
        {
            file_stream_->close();
        }
        file_stream_ = std::make_unique<std::ofstream>(file_path, std::ios::app);
        console_output_ = false;
    }

    void Logger::set_console_output(bool enabled)
    {
        console_output_ = enabled;
    }

    void Logger::set_timestamp_format(const std::string &format)
    {
        timestamp_format_ = format;
    }

    void Logger::trace(const std::string &message)
    {
        if (should_log(LogLevel::TRACE))
        {
            log(LogLevel::TRACE, message);
        }
    }

    void Logger::debug(const std::string &message)
    {
        if (should_log(LogLevel::DEBUG))
        {
            log(LogLevel::DEBUG, message);
        }
    }

    void Logger::info(const std::string &message)
    {
        if (should_log(LogLevel::INFO))
        {
            log(LogLevel::INFO, message);
        }
    }

    void Logger::warn(const std::string &message)
    {
        if (should_log(LogLevel::WARN))
        {
            log(LogLevel::WARN, message);
        }
    }

    void Logger::error(const std::string &message)
    {
        if (should_log(LogLevel::ERROR))
        {
            log(LogLevel::ERROR, message);
        }
    }

    void Logger::fatal(const std::string &message)
    {
        if (should_log(LogLevel::FATAL))
        {
            log(LogLevel::FATAL, message);
        }
    }

    void Logger::flush()
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        if (file_stream_)
        {
            file_stream_->flush();
        }
        if (console_output_)
        {
            std::cout.flush();
            std::cerr.flush();
        }
    }

    void Logger::log(LogLevel level, const std::string &message)
    {
        if (!should_log(level))
        {
            return;
        }

        std::string formatted_message = format_log_entry(level, message);

        std::lock_guard<std::mutex> lock(log_mutex_);

        if (file_stream_ && file_stream_->is_open())
        {
            *file_stream_ << formatted_message << std::endl;
            file_stream_->flush();
        }

        if (console_output_)
        {
            if (level >= LogLevel::ERROR)
            {
                std::cerr << formatted_message << std::endl;
            }
            else
            {
                std::cout << formatted_message << std::endl;
            }
        }
    }

    bool Logger::should_log(LogLevel level) const
    {
        return level >= current_level_ && level >= global_level_;
    }

    std::string Logger::format_timestamp() const
    {
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      now.time_since_epoch()) %
                  1000;

        std::ostringstream oss;
        oss << std::put_time(std::localtime(&time_t), timestamp_format_.c_str());
        oss << "." << std::setfill('0') << std::setw(3) << ms.count();

        return oss.str();
    }

    std::string Logger::format_log_entry(LogLevel level, const std::string &message) const
    {
        std::ostringstream oss;
        oss << "[" << format_timestamp() << "] "
            << "[" << level_to_string(level) << "] "
            << "[" << logger_name_ << "] "
            << message;
        return oss.str();
    }

    std::shared_ptr<Logger> Logger::get_logger(const std::string &name)
    {
        std::lock_guard<std::mutex> lock(global_mutex_);

        auto it = loggers_.find(name);
        if (it != loggers_.end())
        {
            return it->second;
        }

        auto logger = std::make_shared<Logger>(name, global_level_);
        loggers_[name] = logger;
        return logger;
    }

    void Logger::set_global_level(LogLevel level)
    {
        std::lock_guard<std::mutex> lock(global_mutex_);
        global_level_ = level;

        for (auto &pair : loggers_)
        {
            pair.second->set_level(level);
        }
    }

    void Logger::shutdown()
    {
        std::lock_guard<std::mutex> lock(global_mutex_);

        for (auto &pair : loggers_)
        {
            pair.second->flush();
        }

        loggers_.clear();
    }

    LogLevel Logger::string_to_level(const std::string &level_str)
    {
        std::string upper_str = level_str;
        std::transform(upper_str.begin(), upper_str.end(), upper_str.begin(), ::toupper);

        if (upper_str == "TRACE")
            return LogLevel::TRACE;
        if (upper_str == "DEBUG")
            return LogLevel::DEBUG;
        if (upper_str == "INFO")
            return LogLevel::INFO;
        if (upper_str == "WARN" || upper_str == "WARNING")
            return LogLevel::WARN;
        if (upper_str == "ERROR")
            return LogLevel::ERROR;
        if (upper_str == "FATAL")
            return LogLevel::FATAL;

        return LogLevel::INFO;
    }

    std::string Logger::level_to_string(LogLevel level)
    {
        switch (level)
        {
        case LogLevel::TRACE:
            return "TRACE";
        case LogLevel::DEBUG:
            return "DEBUG";
        case LogLevel::INFO:
            return "INFO ";
        case LogLevel::WARN:
            return "WARN ";
        case LogLevel::ERROR:
            return "ERROR";
        case LogLevel::FATAL:
            return "FATAL";
        default:
            return "UNKNOWN";
        }
    }

} // namespace raft
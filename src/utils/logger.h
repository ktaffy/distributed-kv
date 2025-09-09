#pragma once

#include <string>
#include <fstream>
#include <mutex>
#include <memory>
#include <sstream>
#include <chrono>
#include <unordered_map>

namespace raft
{

    enum class LogLevel : uint8_t
    {
        TRACE = 0,
        DEBUG = 1,
        INFO = 2,
        WARN = 3,
        ERROR = 4,
        FATAL = 5
    };

    class Logger
    {
    public:
        Logger(const std::string &name, LogLevel level = LogLevel::INFO);
        Logger(const std::string &name, const std::string &file_path, LogLevel level = LogLevel::INFO);
        ~Logger();

        void set_level(LogLevel level);
        LogLevel get_level() const;

        void set_file_output(const std::string &file_path);
        void set_console_output(bool enabled);
        void set_timestamp_format(const std::string &format);

        void trace(const std::string &message);
        void debug(const std::string &message);
        void info(const std::string &message);
        void warn(const std::string &message);
        void error(const std::string &message);
        void fatal(const std::string &message);

        template <typename... Args>
        void trace(const std::string &format, Args &&...args)
        {
            if (should_log(LogLevel::TRACE))
            {
                log(LogLevel::TRACE, format_string(format, std::forward<Args>(args)...));
            }
        }

        template <typename... Args>
        void debug(const std::string &format, Args &&...args)
        {
            if (should_log(LogLevel::DEBUG))
            {
                log(LogLevel::DEBUG, format_string(format, std::forward<Args>(args)...));
            }
        }

        template <typename... Args>
        void info(const std::string &format, Args &&...args)
        {
            if (should_log(LogLevel::INFO))
            {
                log(LogLevel::INFO, format_string(format, std::forward<Args>(args)...));
            }
        }

        template <typename... Args>
        void warn(const std::string &format, Args &&...args)
        {
            if (should_log(LogLevel::WARN))
            {
                log(LogLevel::WARN, format_string(format, std::forward<Args>(args)...));
            }
        }

        template <typename... Args>
        void error(const std::string &format, Args &&...args)
        {
            if (should_log(LogLevel::ERROR))
            {
                log(LogLevel::ERROR, format_string(format, std::forward<Args>(args)...));
            }
        }

        template <typename... Args>
        void fatal(const std::string &format, Args &&...args)
        {
            if (should_log(LogLevel::FATAL))
            {
                log(LogLevel::FATAL, format_string(format, std::forward<Args>(args)...));
            }
        }

        void flush();

        static std::shared_ptr<Logger> get_logger(const std::string &name);
        static void set_global_level(LogLevel level);
        static void shutdown();

        static LogLevel string_to_level(const std::string &level_str);
        static std::string level_to_string(LogLevel level);

    private:
        void log(LogLevel level, const std::string &message);
        bool should_log(LogLevel level) const;
        std::string format_timestamp() const;
        std::string format_log_entry(LogLevel level, const std::string &message) const;

        template <typename T>
        std::string format_string(const std::string &format, T &&value)
        {
            std::ostringstream oss;
            size_t pos = format.find("{}");
            if (pos != std::string::npos)
            {
                oss << format.substr(0, pos) << std::forward<T>(value) << format.substr(pos + 2);
            }
            else
            {
                oss << format << " " << std::forward<T>(value);
            }
            return oss.str();
        }

        template <typename T, typename... Args>
        std::string format_string(const std::string &format, T &&value, Args &&...args)
        {
            std::ostringstream oss;
            size_t pos = format.find("{}");
            if (pos != std::string::npos)
            {
                oss << format.substr(0, pos) << std::forward<T>(value);
                return format_string(oss.str() + format.substr(pos + 2), std::forward<Args>(args)...);
            }
            else
            {
                oss << format << " " << std::forward<T>(value);
                return format_string(oss.str(), std::forward<Args>(args)...);
            }
        }

        std::string logger_name_;
        LogLevel current_level_;

        bool console_output_;
        std::unique_ptr<std::ofstream> file_stream_;
        std::string timestamp_format_;

        mutable std::mutex log_mutex_;

        static std::mutex global_mutex_;
        static LogLevel global_level_;
        static std::unordered_map<std::string, std::shared_ptr<Logger>> loggers_;
    };

#define LOG_TRACE(logger, ...) logger->trace(__VA_ARGS__)
#define LOG_DEBUG(logger, ...) logger->debug(__VA_ARGS__)
#define LOG_INFO(logger, ...) logger->info(__VA_ARGS__)
#define LOG_WARN(logger, ...) logger->warn(__VA_ARGS__)
#define LOG_ERROR(logger, ...) logger->error(__VA_ARGS__)
#define LOG_FATAL(logger, ...) logger->fatal(__VA_ARGS__)

} // namespace raft
#include "log_storage.h"
#include <filesystem>
#include <algorithm>
#include <cstring>

namespace raft
{

    LogStorage::LogStorage(const std::string &data_dir)
        : data_directory_(data_dir), fsync_enabled_(true),
          max_log_size_bytes_(DEFAULT_MAX_LOG_SIZE),
          snapshot_last_included_index_(0), snapshot_last_included_term_(0),
          has_snapshot_(false)
    {
        reset_stats();
    }

    LogStorage::~LogStorage()
    {
        sync();
    }

    bool LogStorage::initialize()
    {
        try
        {
            std::filesystem::create_directories(data_directory_);
        }
        catch (const std::exception &)
        {
            return false;
        }

        return load();
    }

    bool LogStorage::load()
    {
        bool log_loaded = load_log_file();
        bool snapshot_loaded = load_snapshot_file();

        return log_loaded;
    }

    bool LogStorage::sync()
    {
        if (fsync_enabled_.load())
        {
            return save_log_file();
        }
        return true;
    }

    bool LogStorage::append_entry(const LogEntry &entry)
    {
        std::lock_guard<std::mutex> lock(log_mutex_);

        if (!log_entries_.empty() && entry.index != get_last_index() + 1)
        {
            return false;
        }

        log_entries_.push_back(entry);

        {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            stats_.append_count++;
            stats_.total_entries++;
            stats_.total_size_bytes += entry.estimated_size();
        }

        if (fsync_enabled_.load())
        {
            return append_to_log_file(entry);
        }

        return true;
    }

    bool LogStorage::append_entries(const std::vector<LogEntry> &entries)
    {
        if (entries.empty())
        {
            return true;
        }

        std::lock_guard<std::mutex> lock(log_mutex_);

        for (const auto &entry : entries)
        {
            if (!log_entries_.empty() && entry.index != get_last_index() + 1)
            {
                return false;
            }
            log_entries_.push_back(entry);
        }

        {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            stats_.append_count += entries.size();
            stats_.total_entries += entries.size();
            for (const auto &entry : entries)
            {
                stats_.total_size_bytes += entry.estimated_size();
            }
        }

        if (fsync_enabled_.load())
        {
            return append_to_log_file(entries);
        }

        return true;
    }

    LogEntry LogStorage::get_entry(uint32_t index) const
    {
        std::lock_guard<std::mutex> lock(log_mutex_);

        if (log_entries_.empty() || index == 0)
        {
            return LogEntry();
        }

        uint32_t first_index = get_first_index();
        if (index < first_index || index > get_last_index())
        {
            return LogEntry();
        }

        size_t array_index = index - first_index;
        if (array_index >= log_entries_.size())
        {
            return LogEntry();
        }

        return log_entries_[array_index];
    }

    std::vector<LogEntry> LogStorage::get_entries(uint32_t start_index, uint32_t end_index) const
    {
        std::lock_guard<std::mutex> lock(log_mutex_);

        if (log_entries_.empty() || start_index > end_index)
        {
            return {};
        }

        uint32_t first_index = get_first_index();
        uint32_t last_index = get_last_index();

        if (start_index > last_index || end_index < first_index)
        {
            return {};
        }

        uint32_t actual_start = std::max(start_index, first_index);
        uint32_t actual_end = std::min(end_index, last_index);

        size_t start_array_index = actual_start - first_index;
        size_t end_array_index = actual_end - first_index;

        if (start_array_index >= log_entries_.size())
        {
            return {};
        }

        end_array_index = std::min(end_array_index, log_entries_.size() - 1);

        return std::vector<LogEntry>(
            log_entries_.begin() + start_array_index,
            log_entries_.begin() + end_array_index + 1);
    }

    std::vector<LogEntry> LogStorage::get_entries_from(uint32_t start_index) const
    {
        return get_entries(start_index, get_last_index());
    }

    bool LogStorage::truncate_from(uint32_t index)
    {
        std::lock_guard<std::mutex> lock(log_mutex_);

        if (log_entries_.empty())
        {
            return true;
        }

        uint32_t first_index = get_first_index();
        if (index <= first_index)
        {
            log_entries_.clear();
        }
        else
        {
            uint32_t last_index = get_last_index();
            if (index <= last_index)
            {
                size_t new_size = index - first_index;
                log_entries_.resize(new_size);
            }
        }

        {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            stats_.truncate_count++;
            stats_.total_entries = log_entries_.size();
            stats_.total_size_bytes = calculate_log_size();
        }

        return true;
    }

    bool LogStorage::truncate_to(uint32_t index)
    {
        std::lock_guard<std::mutex> lock(log_mutex_);

        if (log_entries_.empty() || index == 0)
        {
            return true;
        }

        uint32_t first_index = get_first_index();
        uint32_t last_index = get_last_index();

        if (index > last_index)
        {
            return true;
        }

        if (index < first_index)
        {
            log_entries_.clear();
        }
        else
        {
            size_t remove_count = index - first_index + 1;
            log_entries_.erase(log_entries_.begin(), log_entries_.begin() + remove_count);
        }

        {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            stats_.truncate_count++;
            stats_.total_entries = log_entries_.size();
            stats_.total_size_bytes = calculate_log_size();
        }

        return true;
    }

    uint32_t LogStorage::get_last_index() const
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        if (log_entries_.empty())
        {
            return snapshot_last_included_index_;
        }
        return log_entries_.back().index;
    }

    uint32_t LogStorage::get_last_term() const
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        if (log_entries_.empty())
        {
            return snapshot_last_included_term_;
        }
        return log_entries_.back().term;
    }

    uint32_t LogStorage::get_first_index() const
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        if (log_entries_.empty())
        {
            return snapshot_last_included_index_ + 1;
        }
        return log_entries_.front().index;
    }

    size_t LogStorage::get_entry_count() const
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        return log_entries_.size();
    }

    bool LogStorage::has_entry(uint32_t index) const
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        if (log_entries_.empty())
        {
            return false;
        }
        return index >= get_first_index() && index <= get_last_index();
    }

    bool LogStorage::is_empty() const
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        return log_entries_.empty() && !has_snapshot_.load();
    }

    LogEntry LogStorage::get_last_entry() const
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        if (log_entries_.empty())
        {
            return LogEntry();
        }
        return log_entries_.back();
    }

    LogEntry LogStorage::get_first_entry() const
    {
        std::lock_guard<std::mutex> lock(log_mutex_);
        if (log_entries_.empty())
        {
            return LogEntry();
        }
        return log_entries_.front();
    }

    bool LogStorage::create_snapshot(uint32_t last_included_index, uint32_t last_included_term,
                                     const std::string &snapshot_data)
    {
        if (!save_snapshot_file(last_included_index, last_included_term, snapshot_data))
        {
            return false;
        }

        std::lock_guard<std::mutex> lock(log_mutex_);
        snapshot_last_included_index_ = last_included_index;
        snapshot_last_included_term_ = last_included_term;
        snapshot_data_ = snapshot_data;
        has_snapshot_.store(true);

        {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            stats_.snapshot_index = last_included_index;
            stats_.snapshot_size_bytes = snapshot_data.size();
        }

        return true;
    }

    bool LogStorage::load_snapshot(uint32_t &last_included_index, uint32_t &last_included_term,
                                   std::string &snapshot_data)
    {
        std::lock_guard<std::mutex> lock(log_mutex_);

        if (!has_snapshot_.load())
        {
            return false;
        }

        last_included_index = snapshot_last_included_index_;
        last_included_term = snapshot_last_included_term_;
        snapshot_data = snapshot_data_;

        return true;
    }

    bool LogStorage::has_snapshot() const
    {
        return has_snapshot_.load();
    }

    bool LogStorage::compact_log(uint32_t last_included_index)
    {
        std::lock_guard<std::mutex> lock(log_mutex_);

        if (log_entries_.empty())
        {
            return true;
        }

        uint32_t first_index = get_first_index();
        if (last_included_index < first_index)
        {
            return true;
        }

        size_t remove_count = last_included_index - first_index + 1;
        if (remove_count >= log_entries_.size())
        {
            log_entries_.clear();
        }
        else
        {
            log_entries_.erase(log_entries_.begin(), log_entries_.begin() + remove_count);
        }

        {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            stats_.total_entries = log_entries_.size();
            stats_.total_size_bytes = calculate_log_size();
        }

        return true;
    }

    void LogStorage::set_fsync_enabled(bool enabled)
    {
        fsync_enabled_.store(enabled);
    }

    bool LogStorage::get_fsync_enabled() const
    {
        return fsync_enabled_.load();
    }

    void LogStorage::set_max_log_size(size_t max_size_bytes)
    {
        max_log_size_bytes_.store(max_size_bytes);
    }

    size_t LogStorage::get_max_log_size() const
    {
        return max_log_size_bytes_.load();
    }

    LogStorage::LogStats LogStorage::get_stats() const
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        return stats_;
    }

    void LogStorage::reset_stats()
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_ = LogStats{};
    }

    bool LogStorage::verify_integrity() const
    {
        std::lock_guard<std::mutex> lock(log_mutex_);

        if (log_entries_.empty())
        {
            return true;
        }

        for (size_t i = 1; i < log_entries_.size(); ++i)
        {
            if (log_entries_[i].index != log_entries_[i - 1].index + 1)
            {
                return false;
            }
            if (log_entries_[i].term < log_entries_[i - 1].term)
            {
                return false;
            }
        }

        return true;
    }

    bool LogStorage::repair_log()
    {
        return verify_integrity();
    }

    bool LogStorage::load_log_file()
    {
        std::string log_file = get_log_file_path();

        if (!std::filesystem::exists(log_file))
        {
            return true;
        }

        std::ifstream file(log_file, std::ios::binary);
        if (!file.is_open())
        {
            return false;
        }

        LogHeader header;
        file.read(reinterpret_cast<char *>(&header), sizeof(LogHeader));

        if (!file.good() || header.version != LOG_FILE_VERSION)
        {
            return false;
        }

        log_entries_.clear();
        log_entries_.reserve(header.entry_count);

        for (uint32_t i = 0; i < header.entry_count; ++i)
        {
            uint32_t entry_size;
            file.read(reinterpret_cast<char *>(&entry_size), sizeof(entry_size));

            if (!file.good())
            {
                return false;
            }

            std::string entry_data(entry_size, '\0');
            file.read(&entry_data[0], entry_size);

            if (!file.good())
            {
                return false;
            }

            LogEntry entry;
            if (!entry.deserialize(entry_data))
            {
                return false;
            }

            log_entries_.push_back(entry);
        }

        return true;
    }

    bool LogStorage::save_log_file()
    {
        std::string temp_file = get_temp_log_path();
        std::string log_file = get_log_file_path();

        std::ofstream file(temp_file, std::ios::binary);
        if (!file.is_open())
        {
            return false;
        }

        LogHeader header;
        header.version = LOG_FILE_VERSION;
        header.entry_count = log_entries_.size();
        header.first_index = log_entries_.empty() ? 0 : log_entries_.front().index;
        header.last_index = log_entries_.empty() ? 0 : log_entries_.back().index;
        header.file_size = sizeof(LogHeader);

        for (const auto &entry : log_entries_)
        {
            std::string serialized = entry.serialize();
            header.file_size += sizeof(uint32_t) + serialized.size();
        }

        file.write(reinterpret_cast<const char *>(&header), sizeof(LogHeader));

        for (const auto &entry : log_entries_)
        {
            std::string serialized = entry.serialize();
            uint32_t entry_size = serialized.size();

            file.write(reinterpret_cast<const char *>(&entry_size), sizeof(entry_size));
            file.write(serialized.data(), entry_size);
        }

        file.flush();
        file.close();

        if (std::rename(temp_file.c_str(), log_file.c_str()) != 0)
        {
            return false;
        }

        {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            stats_.sync_count++;
        }

        return true;
    }

    bool LogStorage::append_to_log_file(const LogEntry &entry)
    {
        return save_log_file();
    }

    bool LogStorage::append_to_log_file(const std::vector<LogEntry> &entries)
    {
        return save_log_file();
    }

    bool LogStorage::load_snapshot_file()
    {
        std::string snapshot_file = get_snapshot_file_path();

        if (!std::filesystem::exists(snapshot_file))
        {
            has_snapshot_.store(false);
            return true;
        }

        std::ifstream file(snapshot_file, std::ios::binary);
        if (!file.is_open())
        {
            return false;
        }

        SnapshotHeader header;
        file.read(reinterpret_cast<char *>(&header), sizeof(SnapshotHeader));

        if (!file.good() || header.version != SNAPSHOT_FILE_VERSION)
        {
            return false;
        }

        snapshot_data_.resize(header.data_size);
        file.read(&snapshot_data_[0], header.data_size);

        if (!file.good())
        {
            return false;
        }

        snapshot_last_included_index_ = header.last_included_index;
        snapshot_last_included_term_ = header.last_included_term;
        has_snapshot_.store(true);

        return true;
    }

    bool LogStorage::save_snapshot_file(uint32_t last_included_index, uint32_t last_included_term,
                                        const std::string &snapshot_data)
    {
        std::string temp_file = get_temp_snapshot_path();
        std::string snapshot_file = get_snapshot_file_path();

        std::ofstream file(temp_file, std::ios::binary);
        if (!file.is_open())
        {
            return false;
        }

        SnapshotHeader header;
        header.version = SNAPSHOT_FILE_VERSION;
        header.last_included_index = last_included_index;
        header.last_included_term = last_included_term;
        header.data_size = snapshot_data.size();
        header.checksum = calculate_checksum(snapshot_data.data(), snapshot_data.size());

        file.write(reinterpret_cast<const char *>(&header), sizeof(SnapshotHeader));
        file.write(snapshot_data.data(), snapshot_data.size());

        file.flush();
        file.close();

        return std::rename(temp_file.c_str(), snapshot_file.c_str()) == 0;
    }

    uint32_t LogStorage::calculate_checksum(const void *data, size_t size) const
    {
        uint32_t checksum = 0;
        const uint8_t *bytes = reinterpret_cast<const uint8_t *>(data);

        for (size_t i = 0; i < size; ++i)
        {
            checksum = checksum * 31 + bytes[i];
        }

        return checksum;
    }

    bool LogStorage::verify_log_checksum() const
    {
        return true;
    }

    bool LogStorage::verify_snapshot_checksum() const
    {
        return true;
    }

    std::string LogStorage::get_log_file_path() const
    {
        return data_directory_ + "/" + LOG_FILE_NAME;
    }

    std::string LogStorage::get_snapshot_file_path() const
    {
        return data_directory_ + "/" + SNAPSHOT_FILE_NAME;
    }

    std::string LogStorage::get_temp_log_path() const
    {
        return data_directory_ + "/" + TEMP_LOG_NAME;
    }

    std::string LogStorage::get_temp_snapshot_path() const
    {
        return data_directory_ + "/" + TEMP_SNAPSHOT_NAME;
    }

    bool LogStorage::create_backup()
    {
        return true;
    }

    bool LogStorage::restore_from_backup()
    {
        return true;
    }

    size_t LogStorage::calculate_log_size() const
    {
        size_t total_size = 0;
        for (const auto &entry : log_entries_)
        {
            total_size += entry.estimated_size();
        }
        return total_size;
    }

    bool LogStorage::should_compact() const
    {
        return calculate_log_size() > max_log_size_bytes_.load() * COMPACT_THRESHOLD_RATIO;
    }

    LogEntry LogStorage::deserialize_entry(const std::string &data) const
    {
        LogEntry entry;
        entry.deserialize(data);
        return entry;
    }

    std::string LogStorage::serialize_entry(const LogEntry &entry) const
    {
        return entry.serialize();
    }

} // namespace raft
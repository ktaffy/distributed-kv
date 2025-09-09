#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <memory>
#include <fstream>
#include <atomic>
#include "../raft/log_entry.h"

namespace raft
{

    class LogStorage
    {
    public:
        explicit LogStorage(const std::string &data_dir);
        ~LogStorage();

        bool initialize();
        bool load();
        bool sync();

        bool append_entry(const LogEntry &entry);
        bool append_entries(const std::vector<LogEntry> &entries);

        LogEntry get_entry(uint32_t index) const;
        std::vector<LogEntry> get_entries(uint32_t start_index, uint32_t end_index) const;
        std::vector<LogEntry> get_entries_from(uint32_t start_index) const;

        bool truncate_from(uint32_t index);
        bool truncate_to(uint32_t index);

        uint32_t get_last_index() const;
        uint32_t get_last_term() const;
        uint32_t get_first_index() const;
        size_t get_entry_count() const;

        bool has_entry(uint32_t index) const;
        bool is_empty() const;

        LogEntry get_last_entry() const;
        LogEntry get_first_entry() const;

        bool create_snapshot(uint32_t last_included_index, uint32_t last_included_term,
                             const std::string &snapshot_data);
        bool load_snapshot(uint32_t &last_included_index, uint32_t &last_included_term,
                           std::string &snapshot_data);
        bool has_snapshot() const;

        bool compact_log(uint32_t last_included_index);

        void set_fsync_enabled(bool enabled);
        bool get_fsync_enabled() const;

        void set_max_log_size(size_t max_size_bytes);
        size_t get_max_log_size() const;

        struct LogStats
        {
            size_t total_entries;
            size_t total_size_bytes;
            uint32_t first_index;
            uint32_t last_index;
            uint32_t snapshot_index;
            size_t snapshot_size_bytes;
            uint32_t append_count;
            uint32_t truncate_count;
            uint32_t sync_count;
        };

        LogStats get_stats() const;
        void reset_stats();

        bool verify_integrity() const;
        bool repair_log();

    private:
        struct LogHeader
        {
            uint32_t version;
            uint32_t entry_count;
            uint32_t first_index;
            uint32_t last_index;
            uint64_t file_size;
            uint32_t checksum;

            LogHeader() : version(0), entry_count(0), first_index(0),
                          last_index(0), file_size(0), checksum(0) {}
        };

        struct SnapshotHeader
        {
            uint32_t version;
            uint32_t last_included_index;
            uint32_t last_included_term;
            uint64_t data_size;
            uint32_t checksum;

            SnapshotHeader() : version(0), last_included_index(0),
                               last_included_term(0), data_size(0), checksum(0) {}
        };

        bool load_log_file();
        bool save_log_file();
        bool append_to_log_file(const LogEntry &entry);
        bool append_to_log_file(const std::vector<LogEntry> &entries);

        bool load_snapshot_file();
        bool save_snapshot_file(uint32_t last_included_index, uint32_t last_included_term,
                                const std::string &snapshot_data);

        uint32_t calculate_checksum(const void *data, size_t size) const;
        bool verify_log_checksum() const;
        bool verify_snapshot_checksum() const;

        std::string get_log_file_path() const;
        std::string get_snapshot_file_path() const;
        std::string get_temp_log_path() const;
        std::string get_temp_snapshot_path() const;

        bool create_backup();
        bool restore_from_backup();

        size_t calculate_log_size() const;
        bool should_compact() const;

        LogEntry deserialize_entry(const std::string &data) const;
        std::string serialize_entry(const LogEntry &entry) const;

        const std::string data_directory_;

        mutable std::mutex log_mutex_;
        std::vector<LogEntry> log_entries_;

        std::atomic<bool> fsync_enabled_;
        std::atomic<size_t> max_log_size_bytes_;

        uint32_t snapshot_last_included_index_;
        uint32_t snapshot_last_included_term_;
        std::string snapshot_data_;
        std::atomic<bool> has_snapshot_;

        mutable std::mutex stats_mutex_;
        LogStats stats_;

        static constexpr uint32_t LOG_FILE_VERSION = 1;
        static constexpr uint32_t SNAPSHOT_FILE_VERSION = 1;
        static constexpr const char *LOG_FILE_NAME = "raft_log.dat";
        static constexpr const char *SNAPSHOT_FILE_NAME = "raft_snapshot.dat";
        static constexpr const char *TEMP_LOG_NAME = "raft_log.tmp";
        static constexpr const char *TEMP_SNAPSHOT_NAME = "raft_snapshot.tmp";
        static constexpr const char *BACKUP_LOG_NAME = "raft_log.backup";
        static constexpr const char *BACKUP_SNAPSHOT_NAME = "raft_snapshot.backup";
        static constexpr size_t DEFAULT_MAX_LOG_SIZE = 100 * 1024 * 1024;
        static constexpr size_t COMPACT_THRESHOLD_RATIO = 2;
    };

} // namespace raft
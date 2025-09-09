#pragma once

#include <string>
#include <mutex>
#include <memory>
#include <fstream>
#include <atomic>

namespace raft
{

    class PersistentState
    {
    public:
        explicit PersistentState(const std::string &data_dir);
        ~PersistentState();

        bool initialize();
        bool load();
        bool save();
        bool sync();

        uint32_t get_current_term() const;
        void set_current_term(uint32_t term);

        uint32_t get_voted_for() const;
        void set_voted_for(uint32_t node_id);
        void clear_voted_for();

        bool has_voted_in_term(uint32_t term) const;

        std::string get_data_directory() const;
        bool is_initialized() const;

        void set_fsync_enabled(bool enabled);
        bool get_fsync_enabled() const;

        struct StateInfo
        {
            uint32_t current_term;
            uint32_t voted_for;
            uint64_t last_update_time;
            uint32_t save_count;
            bool is_valid;
        };

        StateInfo get_state_info() const;

        static bool create_data_directory(const std::string &path);
        static bool directory_exists(const std::string &path);
        static bool file_exists(const std::string &path);

    private:
        struct StateData
        {
            uint32_t current_term;
            uint32_t voted_for;
            uint64_t timestamp;
            uint32_t checksum;

            StateData() : current_term(0), voted_for(0), timestamp(0), checksum(0) {}
        };

        bool load_from_file(const std::string &file_path);
        bool save_to_file(const std::string &file_path) const;
        bool create_backup() const;
        bool restore_from_backup();

        uint32_t calculate_checksum(const StateData &data) const;
        bool verify_checksum(const StateData &data) const;

        std::string get_state_file_path() const;
        std::string get_backup_file_path() const;
        std::string get_temp_file_path() const;

        uint64_t get_current_timestamp() const;

        const std::string data_directory_;

        mutable std::mutex state_mutex_;
        std::atomic<uint32_t> current_term_;
        std::atomic<uint32_t> voted_for_;
        std::atomic<bool> fsync_enabled_;
        std::atomic<bool> initialized_;
        std::atomic<uint32_t> save_count_;

        static constexpr uint32_t INVALID_NODE_ID = 0;
        static constexpr uint32_t STATE_FILE_VERSION = 1;
        static constexpr const char *STATE_FILE_NAME = "raft_state.dat";
        static constexpr const char *BACKUP_FILE_NAME = "raft_state.backup";
        static constexpr const char *TEMP_FILE_NAME = "raft_state.tmp";
    };

} // namespace raft
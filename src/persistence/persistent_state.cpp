#include "persistent_state.h"
#include <fstream>
#include <filesystem>
#include <cstring>
#include <chrono>

namespace raft
{

    PersistentState::PersistentState(const std::string &data_dir)
        : data_directory_(data_dir), current_term_(0), voted_for_(INVALID_NODE_ID),
          fsync_enabled_(true), initialized_(false), save_count_(0) {}

    PersistentState::~PersistentState()
    {
        sync();
    }

    bool PersistentState::initialize()
    {
        if (!create_data_directory(data_directory_))
        {
            return false;
        }

        if (file_exists(get_state_file_path()))
        {
            if (!load())
            {
                if (file_exists(get_backup_file_path()))
                {
                    if (!restore_from_backup())
                    {
                        return false;
                    }
                }
                else
                {
                    return false;
                }
            }
        }
        else
        {
            current_term_.store(0);
            voted_for_.store(INVALID_NODE_ID);
            if (!save())
            {
                return false;
            }
        }

        initialized_.store(true);
        return true;
    }

    bool PersistentState::load()
    {
        std::string state_file = get_state_file_path();
        return load_from_file(state_file);
    }

    bool PersistentState::save()
    {

        if (!create_backup())
        {
            return false;
        }

        std::string temp_file = get_temp_file_path();
        if (!save_to_file(temp_file))
        {
            return false;
        }

        std::string state_file = get_state_file_path();
        if (std::rename(temp_file.c_str(), state_file.c_str()) != 0)
        {
            return false;
        }

        save_count_.fetch_add(1);
        return true;
    }

    bool PersistentState::sync()
    {
        if (fsync_enabled_.load())
        {
            return save();
        }
        return true;
    }

    uint32_t PersistentState::get_current_term() const
    {
        return current_term_.load();
    }

    void PersistentState::set_current_term(uint32_t term)
    {
        current_term_.store(term);
        save();
    }

    uint32_t PersistentState::get_voted_for() const
    {
        return voted_for_.load();
    }

    void PersistentState::set_voted_for(uint32_t node_id)
    {
        voted_for_.store(node_id);
        save();
    }

    void PersistentState::clear_voted_for()
    {
        voted_for_.store(INVALID_NODE_ID);
        save();
    }

    bool PersistentState::has_voted_in_term(uint32_t term) const
    {
        return current_term_.load() == term && voted_for_.load() != INVALID_NODE_ID;
    }

    std::string PersistentState::get_data_directory() const
    {
        return data_directory_;
    }

    bool PersistentState::is_initialized() const
    {
        return initialized_.load();
    }

    void PersistentState::set_fsync_enabled(bool enabled)
    {
        fsync_enabled_.store(enabled);
    }

    bool PersistentState::get_fsync_enabled() const
    {
        return fsync_enabled_.load();
    }

    PersistentState::StateInfo PersistentState::get_state_info() const
    {
        StateInfo info;
        info.current_term = current_term_.load();
        info.voted_for = voted_for_.load();
        info.last_update_time = get_current_timestamp();
        info.save_count = save_count_.load();
        info.is_valid = initialized_.load();
        return info;
    }

    bool PersistentState::load_from_file(const std::string &file_path)
    {
        std::ifstream file(file_path, std::ios::binary);
        if (!file.is_open())
        {
            return false;
        }

        StateData data;
        file.read(reinterpret_cast<char *>(&data), sizeof(StateData));

        if (!file.good() || !verify_checksum(data))
        {
            return false;
        }

        current_term_.store(data.current_term);
        voted_for_.store(data.voted_for);

        return true;
    }

    bool PersistentState::save_to_file(const std::string &file_path) const
    {
        StateData data;
        data.current_term = current_term_.load();
        data.voted_for = voted_for_.load();
        data.timestamp = get_current_timestamp();
        data.checksum = calculate_checksum(data);

        std::ofstream file(file_path, std::ios::binary);
        if (!file.is_open())
        {
            return false;
        }

        file.write(reinterpret_cast<const char *>(&data), sizeof(StateData));
        file.flush();

        if (fsync_enabled_.load())
        {
            file.close();
        }

        return file.good();
    }

    bool PersistentState::create_backup() const
    {
        std::string state_file = get_state_file_path();
        std::string backup_file = get_backup_file_path();

        if (!file_exists(state_file))
        {
            return true;
        }

        std::ifstream src(state_file, std::ios::binary);
        std::ofstream dst(backup_file, std::ios::binary);

        if (!src.is_open() || !dst.is_open())
        {
            return false;
        }

        dst << src.rdbuf();
        return src.good() && dst.good();
    }

    bool PersistentState::restore_from_backup()
    {
        std::string backup_file = get_backup_file_path();
        if (!file_exists(backup_file))
        {
            return false;
        }

        return load_from_file(backup_file);
    }

    uint32_t PersistentState::calculate_checksum(const StateData &data) const
    {
        uint32_t checksum = 0;
        const uint8_t *bytes = reinterpret_cast<const uint8_t *>(&data);
        size_t size = sizeof(StateData) - sizeof(data.checksum);

        for (size_t i = 0; i < size; ++i)
        {
            checksum = checksum * 31 + bytes[i];
        }

        return checksum;
    }

    bool PersistentState::verify_checksum(const StateData &data) const
    {
        uint32_t expected_checksum = calculate_checksum(data);
        return expected_checksum == data.checksum;
    }

    std::string PersistentState::get_state_file_path() const
    {
        return data_directory_ + "/" + STATE_FILE_NAME;
    }

    std::string PersistentState::get_backup_file_path() const
    {
        return data_directory_ + "/" + BACKUP_FILE_NAME;
    }

    std::string PersistentState::get_temp_file_path() const
    {
        return data_directory_ + "/" + TEMP_FILE_NAME;
    }

    uint64_t PersistentState::get_current_timestamp() const
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

    bool PersistentState::create_data_directory(const std::string &path)
    {
        try
        {
            std::filesystem::create_directories(path);
            return true;
        }
        catch (const std::exception &)
        {
            return false;
        }
    }

    bool PersistentState::directory_exists(const std::string &path)
    {
        try
        {
            return std::filesystem::exists(path) && std::filesystem::is_directory(path);
        }
        catch (const std::exception &)
        {
            return false;
        }
    }

    bool PersistentState::file_exists(const std::string &path)
    {
        try
        {
            return std::filesystem::exists(path) && std::filesystem::is_regular_file(path);
        }
        catch (const std::exception &)
        {
            return false;
        }
    }

} // namespace raft
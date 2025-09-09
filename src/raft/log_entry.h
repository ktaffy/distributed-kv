#pragma once

#include <string>
#include <cstdint>
#include <chrono>
#include <vector>

namespace raft
{

    enum class LogEntryType : uint8_t
    {
        NO_OP = 0,
        CLIENT_COMMAND = 1,
        CONFIGURATION = 2,
        SNAPSHOT = 3
    };

    struct LogEntry
    {
        uint32_t term;
        uint32_t index;
        LogEntryType type;
        std::string command;
        uint64_t timestamp;
        uint64_t client_id;
        uint64_t sequence_num;

        LogEntry()
            : term(0), index(0), type(LogEntryType::NO_OP),
              timestamp(0), client_id(0), sequence_num(0) {}

        LogEntry(uint32_t term_val, uint32_t index_val, LogEntryType entry_type,
                 const std::string &cmd, uint64_t client = 0, uint64_t seq = 0)
            : term(term_val), index(index_val), type(entry_type), command(cmd),
              client_id(client), sequence_num(seq)
        {
            timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
        }

        std::string serialize() const;
        bool deserialize(const std::string &data);

        bool is_no_op() const { return type == LogEntryType::NO_OP; }
        bool is_client_command() const { return type == LogEntryType::CLIENT_COMMAND; }
        bool is_configuration_change() const { return type == LogEntryType::CONFIGURATION; }
        bool is_snapshot_marker() const { return type == LogEntryType::SNAPSHOT; }

        size_t estimated_size() const
        {
            return sizeof(LogEntry) + command.size();
        }

        bool operator==(const LogEntry &other) const
        {
            return term == other.term &&
                   index == other.index &&
                   type == other.type &&
                   command == other.command &&
                   client_id == other.client_id &&
                   sequence_num == other.sequence_num;
        }

        bool operator!=(const LogEntry &other) const
        {
            return !(*this == other);
        }

        std::string to_string() const;
    };

    struct KVOperation
    {
        enum class Type : uint8_t
        {
            GET = 1,
            PUT = 2,
            DELETE = 3
        };

        Type operation;
        std::string key;
        std::string value;

        KVOperation() : operation(Type::GET) {}

        KVOperation(Type op, const std::string &k, const std::string &v = "")
            : operation(op), key(k), value(v) {}

        std::string serialize() const;
        bool deserialize(const std::string &data);

        LogEntry to_log_entry(uint32_t term, uint32_t index,
                              uint64_t client_id = 0, uint64_t seq_num = 0) const;

        static KVOperation from_log_entry(const LogEntry &entry);

        std::string to_string() const;

        static std::string type_to_string(Type op);
        static Type string_to_type(const std::string &op_str);
    };

    struct ConfigurationEntry
    {
        enum class ChangeType : uint8_t
        {
            ADD_NODE = 1,
            REMOVE_NODE = 2,
            REPLACE_NODE = 3
        };

        ChangeType change_type;
        uint32_t node_id;
        std::string node_address;
        uint32_t old_node_id;

        ConfigurationEntry() : change_type(ChangeType::ADD_NODE), node_id(0), old_node_id(0) {}

        std::string serialize() const;
        bool deserialize(const std::string &data);

        LogEntry to_log_entry(uint32_t term, uint32_t index) const;

        static ConfigurationEntry from_log_entry(const LogEntry &entry);

        std::string to_string() const;
    };

    namespace log_utils
    {
        LogEntry create_no_op_entry(uint32_t term, uint32_t index);
        LogEntry create_kv_entry(uint32_t term, uint32_t index,
                                 const KVOperation &operation,
                                 uint64_t client_id = 0, uint64_t seq_num = 0);
        LogEntry create_config_entry(uint32_t term, uint32_t index,
                                     const ConfigurationEntry &config);
        LogEntry create_snapshot_marker(uint32_t term, uint32_t index,
                                        const std::string &snapshot_path);

        bool is_valid_kv_command(const std::string &command);
        bool is_valid_config_command(const std::string &command);

        bool is_valid_log_entry(const LogEntry &entry);

        size_t calculate_entries_size(const std::vector<LogEntry> &entries);
    }

    constexpr uint32_t INVALID_INDEX = 0;
    constexpr uint32_t INVALID_TERM = 0;
    constexpr size_t MAX_ENTRY_SIZE = 1024 * 1024;
    constexpr size_t MAX_BATCH_SIZE = 100;

} // namespace raft
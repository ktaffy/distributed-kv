#include "log_entry.h"
#include <sstream>
#include <iomanip>

namespace raft
{

    std::string LogEntry::serialize() const
    {
        std::ostringstream oss;
        oss << term << "|"
            << index << "|"
            << static_cast<uint8_t>(type) << "|"
            << command << "|"
            << timestamp << "|"
            << client_id << "|"
            << sequence_num;
        return oss.str();
    }

    bool LogEntry::deserialize(const std::string &data)
    {
        std::istringstream iss(data);
        std::string token;

        if (!std::getline(iss, token, '|'))
            return false;
        term = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        index = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        type = static_cast<LogEntryType>(std::stoi(token));

        if (!std::getline(iss, token, '|'))
            return false;
        command = token;

        if (!std::getline(iss, token, '|'))
            return false;
        timestamp = std::stoull(token);

        if (!std::getline(iss, token, '|'))
            return false;
        client_id = std::stoull(token);

        if (!std::getline(iss, token, '|'))
            return false;
        sequence_num = std::stoull(token);

        return true;
    }

    std::string LogEntry::to_string() const
    {
        std::ostringstream oss;
        oss << "LogEntry{term=" << term
            << ", index=" << index
            << ", type=" << static_cast<int>(type)
            << ", command='" << command << "'"
            << ", timestamp=" << timestamp
            << ", client_id=" << client_id
            << ", sequence_num=" << sequence_num << "}";
        return oss.str();
    }

    std::string KVOperation::serialize() const
    {
        std::ostringstream oss;
        oss << static_cast<uint8_t>(operation) << "|"
            << key << "|"
            << value;
        return oss.str();
    }

    bool KVOperation::deserialize(const std::string &data)
    {
        std::istringstream iss(data);
        std::string token;

        if (!std::getline(iss, token, '|'))
            return false;
        operation = static_cast<Type>(std::stoi(token));

        if (!std::getline(iss, token, '|'))
            return false;
        key = token;

        if (!std::getline(iss, token, '|'))
            return false;
        value = token;

        return true;
    }

    LogEntry KVOperation::to_log_entry(uint32_t term, uint32_t index,
                                       uint64_t client_id, uint64_t seq_num) const
    {
        return LogEntry(term, index, LogEntryType::CLIENT_COMMAND,
                        serialize(), client_id, seq_num);
    }

    KVOperation KVOperation::from_log_entry(const LogEntry &entry)
    {
        KVOperation op;
        if (entry.type == LogEntryType::CLIENT_COMMAND)
        {
            op.deserialize(entry.command);
        }
        return op;
    }

    std::string KVOperation::to_string() const
    {
        std::ostringstream oss;
        oss << type_to_string(operation) << " " << key;
        if (operation == Type::PUT)
        {
            oss << " = " << value;
        }
        return oss.str();
    }

    std::string KVOperation::type_to_string(Type op)
    {
        switch (op)
        {
        case Type::GET:
            return "GET";
        case Type::PUT:
            return "PUT";
        case Type::DELETE:
            return "DELETE";
        default:
            return "UNKNOWN";
        }
    }

    KVOperation::Type KVOperation::string_to_type(const std::string &op_str)
    {
        if (op_str == "GET")
            return Type::GET;
        if (op_str == "PUT")
            return Type::PUT;
        if (op_str == "DELETE")
            return Type::DELETE;
        return Type::GET;
    }

    std::string ConfigurationEntry::serialize() const
    {
        std::ostringstream oss;
        oss << static_cast<uint8_t>(change_type) << "|"
            << node_id << "|"
            << node_address << "|"
            << old_node_id;
        return oss.str();
    }

    bool ConfigurationEntry::deserialize(const std::string &data)
    {
        std::istringstream iss(data);
        std::string token;

        if (!std::getline(iss, token, '|'))
            return false;
        change_type = static_cast<ChangeType>(std::stoi(token));

        if (!std::getline(iss, token, '|'))
            return false;
        node_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        node_address = token;

        if (!std::getline(iss, token, '|'))
            return false;
        old_node_id = std::stoul(token);

        return true;
    }

    LogEntry ConfigurationEntry::to_log_entry(uint32_t term, uint32_t index) const
    {
        return LogEntry(term, index, LogEntryType::CONFIGURATION, serialize());
    }

    ConfigurationEntry ConfigurationEntry::from_log_entry(const LogEntry &entry)
    {
        ConfigurationEntry config;
        if (entry.type == LogEntryType::CONFIGURATION)
        {
            config.deserialize(entry.command);
        }
        return config;
    }

    std::string ConfigurationEntry::to_string() const
    {
        std::ostringstream oss;
        switch (change_type)
        {
        case ChangeType::ADD_NODE:
            oss << "ADD_NODE " << node_id << " " << node_address;
            break;
        case ChangeType::REMOVE_NODE:
            oss << "REMOVE_NODE " << node_id;
            break;
        case ChangeType::REPLACE_NODE:
            oss << "REPLACE_NODE " << old_node_id << " -> " << node_id << " " << node_address;
            break;
        }
        return oss.str();
    }

    namespace log_utils
    {

        LogEntry create_no_op_entry(uint32_t term, uint32_t index)
        {
            return LogEntry(term, index, LogEntryType::NO_OP, "");
        }

        LogEntry create_kv_entry(uint32_t term, uint32_t index,
                                 const KVOperation &operation,
                                 uint64_t client_id, uint64_t seq_num)
        {
            return operation.to_log_entry(term, index, client_id, seq_num);
        }

        LogEntry create_config_entry(uint32_t term, uint32_t index,
                                     const ConfigurationEntry &config)
        {
            return config.to_log_entry(term, index);
        }

        LogEntry create_snapshot_marker(uint32_t term, uint32_t index,
                                        const std::string &snapshot_path)
        {
            return LogEntry(term, index, LogEntryType::SNAPSHOT, snapshot_path);
        }

        bool is_valid_kv_command(const std::string &command)
        {
            KVOperation op;
            return op.deserialize(command);
        }

        bool is_valid_config_command(const std::string &command)
        {
            ConfigurationEntry config;
            return config.deserialize(command);
        }

        bool is_valid_log_entry(const LogEntry &entry)
        {
            if (entry.term == INVALID_TERM || entry.index == INVALID_INDEX)
            {
                return false;
            }

            if (entry.estimated_size() > MAX_ENTRY_SIZE)
            {
                return false;
            }

            switch (entry.type)
            {
            case LogEntryType::NO_OP:
                return entry.command.empty();
            case LogEntryType::CLIENT_COMMAND:
                return is_valid_kv_command(entry.command);
            case LogEntryType::CONFIGURATION:
                return is_valid_config_command(entry.command);
            case LogEntryType::SNAPSHOT:
                return !entry.command.empty();
            default:
                return false;
            }
        }

        size_t calculate_entries_size(const std::vector<LogEntry> &entries)
        {
            size_t total_size = 0;
            for (const auto &entry : entries)
            {
                total_size += entry.estimated_size();
            }
            return total_size;
        }

    } // namespace log_utils

} // namespace raft
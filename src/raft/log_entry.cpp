#include "log_entry.h"
#include "../utils/codec.h"
#include <sstream>
#include <iomanip>

namespace raft
{
    void LogEntry::encode(Writer &w) const
    {
        w.u32(term);
        w.u32(index);
        w.u8(static_cast<uint8_t>(type));
        w.str(command);
        w.u64(timestamp);
        w.u64(client_id);
        w.u64(sequence_num);
    }

    bool LogEntry::decode(Reader &r)
    {
        uint8_t t;
        if (!r.u32(term) || !r.u32(index) || !r.u8(t) || !r.str(command) ||
            !r.u64(timestamp) || !r.u64(client_id) || !r.u64(sequence_num))
            return false;
        type = static_cast<LogEntryType>(t);
        return true;
    }

    std::string LogEntry::serialize() const
    {
        Writer w;
        encode(w);
        return w.take();
    }

    bool LogEntry::deserialize(const std::string &data)
    {
        Reader r(data);
        return decode(r) && r.done();
    }

    std::string KVOperation::serialize() const
    {
        Writer w;
        w.u8(static_cast<uint8_t>(operation));
        w.str(key);
        w.str(value);
        return w.take();
    }

    bool KVOperation::deserialize(const std::string &data)
    {
        Reader r(data);
        uint8_t op;
        if (!r.u8(op) || !r.str(key) || !r.str(value) || !r.done())
            return false;
        operation = static_cast<Type>(op);
        return true;
    }

    std::string ConfigurationEntry::serialize() const
    {
        Writer w;
        w.u8(static_cast<uint8_t>(change_type));
        w.u32(node_id);
        w.str(node_address);
        w.u32(old_node_id);
        return w.take();
    }

    bool ConfigurationEntry::deserialize(const std::string &data)
    {
        Reader r(data);
        uint8_t ct;
        if (!r.u8(ct) || !r.u32(node_id) || !r.str(node_address) || !r.u32(old_node_id) || !r.done())
            return false;
        change_type = static_cast<ChangeType>(ct);
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
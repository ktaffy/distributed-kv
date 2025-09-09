#include "message.h"
#include <sstream>
#include <iomanip>
#include <cstring>
#include "../raft/log_entry.h"

namespace raft
{

    std::string RequestVoteRPC::serialize() const
    {
        std::ostringstream oss;
        oss << static_cast<uint8_t>(type) << "|"
            << message_id << "|"
            << source_node_id << "|"
            << dest_node_id << "|"
            << timestamp << "|"
            << term << "|"
            << candidate_id << "|"
            << last_log_index << "|"
            << last_log_term;
        return oss.str();
    }

    bool RequestVoteRPC::deserialize(const std::string &data)
    {
        std::istringstream iss(data);
        std::string token;

        if (!std::getline(iss, token, '|'))
            return false;
        type = static_cast<MessageType>(std::stoi(token));

        if (!std::getline(iss, token, '|'))
            return false;
        message_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        source_node_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        dest_node_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        timestamp = std::stoull(token);

        if (!std::getline(iss, token, '|'))
            return false;
        term = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        candidate_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        last_log_index = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        last_log_term = std::stoul(token);

        return true;
    }

    std::string RequestVoteResponse::serialize() const
    {
        std::ostringstream oss;
        oss << static_cast<uint8_t>(type) << "|"
            << message_id << "|"
            << source_node_id << "|"
            << dest_node_id << "|"
            << timestamp << "|"
            << term << "|"
            << (vote_granted ? 1 : 0);
        return oss.str();
    }

    bool RequestVoteResponse::deserialize(const std::string &data)
    {
        std::istringstream iss(data);
        std::string token;

        if (!std::getline(iss, token, '|'))
            return false;
        type = static_cast<MessageType>(std::stoi(token));

        if (!std::getline(iss, token, '|'))
            return false;
        message_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        source_node_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        dest_node_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        timestamp = std::stoull(token);

        if (!std::getline(iss, token, '|'))
            return false;
        term = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        vote_granted = (std::stoi(token) == 1);

        return true;
    }

    std::string AppendEntriesRPC::serialize() const
    {
        std::ostringstream oss;
        oss << static_cast<uint8_t>(type) << "|"
            << message_id << "|"
            << source_node_id << "|"
            << dest_node_id << "|"
            << timestamp << "|"
            << term << "|"
            << leader_id << "|"
            << prev_log_index << "|"
            << prev_log_term << "|"
            << leader_commit << "|"
            << entries.size();

        for (const auto &entry : entries)
        {
            oss << "|" << entry.serialize();
        }

        return oss.str();
    }

    bool AppendEntriesRPC::deserialize(const std::string &data)
    {
        std::istringstream iss(data);
        std::string token;

        if (!std::getline(iss, token, '|'))
            return false;
        type = static_cast<MessageType>(std::stoi(token));

        if (!std::getline(iss, token, '|'))
            return false;
        message_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        source_node_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        dest_node_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        timestamp = std::stoull(token);

        if (!std::getline(iss, token, '|'))
            return false;
        term = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        leader_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        prev_log_index = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        prev_log_term = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        leader_commit = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        size_t entry_count = std::stoul(token);

        entries.clear();
        entries.reserve(entry_count);

        for (size_t i = 0; i < entry_count; ++i)
        {
            if (!std::getline(iss, token, '|'))
                return false;
            LogEntry entry;
            if (!entry.deserialize(token))
                return false;
            entries.push_back(entry);
        }

        return true;
    }

    std::string AppendEntriesResponse::serialize() const
    {
        std::ostringstream oss;
        oss << static_cast<uint8_t>(type) << "|"
            << message_id << "|"
            << source_node_id << "|"
            << dest_node_id << "|"
            << timestamp << "|"
            << term << "|"
            << (success ? 1 : 0) << "|"
            << match_index << "|"
            << conflict_index << "|"
            << conflict_term;
        return oss.str();
    }

    bool AppendEntriesResponse::deserialize(const std::string &data)
    {
        std::istringstream iss(data);
        std::string token;

        if (!std::getline(iss, token, '|'))
            return false;
        type = static_cast<MessageType>(std::stoi(token));

        if (!std::getline(iss, token, '|'))
            return false;
        message_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        source_node_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        dest_node_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        timestamp = std::stoull(token);

        if (!std::getline(iss, token, '|'))
            return false;
        term = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        success = (std::stoi(token) == 1);

        if (!std::getline(iss, token, '|'))
            return false;
        match_index = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        conflict_index = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        conflict_term = std::stoul(token);

        return true;
    }

    std::string ClientRequest::serialize() const
    {
        std::ostringstream oss;
        oss << static_cast<uint8_t>(type) << "|"
            << message_id << "|"
            << source_node_id << "|"
            << dest_node_id << "|"
            << timestamp << "|"
            << operation << "|"
            << key << "|"
            << value << "|"
            << client_id << "|"
            << sequence_num;
        return oss.str();
    }

    bool ClientRequest::deserialize(const std::string &data)
    {
        std::istringstream iss(data);
        std::string token;

        if (!std::getline(iss, token, '|'))
            return false;
        type = static_cast<MessageType>(std::stoi(token));

        if (!std::getline(iss, token, '|'))
            return false;
        message_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        source_node_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        dest_node_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        timestamp = std::stoull(token);

        if (!std::getline(iss, token, '|'))
            return false;
        operation = token;

        if (!std::getline(iss, token, '|'))
            return false;
        key = token;

        if (!std::getline(iss, token, '|'))
            return false;
        value = token;

        if (!std::getline(iss, token, '|'))
            return false;
        client_id = std::stoull(token);

        if (!std::getline(iss, token, '|'))
            return false;
        sequence_num = std::stoull(token);

        return true;
    }

    std::string ClientResponse::serialize() const
    {
        std::ostringstream oss;
        oss << static_cast<uint8_t>(type) << "|"
            << message_id << "|"
            << source_node_id << "|"
            << dest_node_id << "|"
            << timestamp << "|"
            << (success ? 1 : 0) << "|"
            << value << "|"
            << error_message << "|"
            << leader_hint;
        return oss.str();
    }

    bool ClientResponse::deserialize(const std::string &data)
    {
        std::istringstream iss(data);
        std::string token;

        if (!std::getline(iss, token, '|'))
            return false;
        type = static_cast<MessageType>(std::stoi(token));

        if (!std::getline(iss, token, '|'))
            return false;
        message_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        source_node_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        dest_node_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        timestamp = std::stoull(token);

        if (!std::getline(iss, token, '|'))
            return false;
        success = (std::stoi(token) == 1);

        if (!std::getline(iss, token, '|'))
            return false;
        value = token;

        if (!std::getline(iss, token, '|'))
            return false;
        error_message = token;

        if (!std::getline(iss, token, '|'))
            return false;
        leader_hint = std::stoul(token);

        return true;
    }

    std::string HeartbeatMessage::serialize() const
    {
        std::ostringstream oss;
        oss << static_cast<uint8_t>(type) << "|"
            << message_id << "|"
            << source_node_id << "|"
            << dest_node_id << "|"
            << timestamp << "|"
            << term << "|"
            << leader_id << "|"
            << commit_index;
        return oss.str();
    }

    bool HeartbeatMessage::deserialize(const std::string &data)
    {
        std::istringstream iss(data);
        std::string token;

        if (!std::getline(iss, token, '|'))
            return false;
        type = static_cast<MessageType>(std::stoi(token));

        if (!std::getline(iss, token, '|'))
            return false;
        message_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        source_node_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        dest_node_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        timestamp = std::stoull(token);

        if (!std::getline(iss, token, '|'))
            return false;
        term = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        leader_id = std::stoul(token);

        if (!std::getline(iss, token, '|'))
            return false;
        commit_index = std::stoul(token);

        return true;
    }

    std::unique_ptr<Message> create_message_from_data(const std::string &data)
    {
        if (data.empty())
            return nullptr;

        std::istringstream iss(data);
        std::string type_str;
        if (!std::getline(iss, type_str, '|'))
            return nullptr;

        MessageType type = static_cast<MessageType>(std::stoi(type_str));

        switch (type)
        {
        case MessageType::REQUEST_VOTE:
        {
            auto msg = std::make_unique<RequestVoteRPC>();
            if (msg->deserialize(data))
                return std::move(msg);
            break;
        }
        case MessageType::REQUEST_VOTE_RESPONSE:
        {
            auto msg = std::make_unique<RequestVoteResponse>();
            if (msg->deserialize(data))
                return std::move(msg);
            break;
        }
        case MessageType::APPEND_ENTRIES:
        {
            auto msg = std::make_unique<AppendEntriesRPC>();
            if (msg->deserialize(data))
                return std::move(msg);
            break;
        }
        case MessageType::APPEND_ENTRIES_RESPONSE:
        {
            auto msg = std::make_unique<AppendEntriesResponse>();
            if (msg->deserialize(data))
                return std::move(msg);
            break;
        }
        case MessageType::CLIENT_GET:
        case MessageType::CLIENT_PUT:
        case MessageType::CLIENT_DELETE:
        {
            auto msg = std::make_unique<ClientRequest>(type);
            if (msg->deserialize(data))
                return std::move(msg);
            break;
        }
        case MessageType::CLIENT_RESPONSE:
        {
            auto msg = std::make_unique<ClientResponse>();
            if (msg->deserialize(data))
                return std::move(msg);
            break;
        }
        case MessageType::HEARTBEAT:
        {
            auto msg = std::make_unique<HeartbeatMessage>();
            if (msg->deserialize(data))
                return std::move(msg);
            break;
        }
        default:
            break;
        }

        return nullptr;
    }

    std::string message_type_to_string(MessageType type)
    {
        switch (type)
        {
        case MessageType::REQUEST_VOTE:
            return "REQUEST_VOTE";
        case MessageType::REQUEST_VOTE_RESPONSE:
            return "REQUEST_VOTE_RESPONSE";
        case MessageType::APPEND_ENTRIES:
            return "APPEND_ENTRIES";
        case MessageType::APPEND_ENTRIES_RESPONSE:
            return "APPEND_ENTRIES_RESPONSE";
        case MessageType::CLIENT_GET:
            return "CLIENT_GET";
        case MessageType::CLIENT_PUT:
            return "CLIENT_PUT";
        case MessageType::CLIENT_DELETE:
            return "CLIENT_DELETE";
        case MessageType::CLIENT_RESPONSE:
            return "CLIENT_RESPONSE";
        case MessageType::HEARTBEAT:
            return "HEARTBEAT";
        default:
            return "UNKNOWN";
        }
    }

    MessageType string_to_message_type(const std::string &type_str)
    {
        if (type_str == "REQUEST_VOTE")
            return MessageType::REQUEST_VOTE;
        if (type_str == "REQUEST_VOTE_RESPONSE")
            return MessageType::REQUEST_VOTE_RESPONSE;
        if (type_str == "APPEND_ENTRIES")
            return MessageType::APPEND_ENTRIES;
        if (type_str == "APPEND_ENTRIES_RESPONSE")
            return MessageType::APPEND_ENTRIES_RESPONSE;
        if (type_str == "CLIENT_GET")
            return MessageType::CLIENT_GET;
        if (type_str == "CLIENT_PUT")
            return MessageType::CLIENT_PUT;
        if (type_str == "CLIENT_DELETE")
            return MessageType::CLIENT_DELETE;
        if (type_str == "CLIENT_RESPONSE")
            return MessageType::CLIENT_RESPONSE;
        if (type_str == "HEARTBEAT")
            return MessageType::HEARTBEAT;
        return MessageType::REQUEST_VOTE;
    }

} // namespace raft
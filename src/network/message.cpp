#include "message.h"
#include "../utils/codec.h"

namespace raft
{
    namespace
    {
        void write_header(Writer &w, const Message &m)
        {
            w.u8(static_cast<uint8_t>(m.type));
            w.u32(m.message_id);
            w.u32(m.source_node_id);
            w.u32(m.dest_node_id);
            w.u64(m.timestamp);
        }

        bool read_header(Reader &r, Message &m)
        {
            uint8_t type;
            if (!r.u8(type) || !r.u32(m.message_id) || !r.u32(m.source_node_id) ||
                !r.u32(m.dest_node_id) || !r.u64(m.timestamp))
                return false;
            m.type = static_cast<MessageType>(type);
            return true;
        }

        bool read_bool(Reader &r, bool &b)
        {
            uint8_t v;
            if (!r.u8(v))
                return false;
            b = v != 0;
            return true;
        }
    }

    std::string RequestVoteRPC::serialize() const
    {
        Writer w;
        write_header(w, *this);
        w.u32(term);
        w.u32(candidate_id);
        w.u32(last_log_index);
        w.u32(last_log_term);
        return w.take();
    }

    bool RequestVoteRPC::deserialize(const std::string &data)
    {
        Reader r(data);
        return read_header(r, *this) && r.u32(term) && r.u32(candidate_id) &&
               r.u32(last_log_index) && r.u32(last_log_term) && r.done();
    }

    std::string RequestVoteResponse::serialize() const
    {
        Writer w;
        write_header(w, *this);
        w.u32(term);
        w.u8(vote_granted ? 1 : 0);
        return w.take();
    }

    bool RequestVoteResponse::deserialize(const std::string &data)
    {
        Reader r(data);
        return read_header(r, *this) && r.u32(term) && read_bool(r, vote_granted) && r.done();
    }

    std::string AppendEntriesRPC::serialize() const
    {
        Writer w;
        write_header(w, *this);
        w.u32(term);
        w.u32(leader_id);
        w.u32(prev_log_index);
        w.u32(prev_log_term);
        w.u32(leader_commit);
        w.u32(static_cast<uint32_t>(entries.size()));
        for (const auto &entry : entries)
            entry.encode(w);
        return w.take();
    }

    bool AppendEntriesRPC::deserialize(const std::string &data)
    {
        Reader r(data);
        uint32_t count;
        if (!read_header(r, *this) || !r.u32(term) || !r.u32(leader_id) ||
            !r.u32(prev_log_index) || !r.u32(prev_log_term) || !r.u32(leader_commit) ||
            !r.u32(count))
            return false;

        entries.clear();
        for (uint32_t i = 0; i < count; ++i)
        {
            LogEntry entry;
            if (!entry.decode(r))
                return false;
            entries.push_back(std::move(entry));
        }
        return r.done();
    }

    std::string AppendEntriesResponse::serialize() const
    {
        Writer w;
        write_header(w, *this);
        w.u32(term);
        w.u8(success ? 1 : 0);
        w.u32(match_index);
        w.u32(conflict_index);
        w.u32(conflict_term);
        return w.take();
    }

    bool AppendEntriesResponse::deserialize(const std::string &data)
    {
        Reader r(data);
        return read_header(r, *this) && r.u32(term) && read_bool(r, success) &&
               r.u32(match_index) && r.u32(conflict_index) && r.u32(conflict_term) && r.done();
    }

    std::string ClientRequest::serialize() const
    {
        Writer w;
        write_header(w, *this);
        w.str(operation);
        w.str(key);
        w.str(value);
        w.u64(client_id);
        w.u64(sequence_num);
        return w.take();
    }

    bool ClientRequest::deserialize(const std::string &data)
    {
        Reader r(data);
        return read_header(r, *this) && r.str(operation) && r.str(key) && r.str(value) &&
               r.u64(client_id) && r.u64(sequence_num) && r.done();
    }

    std::string ClientResponse::serialize() const
    {
        Writer w;
        write_header(w, *this);
        w.u8(success ? 1 : 0);
        w.str(value);
        w.str(error_message);
        w.u32(leader_hint);
        return w.take();
    }

    bool ClientResponse::deserialize(const std::string &data)
    {
        Reader r(data);
        return read_header(r, *this) && read_bool(r, success) && r.str(value) &&
               r.str(error_message) && r.u32(leader_hint) && r.done();
    }

    std::string HeartbeatMessage::serialize() const
    {
        Writer w;
        write_header(w, *this);
        w.u32(term);
        w.u32(leader_id);
        w.u32(commit_index);
        return w.take();
    }

    bool HeartbeatMessage::deserialize(const std::string &data)
    {
        Reader r(data);
        return read_header(r, *this) && r.u32(term) && r.u32(leader_id) &&
               r.u32(commit_index) && r.done();
    }

    std::unique_ptr<Message> create_message_from_data(const std::string &data)
    {
        if (data.empty())
            return nullptr;

        auto type = static_cast<MessageType>(static_cast<uint8_t>(data[0]));
        std::unique_ptr<Message> msg;

        switch (type)
        {
        case MessageType::REQUEST_VOTE:
            msg = std::make_unique<RequestVoteRPC>();
            break;
        case MessageType::REQUEST_VOTE_RESPONSE:
            msg = std::make_unique<RequestVoteResponse>();
            break;
        case MessageType::APPEND_ENTRIES:
            msg = std::make_unique<AppendEntriesRPC>();
            break;
        case MessageType::APPEND_ENTRIES_RESPONSE:
            msg = std::make_unique<AppendEntriesResponse>();
            break;
        case MessageType::CLIENT_GET:
        case MessageType::CLIENT_PUT:
        case MessageType::CLIENT_DELETE:
            msg = std::make_unique<ClientRequest>(type);
            break;
        case MessageType::CLIENT_RESPONSE:
            msg = std::make_unique<ClientResponse>();
            break;
        case MessageType::HEARTBEAT:
            msg = std::make_unique<HeartbeatMessage>();
            break;
        default:
            return nullptr;
        }

        if (!msg->deserialize(data))
            return nullptr;
        return msg;
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
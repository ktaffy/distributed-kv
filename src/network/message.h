#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include <memory>
#include "../raft/log_entry.h"

namespace raft
{

    enum class MessageType : uint8_t
    {
        REQUEST_VOTE = 1,
        REQUEST_VOTE_RESPONSE = 2,
        APPEND_ENTRIES = 3,
        APPEND_ENTRIES_RESPONSE = 4,

        CLIENT_GET = 10,
        CLIENT_PUT = 11,
        CLIENT_DELETE = 12,
        CLIENT_RESPONSE = 13,

        ADD_NODE = 20,
        REMOVE_NODE = 21,

        HEARTBEAT = 30,
        PING = 31,
        PONG = 32
    };

    struct Message
    {
        MessageType type;
        uint32_t message_id;
        uint32_t source_node_id;
        uint32_t dest_node_id;
        uint64_t timestamp;

        Message(MessageType msg_type, uint32_t src, uint32_t dst)
            : type(msg_type), message_id(0), source_node_id(src),
              dest_node_id(dst), timestamp(0) {}

        virtual ~Message() = default;
        virtual std::string serialize() const = 0;
        virtual bool deserialize(const std::string &data) = 0;
    };

    struct RequestVoteRPC : public Message
    {
        uint32_t term;
        uint32_t candidate_id;
        uint32_t last_log_index;
        uint32_t last_log_term;

        RequestVoteRPC(uint32_t src = 0, uint32_t dst = 0)
            : Message(MessageType::REQUEST_VOTE, src, dst),
              term(0), candidate_id(0), last_log_index(0), last_log_term(0) {}

        std::string serialize() const override;
        bool deserialize(const std::string &data) override;
    };

    struct RequestVoteResponse : public Message
    {
        uint32_t term;
        bool vote_granted;

        RequestVoteResponse(uint32_t src = 0, uint32_t dst = 0)
            : Message(MessageType::REQUEST_VOTE_RESPONSE, src, dst),
              term(0), vote_granted(false) {}

        std::string serialize() const override;
        bool deserialize(const std::string &data) override;
    };

    struct AppendEntriesRPC : public Message
    {
        uint32_t term;
        uint32_t leader_id;
        uint32_t prev_log_index;
        uint32_t prev_log_term;
        uint32_t leader_commit;
        std::vector<LogEntry> entries;

        AppendEntriesRPC(uint32_t src = 0, uint32_t dst = 0)
            : Message(MessageType::APPEND_ENTRIES, src, dst),
              term(0), leader_id(0), prev_log_index(0),
              prev_log_term(0), leader_commit(0) {}

        std::string serialize() const override;
        bool deserialize(const std::string &data) override;
    };

    struct AppendEntriesResponse : public Message
    {
        uint32_t term;
        bool success;
        uint32_t match_index;
        uint32_t conflict_index;
        uint32_t conflict_term;

        AppendEntriesResponse(uint32_t src = 0, uint32_t dst = 0)
            : Message(MessageType::APPEND_ENTRIES_RESPONSE, src, dst),
              term(0), success(false), match_index(0),
              conflict_index(0), conflict_term(0) {}

        std::string serialize() const override;
        bool deserialize(const std::string &data) override;
    };

    struct ClientRequest : public Message
    {
        std::string operation;
        std::string key;
        std::string value;
        uint64_t client_id;
        uint64_t sequence_num;

        ClientRequest(MessageType op_type, uint32_t src = 0, uint32_t dst = 0)
            : Message(op_type, src, dst), client_id(0), sequence_num(0) {}

        std::string serialize() const override;
        bool deserialize(const std::string &data) override;
    };

    struct ClientResponse : public Message
    {
        bool success;
        std::string value;
        std::string error_message;
        uint32_t leader_hint;

        ClientResponse(uint32_t src = 0, uint32_t dst = 0)
            : Message(MessageType::CLIENT_RESPONSE, src, dst),
              success(false), leader_hint(0) {}

        std::string serialize() const override;
        bool deserialize(const std::string &data) override;
    };

    struct HeartbeatMessage : public Message
    {
        uint32_t term;
        uint32_t leader_id;
        uint32_t commit_index;

        HeartbeatMessage(uint32_t src = 0, uint32_t dst = 0)
            : Message(MessageType::HEARTBEAT, src, dst),
              term(0), leader_id(0), commit_index(0) {}

        std::string serialize() const override;
        bool deserialize(const std::string &data) override;
    };

    std::unique_ptr<Message> create_message_from_data(const std::string &data);

    std::string message_type_to_string(MessageType type);
    MessageType string_to_message_type(const std::string &type_str);

} // namespace raft
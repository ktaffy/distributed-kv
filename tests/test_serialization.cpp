#include "test_framework.h"
#include "network/message.h"
#include "raft/log_entry.h"

using namespace raft;

TEST(log_entry_round_trips_binary_command)
{
    std::string cmd("a|b\0c", 5);
    LogEntry in(3, 7, LogEntryType::CLIENT_COMMAND, cmd, 42, 9);
    LogEntry out;
    CHECK(out.deserialize(in.serialize()));
    CHECK(out == in);
    CHECK_EQ(out.timestamp, in.timestamp);
}

TEST(kv_operation_round_trips_through_log_entry)
{
    KVOperation op(KVOperation::Type::PUT, "k|ey", "va|lue");
    KVOperation out = KVOperation::from_log_entry(op.to_log_entry(1, 1));
    CHECK(out.operation == KVOperation::Type::PUT);
    CHECK_EQ(out.key, std::string("k|ey"));
    CHECK_EQ(out.value, std::string("va|lue"));
}

TEST(truncated_log_entry_is_rejected)
{
    std::string data = log_utils::create_no_op_entry(1, 1).serialize();
    data.pop_back();
    LogEntry out;
    CHECK(!out.deserialize(data));
}

TEST(request_vote_round_trips)
{
    RequestVoteRPC in(2, 1);
    in.term = 4;
    in.candidate_id = 2;
    in.last_log_index = 10;
    in.last_log_term = 3;

    auto msg = create_message_from_data(in.serialize());
    auto *out = dynamic_cast<RequestVoteRPC *>(msg.get());
    CHECK(out != nullptr);
    if (!out)
        return;
    CHECK_EQ(out->term, 4u);
    CHECK_EQ(out->candidate_id, 2u);
    CHECK_EQ(out->last_log_index, 10u);
    CHECK_EQ(out->source_node_id, 2u);
}

TEST(append_entries_round_trips_with_entries)
{
    AppendEntriesRPC in(1, 2);
    in.term = 5;
    in.leader_id = 1;
    in.prev_log_index = 3;
    in.prev_log_term = 4;
    in.leader_commit = 2;
    in.entries.push_back(KVOperation(KVOperation::Type::PUT, "a|b", "1").to_log_entry(5, 4));
    in.entries.push_back(log_utils::create_no_op_entry(5, 5));

    auto msg = create_message_from_data(in.serialize());
    auto *out = dynamic_cast<AppendEntriesRPC *>(msg.get());
    CHECK(out != nullptr);
    if (!out)
        return;
    CHECK_EQ(out->term, 5u);
    CHECK_EQ(out->prev_log_index, 3u);
    CHECK_EQ(out->entries.size(), 2u);
    CHECK(out->entries == in.entries);
}

TEST(client_response_round_trips)
{
    ClientResponse in(1, 9);
    in.success = true;
    in.value = "x|y";
    in.leader_hint = 3;

    auto msg = create_message_from_data(in.serialize());
    auto *out = dynamic_cast<ClientResponse *>(msg.get());
    CHECK(out != nullptr);
    if (!out)
        return;
    CHECK(out->success);
    CHECK_EQ(out->value, std::string("x|y"));
    CHECK_EQ(out->leader_hint, 3u);
}
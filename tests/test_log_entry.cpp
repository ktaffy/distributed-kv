#include "test_framework.h"
#include "raft/log_entry.h"

using namespace raft;

TEST(default_entry_is_invalid_noop)
{
    LogEntry e;
    CHECK(e.is_no_op());
    CHECK_EQ(e.index, 0u);
    CHECK(!log_utils::is_valid_log_entry(e));
}

TEST(noop_helper_builds_valid_entry)
{
    LogEntry e = log_utils::create_no_op_entry(3, 7);
    CHECK_EQ(e.term, 3u);
    CHECK_EQ(e.index, 7u);
    CHECK(e.is_no_op());
    CHECK(e.command.empty());
    CHECK(log_utils::is_valid_log_entry(e));
}

TEST(kv_entry_carries_client_id_and_sequence)
{
    KVOperation op(KVOperation::Type::PUT, "k", "v");
    LogEntry e = log_utils::create_kv_entry(2, 5, op, 42, 9);
    CHECK(e.is_client_command());
    CHECK_EQ(e.term, 2u);
    CHECK_EQ(e.index, 5u);
    CHECK_EQ(e.client_id, 42u);
    CHECK_EQ(e.sequence_num, 9u);
}

TEST(equality_ignores_timestamp)
{
    LogEntry a(1, 1, LogEntryType::CLIENT_COMMAND, "cmd");
    LogEntry b(1, 1, LogEntryType::CLIENT_COMMAND, "cmd");
    a.timestamp = 1;
    b.timestamp = 2;
    CHECK(a == b);

    b.command = "other";
    CHECK(a != b);
}
#include "test_framework.h"
#include "kvstore/state_machine.h"

using namespace raft;

static KVOperation put(const std::string &k, const std::string &v)
{
    return KVOperation(KVOperation::Type::PUT, k, v);
}

TEST(retried_put_does_not_overwrite_later_write)
{
    KVStateMachine sm;
    std::string out;

    sm.apply(put("k", "a"), 1, 1, out);
    sm.apply(put("k", "b"), 2, 1, out);
    sm.apply(put("k", "a"), 1, 1, out);

    std::string value;
    CHECK(sm.get("k", value));
    CHECK_EQ(value, std::string("b"));
}

TEST(retried_delete_returns_original_result)
{
    KVStateMachine sm;
    std::string out;

    sm.apply(put("k", "v"), 1, 1, out);
    CHECK(sm.apply(KVOperation(KVOperation::Type::DELETE, "k"), 1, 2, out));
    CHECK(sm.apply(KVOperation(KVOperation::Type::DELETE, "k"), 1, 2, out));
}

TEST(new_sequence_number_applies_normally)
{
    KVStateMachine sm;
    std::string out;

    sm.apply(put("k", "a"), 1, 1, out);
    sm.apply(put("k", "b"), 1, 2, out);

    std::string value;
    CHECK(sm.get("k", value));
    CHECK_EQ(value, std::string("b"));
}

TEST(requests_without_client_id_are_not_deduplicated)
{
    KVStateMachine sm;
    std::string out;

    sm.apply(put("k", "v"), 0, 0, out);
    CHECK(sm.apply(KVOperation(KVOperation::Type::DELETE, "k"), 0, 0, out));
    CHECK(!sm.apply(KVOperation(KVOperation::Type::DELETE, "k"), 0, 0, out));
}
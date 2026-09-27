#include "test_framework.h"
#include "raft_test_util.h"
#include <vector>

using namespace raft;
using kvtest::TempDir;
using kvtest::make_config;
using kvtest::request_vote;

TEST(fresh_node_is_follower_in_term_zero)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    CHECK(node.get_state() == NodeState::FOLLOWER);
    CHECK_EQ(node.get_current_term(), 0);
}

TEST(vote_survives_restart)
{
    TempDir dir;
    {
        RaftNode node(1, make_config(1, dir.path));
        CHECK(request_vote(node, 2, 1));
    }
    RaftNode restarted(1, make_config(1, dir.path));
    CHECK(!request_vote(restarted, 3, 1));
}

TEST(equal_term_step_down_keeps_vote)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    RaftNodeTestPeer peer(node);

    CHECK(request_vote(node, 2, 1));
    peer.become_follower(1);

    CHECK_EQ(peer.voted_for(), 2u);
    CHECK_EQ(peer.persisted_voted_for(), 2u);
    CHECK(!request_vote(node, 3, 1));
}

TEST(higher_term_step_down_clears_vote)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    RaftNodeTestPeer peer(node);

    CHECK(request_vote(node, 2, 1));
    peer.become_follower(2);

    CHECK_EQ(peer.voted_for(), 0u);
    CHECK(request_vote(node, 3, 2));
}

TEST(candidate_steps_down_on_higher_term_vote_reply)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    RaftNodeTestPeer peer(node);
    peer.set_role(NodeState::CANDIDATE, 1, 1);

    RequestVoteResponse resp(2, 1);
    resp.term = 2;
    peer.process_vote_response(resp);

    CHECK(node.get_state() == NodeState::FOLLOWER);
    CHECK_EQ(node.get_current_term(), 2);
}

TEST(leader_steps_down_on_higher_term_append_reply)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    RaftNodeTestPeer peer(node);
    peer.set_role(NodeState::LEADER, 1, 1);

    AppendEntriesResponse resp(2, 1);
    resp.term = 3;
    peer.process_append_entries_response(2, resp);

    CHECK(node.get_state() == NodeState::FOLLOWER);
    CHECK_EQ(node.get_current_term(), 3);
}

TEST(candidate_yields_to_equal_term_leader)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    RaftNodeTestPeer peer(node);
    peer.set_role(NodeState::CANDIDATE, 1, 1);

    AppendEntriesRPC req(2, 1);
    req.term = 1;
    req.leader_id = 2;
    AppendEntriesResponse resp;
    node.handle_append_entries(req, resp);

    CHECK(node.get_state() == NodeState::FOLLOWER);
    CHECK_EQ(node.get_leader_id(), 2);
    CHECK_EQ(peer.voted_for(), 1u);
}

static LogEntry kv(uint32_t term, uint32_t index)
{
    return log_utils::create_kv_entry(term, index, KVOperation(KVOperation::Type::PUT, "k", "v"));
}

static AppendEntriesResponse append_entries(RaftNode &node, uint32_t term, uint32_t prev_index,
                                            uint32_t prev_term, std::vector<LogEntry> entries,
                                            uint32_t leader_commit = 0)
{
    AppendEntriesRPC req(2, 1);
    req.term = term;
    req.leader_id = 2;
    req.prev_log_index = prev_index;
    req.prev_log_term = prev_term;
    req.leader_commit = leader_commit;
    req.entries = std::move(entries);
    AppendEntriesResponse resp;
    node.handle_append_entries(req, resp);
    return resp;
}

TEST(match_index_comes_from_request_not_log)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    RaftNodeTestPeer peer(node);
    peer.append(kv(1, 1));
    peer.append(kv(1, 2));
    peer.append(kv(1, 3));

    auto resp = append_entries(node, 1, 1, 1, {kv(1, 2)});

    CHECK(resp.success);
    CHECK_EQ(resp.match_index, 2u);
    CHECK_EQ(peer.last_index(), 3u);
}

TEST(retried_append_does_not_duplicate)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    RaftNodeTestPeer peer(node);

    append_entries(node, 1, 0, 0, {kv(1, 1), kv(1, 2)});
    auto resp = append_entries(node, 1, 0, 0, {kv(1, 1), kv(1, 2)});

    CHECK(resp.success);
    CHECK_EQ(peer.last_index(), 2u);
}

TEST(conflicting_suffix_is_replaced)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    RaftNodeTestPeer peer(node);
    peer.append(kv(1, 1));
    peer.append(kv(1, 2));
    peer.append(kv(1, 3));

    auto resp = append_entries(node, 2, 1, 1, {kv(2, 2)});

    CHECK(resp.success);
    CHECK_EQ(peer.last_index(), 2u);
    CHECK_EQ(peer.entry_at(2).term, 2u);
}

TEST(follower_commit_bounded_by_last_new_entry)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    RaftNodeTestPeer peer(node);
    peer.append(kv(1, 1));
    peer.append(kv(1, 2));
    peer.append(kv(1, 3));

    append_entries(node, 2, 1, 1, {}, 3);

    CHECK_EQ(peer.commit_index(), 1u);
}

TEST(heartbeat_never_lowers_commit)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    RaftNodeTestPeer peer(node);
    peer.append(kv(1, 1));
    peer.append(kv(1, 2));
    peer.set_commit_index(2);

    append_entries(node, 1, 1, 1, {}, 3);

    CHECK_EQ(peer.commit_index(), 2u);
}

TEST(conflict_index_is_first_index_of_conflicting_term)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    RaftNodeTestPeer peer(node);
    peer.append(kv(1, 1));
    peer.append(kv(1, 2));

    auto resp = append_entries(node, 2, 2, 2, {});

    CHECK(!resp.success);
    CHECK_EQ(resp.conflict_term, 1u);
    CHECK_EQ(resp.conflict_index, 1u);
}

TEST(new_leader_appends_noop_in_its_term)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    RaftNodeTestPeer peer(node);
    peer.append(kv(1, 1));
    peer.set_role(NodeState::CANDIDATE, 2, 1);

    peer.become_leader();

    CHECK_EQ(peer.last_index(), 2u);
    CHECK(peer.entry_at(2).is_no_op());
    CHECK_EQ(peer.entry_at(2).term, 2u);
    CHECK_EQ(peer.next_index(2), 2u);
}

TEST(leader_backoff_with_empty_log_does_not_underflow)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    RaftNodeTestPeer peer(node);
    peer.set_role(NodeState::LEADER, 1, 1);

    AppendEntriesResponse resp(2, 1);
    resp.term = 1;
    resp.conflict_term = 1;
    resp.conflict_index = 1;
    peer.process_append_entries_response(2, resp);

    CHECK_EQ(peer.next_index(2), 1u);
}

TEST(leader_skips_to_end_of_matching_term)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    RaftNodeTestPeer peer(node);
    peer.append(kv(1, 1));
    peer.append(kv(2, 2));
    peer.append(kv(2, 3));
    peer.append(kv(3, 4));
    peer.set_role(NodeState::LEADER, 3, 1);

    AppendEntriesResponse resp(2, 1);
    resp.term = 3;
    resp.conflict_term = 2;
    resp.conflict_index = 2;
    peer.process_append_entries_response(2, resp);

    CHECK_EQ(peer.next_index(2), 4u);
}

TEST(stale_success_reply_does_not_lower_match_index)
{
    TempDir dir;
    RaftNode node(1, make_config(1, dir.path));
    RaftNodeTestPeer peer(node);
    peer.set_role(NodeState::LEADER, 1, 1);

    AppendEntriesResponse fresh(2, 1);
    fresh.term = 1;
    fresh.success = true;
    fresh.match_index = 5;
    peer.process_append_entries_response(2, fresh);

    AppendEntriesResponse stale = fresh;
    stale.match_index = 3;
    peer.process_append_entries_response(2, stale);

    CHECK_EQ(peer.match_index(2), 5u);
    CHECK_EQ(peer.next_index(2), 6u);
}
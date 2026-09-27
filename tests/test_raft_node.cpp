#include "test_framework.h"
#include "raft_test_util.h"
#include <vector>
#include <chrono>
#include <thread>

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

TEST(single_node_elects_itself_and_stops_promptly)
{
    TempDir dir;
    auto config = make_config(1, dir.path, 1);
    config.set_listen_port(0);
    RaftNode node(1, config);

    auto start = std::chrono::steady_clock::now();
    node.start();

    for (int i = 0; i < 200 && !node.is_leader(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

    CHECK(node.is_leader());
    node.stop();

    auto elapsed = std::chrono::steady_clock::now() - start;
    CHECK(elapsed < std::chrono::seconds(3));
}

TEST(three_node_cluster_elects_one_leader)
{
    TempDir d1, d2, d3;
    auto ports = kvtest::free_ports(3);
    RaftNode n1(1, make_config(1, d1.path, ports));
    RaftNode n2(2, make_config(2, d2.path, ports));
    RaftNode n3(3, make_config(3, d3.path, ports));
    std::vector<RaftNode *> nodes{&n1, &n2, &n3};

    for (auto *n : nodes)
        n->start();

    auto leader_count = [&] {
        int count = 0;
        for (auto *n : nodes)
            count += n->is_leader() ? 1 : 0;
        return count;
    };
    auto agreed = [&] {
        int leader = nodes[0]->get_leader_id();
        for (auto *n : nodes)
            if (n->get_leader_id() == 0 || n->get_leader_id() != leader)
                return false;
        return true;
    };

    for (int i = 0; i < 500 && !(leader_count() == 1 && agreed()); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));

    CHECK_EQ(leader_count(), 1);
    CHECK(agreed());

    for (auto *n : nodes)
        n->stop();
}

template <typename Pred>
static bool eventually(Pred pred, int timeout_ms = 3000)
{
    for (int waited = 0; waited < timeout_ms; waited += 10)
    {
        if (pred())
            return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

TEST(single_node_applies_put_get_delete)
{
    TempDir dir;
    auto config = make_config(1, dir.path, 1);
    config.set_listen_port(0);
    RaftNode node(1, config);
    node.start();
    CHECK(eventually([&] { return node.is_leader(); }));

    auto put = node.submit(KVOperation(KVOperation::Type::PUT, "k", "v"), std::chrono::seconds(1));
    CHECK(put.ok);

    auto get = node.submit(KVOperation(KVOperation::Type::GET, "k"), std::chrono::seconds(1));
    CHECK(get.ok);
    CHECK(get.found);
    CHECK_EQ(get.value, std::string("v"));

    auto del = node.submit(KVOperation(KVOperation::Type::DELETE, "k"), std::chrono::seconds(1));
    CHECK(del.ok);
    CHECK(del.found);

    auto missing = node.submit(KVOperation(KVOperation::Type::GET, "k"), std::chrono::seconds(1));
    CHECK(missing.ok);
    CHECK(!missing.found);

    node.stop();
}

TEST(cluster_replicates_committed_writes)
{
    TempDir d1, d2, d3;
    auto ports = kvtest::free_ports(3);
    RaftNode n1(1, make_config(1, d1.path, ports));
    RaftNode n2(2, make_config(2, d2.path, ports));
    RaftNode n3(3, make_config(3, d3.path, ports));
    std::vector<RaftNode *> nodes{&n1, &n2, &n3};

    for (auto *n : nodes)
        n->start();

    RaftNode *leader = nullptr;
    CHECK(eventually([&] {
        for (auto *n : nodes)
            if (n->is_leader())
                leader = n;
        return leader != nullptr;
    }));
    if (!leader)
        return;

    auto put = leader->submit(KVOperation(KVOperation::Type::PUT, "k", "v"), std::chrono::seconds(2));
    CHECK(put.ok);

    for (auto *n : nodes)
    {
        RaftNodeTestPeer peer(*n);
        CHECK(eventually([&] {
            std::string value;
            return peer.sm_get("k", value) && value == "v";
        }));
    }

    for (auto *n : nodes)
    {
        if (n == leader)
            continue;
        auto rejected = n->submit(KVOperation(KVOperation::Type::PUT, "x", "y"), std::chrono::seconds(1));
        CHECK(!rejected.ok);
        CHECK_EQ(rejected.leader_hint, leader->get_leader_id());
    }

    for (auto *n : nodes)
        n->stop();
}


#include "test_framework.h"
#include "raft_test_util.h"

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
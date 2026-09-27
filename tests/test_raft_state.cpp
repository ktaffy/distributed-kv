#include "test_framework.h"
#include "raft/raft_state.h"

using raft::RaftState;

TEST(starts_as_follower_in_term_zero)
{
    RaftState state(1);
    CHECK(state.is_follower());
    CHECK_EQ(state.get_current_term(), 0u);
    CHECK_EQ(state.get_voted_for(), 0u);
    CHECK_EQ(state.get_commit_index(), 0u);
    CHECK_EQ(state.get_last_applied(), 0u);
}

TEST(majority_is_floor_half_plus_one)
{
    RaftState three(1);
    three.set_cluster_nodes({1, 2, 3});
    CHECK_EQ(three.get_majority_size(), 2u);

    RaftState five(1);
    five.set_cluster_nodes({1, 2, 3, 4, 5});
    CHECK_EQ(five.get_majority_size(), 3u);
}

TEST(self_vote_alone_is_not_a_majority)
{
    RaftState state(1);
    state.set_cluster_nodes({1, 2, 3});
    state.reset_election_state();
    CHECK_EQ(state.get_vote_count(), 1u);
    CHECK(!state.has_majority_votes());
}

TEST(one_granted_peer_vote_wins_three_node_election)
{
    RaftState state(1);
    state.set_cluster_nodes({1, 2, 3});
    state.reset_election_state();
    state.record_vote(2, true);
    CHECK(state.has_majority_votes());
}

TEST(denied_votes_do_not_count)
{
    RaftState state(1);
    state.set_cluster_nodes({1, 2, 3});
    state.reset_election_state();
    state.record_vote(2, false);
    state.record_vote(3, false);
    CHECK_EQ(state.get_vote_count(), 1u);
    CHECK(!state.has_majority_votes());
}

TEST(duplicate_vote_from_same_peer_counts_once)
{
    RaftState state(1);
    state.set_cluster_nodes({1, 2, 3, 4, 5});
    state.reset_election_state();
    state.record_vote(2, true);
    state.record_vote(2, true);
    CHECK_EQ(state.get_vote_count(), 2u);
    CHECK(!state.has_majority_votes());
}

TEST(leader_state_starts_after_last_log_index)
{
    RaftState state(1);
    state.set_cluster_nodes({1, 2, 3});
    state.initialize_leader_state(5);
    CHECK_EQ(state.get_next_index(2), 6u);
    CHECK_EQ(state.get_next_index(3), 6u);
    CHECK_EQ(state.get_match_index(2), 0u);
    CHECK_EQ(state.get_match_index(3), 0u);
}
#include "raft_state.h"
#include <algorithm>
#include <sstream>

namespace raft
{

    RaftState::RaftState(uint32_t node_id)
        : node_id_(node_id), current_state_(NodeState::FOLLOWER),
          current_term_(0), voted_for_(INVALID_NODE_ID), leader_id_(INVALID_NODE_ID),
          commit_index_(0), last_applied_(0) {}

    NodeState RaftState::get_state() const
    {
        return current_state_.load();
    }

    void RaftState::set_state(NodeState new_state)
    {
        current_state_.store(new_state);
    }

    uint32_t RaftState::get_current_term() const
    {
        return current_term_.load();
    }

    void RaftState::set_current_term(uint32_t term)
    {
        current_term_.store(term);
    }

    void RaftState::increment_current_term()
    {
        current_term_.fetch_add(1);
    }

    uint32_t RaftState::get_voted_for() const
    {
        return voted_for_.load();
    }

    void RaftState::set_voted_for(uint32_t candidate_id)
    {
        voted_for_.store(candidate_id);
    }

    void RaftState::clear_voted_for()
    {
        voted_for_.store(INVALID_NODE_ID);
    }

    uint32_t RaftState::get_leader_id() const
    {
        return leader_id_.load();
    }

    void RaftState::set_leader_id(uint32_t leader)
    {
        leader_id_.store(leader);
    }

    void RaftState::clear_leader_id()
    {
        leader_id_.store(INVALID_NODE_ID);
    }

    uint32_t RaftState::get_commit_index() const
    {
        return commit_index_.load();
    }

    void RaftState::set_commit_index(uint32_t index)
    {
        commit_index_.store(index);
    }

    uint32_t RaftState::get_last_applied() const
    {
        return last_applied_.load();
    }

    void RaftState::set_last_applied(uint32_t index)
    {
        last_applied_.store(index);
    }

    uint32_t RaftState::get_node_id() const
    {
        return node_id_;
    }

    std::vector<uint32_t> RaftState::get_cluster_nodes() const
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        return cluster_nodes_;
    }

    void RaftState::set_cluster_nodes(const std::vector<uint32_t> &nodes)
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        cluster_nodes_ = nodes;
    }

    void RaftState::add_cluster_node(uint32_t node_id)
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (std::find(cluster_nodes_.begin(), cluster_nodes_.end(), node_id) == cluster_nodes_.end())
        {
            cluster_nodes_.push_back(node_id);
        }
    }

    void RaftState::remove_cluster_node(uint32_t node_id)
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        cluster_nodes_.erase(
            std::remove(cluster_nodes_.begin(), cluster_nodes_.end(), node_id),
            cluster_nodes_.end());
    }

    bool RaftState::is_cluster_node(uint32_t node_id) const
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        return std::find(cluster_nodes_.begin(), cluster_nodes_.end(), node_id) != cluster_nodes_.end();
    }

    size_t RaftState::get_cluster_size() const
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        return cluster_nodes_.size();
    }

    size_t RaftState::get_majority_size() const
    {
        return (get_cluster_size() / 2) + 1;
    }

    void RaftState::reset_election_state()
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        votes_received_.clear();
        votes_received_[node_id_] = true;
    }

    void RaftState::record_vote(uint32_t node_id, bool granted)
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        votes_received_[node_id] = granted;
    }

    bool RaftState::has_majority_votes() const
    {
        std::lock_guard<std::mutex> lock(state_mutex_);

        size_t positive_votes = 0;
        for (const auto &vote : votes_received_)
        {
            if (vote.second)
            {
                positive_votes++;
            }
        }

        return positive_votes >= get_majority_size();
    }

    size_t RaftState::get_vote_count() const
    {
        std::lock_guard<std::mutex> lock(state_mutex_);

        size_t positive_votes = 0;
        for (const auto &vote : votes_received_)
        {
            if (vote.second)
            {
                positive_votes++;
            }
        }

        return positive_votes;
    }

    void RaftState::set_next_index(uint32_t node_id, uint32_t index)
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        next_index_[node_id] = index;
    }

    uint32_t RaftState::get_next_index(uint32_t node_id) const
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto it = next_index_.find(node_id);
        return (it != next_index_.end()) ? it->second : 1;
    }

    void RaftState::increment_next_index(uint32_t node_id)
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        next_index_[node_id]++;
    }

    void RaftState::decrement_next_index(uint32_t node_id)
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (next_index_[node_id] > 1)
        {
            next_index_[node_id]--;
        }
    }

    void RaftState::set_match_index(uint32_t node_id, uint32_t index)
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        match_index_[node_id] = index;
    }

    uint32_t RaftState::get_match_index(uint32_t node_id) const
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto it = match_index_.find(node_id);
        return (it != match_index_.end()) ? it->second : 0;
    }

    void RaftState::reset_leader_state()
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        next_index_.clear();
        match_index_.clear();
    }

    void RaftState::initialize_leader_state(uint32_t last_log_index)
    {
        std::lock_guard<std::mutex> lock(state_mutex_);

        next_index_.clear();
        match_index_.clear();

        for (uint32_t node_id : cluster_nodes_)
        {
            if (node_id != node_id_)
            {
                next_index_[node_id] = last_log_index + 1;
                match_index_[node_id] = 0;
            }
        }
    }

    bool RaftState::is_leader() const
    {
        return current_state_.load() == NodeState::LEADER;
    }

    bool RaftState::is_candidate() const
    {
        return current_state_.load() == NodeState::CANDIDATE;
    }

    bool RaftState::is_follower() const
    {
        return current_state_.load() == NodeState::FOLLOWER;
    }

    std::string RaftState::state_to_string() const
    {
        switch (current_state_.load())
        {
        case NodeState::FOLLOWER:
            return "FOLLOWER";
        case NodeState::CANDIDATE:
            return "CANDIDATE";
        case NodeState::LEADER:
            return "LEADER";
        default:
            return "UNKNOWN";
        }
    }

    std::string RaftState::get_debug_info() const
    {
        std::ostringstream oss;
        oss << "RaftState{";
        oss << "node_id=" << node_id_;
        oss << ", state=" << state_to_string();
        oss << ", term=" << current_term_.load();
        oss << ", voted_for=" << voted_for_.load();
        oss << ", leader_id=" << leader_id_.load();
        oss << ", commit_index=" << commit_index_.load();
        oss << ", last_applied=" << last_applied_.load();
        oss << ", cluster_size=" << get_cluster_size();

        if (is_candidate())
        {
            oss << ", votes=" << get_vote_count() << "/" << get_majority_size();
        }

        if (is_leader())
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            oss << ", next_index={";
            bool first = true;
            for (const auto &pair : next_index_)
            {
                if (!first)
                    oss << ",";
                oss << pair.first << ":" << pair.second;
                first = false;
            }
            oss << "}";

            oss << ", match_index={";
            first = true;
            for (const auto &pair : match_index_)
            {
                if (!first)
                    oss << ",";
                oss << pair.first << ":" << pair.second;
                first = false;
            }
            oss << "}";
        }

        oss << "}";
        return oss.str();
    }

} // namespace raft
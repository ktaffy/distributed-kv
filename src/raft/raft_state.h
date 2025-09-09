#pragma once

#include <atomic>
#include <mutex>
#include <vector>
#include <unordered_map>
#include "log_entry.h"

namespace raft
{

    enum class NodeState : uint8_t
    {
        FOLLOWER = 0,
        CANDIDATE = 1,
        LEADER = 2
    };

    class RaftState
    {
    public:
        RaftState(uint32_t node_id);
        ~RaftState() = default;

        NodeState get_state() const;
        void set_state(NodeState new_state);

        uint32_t get_current_term() const;
        void set_current_term(uint32_t term);
        void increment_current_term();

        uint32_t get_voted_for() const;
        void set_voted_for(uint32_t candidate_id);
        void clear_voted_for();

        uint32_t get_leader_id() const;
        void set_leader_id(uint32_t leader);
        void clear_leader_id();

        uint32_t get_commit_index() const;
        void set_commit_index(uint32_t index);

        uint32_t get_last_applied() const;
        void set_last_applied(uint32_t index);

        uint32_t get_node_id() const;

        std::vector<uint32_t> get_cluster_nodes() const;
        void set_cluster_nodes(const std::vector<uint32_t> &nodes);
        void add_cluster_node(uint32_t node_id);
        void remove_cluster_node(uint32_t node_id);
        bool is_cluster_node(uint32_t node_id) const;
        size_t get_cluster_size() const;
        size_t get_majority_size() const;

        void reset_election_state();
        void record_vote(uint32_t node_id, bool granted);
        bool has_majority_votes() const;
        size_t get_vote_count() const;

        void set_next_index(uint32_t node_id, uint32_t index);
        uint32_t get_next_index(uint32_t node_id) const;
        void increment_next_index(uint32_t node_id);
        void decrement_next_index(uint32_t node_id);

        void set_match_index(uint32_t node_id, uint32_t index);
        uint32_t get_match_index(uint32_t node_id) const;

        void reset_leader_state();
        void initialize_leader_state(uint32_t last_log_index);

        bool is_leader() const;
        bool is_candidate() const;
        bool is_follower() const;

        std::string state_to_string() const;
        std::string get_debug_info() const;

    private:
        mutable std::mutex state_mutex_;

        const uint32_t node_id_;

        std::atomic<NodeState> current_state_;
        std::atomic<uint32_t> current_term_;
        std::atomic<uint32_t> voted_for_;
        std::atomic<uint32_t> leader_id_;
        std::atomic<uint32_t> commit_index_;
        std::atomic<uint32_t> last_applied_;

        std::vector<uint32_t> cluster_nodes_;

        std::unordered_map<uint32_t, bool> votes_received_;

        std::unordered_map<uint32_t, uint32_t> next_index_;
        std::unordered_map<uint32_t, uint32_t> match_index_;

        static constexpr uint32_t INVALID_NODE_ID = 0;
    };

} // namespace raft
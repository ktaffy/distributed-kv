#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>
#include <optional>

#include "../kvstore/state_machine.h"
#include "raft_state.h"
#include "log_entry.h"
#include "../network/network_manager.h"
#include "../network/message.h"
#include "../persistence/persistent_state.h"
#include "../persistence/log_storage.h"
#include "../utils/logger.h"
#include "../utils/config.h"

namespace raft
{

    class RaftNode
    {
    friend class RaftNodeTestPeer;
    public:
        struct ClientResult
        {
            bool ok = false;
            bool found = false;
            std::string value;
            std::string error;
            int leader_hint = 0;
        };
        
        ClientResult submit(const KVOperation &op, std::chrono::milliseconds timeout,
            uint64_t client_id = 0, uint64_t sequence_num = 0);
        RaftNode(int node_id, const Config &config);
        ~RaftNode();

        void start();
        void stop();

        bool is_leader() const;
        bool is_running() const;
        int get_current_term() const;
        int get_leader_id() const;
        NodeState get_state() const;

        // Raft RPC handlers
        void handle_request_vote(const RequestVoteRPC &request, RequestVoteResponse &response);
        void handle_append_entries(const AppendEntriesRPC &request, AppendEntriesResponse &response);
        void handle_connection_change(uint32_t node_id, bool connected);
        void handle_message(std::unique_ptr<Message> message);

        size_t get_log_size() const;
        const LogEntry &get_log_entry(size_t index) const;

    private:
        struct ApplyResult
        {
            uint32_t term = 0;
            bool found = false;
            std::string value;
        };
        
        KVStateMachine state_machine_;
        std::unordered_map<uint32_t, std::optional<ApplyResult>> waiters_;
        void flush_outbox();
        void send_outbox(std::unique_lock<std::mutex> &lock);

        std::vector<std::unique_ptr<Message>> outbox_;
        void become_follower(int term);
        void become_candidate();
        void become_leader();

        void start_election();
        void send_request_vote();
        void process_vote_response(const RequestVoteResponse &response);

        void send_append_entries();
        void send_append_entries_to_node(int node_id);
        void process_append_entries_response(int node_id, const AppendEntriesResponse &response);

        void election_timer_thread();
        void heartbeat_timer_thread();

        int generate_election_timeout();
        void reset_election_timeout();
        bool should_grant_vote(const RequestVoteRPC &request) const;
        void advance_commit_index();
        void apply_committed_entries();

        const int node_id_;
        const Config config_;

        std::unique_ptr<RaftState> state_;
        std::unique_ptr<PersistentState> persistent_state_;
        std::unique_ptr<LogStorage> log_storage_;

        std::unique_ptr<NetworkManager> network_manager_;
        std::vector<int> peer_nodes_;

        mutable std::mutex state_mutex_;
        std::condition_variable state_cv_;
        std::atomic<bool> running_;

        std::thread election_timer_thread_;
        std::thread heartbeat_timer_thread_;

        std::atomic<std::chrono::steady_clock::time_point> last_heartbeat_;
        std::atomic<int> election_timeout_ms_;
        std::unordered_map<int, bool> votes_received_;

        std::unordered_map<int, int> next_index_;
        std::unordered_map<int, int> match_index_;
        std::chrono::steady_clock::time_point last_heartbeat_sent_;

        std::random_device rd_;
        std::mt19937 gen_;
        std::uniform_int_distribution<> timeout_dist_;

        std::unique_ptr<Logger> logger_;

        static constexpr int MIN_ELECTION_TIMEOUT_MS = 150;
        static constexpr int MAX_ELECTION_TIMEOUT_MS = 300;
        static constexpr int HEARTBEAT_INTERVAL_MS = 50;
        static constexpr int RPC_TIMEOUT_MS = 100;
    };

} // namespace raft
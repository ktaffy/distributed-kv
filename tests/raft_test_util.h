#pragma once

#include <atomic>
#include <filesystem>
#include <mutex>
#include <string>
#include <system_error>
#include <unistd.h>

#include "raft/raft_node.h"
#include "utils/config.h"

namespace raft
{
    class RaftNodeTestPeer
    {
    public:
        explicit RaftNodeTestPeer(RaftNode &node) : node_(node) {}

        void set_role(NodeState state, uint32_t term, uint32_t voted_for = 0)
        {
            std::lock_guard<std::mutex> lock(node_.state_mutex_);
            node_.state_->set_state(state);
            node_.state_->set_current_term(term);
            node_.state_->set_voted_for(voted_for);
        }

        void become_follower(int term)
        {
            std::lock_guard<std::mutex> lock(node_.state_mutex_);
            node_.become_follower(term);
        }

        void process_vote_response(const RequestVoteResponse &resp)
        {
            node_.process_vote_response(resp);
        }

        void process_append_entries_response(int from, const AppendEntriesResponse &resp)
        {
            node_.process_append_entries_response(from, resp);
        }

        uint32_t voted_for() const { return node_.state_->get_voted_for(); }
        uint32_t persisted_voted_for() const { return node_.persistent_state_->get_voted_for(); }

        void become_leader()
        {
            std::lock_guard<std::mutex> lock(node_.state_mutex_);
            node_.become_leader();
        }

        void append(const LogEntry &entry) { node_.log_storage_->append_entry(entry); }
        LogEntry entry_at(uint32_t index) const { return node_.log_storage_->get_entry(index); }
        uint32_t last_index() const { return node_.log_storage_->get_last_index(); }
        uint32_t commit_index() const { return node_.state_->get_commit_index(); }
        void set_commit_index(uint32_t index) { node_.state_->set_commit_index(index); }
        uint32_t next_index(uint32_t peer) const { return node_.state_->get_next_index(peer); }
        uint32_t match_index(uint32_t peer) const { return node_.state_->get_match_index(peer); }

    private:
        RaftNode &node_;
    };
}

namespace kvtest
{
    struct TempDir
    {
        std::filesystem::path path;

        TempDir()
        {
            static std::atomic<int> counter{0};
            path = std::filesystem::temp_directory_path() /
                   ("kvtest_" + std::to_string(::getpid()) + "_" + std::to_string(counter++));
            std::filesystem::create_directories(path);
        }

        ~TempDir()
        {
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
    };

    inline raft::Config make_config(uint32_t self, const std::filesystem::path &data_dir)
    {
        raft::Config config;
        config.set_node_id(self);
        config.set_listen_address("127.0.0.1");
        config.set_listen_port(static_cast<uint16_t>(19000 + self));
        config.set_data_directory(data_dir.string());

        for (uint32_t id = 1; id <= 3; ++id)
        {
            raft::NodeConfig node;
            node.node_id = id;
            node.address = "127.0.0.1";
            node.port = static_cast<uint16_t>(19000 + id);
            config.add_cluster_node(node);
        }
        return config;
    }

    inline bool request_vote(raft::RaftNode &node, uint32_t candidate, uint32_t term)
    {
        raft::RequestVoteRPC req(candidate, 0);
        req.term = term;
        req.candidate_id = candidate;
        raft::RequestVoteResponse resp;
        node.handle_request_vote(req, resp);
        return resp.vote_granted;
    }
}
#pragma once

#include <atomic>
#include <filesystem>
#include <mutex>
#include <string>
#include <system_error>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <vector>

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

        bool sm_get(const std::string &key, std::string &value)
        {
            std::lock_guard<std::mutex> lock(node_.state_mutex_);
            return node_.state_machine_.get(key, value);
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
    inline std::vector<uint16_t> free_ports(size_t count)
    {
        std::vector<int> fds;
        std::vector<uint16_t> ports;

        for (size_t i = 0; i < count; ++i)
        {
            int fd = socket(AF_INET, SOCK_STREAM, 0);
            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));

            socklen_t len = sizeof(addr);
            getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len);
            ports.push_back(ntohs(addr.sin_port));
            fds.push_back(fd);
        }

        for (int fd : fds)
            close(fd);
        return ports;
    }

    inline raft::Config make_config(uint32_t self, const std::filesystem::path &data_dir,
                                    const std::vector<uint16_t> &ports)
    {
        raft::Config config;
        config.set_node_id(self);
        config.set_listen_address("127.0.0.1");
        config.set_listen_port(ports[self - 1]);
        config.set_data_directory(data_dir.string());

        for (uint32_t id = 1; id <= ports.size(); ++id)
        {
            raft::NodeConfig node;
            node.node_id = id;
            node.address = "127.0.0.1";
            node.port = ports[id - 1];
            config.add_cluster_node(node);
        }
        return config;
    }
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

    inline raft::Config make_config(uint32_t self, const std::filesystem::path &data_dir, uint32_t cluster_size = 3)
    {
        raft::Config config;
        config.set_node_id(self);
        config.set_listen_address("127.0.0.1");
        config.set_listen_port(static_cast<uint16_t>(19000 + self));
        config.set_data_directory(data_dir.string());

        for (uint32_t id = 1; id <= cluster_size; ++id)
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
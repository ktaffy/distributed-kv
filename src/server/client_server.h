#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include "../raft/raft_node.h"
#include "../utils/config.h"

namespace raft
{
    inline uint16_t client_port_for(uint16_t raft_port)
    {
        return static_cast<uint16_t>(raft_port + 1000);
    }

    class ClientServer
    {
    public:
        ClientServer(RaftNode &node, const Config &config);
        ~ClientServer();

        bool start(uint16_t port);
        void stop();
        uint16_t port() const { return port_; }

    private:
        void accept_loop();
        void serve(int fd);
        std::string handle(const std::string &frame);

        RaftNode &node_;
        Config config_;
        std::atomic<bool> running_{false};
        int listen_fd_ = -1;
        uint16_t port_ = 0;
        std::thread accept_thread_;
        std::mutex mutex_;
        std::condition_variable idle_cv_;
        std::unordered_set<int> client_fds_;
        int active_ = 0;
    };
}
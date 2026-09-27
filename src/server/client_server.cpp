#include "client_server.h"
#include "../network/framing.h"
#include <algorithm>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>

namespace raft
{
    ClientServer::ClientServer(RaftNode &node, const Config &config)
        : node_(node), config_(config) {}

    ClientServer::~ClientServer()
    {
        stop();
    }

    bool ClientServer::start(uint16_t port)
    {
        listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0)
            return false;

        int reuse = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        inet_pton(AF_INET, config_.get_listen_address().c_str(), &addr.sin_addr);

        if (bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0 ||
            listen(listen_fd_, 64) < 0)
        {
            close(listen_fd_);
            listen_fd_ = -1;
            return false;
        }

        socklen_t len = sizeof(addr);
        getsockname(listen_fd_, reinterpret_cast<sockaddr *>(&addr), &len);
        port_ = ntohs(addr.sin_port);

        running_.store(true);
        accept_thread_ = std::thread(&ClientServer::accept_loop, this);
        return true;
    }

    void ClientServer::stop()
    {
        if (!running_.exchange(false))
            return;

        shutdown(listen_fd_, SHUT_RDWR);
        if (accept_thread_.joinable())
            accept_thread_.join();
        close(listen_fd_);
        listen_fd_ = -1;

        std::unique_lock<std::mutex> lock(mutex_);
        for (int fd : client_fds_)
            shutdown(fd, SHUT_RDWR);
        idle_cv_.wait(lock, [this] { return active_ == 0; });
    }

    void ClientServer::accept_loop()
    {
        while (running_.load())
        {
            int fd = accept(listen_fd_, nullptr, nullptr);
            if (fd < 0)
            {
                if (running_.load())
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            int one = 1;
            setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

            std::lock_guard<std::mutex> lock(mutex_);
            if (!running_.load())
            {
                close(fd);
                break;
            }
            client_fds_.insert(fd);
            ++active_;
            std::thread(&ClientServer::serve, this, fd).detach();
        }
    }

    void ClientServer::serve(int fd)
    {
        std::string frame;
        while (read_frame(fd, frame) && write_frame(fd, handle(frame)))
        {
        }

        std::lock_guard<std::mutex> lock(mutex_);
        client_fds_.erase(fd);
        close(fd);
        --active_;
        idle_cv_.notify_all();
    }

    std::string ClientServer::handle(const std::string &frame)
    {
        ClientResponse response(config_.get_node_id(), 0);

        auto message = create_message_from_data(frame);
        auto *request = dynamic_cast<ClientRequest *>(message.get());
        if (!request)
        {
            response.error_message = "bad request";
            return response.serialize();
        }
        response.message_id = request->message_id;

        KVOperation::Type type = KVOperation::Type::GET;
        if (request->type == MessageType::CLIENT_PUT)
            type = KVOperation::Type::PUT;
        else if (request->type == MessageType::CLIENT_DELETE)
            type = KVOperation::Type::DELETE;

        auto timeout = std::chrono::milliseconds(std::max<uint32_t>(config_.get_client_timeout_ms(), 1000));
        auto result = node_.submit(KVOperation(type, request->key, request->value), timeout, request->client_id, request->sequence_num);

        response.success = result.ok;
        response.found = result.found;
        response.value = result.value;
        response.error_message = result.error;
        response.leader_hint = static_cast<uint32_t>(result.leader_hint);

        if (result.leader_hint > 0)
        {
            NodeConfig leader = config_.get_node_config(result.leader_hint);
            if (leader.node_id != 0)
                response.leader_address = leader.address + ":" + std::to_string(client_port_for(leader.port));
        }

        return response.serialize();
    }
}
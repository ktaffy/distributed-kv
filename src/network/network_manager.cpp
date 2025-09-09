#include "network_manager.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <algorithm>

namespace raft
{

    NetworkManager::NetworkManager(uint32_t node_id, const std::string &listen_address, uint16_t listen_port)
        : node_id_(node_id), listen_address_(listen_address), listen_port_(listen_port),
          running_(false), server_socket_(-1), next_message_id_(1),
          connection_timeout_(DEFAULT_CONNECTION_TIMEOUT),
          send_timeout_(DEFAULT_SEND_TIMEOUT),
          heartbeat_interval_(DEFAULT_HEARTBEAT_INTERVAL)
    {
        reset_stats();
    }

    NetworkManager::~NetworkManager()
    {
        stop();
    }

    bool NetworkManager::start()
    {
        if (running_.load())
        {
            return false;
        }

        server_socket_ = create_server_socket();
        if (server_socket_ < 0)
        {
            return false;
        }

        running_.store(true);

        listen_thread_ = std::thread(&NetworkManager::listen_thread, this);
        connection_manager_thread_ = std::thread(&NetworkManager::connection_manager_thread, this);
        message_processor_thread_ = std::thread(&NetworkManager::message_processor_thread, this);
        heartbeat_thread_ = std::thread(&NetworkManager::heartbeat_thread, this);
        request_timeout_thread_ = std::thread(&NetworkManager::request_timeout_thread, this);

        return true;
    }

    void NetworkManager::stop()
    {
        if (!running_.load())
        {
            return;
        }

        running_.store(false);

        if (server_socket_ >= 0)
        {
            close(server_socket_);
            server_socket_ = -1;
        }

        {
            std::lock_guard<std::mutex> lock(connections_mutex_);
            for (auto &pair : connections_)
            {
                cleanup_connection(pair.second.get());
            }
            connections_.clear();
            node_connections_.clear();
        }

        messages_cv_.notify_all();

        if (listen_thread_.joinable())
            listen_thread_.join();
        if (connection_manager_thread_.joinable())
            connection_manager_thread_.join();
        if (message_processor_thread_.joinable())
            message_processor_thread_.join();
        if (heartbeat_thread_.joinable())
            heartbeat_thread_.join();
        if (request_timeout_thread_.joinable())
            request_timeout_thread_.join();
    }

    void NetworkManager::set_message_handler(MessageHandler handler)
    {
        message_handler_ = std::move(handler);
    }

    void NetworkManager::set_connection_callback(ConnectionCallback callback)
    {
        connection_callback_ = std::move(callback);
    }

    bool NetworkManager::send_message(uint32_t target_node_id, std::unique_ptr<Message> message)
    {
        if (!running_.load())
        {
            return false;
        }

        message->source_node_id = node_id_;
        message->dest_node_id = target_node_id;
        message->timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();

        std::lock_guard<std::mutex> lock(connections_mutex_);
        auto it = node_connections_.find(target_node_id);

        if (it == node_connections_.end() || !it->second->connected.load())
        {
            {
                std::lock_guard<std::mutex> stats_lock(stats_mutex_);
                stats_.dropped_messages++;
            }
            return false;
        }

        std::string serialized_data = message->serialize();
        bool success = send_raw_data(it->second, serialized_data);

        {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            if (success)
            {
                stats_.messages_sent++;
                stats_.bytes_sent += serialized_data.size();
            }
            else
            {
                stats_.dropped_messages++;
            }
        }

        return success;
    }

    std::future<std::unique_ptr<Message>> NetworkManager::send_request(uint32_t target_node_id,
                                                                       std::unique_ptr<Message> request,
                                                                       std::chrono::milliseconds timeout)
    {
        uint32_t message_id = generate_message_id();
        request->message_id = message_id;

        auto pending_request = std::make_unique<PendingRequest>(message_id);
        auto future = pending_request->promise.get_future();
        pending_request->deadline = std::chrono::steady_clock::now() + timeout;

        {
            std::lock_guard<std::mutex> lock(requests_mutex_);
            pending_requests_[message_id] = std::move(pending_request);
        }

        if (!send_message(target_node_id, std::move(request)))
        {
            std::lock_guard<std::mutex> lock(requests_mutex_);
            auto it = pending_requests_.find(message_id);
            if (it != pending_requests_.end())
            {
                it->second->promise.set_value(nullptr);
                pending_requests_.erase(it);
            }
        }

        return future;
    }

    bool NetworkManager::add_peer(uint32_t node_id, const std::string &address, uint16_t port)
    {
        std::lock_guard<std::mutex> lock(peers_mutex_);

        if (peers_.find(node_id) != peers_.end())
        {
            return false;
        }

        peers_[node_id] = std::make_unique<PeerInfo>(node_id, address, port);
        return true;
    }

    void NetworkManager::remove_peer(uint32_t node_id)
    {
        std::lock_guard<std::mutex> peers_lock(peers_mutex_);
        std::lock_guard<std::mutex> connections_lock(connections_mutex_);

        auto peer_it = peers_.find(node_id);
        if (peer_it != peers_.end())
        {
            if (peer_it->second->connection)
            {
                cleanup_connection(peer_it->second->connection.get());
            }
            peers_.erase(peer_it);
        }

        auto conn_it = node_connections_.find(node_id);
        if (conn_it != node_connections_.end())
        {
            node_connections_.erase(conn_it);
        }
    }

    bool NetworkManager::is_connected(uint32_t node_id) const
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        auto it = node_connections_.find(node_id);
        return it != node_connections_.end() && it->second->connected.load();
    }

    std::vector<uint32_t> NetworkManager::get_connected_peers() const
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        std::vector<uint32_t> connected_peers;

        for (const auto &pair : node_connections_)
        {
            if (pair.second->connected.load())
            {
                connected_peers.push_back(pair.first);
            }
        }

        return connected_peers;
    }

    size_t NetworkManager::get_connection_count() const
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        return node_connections_.size();
    }

    std::string NetworkManager::get_peer_address(uint32_t node_id) const
    {
        std::lock_guard<std::mutex> lock(peers_mutex_);
        auto it = peers_.find(node_id);
        return (it != peers_.end()) ? it->second->address : "";
    }

    uint16_t NetworkManager::get_peer_port(uint32_t node_id) const
    {
        std::lock_guard<std::mutex> lock(peers_mutex_);
        auto it = peers_.find(node_id);
        return (it != peers_.end()) ? it->second->port : 0;
    }

    void NetworkManager::set_connection_timeout(std::chrono::milliseconds timeout)
    {
        connection_timeout_ = timeout;
    }

    void NetworkManager::set_send_timeout(std::chrono::milliseconds timeout)
    {
        send_timeout_ = timeout;
    }

    void NetworkManager::set_heartbeat_interval(std::chrono::milliseconds interval)
    {
        heartbeat_interval_ = interval;
    }

    bool NetworkManager::is_running() const
    {
        return running_.load();
    }

    NetworkManager::NetworkStats NetworkManager::get_stats() const
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        return stats_;
    }

    void NetworkManager::reset_stats()
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_ = NetworkStats{};
    }

    void NetworkManager::listen_thread()
    {
        while (running_.load())
        {
            sockaddr_in client_addr;
            socklen_t client_addr_len = sizeof(client_addr);

            int client_socket = accept(server_socket_,
                                       reinterpret_cast<sockaddr *>(&client_addr),
                                       &client_addr_len);

            if (client_socket < 0)
            {
                if (running_.load())
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
                continue;
            }

            set_socket_options(client_socket);

            char client_ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);

            handle_new_connection(client_socket, std::string(client_ip));
        }
    }

    void NetworkManager::connection_manager_thread()
    {
        while (running_.load())
        {
            {
                std::lock_guard<std::mutex> peers_lock(peers_mutex_);
                std::lock_guard<std::mutex> connections_lock(connections_mutex_);

                for (auto &pair : peers_)
                {
                    PeerInfo &peer = *pair.second;

                    if (!peer.connection || !peer.connection->connected.load())
                    {
                        auto now = std::chrono::steady_clock::now();
                        if (now - peer.last_attempt > std::chrono::seconds(1))
                        {
                            if (connect_to_peer(peer))
                            {
                                {
                                    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
                                    stats_.connection_attempts++;
                                }
                            }
                            else
                            {
                                peer.retry_count++;
                                {
                                    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
                                    stats_.failed_connections++;
                                }
                            }
                            peer.last_attempt = now;
                        }
                    }
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }

    void NetworkManager::message_processor_thread()
    {
        while (running_.load())
        {
            std::unique_lock<std::mutex> lock(messages_mutex_);
            messages_cv_.wait(lock, [this]
                              { return !incoming_messages_.empty() || !running_.load(); });

            while (!incoming_messages_.empty())
            {
                auto message = std::move(incoming_messages_.front());
                incoming_messages_.pop();
                lock.unlock();

                process_received_message(std::move(message));

                lock.lock();
            }
        }
    }

    void NetworkManager::heartbeat_thread()
    {
        while (running_.load())
        {
            {
                std::lock_guard<std::mutex> lock(connections_mutex_);
                for (const auto &pair : node_connections_)
                {
                    if (pair.second->connected.load())
                    {
                        auto heartbeat = std::make_unique<HeartbeatMessage>(node_id_, pair.first);
                        send_message(pair.first, std::move(heartbeat));
                    }
                }
            }

            std::this_thread::sleep_for(heartbeat_interval_);
        }
    }

    void NetworkManager::request_timeout_thread()
    {
        while (running_.load())
        {
            cleanup_expired_requests();
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }

    void NetworkManager::handle_new_connection(int client_socket, const std::string &client_addr)
    {
        auto connection = std::make_unique<Connection>();
        connection->socket_fd = client_socket;
        connection->remote_address = client_addr;
        connection->connected.store(true);
        connection->last_activity = std::chrono::steady_clock::now();

        Connection *conn_ptr = connection.get();

        {
            std::lock_guard<std::mutex> lock(connections_mutex_);
            connections_[client_socket] = std::move(connection);
        }

        std::thread([this, conn_ptr]()
                    { handle_connection_data(conn_ptr); })
            .detach();
    }

    void NetworkManager::handle_connection_data(Connection *conn)
    {
        while (running_.load() && conn->connected.load())
        {
            std::string data = receive_raw_data(conn);

            if (data.empty())
            {
                break;
            }

            auto message = create_message_from_data(data);
            if (message)
            {
                {
                    std::lock_guard<std::mutex> stats_lock(stats_mutex_);
                    stats_.messages_received++;
                    stats_.bytes_received += data.size();
                }

                std::lock_guard<std::mutex> lock(messages_mutex_);
                incoming_messages_.push(std::move(message));
                messages_cv_.notify_one();
            }

            conn->last_activity = std::chrono::steady_clock::now();
        }

        cleanup_connection(conn);
    }

    void NetworkManager::process_received_message(std::unique_ptr<Message> message)
    {
        if (message->type == MessageType::REQUEST_VOTE_RESPONSE ||
            message->type == MessageType::APPEND_ENTRIES_RESPONSE ||
            message->type == MessageType::CLIENT_RESPONSE)
        {
            handle_request_response(std::move(message));
        }
        else if (message_handler_)
        {
            message_handler_(std::move(message));
        }
    }

    bool NetworkManager::connect_to_peer(PeerInfo &peer)
    {
        int sock = create_client_socket();
        if (sock < 0)
        {
            return false;
        }

        sockaddr_in server_addr{};
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(peer.port);

        if (inet_pton(AF_INET, peer.address.c_str(), &server_addr.sin_addr) <= 0)
        {
            close(sock);
            return false;
        }

        if (connect(sock, reinterpret_cast<sockaddr *>(&server_addr), sizeof(server_addr)) < 0)
        {
            close(sock);
            return false;
        }

        set_socket_options(sock);

        auto connection = std::make_unique<Connection>();
        connection->socket_fd = sock;
        connection->remote_address = peer.address;
        connection->remote_port = peer.port;
        connection->node_id = peer.node_id;
        connection->connected.store(true);
        connection->last_activity = std::chrono::steady_clock::now();

        Connection *conn_ptr = connection.get();
        peer.connection = std::move(connection);

        {
            std::lock_guard<std::mutex> lock(connections_mutex_);
            node_connections_[peer.node_id] = peer.connection.get();
        }

        if (connection_callback_)
        {
            connection_callback_(peer.node_id, true);
        }

        std::thread([this, conn_ptr]()
                    { handle_connection_data(conn_ptr); })
            .detach();

        return true;
    }

    void NetworkManager::disconnect_peer(uint32_t node_id)
    {
        std::lock_guard<std::mutex> connections_lock(connections_mutex_);
        auto it = node_connections_.find(node_id);

        if (it != node_connections_.end())
        {
            cleanup_connection(it->second);
            node_connections_.erase(it);

            if (connection_callback_)
            {
                connection_callback_(node_id, false);
            }
        }
    }

    void NetworkManager::cleanup_connection(Connection *conn)
    {
        if (!conn)
            return;

        conn->connected.store(false);

        if (conn->socket_fd >= 0)
        {
            close(conn->socket_fd);
            conn->socket_fd = -1;
        }
    }

    bool NetworkManager::send_raw_data(Connection *conn, const std::string &data)
    {
        if (!conn || !conn->connected.load())
        {
            return false;
        }

        uint32_t data_size = data.size();

        ssize_t bytes_sent = send(conn->socket_fd, &data_size, sizeof(data_size), MSG_NOSIGNAL);
        if (bytes_sent != sizeof(data_size))
        {
            return false;
        }

        bytes_sent = send(conn->socket_fd, data.data(), data.size(), MSG_NOSIGNAL);
        return bytes_sent == static_cast<ssize_t>(data.size());
    }

    std::string NetworkManager::receive_raw_data(Connection *conn)
    {
        if (!conn || !conn->connected.load())
        {
            return "";
        }

        uint32_t data_size;
        ssize_t bytes_received = recv(conn->socket_fd, &data_size, sizeof(data_size), MSG_WAITALL);

        if (bytes_received != sizeof(data_size))
        {
            return "";
        }

        if (data_size > MAX_MESSAGE_SIZE)
        {
            return "";
        }

        std::string data(data_size, '\0');
        bytes_received = recv(conn->socket_fd, &data[0], data_size, MSG_WAITALL);

        if (bytes_received != static_cast<ssize_t>(data_size))
        {
            return "";
        }

        return data;
    }

    uint32_t NetworkManager::generate_message_id()
    {
        return next_message_id_.fetch_add(1);
    }

    void NetworkManager::handle_request_response(std::unique_ptr<Message> message)
    {
        std::lock_guard<std::mutex> lock(requests_mutex_);
        auto it = pending_requests_.find(message->message_id);

        if (it != pending_requests_.end())
        {
            it->second->promise.set_value(std::move(message));
            pending_requests_.erase(it);
        }
    }

    void NetworkManager::cleanup_expired_requests()
    {
        std::lock_guard<std::mutex> lock(requests_mutex_);
        auto now = std::chrono::steady_clock::now();

        auto it = pending_requests_.begin();
        while (it != pending_requests_.end())
        {
            if (now > it->second->deadline)
            {
                it->second->promise.set_value(nullptr);
                it = pending_requests_.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }

    int NetworkManager::create_server_socket()
    {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0)
        {
            return -1;
        }

        set_socket_options(sock);

        sockaddr_in server_addr{};
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(listen_port_);

        if (inet_pton(AF_INET, listen_address_.c_str(), &server_addr.sin_addr) <= 0)
        {
            close(sock);
            return -1;
        }

        if (bind(sock, reinterpret_cast<sockaddr *>(&server_addr), sizeof(server_addr)) < 0)
        {
            close(sock);
            return -1;
        }

        if (listen(sock, 10) < 0)
        {
            close(sock);
            return -1;
        }

        return sock;
    }

    int NetworkManager::create_client_socket()
    {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0)
        {
            return -1;
        }

        set_socket_options(sock);
        return sock;
    }

    void NetworkManager::set_socket_options(int socket_fd)
    {
        int reuse = 1;
        setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

        timeval timeout;
        timeout.tv_sec = send_timeout_.count() / 1000;
        timeout.tv_usec = (send_timeout_.count() % 1000) * 1000;
        setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    }

} // namespace raft
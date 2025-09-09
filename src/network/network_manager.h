#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <thread>
#include <mutex>
#include <atomic>
#include <queue>
#include <condition_variable>
#include <functional>
#include <future>
#include <chrono>
#include "message.h"

namespace raft
{

    struct Connection
    {
        int socket_fd;
        std::string remote_address;
        uint16_t remote_port;
        uint32_t node_id;
        std::atomic<bool> connected;
        std::chrono::steady_clock::time_point last_activity;

        Connection() : socket_fd(-1), remote_port(0), node_id(0), connected(false) {}
    };

    class NetworkManager
    {
    public:
        using MessageHandler = std::function<void(std::unique_ptr<Message>)>;
        using ConnectionCallback = std::function<void(uint32_t node_id, bool connected)>;

        NetworkManager(uint32_t node_id, const std::string &listen_address, uint16_t listen_port);
        ~NetworkManager();

        bool start();
        void stop();

        void set_message_handler(MessageHandler handler);
        void set_connection_callback(ConnectionCallback callback);

        bool send_message(uint32_t target_node_id, std::unique_ptr<Message> message);
        std::future<std::unique_ptr<Message>> send_request(uint32_t target_node_id,
                                                           std::unique_ptr<Message> request,
                                                           std::chrono::milliseconds timeout);

        bool add_peer(uint32_t node_id, const std::string &address, uint16_t port);
        void remove_peer(uint32_t node_id);

        bool is_connected(uint32_t node_id) const;
        std::vector<uint32_t> get_connected_peers() const;
        size_t get_connection_count() const;

        std::string get_peer_address(uint32_t node_id) const;
        uint16_t get_peer_port(uint32_t node_id) const;

        void set_connection_timeout(std::chrono::milliseconds timeout);
        void set_send_timeout(std::chrono::milliseconds timeout);
        void set_heartbeat_interval(std::chrono::milliseconds interval);

        bool is_running() const;

        struct NetworkStats
        {
            uint64_t messages_sent;
            uint64_t messages_received;
            uint64_t bytes_sent;
            uint64_t bytes_received;
            uint64_t connection_attempts;
            uint64_t failed_connections;
            uint64_t dropped_messages;
        };

        NetworkStats get_stats() const;
        void reset_stats();

    private:
        struct PendingRequest
        {
            uint32_t message_id;
            std::promise<std::unique_ptr<Message>> promise;
            std::chrono::steady_clock::time_point deadline;

            PendingRequest(uint32_t id) : message_id(id) {}
        };

        struct PeerInfo
        {
            uint32_t node_id;
            std::string address;
            uint16_t port;
            std::unique_ptr<Connection> connection;
            std::chrono::steady_clock::time_point last_attempt;
            uint32_t retry_count;

            PeerInfo(uint32_t id, const std::string &addr, uint16_t p)
                : node_id(id), address(addr), port(p), retry_count(0) {}
        };

        void listen_thread();
        void connection_manager_thread();
        void message_processor_thread();
        void heartbeat_thread();
        void request_timeout_thread();

        void handle_new_connection(int client_socket, const std::string &client_addr);
        void handle_connection_data(Connection *conn);
        void process_received_message(std::unique_ptr<Message> message);

        bool connect_to_peer(PeerInfo &peer);
        void disconnect_peer(uint32_t node_id);
        void cleanup_connection(Connection *conn);

        bool send_raw_data(Connection *conn, const std::string &data);
        std::string receive_raw_data(Connection *conn);

        uint32_t generate_message_id();
        void handle_request_response(std::unique_ptr<Message> message);
        void cleanup_expired_requests();

        int create_server_socket();
        int create_client_socket();
        void set_socket_options(int socket_fd);

        const uint32_t node_id_;
        const std::string listen_address_;
        const uint16_t listen_port_;

        std::atomic<bool> running_;
        int server_socket_;

        std::unordered_map<uint32_t, std::unique_ptr<PeerInfo>> peers_;
        std::unordered_map<int, std::unique_ptr<Connection>> connections_;
        std::unordered_map<uint32_t, Connection *> node_connections_;

        std::queue<std::unique_ptr<Message>> incoming_messages_;
        std::unordered_map<uint32_t, std::unique_ptr<PendingRequest>> pending_requests_;

        MessageHandler message_handler_;
        ConnectionCallback connection_callback_;

        std::atomic<uint32_t> next_message_id_;

        std::chrono::milliseconds connection_timeout_;
        std::chrono::milliseconds send_timeout_;
        std::chrono::milliseconds heartbeat_interval_;

        mutable std::mutex peers_mutex_;
        mutable std::mutex connections_mutex_;
        mutable std::mutex messages_mutex_;
        mutable std::mutex requests_mutex_;

        std::condition_variable messages_cv_;

        std::thread listen_thread_;
        std::thread connection_manager_thread_;
        std::thread message_processor_thread_;
        std::thread heartbeat_thread_;
        std::thread request_timeout_thread_;

        NetworkStats stats_;
        mutable std::mutex stats_mutex_;

        static constexpr size_t MAX_MESSAGE_SIZE = 1024 * 1024;
        static constexpr size_t BUFFER_SIZE = 8192;
        static constexpr uint32_t MAX_RETRY_COUNT = 5;
        static constexpr std::chrono::milliseconds DEFAULT_CONNECTION_TIMEOUT{5000};
        static constexpr std::chrono::milliseconds DEFAULT_SEND_TIMEOUT{1000};
        static constexpr std::chrono::milliseconds DEFAULT_HEARTBEAT_INTERVAL{30000};
    };

} // namespace raft
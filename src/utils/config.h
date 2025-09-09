#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>

namespace raft
{

    struct NodeConfig
    {
        uint32_t node_id;
        std::string address;
        uint16_t port;
        std::string data_dir;

        NodeConfig() : node_id(0), port(0) {}

        std::string get_endpoint() const
        {
            return address + ":" + std::to_string(port);
        }
    };

    class Config
    {
    public:
        Config();
        explicit Config(const std::string &config_file);

        bool load_from_file(const std::string &config_file);
        bool load_from_string(const std::string &config_data);

        uint32_t get_node_id() const { return node_id_; }
        void set_node_id(uint32_t id) { node_id_ = id; }

        std::string get_listen_address() const { return listen_address_; }
        void set_listen_address(const std::string &addr) { listen_address_ = addr; }

        uint16_t get_listen_port() const { return listen_port_; }
        void set_listen_port(uint16_t port) { listen_port_ = port; }

        std::string get_data_directory() const { return data_directory_; }
        void set_data_directory(const std::string &dir) { data_directory_ = dir; }

        std::string get_log_level() const { return log_level_; }
        void set_log_level(const std::string &level) { log_level_ = level; }

        uint32_t get_election_timeout_min_ms() const { return election_timeout_min_ms_; }
        void set_election_timeout_min_ms(uint32_t ms) { election_timeout_min_ms_ = ms; }

        uint32_t get_election_timeout_max_ms() const { return election_timeout_max_ms_; }
        void set_election_timeout_max_ms(uint32_t ms) { election_timeout_max_ms_ = ms; }

        uint32_t get_heartbeat_interval_ms() const { return heartbeat_interval_ms_; }
        void set_heartbeat_interval_ms(uint32_t ms) { heartbeat_interval_ms_ = ms; }

        uint32_t get_rpc_timeout_ms() const { return rpc_timeout_ms_; }
        void set_rpc_timeout_ms(uint32_t ms) { rpc_timeout_ms_ = ms; }

        uint32_t get_max_log_size_mb() const { return max_log_size_mb_; }
        void set_max_log_size_mb(uint32_t mb) { max_log_size_mb_ = mb; }

        uint32_t get_snapshot_interval() const { return snapshot_interval_; }
        void set_snapshot_interval(uint32_t interval) { snapshot_interval_ = interval; }

        bool get_fsync_enabled() const { return fsync_enabled_; }
        void set_fsync_enabled(bool enabled) { fsync_enabled_ = enabled; }

        uint32_t get_replication_factor() const { return replication_factor_; }
        void set_replication_factor(uint32_t factor) { replication_factor_ = factor; }

        uint32_t get_max_entries_per_request() const { return max_entries_per_request_; }
        void set_max_entries_per_request(uint32_t count) { max_entries_per_request_ = count; }

        uint32_t get_client_timeout_ms() const { return client_timeout_ms_; }
        void set_client_timeout_ms(uint32_t ms) { client_timeout_ms_ = ms; }

        std::vector<NodeConfig> get_cluster_nodes() const { return cluster_nodes_; }
        void set_cluster_nodes(const std::vector<NodeConfig> &nodes) { cluster_nodes_ = nodes; }
        void add_cluster_node(const NodeConfig &node) { cluster_nodes_.push_back(node); }

        NodeConfig get_node_config(uint32_t node_id) const;
        bool has_node(uint32_t node_id) const;
        size_t get_cluster_size() const { return cluster_nodes_.size(); }

        bool validate() const;
        std::string to_string() const;

        static Config create_default_config(uint32_t node_id, uint16_t port);
        static Config create_cluster_config(const std::vector<std::pair<uint32_t, uint16_t>> &nodes);

    private:
        uint32_t node_id_;
        std::string listen_address_;
        uint16_t listen_port_;
        std::string data_directory_;
        std::string log_level_;

        uint32_t election_timeout_min_ms_;
        uint32_t election_timeout_max_ms_;
        uint32_t heartbeat_interval_ms_;
        uint32_t rpc_timeout_ms_;

        uint32_t max_log_size_mb_;
        uint32_t snapshot_interval_;
        bool fsync_enabled_;

        uint32_t replication_factor_;
        uint32_t max_entries_per_request_;
        uint32_t client_timeout_ms_;

        std::vector<NodeConfig> cluster_nodes_;

        bool parse_config_line(const std::string &line);
        std::string trim(const std::string &str) const;
        std::vector<std::string> split(const std::string &str, char delimiter) const;

        void set_defaults();
    };

} // namespace raft
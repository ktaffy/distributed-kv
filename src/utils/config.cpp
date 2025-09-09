#include "config.h"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <iostream>

namespace raft
{

    Config::Config()
    {
        set_defaults();
    }

    Config::Config(const std::string &config_file)
    {
        set_defaults();
        load_from_file(config_file);
    }

    void Config::set_defaults()
    {
        node_id_ = 1;
        listen_address_ = "127.0.0.1";
        listen_port_ = 8001;
        data_directory_ = "./data/node1";
        log_level_ = "INFO";

        election_timeout_min_ms_ = 150;
        election_timeout_max_ms_ = 300;
        heartbeat_interval_ms_ = 50;
        rpc_timeout_ms_ = 100;

        max_log_size_mb_ = 100;
        snapshot_interval_ = 1000;
        fsync_enabled_ = true;

        replication_factor_ = 3;
        max_entries_per_request_ = 100;
        client_timeout_ms_ = 5000;

        cluster_nodes_.clear();
    }

    bool Config::load_from_file(const std::string &config_file)
    {
        std::ifstream file(config_file);
        if (!file.is_open())
        {
            return false;
        }

        std::string line;
        while (std::getline(file, line))
        {
            if (!parse_config_line(line))
            {
                return false;
            }
        }

        return true;
    }

    bool Config::load_from_string(const std::string &config_data)
    {
        std::istringstream iss(config_data);
        std::string line;

        while (std::getline(iss, line))
        {
            if (!parse_config_line(line))
            {
                return false;
            }
        }

        return true;
    }

    bool Config::parse_config_line(const std::string &line)
    {
        std::string trimmed = trim(line);

        if (trimmed.empty() || trimmed[0] == '#')
        {
            return true;
        }

        size_t eq_pos = trimmed.find('=');
        if (eq_pos == std::string::npos)
        {
            return true;
        }

        std::string key = trim(trimmed.substr(0, eq_pos));
        std::string value = trim(trimmed.substr(eq_pos + 1));

        if (key == "node_id")
        {
            node_id_ = std::stoul(value);
        }
        else if (key == "listen_address")
        {
            listen_address_ = value;
        }
        else if (key == "listen_port")
        {
            listen_port_ = std::stoul(value);
        }
        else if (key == "data_dir")
        {
            data_directory_ = value;
        }
        else if (key == "log_level")
        {
            log_level_ = value;
        }
        else if (key == "election_timeout_min_ms")
        {
            election_timeout_min_ms_ = std::stoul(value);
        }
        else if (key == "election_timeout_max_ms")
        {
            election_timeout_max_ms_ = std::stoul(value);
        }
        else if (key == "heartbeat_interval_ms")
        {
            heartbeat_interval_ms_ = std::stoul(value);
        }
        else if (key == "rpc_timeout_ms")
        {
            rpc_timeout_ms_ = std::stoul(value);
        }
        else if (key == "max_log_size_mb")
        {
            max_log_size_mb_ = std::stoul(value);
        }
        else if (key == "snapshot_interval")
        {
            snapshot_interval_ = std::stoul(value);
        }
        else if (key == "fsync_enabled")
        {
            fsync_enabled_ = (value == "true" || value == "1");
        }
        else if (key == "replication_factor")
        {
            replication_factor_ = std::stoul(value);
        }
        else if (key == "max_entries_per_request")
        {
            max_entries_per_request_ = std::stoul(value);
        }
        else if (key == "client_timeout_ms")
        {
            client_timeout_ms_ = std::stoul(value);
        }
        else if (key == "cluster_nodes")
        {
            cluster_nodes_.clear();
            auto node_specs = split(value, ',');
            for (const auto &spec : node_specs)
            {
                auto parts = split(spec, ':');
                if (parts.size() == 3)
                {
                    NodeConfig node;
                    node.node_id = std::stoul(parts[0]);
                    node.address = parts[1];
                    node.port = std::stoul(parts[2]);
                    cluster_nodes_.push_back(node);
                }
            }
        }

        return true;
    }

    std::string Config::trim(const std::string &str) const
    {
        size_t start = str.find_first_not_of(" \t\r\n");
        if (start == std::string::npos)
        {
            return "";
        }

        size_t end = str.find_last_not_of(" \t\r\n");
        return str.substr(start, end - start + 1);
    }

    std::vector<std::string> Config::split(const std::string &str, char delimiter) const
    {
        std::vector<std::string> tokens;
        std::istringstream iss(str);
        std::string token;

        while (std::getline(iss, token, delimiter))
        {
            tokens.push_back(trim(token));
        }

        return tokens;
    }

    NodeConfig Config::get_node_config(uint32_t node_id) const
    {
        for (const auto &node : cluster_nodes_)
        {
            if (node.node_id == node_id)
            {
                return node;
            }
        }
        return NodeConfig();
    }

    bool Config::has_node(uint32_t node_id) const
    {
        for (const auto &node : cluster_nodes_)
        {
            if (node.node_id == node_id)
            {
                return true;
            }
        }
        return false;
    }

    bool Config::validate() const
    {
        if (node_id_ == 0)
        {
            std::cerr << "Error: node_id must be non-zero" << std::endl;
            return false;
        }

        if (listen_port_ == 0 || listen_port_ > 65535)
        {
            std::cerr << "Error: invalid listen_port: " << listen_port_ << std::endl;
            return false;
        }

        if (data_directory_.empty())
        {
            std::cerr << "Error: data_directory cannot be empty" << std::endl;
            return false;
        }

        if (election_timeout_min_ms_ >= election_timeout_max_ms_)
        {
            std::cerr << "Error: election_timeout_min_ms must be less than election_timeout_max_ms" << std::endl;
            return false;
        }

        if (heartbeat_interval_ms_ >= election_timeout_min_ms_)
        {
            std::cerr << "Error: heartbeat_interval_ms must be less than election_timeout_min_ms" << std::endl;
            return false;
        }

        if (replication_factor_ == 0)
        {
            std::cerr << "Error: replication_factor must be non-zero" << std::endl;
            return false;
        }

        if (cluster_nodes_.empty())
        {
            std::cerr << "Error: cluster_nodes cannot be empty" << std::endl;
            return false;
        }

        if (replication_factor_ > cluster_nodes_.size())
        {
            std::cerr << "Error: replication_factor (" << replication_factor_
                      << ") cannot exceed cluster size (" << cluster_nodes_.size() << ")" << std::endl;
            return false;
        }

        bool found_self = false;
        for (const auto &node : cluster_nodes_)
        {
            if (node.node_id == node_id_)
            {
                found_self = true;
                if (node.address != listen_address_ || node.port != listen_port_)
                {
                    std::cerr << "Warning: cluster configuration for node " << node_id_
                              << " doesn't match listen address/port" << std::endl;
                }
                break;
            }
        }

        if (!found_self)
        {
            std::cerr << "Error: node " << node_id_ << " not found in cluster configuration" << std::endl;
            return false;
        }

        return true;
    }

    std::string Config::to_string() const
    {
        std::ostringstream oss;
        oss << "Config{\n";
        oss << "  node_id: " << node_id_ << "\n";
        oss << "  listen_address: " << listen_address_ << "\n";
        oss << "  listen_port: " << listen_port_ << "\n";
        oss << "  data_directory: " << data_directory_ << "\n";
        oss << "  log_level: " << log_level_ << "\n";
        oss << "  election_timeout: " << election_timeout_min_ms_ << "-" << election_timeout_max_ms_ << "ms\n";
        oss << "  heartbeat_interval: " << heartbeat_interval_ms_ << "ms\n";
        oss << "  rpc_timeout: " << rpc_timeout_ms_ << "ms\n";
        oss << "  max_log_size: " << max_log_size_mb_ << "MB\n";
        oss << "  snapshot_interval: " << snapshot_interval_ << "\n";
        oss << "  fsync_enabled: " << (fsync_enabled_ ? "true" : "false") << "\n";
        oss << "  replication_factor: " << replication_factor_ << "\n";
        oss << "  max_entries_per_request: " << max_entries_per_request_ << "\n";
        oss << "  client_timeout: " << client_timeout_ms_ << "ms\n";
        oss << "  cluster_nodes: [\n";
        for (const auto &node : cluster_nodes_)
        {
            oss << "    " << node.node_id << ":" << node.address << ":" << node.port << "\n";
        }
        oss << "  ]\n";
        oss << "}";
        return oss.str();
    }

    Config Config::create_default_config(uint32_t node_id, uint16_t port)
    {
        Config config;
        config.set_node_id(node_id);
        config.set_listen_port(port);

        std::string data_dir = "./data/node" + std::to_string(node_id);
        config.set_data_directory(data_dir);

        NodeConfig node;
        node.node_id = node_id;
        node.address = "127.0.0.1";
        node.port = port;
        config.add_cluster_node(node);

        return config;
    }

    Config Config::create_cluster_config(const std::vector<std::pair<uint32_t, uint16_t>> &nodes)
    {
        if (nodes.empty())
        {
            return Config();
        }

        Config config = create_default_config(nodes[0].first, nodes[0].second);
        config.cluster_nodes_.clear();

        for (const auto &node_info : nodes)
        {
            NodeConfig node;
            node.node_id = node_info.first;
            node.address = "127.0.0.1";
            node.port = node_info.second;
            config.add_cluster_node(node);
        }

        config.set_replication_factor(std::min(3u, static_cast<uint32_t>(nodes.size())));

        return config;
    }

} // namespace raft
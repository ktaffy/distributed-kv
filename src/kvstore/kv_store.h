#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <memory>
#include <atomic>
#include <chrono>
#include "consistent_hash.h"

namespace raft
{

    class KVStore
    {
    public:
        KVStore();
        explicit KVStore(uint32_t replication_factor);
        ~KVStore() = default;

        bool put(const std::string &key, const std::string &value);
        bool get(const std::string &key, std::string &value) const;
        bool remove(const std::string &key);
        bool exists(const std::string &key) const;

        std::vector<std::string> get_keys() const;
        std::vector<std::string> get_keys_with_prefix(const std::string &prefix) const;
        size_t size() const;
        bool empty() const;
        void clear();

        bool put_batch(const std::vector<std::pair<std::string, std::string>> &kvpairs);
        bool remove_batch(const std::vector<std::string> &keys);
        std::unordered_map<std::string, std::string> get_batch(const std::vector<std::string> &keys) const;

        std::string create_snapshot() const;
        bool restore_from_snapshot(const std::string &snapshot_data);

        void set_nodes(const std::vector<uint32_t> &node_ids);
        std::vector<uint32_t> get_responsible_nodes(const std::string &key) const;
        bool is_responsible_for_key(const std::string &key, uint32_t node_id) const;

        void set_local_node_id(uint32_t node_id);
        uint32_t get_local_node_id() const;

        void set_replication_factor(uint32_t factor);
        uint32_t get_replication_factor() const;

        struct KVStats
        {
            size_t total_keys;
            size_t total_size_bytes;
            uint64_t get_count;
            uint64_t put_count;
            uint64_t remove_count;
            uint64_t get_hit_count;
            uint64_t get_miss_count;
            std::chrono::steady_clock::time_point start_time;
        };

        KVStats get_stats() const;
        void reset_stats();

        double get_hit_ratio() const;
        size_t get_memory_usage() const;

        struct KeyInfo
        {
            std::string key;
            std::string value;
            size_t value_size;
            std::chrono::steady_clock::time_point created_time;
            std::chrono::steady_clock::time_point modified_time;
            uint64_t access_count;
        };

        std::vector<KeyInfo> get_key_info() const;
        KeyInfo get_key_info(const std::string &key) const;

        bool import_data(const std::unordered_map<std::string, std::string> &data);
        std::unordered_map<std::string, std::string> export_data() const;

        void set_max_key_size(size_t max_size);
        void set_max_value_size(size_t max_size);
        void set_max_total_size(size_t max_size);

        size_t get_max_key_size() const;
        size_t get_max_value_size() const;
        size_t get_max_total_size() const;

        bool validate_key(const std::string &key) const;
        bool validate_value(const std::string &value) const;
        bool has_capacity_for(const std::string &key, const std::string &value) const;

    private:
        struct ValueMetadata
        {
            std::string value;
            std::chrono::steady_clock::time_point created_time;
            std::chrono::steady_clock::time_point modified_time;
            mutable uint64_t access_count;

            ValueMetadata(const std::string &val)
                : value(val), access_count(0)
            {
                auto now = std::chrono::steady_clock::now();
                created_time = now;
                modified_time = now;
            }
        };

        bool put_internal(const std::string &key, const std::string &value);
        bool remove_internal(const std::string &key);

        size_t calculate_size(const std::string &key, const std::string &value) const;
        void update_stats_on_get(bool hit) const;
        void update_stats_on_put() const;
        void update_stats_on_remove() const;

        bool check_limits(const std::string &key, const std::string &value) const;
        void enforce_size_limits();

        mutable std::mutex store_mutex_;
        std::unordered_map<std::string, std::unique_ptr<ValueMetadata>> data_;

        std::unique_ptr<ConsistentHash> consistent_hash_;
        uint32_t local_node_id_;
        uint32_t replication_factor_;

        size_t max_key_size_;
        size_t max_value_size_;
        size_t max_total_size_;

        mutable std::mutex stats_mutex_;
        mutable KVStats stats_;

        static constexpr size_t DEFAULT_MAX_KEY_SIZE = 1024;
        static constexpr size_t DEFAULT_MAX_VALUE_SIZE = 1024 * 1024;
        static constexpr size_t DEFAULT_MAX_TOTAL_SIZE = 100 * 1024 * 1024;
        static constexpr uint32_t DEFAULT_REPLICATION_FACTOR = 3;
    };

    class KVStoreClient
    {
    public:
        KVStoreClient(const std::vector<std::string> &cluster_endpoints);
        ~KVStoreClient();

        bool connect();
        void disconnect();
        bool is_connected() const;

        bool put(const std::string &key, const std::string &value);
        bool get(const std::string &key, std::string &value);
        bool remove(const std::string &key);
        bool exists(const std::string &key);

        std::vector<std::string> get_keys();
        std::vector<std::string> get_keys_with_prefix(const std::string &prefix);

        bool put_batch(const std::vector<std::pair<std::string, std::string>> &kvpairs);
        bool remove_batch(const std::vector<std::string> &keys);
        std::unordered_map<std::string, std::string> get_batch(const std::vector<std::string> &keys);

        void set_timeout(std::chrono::milliseconds timeout);
        std::chrono::milliseconds get_timeout() const;

        void set_retry_count(uint32_t count);
        uint32_t get_retry_count() const;

        std::string get_leader_endpoint() const;
        std::vector<std::string> get_cluster_endpoints() const;

    private:
        struct Endpoint
        {
            std::string address;
            uint16_t port;
            bool is_leader;
            std::chrono::steady_clock::time_point last_contact;

            Endpoint(const std::string &addr, uint16_t p)
                : address(addr), port(p), is_leader(false) {}
        };

        bool send_request(const std::string &request, std::string &response);
        bool try_endpoint(const Endpoint &endpoint, const std::string &request, std::string &response);
        bool discover_leader();
        void update_leader_info(const std::string &leader_hint);

        std::vector<std::unique_ptr<Endpoint>> endpoints_;
        Endpoint *current_leader_;

        std::chrono::milliseconds timeout_;
        uint32_t retry_count_;

        mutable std::mutex client_mutex_;
        std::atomic<bool> connected_;
    };

} // namespace raft
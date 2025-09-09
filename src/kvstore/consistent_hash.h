#pragma once

#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <cstdint>
#include <functional>

namespace raft
{

    class ConsistentHash
    {
    public:
        ConsistentHash();
        explicit ConsistentHash(uint32_t virtual_nodes_per_node);
        ~ConsistentHash() = default;

        void add_node(uint32_t node_id);
        void remove_node(uint32_t node_id);
        bool has_node(uint32_t node_id) const;

        uint32_t get_node(const std::string &key) const;
        std::vector<uint32_t> get_nodes(const std::string &key, uint32_t count) const;

        std::vector<uint32_t> get_all_nodes() const;
        size_t get_node_count() const;
        bool empty() const;

        void set_virtual_nodes_per_node(uint32_t count);
        uint32_t get_virtual_nodes_per_node() const;

        std::vector<std::string> get_keys_for_node(uint32_t node_id,
                                                   const std::vector<std::string> &all_keys) const;

        struct NodeRange
        {
            uint64_t start_hash;
            uint64_t end_hash;
            uint32_t node_id;
        };

        std::vector<NodeRange> get_node_ranges() const;
        std::vector<NodeRange> get_ranges_for_node(uint32_t node_id) const;

        double get_load_balance_ratio() const;
        std::unordered_map<uint32_t, double> get_node_load_distribution(
            const std::vector<std::string> &keys) const;

        void rebalance();
        void clear();

        struct HashStats
        {
            size_t total_virtual_nodes;
            size_t nodes_count;
            double max_load_ratio;
            double min_load_ratio;
            double load_balance_score;
        };

        HashStats get_stats() const;

    private:
        struct VirtualNode
        {
            uint64_t hash;
            uint32_t node_id;
            uint32_t virtual_index;

            VirtualNode(uint64_t h, uint32_t id, uint32_t idx)
                : hash(h), node_id(id), virtual_index(idx) {}

            bool operator<(const VirtualNode &other) const
            {
                return hash < other.hash;
            }
        };

        uint64_t hash_key(const std::string &key) const;
        uint64_t hash_node(uint32_t node_id, uint32_t virtual_index) const;

        std::string create_virtual_node_key(uint32_t node_id, uint32_t virtual_index) const;

        void rebuild_hash_ring();
        void add_virtual_nodes(uint32_t node_id);
        void remove_virtual_nodes(uint32_t node_id);

        uint32_t find_successor_node(uint64_t hash) const;
        std::vector<uint32_t> find_successor_nodes(uint64_t hash, uint32_t count) const;

        std::map<uint64_t, uint32_t> hash_ring_;
        std::unordered_map<uint32_t, std::vector<uint64_t>> node_virtual_hashes_;

        uint32_t virtual_nodes_per_node_;

        std::hash<std::string> hasher_;

        static constexpr uint32_t DEFAULT_VIRTUAL_NODES = 150;
        static constexpr uint64_t HASH_RING_SIZE = UINT64_MAX;
    };

    namespace hash_utils
    {
        uint64_t hash_string(const std::string &str);
        uint64_t hash_combine(uint64_t hash1, uint64_t hash2);

        std::string uint64_to_hex(uint64_t value);
        uint64_t hex_to_uint64(const std::string &hex_str);

        double calculate_load_balance_score(const std::vector<double> &loads);
        std::vector<uint32_t> sort_nodes_by_load(const std::unordered_map<uint32_t, double> &loads);
    }

} // namespace raft
#include "consistent_hash.h"
#include <algorithm>
#include <numeric>
#include <sstream>
#include <iomanip>
#include <unordered_set>
#include <cmath>

namespace raft
{

    ConsistentHash::ConsistentHash() : virtual_nodes_per_node_(DEFAULT_VIRTUAL_NODES) {}

    ConsistentHash::ConsistentHash(uint32_t virtual_nodes_per_node)
        : virtual_nodes_per_node_(virtual_nodes_per_node) {}

    void ConsistentHash::add_node(uint32_t node_id)
    {
        if (node_virtual_hashes_.find(node_id) != node_virtual_hashes_.end())
        {
            return;
        }

        add_virtual_nodes(node_id);
        rebuild_hash_ring();
    }

    void ConsistentHash::remove_node(uint32_t node_id)
    {
        auto it = node_virtual_hashes_.find(node_id);
        if (it == node_virtual_hashes_.end())
        {
            return;
        }

        remove_virtual_nodes(node_id);
        node_virtual_hashes_.erase(it);
        rebuild_hash_ring();
    }

    bool ConsistentHash::has_node(uint32_t node_id) const
    {
        return node_virtual_hashes_.find(node_id) != node_virtual_hashes_.end();
    }

    uint32_t ConsistentHash::get_node(const std::string &key) const
    {
        if (hash_ring_.empty())
        {
            return 0;
        }

        uint64_t hash = hash_key(key);
        return find_successor_node(hash);
    }

    std::vector<uint32_t> ConsistentHash::get_nodes(const std::string &key, uint32_t count) const
    {
        if (hash_ring_.empty() || count == 0)
        {
            return {};
        }

        uint64_t hash = hash_key(key);
        return find_successor_nodes(hash, count);
    }

    std::vector<uint32_t> ConsistentHash::get_all_nodes() const
    {
        std::vector<uint32_t> nodes;
        for (const auto &pair : node_virtual_hashes_)
        {
            nodes.push_back(pair.first);
        }
        return nodes;
    }

    size_t ConsistentHash::get_node_count() const
    {
        return node_virtual_hashes_.size();
    }

    bool ConsistentHash::empty() const
    {
        return hash_ring_.empty();
    }

    void ConsistentHash::set_virtual_nodes_per_node(uint32_t count)
    {
        virtual_nodes_per_node_ = count;
        rebalance();
    }

    uint32_t ConsistentHash::get_virtual_nodes_per_node() const
    {
        return virtual_nodes_per_node_;
    }

    std::vector<std::string> ConsistentHash::get_keys_for_node(uint32_t node_id,
                                                               const std::vector<std::string> &all_keys) const
    {
        std::vector<std::string> node_keys;

        for (const auto &key : all_keys)
        {
            if (get_node(key) == node_id)
            {
                node_keys.push_back(key);
            }
        }

        return node_keys;
    }

    std::vector<ConsistentHash::NodeRange> ConsistentHash::get_node_ranges() const
    {
        std::vector<NodeRange> ranges;

        if (hash_ring_.empty())
        {
            return ranges;
        }

        auto it = hash_ring_.begin();
        uint64_t prev_hash = 0;

        for (auto current = hash_ring_.begin(); current != hash_ring_.end(); ++current)
        {
            NodeRange range;
            range.start_hash = prev_hash;
            range.end_hash = current->first;
            range.node_id = current->second;
            ranges.push_back(range);

            prev_hash = current->first + 1;
        }

        if (!ranges.empty())
        {
            NodeRange wrap_range;
            wrap_range.start_hash = hash_ring_.rbegin()->first + 1;
            wrap_range.end_hash = HASH_RING_SIZE;
            wrap_range.node_id = hash_ring_.begin()->second;
            ranges.push_back(wrap_range);
        }

        return ranges;
    }

    std::vector<ConsistentHash::NodeRange> ConsistentHash::get_ranges_for_node(uint32_t node_id) const
    {
        std::vector<NodeRange> node_ranges;
        auto all_ranges = get_node_ranges();

        for (const auto &range : all_ranges)
        {
            if (range.node_id == node_id)
            {
                node_ranges.push_back(range);
            }
        }

        return node_ranges;
    }

    double ConsistentHash::get_load_balance_ratio() const
    {
        if (node_virtual_hashes_.empty())
        {
            return 1.0;
        }

        auto ranges = get_node_ranges();
        std::vector<double> loads;

        for (uint32_t node_id : get_all_nodes())
        {
            uint64_t total_range = 0;
            for (const auto &range : ranges)
            {
                if (range.node_id == node_id)
                {
                    total_range += (range.end_hash - range.start_hash);
                }
            }
            loads.push_back(static_cast<double>(total_range) / HASH_RING_SIZE);
        }

        if (loads.empty())
        {
            return 1.0;
        }

        double max_load = *std::max_element(loads.begin(), loads.end());
        double min_load = *std::min_element(loads.begin(), loads.end());

        return (min_load > 0) ? (min_load / max_load) : 0.0;
    }

    std::unordered_map<uint32_t, double> ConsistentHash::get_node_load_distribution(
        const std::vector<std::string> &keys) const
    {

        std::unordered_map<uint32_t, double> distribution;

        for (uint32_t node_id : get_all_nodes())
        {
            distribution[node_id] = 0.0;
        }

        if (keys.empty())
        {
            return distribution;
        }

        for (const auto &key : keys)
        {
            uint32_t node_id = get_node(key);
            if (distribution.find(node_id) != distribution.end())
            {
                distribution[node_id] += 1.0;
            }
        }

        for (auto &pair : distribution)
        {
            pair.second /= keys.size();
        }

        return distribution;
    }

    void ConsistentHash::rebalance()
    {
        std::vector<uint32_t> nodes = get_all_nodes();
        hash_ring_.clear();
        node_virtual_hashes_.clear();

        for (uint32_t node_id : nodes)
        {
            add_virtual_nodes(node_id);
        }

        rebuild_hash_ring();
    }

    void ConsistentHash::clear()
    {
        hash_ring_.clear();
        node_virtual_hashes_.clear();
    }

    ConsistentHash::HashStats ConsistentHash::get_stats() const
    {
        HashStats stats;
        stats.total_virtual_nodes = hash_ring_.size();
        stats.nodes_count = node_virtual_hashes_.size();

        auto ranges = get_node_ranges();
        std::vector<double> loads;

        for (uint32_t node_id : get_all_nodes())
        {
            uint64_t total_range = 0;
            for (const auto &range : ranges)
            {
                if (range.node_id == node_id)
                {
                    total_range += (range.end_hash - range.start_hash);
                }
            }
            loads.push_back(static_cast<double>(total_range) / HASH_RING_SIZE);
        }

        if (!loads.empty())
        {
            stats.max_load_ratio = *std::max_element(loads.begin(), loads.end());
            stats.min_load_ratio = *std::min_element(loads.begin(), loads.end());
            stats.load_balance_score = hash_utils::calculate_load_balance_score(loads);
        }
        else
        {
            stats.max_load_ratio = 0.0;
            stats.min_load_ratio = 0.0;
            stats.load_balance_score = 1.0;
        }

        return stats;
    }

    uint64_t ConsistentHash::hash_key(const std::string &key) const
    {
        return hasher_(key);
    }

    uint64_t ConsistentHash::hash_node(uint32_t node_id, uint32_t virtual_index) const
    {
        std::string virtual_key = create_virtual_node_key(node_id, virtual_index);
        return hasher_(virtual_key);
    }

    std::string ConsistentHash::create_virtual_node_key(uint32_t node_id, uint32_t virtual_index) const
    {
        return std::to_string(node_id) + ":" + std::to_string(virtual_index);
    }

    void ConsistentHash::rebuild_hash_ring()
    {
        hash_ring_.clear();

        for (const auto &node_pair : node_virtual_hashes_)
        {
            uint32_t node_id = node_pair.first;
            const auto &virtual_hashes = node_pair.second;

            for (uint64_t hash : virtual_hashes)
            {
                hash_ring_[hash] = node_id;
            }
        }
    }

    void ConsistentHash::add_virtual_nodes(uint32_t node_id)
    {
        std::vector<uint64_t> virtual_hashes;
        virtual_hashes.reserve(virtual_nodes_per_node_);

        for (uint32_t i = 0; i < virtual_nodes_per_node_; ++i)
        {
            uint64_t hash = hash_node(node_id, i);
            virtual_hashes.push_back(hash);
        }

        node_virtual_hashes_[node_id] = std::move(virtual_hashes);
    }

    void ConsistentHash::remove_virtual_nodes(uint32_t node_id)
    {
        auto it = node_virtual_hashes_.find(node_id);
        if (it != node_virtual_hashes_.end())
        {
            for (uint64_t hash : it->second)
            {
                hash_ring_.erase(hash);
            }
        }
    }

    uint32_t ConsistentHash::find_successor_node(uint64_t hash) const
    {
        auto it = hash_ring_.upper_bound(hash);
        if (it == hash_ring_.end())
        {
            it = hash_ring_.begin();
        }
        return it->second;
    }

    std::vector<uint32_t> ConsistentHash::find_successor_nodes(uint64_t hash, uint32_t count) const
    {
        std::vector<uint32_t> nodes;
        std::unordered_set<uint32_t> seen_nodes;

        auto it = hash_ring_.upper_bound(hash);

        while (nodes.size() < count && seen_nodes.size() < node_virtual_hashes_.size())
        {
            if (it == hash_ring_.end())
            {
                it = hash_ring_.begin();
            }

            uint32_t node_id = it->second;
            if (seen_nodes.find(node_id) == seen_nodes.end())
            {
                nodes.push_back(node_id);
                seen_nodes.insert(node_id);
            }

            ++it;
        }

        return nodes;
    }

    namespace hash_utils
    {

        uint64_t hash_string(const std::string &str)
        {
            std::hash<std::string> hasher;
            return hasher(str);
        }

        uint64_t hash_combine(uint64_t hash1, uint64_t hash2)
        {
            return hash1 ^ (hash2 + 0x9e3779b9 + (hash1 << 6) + (hash1 >> 2));
        }

        std::string uint64_to_hex(uint64_t value)
        {
            std::ostringstream oss;
            oss << std::hex << value;
            return oss.str();
        }

        uint64_t hex_to_uint64(const std::string &hex_str)
        {
            uint64_t value;
            std::istringstream iss(hex_str);
            iss >> std::hex >> value;
            return value;
        }

        double calculate_load_balance_score(const std::vector<double> &loads)
        {
            if (loads.empty())
            {
                return 1.0;
            }

            double mean = std::accumulate(loads.begin(), loads.end(), 0.0) / loads.size();

            if (mean == 0.0)
            {
                return 1.0;
            }

            double variance = 0.0;
            for (double load : loads)
            {
                variance += (load - mean) * (load - mean);
            }
            variance /= loads.size();

            double coefficient_of_variation = std::sqrt(variance) / mean;
            return 1.0 / (1.0 + coefficient_of_variation);
        }

        std::vector<uint32_t> sort_nodes_by_load(const std::unordered_map<uint32_t, double> &loads)
        {
            std::vector<std::pair<uint32_t, double>> node_loads;

            for (const auto &pair : loads)
            {
                node_loads.emplace_back(pair.first, pair.second);
            }

            std::sort(node_loads.begin(), node_loads.end(),
                      [](const auto &a, const auto &b)
                      {
                          return a.second < b.second;
                      });

            std::vector<uint32_t> sorted_nodes;
            for (const auto &pair : node_loads)
            {
                sorted_nodes.push_back(pair.first);
            }

            return sorted_nodes;
        }

    } // namespace hash_utils

} // namespace raft
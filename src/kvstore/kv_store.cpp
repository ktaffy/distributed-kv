#include "kv_store.h"
#include <sstream>
#include <algorithm>
#include <thread>
#include <chrono>

namespace raft
{

    KVStore::KVStore() : KVStore(DEFAULT_REPLICATION_FACTOR) {}

    KVStore::KVStore(uint32_t replication_factor)
        : consistent_hash_(std::make_unique<ConsistentHash>()),
          local_node_id_(0), replication_factor_(replication_factor),
          max_key_size_(DEFAULT_MAX_KEY_SIZE),
          max_value_size_(DEFAULT_MAX_VALUE_SIZE),
          max_total_size_(DEFAULT_MAX_TOTAL_SIZE)
    {
        reset_stats();
    }

    bool KVStore::put(const std::string &key, const std::string &value)
    {
        if (!validate_key(key) || !validate_value(value))
        {
            return false;
        }

        if (!has_capacity_for(key, value))
        {
            return false;
        }

        return put_internal(key, value);
    }

    bool KVStore::get(const std::string &key, std::string &value) const
    {
        if (!validate_key(key))
        {
            update_stats_on_get(false);
            return false;
        }

        std::lock_guard<std::mutex> lock(store_mutex_);
        auto it = data_.find(key);

        if (it != data_.end())
        {
            value = it->second->value;
            it->second->access_count++;
            update_stats_on_get(true);
            return true;
        }

        update_stats_on_get(false);
        return false;
    }

    bool KVStore::remove(const std::string &key)
    {
        if (!validate_key(key))
        {
            return false;
        }

        return remove_internal(key);
    }

    bool KVStore::exists(const std::string &key) const
    {
        if (!validate_key(key))
        {
            return false;
        }

        std::lock_guard<std::mutex> lock(store_mutex_);
        return data_.find(key) != data_.end();
    }

    std::vector<std::string> KVStore::get_keys() const
    {
        std::lock_guard<std::mutex> lock(store_mutex_);
        std::vector<std::string> keys;
        keys.reserve(data_.size());

        for (const auto &pair : data_)
        {
            keys.push_back(pair.first);
        }

        return keys;
    }

    std::vector<std::string> KVStore::get_keys_with_prefix(const std::string &prefix) const
    {
        std::lock_guard<std::mutex> lock(store_mutex_);
        std::vector<std::string> keys;

        for (const auto &pair : data_)
        {
            if (pair.first.substr(0, prefix.length()) == prefix)
            {
                keys.push_back(pair.first);
            }
        }

        return keys;
    }

    size_t KVStore::size() const
    {
        std::lock_guard<std::mutex> lock(store_mutex_);
        return data_.size();
    }

    bool KVStore::empty() const
    {
        std::lock_guard<std::mutex> lock(store_mutex_);
        return data_.empty();
    }

    void KVStore::clear()
    {
        std::lock_guard<std::mutex> lock(store_mutex_);
        data_.clear();
        reset_stats();
    }

    bool KVStore::put_batch(const std::vector<std::pair<std::string, std::string>> &kvpairs)
    {
        for (const auto &pair : kvpairs)
        {
            if (!put(pair.first, pair.second))
            {
                return false;
            }
        }
        return true;
    }

    bool KVStore::remove_batch(const std::vector<std::string> &keys)
    {
        for (const auto &key : keys)
        {
            if (!remove(key))
            {
                return false;
            }
        }
        return true;
    }

    std::unordered_map<std::string, std::string> KVStore::get_batch(const std::vector<std::string> &keys) const
    {
        std::unordered_map<std::string, std::string> result;

        for (const auto &key : keys)
        {
            std::string value;
            if (get(key, value))
            {
                result[key] = value;
            }
        }

        return result;
    }

    std::string KVStore::create_snapshot() const
    {
        std::lock_guard<std::mutex> lock(store_mutex_);
        std::ostringstream oss;

        oss << data_.size() << "\n";
        for (const auto &pair : data_)
        {
            oss << pair.first.length() << " " << pair.first << " "
                << pair.second->value.length() << " " << pair.second->value << "\n";
        }

        return oss.str();
    }

    bool KVStore::restore_from_snapshot(const std::string &snapshot_data)
    {
        std::istringstream iss(snapshot_data);
        size_t count;

        if (!(iss >> count))
        {
            return false;
        }

        std::unordered_map<std::string, std::unique_ptr<ValueMetadata>> new_data;

        for (size_t i = 0; i < count; ++i)
        {
            size_t key_len, value_len;
            std::string key, value;

            if (!(iss >> key_len))
                return false;
            iss.ignore(1);

            key.resize(key_len);
            iss.read(&key[0], key_len);

            if (!(iss >> value_len))
                return false;
            iss.ignore(1);

            value.resize(value_len);
            iss.read(&value[0], value_len);
            iss.ignore(1);

            new_data[key] = std::make_unique<ValueMetadata>(value);
        }

        std::lock_guard<std::mutex> lock(store_mutex_);
        data_ = std::move(new_data);

        return true;
    }

    void KVStore::set_nodes(const std::vector<uint32_t> &node_ids)
    {
        consistent_hash_->clear();
        for (uint32_t node_id : node_ids)
        {
            consistent_hash_->add_node(node_id);
        }
    }

    std::vector<uint32_t> KVStore::get_responsible_nodes(const std::string &key) const
    {
        return consistent_hash_->get_nodes(key, replication_factor_);
    }

    bool KVStore::is_responsible_for_key(const std::string &key, uint32_t node_id) const
    {
        auto responsible_nodes = get_responsible_nodes(key);
        return std::find(responsible_nodes.begin(), responsible_nodes.end(), node_id) != responsible_nodes.end();
    }

    void KVStore::set_local_node_id(uint32_t node_id)
    {
        local_node_id_ = node_id;
    }

    uint32_t KVStore::get_local_node_id() const
    {
        return local_node_id_;
    }

    void KVStore::set_replication_factor(uint32_t factor)
    {
        replication_factor_ = factor;
    }

    uint32_t KVStore::get_replication_factor() const
    {
        return replication_factor_;
    }

    KVStore::KVStats KVStore::get_stats() const
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        KVStats current_stats = stats_;
        current_stats.total_keys = size();
        current_stats.total_size_bytes = get_memory_usage();
        return current_stats;
    }

    void KVStore::reset_stats()
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_ = KVStats{};
        stats_.start_time = std::chrono::steady_clock::now();
    }

    double KVStore::get_hit_ratio() const
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        uint64_t total_gets = stats_.get_hit_count + stats_.get_miss_count;
        return (total_gets > 0) ? static_cast<double>(stats_.get_hit_count) / total_gets : 0.0;
    }

    size_t KVStore::get_memory_usage() const
    {
        std::lock_guard<std::mutex> lock(store_mutex_);
        size_t total_size = 0;

        for (const auto &pair : data_)
        {
            total_size += calculate_size(pair.first, pair.second->value);
        }

        return total_size;
    }

    std::vector<KVStore::KeyInfo> KVStore::get_key_info() const
    {
        std::lock_guard<std::mutex> lock(store_mutex_);
        std::vector<KeyInfo> info_list;
        info_list.reserve(data_.size());

        for (const auto &pair : data_)
        {
            KeyInfo info;
            info.key = pair.first;
            info.value = pair.second->value;
            info.value_size = pair.second->value.size();
            info.created_time = pair.second->created_time;
            info.modified_time = pair.second->modified_time;
            info.access_count = pair.second->access_count;
            info_list.push_back(info);
        }

        return info_list;
    }

    KVStore::KeyInfo KVStore::get_key_info(const std::string &key) const
    {
        std::lock_guard<std::mutex> lock(store_mutex_);
        KeyInfo info;

        auto it = data_.find(key);
        if (it != data_.end())
        {
            info.key = key;
            info.value = it->second->value;
            info.value_size = it->second->value.size();
            info.created_time = it->second->created_time;
            info.modified_time = it->second->modified_time;
            info.access_count = it->second->access_count;
        }

        return info;
    }

    bool KVStore::import_data(const std::unordered_map<std::string, std::string> &data)
    {
        std::lock_guard<std::mutex> lock(store_mutex_);

        for (const auto &pair : data)
        {
            if (!validate_key(pair.first) || !validate_value(pair.second))
            {
                return false;
            }
            data_[pair.first] = std::make_unique<ValueMetadata>(pair.second);
        }

        return true;
    }

    std::unordered_map<std::string, std::string> KVStore::export_data() const
    {
        std::lock_guard<std::mutex> lock(store_mutex_);
        std::unordered_map<std::string, std::string> exported;

        for (const auto &pair : data_)
        {
            exported[pair.first] = pair.second->value;
        }

        return exported;
    }

    void KVStore::set_max_key_size(size_t max_size)
    {
        max_key_size_ = max_size;
    }

    void KVStore::set_max_value_size(size_t max_size)
    {
        max_value_size_ = max_size;
    }

    void KVStore::set_max_total_size(size_t max_size)
    {
        max_total_size_ = max_size;
    }

    size_t KVStore::get_max_key_size() const
    {
        return max_key_size_;
    }

    size_t KVStore::get_max_value_size() const
    {
        return max_value_size_;
    }

    size_t KVStore::get_max_total_size() const
    {
        return max_total_size_;
    }

    bool KVStore::validate_key(const std::string &key) const
    {
        return !key.empty() && key.size() <= max_key_size_;
    }

    bool KVStore::validate_value(const std::string &value) const
    {
        return value.size() <= max_value_size_;
    }

    bool KVStore::has_capacity_for(const std::string &key, const std::string &value) const
    {
        size_t entry_size = calculate_size(key, value);
        return get_memory_usage() + entry_size <= max_total_size_;
    }

    bool KVStore::put_internal(const std::string &key, const std::string &value)
    {
        std::lock_guard<std::mutex> lock(store_mutex_);

        auto it = data_.find(key);
        if (it != data_.end())
        {
            it->second->value = value;
            it->second->modified_time = std::chrono::steady_clock::now();
        }
        else
        {
            data_[key] = std::make_unique<ValueMetadata>(value);
        }

        update_stats_on_put();
        return true;
    }

    bool KVStore::remove_internal(const std::string &key)
    {
        std::lock_guard<std::mutex> lock(store_mutex_);

        auto it = data_.find(key);
        if (it != data_.end())
        {
            data_.erase(it);
            update_stats_on_remove();
            return true;
        }

        return false;
    }

    size_t KVStore::calculate_size(const std::string &key, const std::string &value) const
    {
        return key.size() + value.size() + sizeof(ValueMetadata);
    }

    void KVStore::update_stats_on_get(bool hit) const
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.get_count++;
        if (hit)
        {
            stats_.get_hit_count++;
        }
        else
        {
            stats_.get_miss_count++;
        }
    }

    void KVStore::update_stats_on_put() const
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.put_count++;
    }

    void KVStore::update_stats_on_remove() const
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.remove_count++;
    }

    bool KVStore::check_limits(const std::string &key, const std::string &value) const
    {
        return validate_key(key) && validate_value(value) && has_capacity_for(key, value);
    }

    void KVStore::enforce_size_limits()
    {
        if (get_memory_usage() <= max_total_size_)
        {
            return;
        }

        std::lock_guard<std::mutex> lock(store_mutex_);
        std::vector<std::pair<std::string, uint64_t>> access_times;

        for (const auto &pair : data_)
        {
            access_times.emplace_back(pair.first, pair.second->access_count);
        }

        std::sort(access_times.begin(), access_times.end(),
                  [](const auto &a, const auto &b)
                  {
                      return a.second < b.second;
                  });

        size_t remove_count = data_.size() / 10;
        for (size_t i = 0; i < remove_count && get_memory_usage() > max_total_size_; ++i)
        {
            data_.erase(access_times[i].first);
        }
    }

    KVStoreClient::KVStoreClient(const std::vector<std::string> &cluster_endpoints)
        : current_leader_(nullptr), timeout_(std::chrono::milliseconds(5000)),
          retry_count_(3), connected_(false)
    {

        for (const auto &endpoint : cluster_endpoints)
        {
            size_t colon_pos = endpoint.find(':');
            if (colon_pos != std::string::npos)
            {
                std::string addr = endpoint.substr(0, colon_pos);
                uint16_t port = std::stoi(endpoint.substr(colon_pos + 1));
                endpoints_.push_back(std::make_unique<Endpoint>(addr, port));
            }
        }
    }

    KVStoreClient::~KVStoreClient()
    {
        disconnect();
    }

    bool KVStoreClient::connect()
    {
        std::lock_guard<std::mutex> lock(client_mutex_);

        if (endpoints_.empty())
        {
            return false;
        }

        if (!discover_leader())
        {
            current_leader_ = endpoints_[0].get();
        }

        connected_.store(true);
        return true;
    }

    void KVStoreClient::disconnect()
    {
        std::lock_guard<std::mutex> lock(client_mutex_);
        connected_.store(false);
        current_leader_ = nullptr;
    }

    bool KVStoreClient::is_connected() const
    {
        return connected_.load();
    }

    bool KVStoreClient::put(const std::string &key, const std::string &value)
    {
        std::string request = "PUT " + key + " " + value;
        std::string response;
        return send_request(request, response) && response == "OK";
    }

    bool KVStoreClient::get(const std::string &key, std::string &value)
    {
        std::string request = "GET " + key;
        std::string response;

        if (send_request(request, response) && response.substr(0, 3) != "ERR")
        {
            value = response;
            return true;
        }

        return false;
    }

    bool KVStoreClient::remove(const std::string &key)
    {
        std::string request = "DELETE " + key;
        std::string response;
        return send_request(request, response) && response == "OK";
    }

    bool KVStoreClient::exists(const std::string &key)
    {
        std::string request = "EXISTS " + key;
        std::string response;
        return send_request(request, response) && response == "true";
    }

    std::vector<std::string> KVStoreClient::get_keys()
    {
        std::string request = "LIST";
        std::string response;

        if (!send_request(request, response))
        {
            return {};
        }

        std::vector<std::string> keys;
        std::istringstream iss(response);
        std::string key;
        while (std::getline(iss, key))
        {
            if (!key.empty())
            {
                keys.push_back(key);
            }
        }

        return keys;
    }

    std::vector<std::string> KVStoreClient::get_keys_with_prefix(const std::string &prefix)
    {
        std::string request = "LIST " + prefix;
        std::string response;

        if (!send_request(request, response))
        {
            return {};
        }

        std::vector<std::string> keys;
        std::istringstream iss(response);
        std::string key;
        while (std::getline(iss, key))
        {
            if (!key.empty())
            {
                keys.push_back(key);
            }
        }

        return keys;
    }

    bool KVStoreClient::put_batch(const std::vector<std::pair<std::string, std::string>> &kvpairs)
    {
        for (const auto &pair : kvpairs)
        {
            if (!put(pair.first, pair.second))
            {
                return false;
            }
        }
        return true;
    }

    bool KVStoreClient::remove_batch(const std::vector<std::string> &keys)
    {
        for (const auto &key : keys)
        {
            if (!remove(key))
            {
                return false;
            }
        }
        return true;
    }

    std::unordered_map<std::string, std::string> KVStoreClient::get_batch(const std::vector<std::string> &keys)
    {
        std::unordered_map<std::string, std::string> result;

        for (const auto &key : keys)
        {
            std::string value;
            if (get(key, value))
            {
                result[key] = value;
            }
        }

        return result;
    }

    void KVStoreClient::set_timeout(std::chrono::milliseconds timeout)
    {
        timeout_ = timeout;
    }

    std::chrono::milliseconds KVStoreClient::get_timeout() const
    {
        return timeout_;
    }

    void KVStoreClient::set_retry_count(uint32_t count)
    {
        retry_count_ = count;
    }

    uint32_t KVStoreClient::get_retry_count() const
    {
        return retry_count_;
    }

    std::string KVStoreClient::get_leader_endpoint() const
    {
        std::lock_guard<std::mutex> lock(client_mutex_);
        if (current_leader_)
        {
            return current_leader_->address + ":" + std::to_string(current_leader_->port);
        }
        return "";
    }

    std::vector<std::string> KVStoreClient::get_cluster_endpoints() const
    {
        std::lock_guard<std::mutex> lock(client_mutex_);
        std::vector<std::string> endpoints;

        for (const auto &endpoint : endpoints_)
        {
            endpoints.push_back(endpoint->address + ":" + std::to_string(endpoint->port));
        }

        return endpoints;
    }

    bool KVStoreClient::send_request(const std::string &request, std::string &response)
    {
        std::lock_guard<std::mutex> lock(client_mutex_);

        if (!connected_.load() || endpoints_.empty())
        {
            return false;
        }

        for (uint32_t attempt = 0; attempt < retry_count_; ++attempt)
        {
            if (current_leader_ && try_endpoint(*current_leader_, request, response))
            {
                return true;
            }

            for (const auto &endpoint : endpoints_)
            {
                if (endpoint.get() != current_leader_ &&
                    try_endpoint(*endpoint, request, response))
                {
                    current_leader_ = endpoint.get();
                    current_leader_->is_leader = true;
                    return true;
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        return false;
    }

    bool KVStoreClient::try_endpoint(const Endpoint &endpoint, const std::string &request, std::string &response)
    {
        response = "MOCK_RESPONSE";
        return true;
    }

    bool KVStoreClient::discover_leader()
    {
        for (const auto &endpoint : endpoints_)
        {
            std::string request = "PING";
            std::string response;
            if (try_endpoint(*endpoint, request, response))
            {
                current_leader_ = endpoint.get();
                current_leader_->is_leader = true;
                return true;
            }
        }
        return false;
    }

    void KVStoreClient::update_leader_info(const std::string &leader_hint)
    {
        for (const auto &endpoint : endpoints_)
        {
            std::string endpoint_str = endpoint->address + ":" + std::to_string(endpoint->port);
            if (endpoint_str == leader_hint)
            {
                current_leader_ = endpoint.get();
                current_leader_->is_leader = true;
                break;
            }
        }
    }

} // namespace raft
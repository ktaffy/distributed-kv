#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include "../raft/log_entry.h"

namespace raft
{
    class KVStateMachine
    {
    public:
        bool apply(const KVOperation &op, uint64_t client_id, uint64_t seq, std::string &value)
        {
            if (client_id != 0)
            {
                auto it = sessions_.find(client_id);
                if (it != sessions_.end() && seq <= it->second.last_seq)
                {
                    value = it->second.value;
                    return it->second.found;
                }
            }

            bool found = execute(op, value);

            if (client_id != 0)
                sessions_[client_id] = Session{seq, found, value};
            return found;
        }

        bool get(const std::string &key, std::string &value) const
        {
            auto it = data_.find(key);
            if (it == data_.end())
                return false;
            value = it->second;
            return true;
        }

        size_t size() const { return data_.size(); }

    private:
        struct Session
        {
            uint64_t last_seq = 0;
            bool found = false;
            std::string value;
        };

        bool execute(const KVOperation &op, std::string &value)
        {
            switch (op.operation)
            {
            case KVOperation::Type::PUT:
                data_[op.key] = op.value;
                return true;
            case KVOperation::Type::DELETE:
                return data_.erase(op.key) > 0;
            case KVOperation::Type::GET:
                return get(op.key, value);
            }
            return false;
        }

        std::unordered_map<std::string, std::string> data_;
        std::unordered_map<uint64_t, Session> sessions_;
    };
}
#pragma once

#include <string>
#include <unordered_map>
#include "../raft/log_entry.h"

namespace raft
{
    class KVStateMachine
    {
    public:
        bool apply(const KVOperation &op, std::string &value)
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
        std::unordered_map<std::string, std::string> data_;
    };
}
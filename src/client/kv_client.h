#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>
#include "../network/message.h"

namespace raft
{
    class KVClient
    {
    public:
        struct Result
        {
            bool ok = false;
            bool found = false;
            std::string value;
            std::string error;
        };

        explicit KVClient(std::vector<std::string> endpoints,
                          std::chrono::milliseconds timeout = std::chrono::seconds(5));
        ~KVClient();

        Result put(const std::string &key, const std::string &value);
        Result get(const std::string &key);
        Result remove(const std::string &key);

    private:
        Result call(MessageType type, const std::string &key, const std::string &value);
        bool connect_to(const std::string &endpoint);
        void disconnect();
        bool exchange(const ClientRequest &request, ClientResponse &response);

        std::vector<std::string> endpoints_;
        std::chrono::milliseconds timeout_;
        std::string connected_to_;
        int fd_ = -1;
        uint32_t next_message_id_ = 1;
    };
}
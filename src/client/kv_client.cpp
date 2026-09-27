#include "kv_client.h"
#include "../network/framing.h"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace raft
{
    KVClient::KVClient(std::vector<std::string> endpoints, std::chrono::milliseconds timeout)
        : endpoints_(std::move(endpoints)), timeout_(timeout) {}

    KVClient::~KVClient()
    {
        disconnect();
    }

    KVClient::Result KVClient::put(const std::string &key, const std::string &value)
    {
        return call(MessageType::CLIENT_PUT, key, value);
    }

    KVClient::Result KVClient::get(const std::string &key)
    {
        return call(MessageType::CLIENT_GET, key, "");
    }

    KVClient::Result KVClient::remove(const std::string &key)
    {
        return call(MessageType::CLIENT_DELETE, key, "");
    }

    KVClient::Result KVClient::call(MessageType type, const std::string &key, const std::string &value)
    {
        Result result;
        if (endpoints_.empty())
        {
            result.error = "no endpoints";
            return result;
        }

        size_t rotation = 0;
        std::string target = connected_to_.empty() ? endpoints_[0] : connected_to_;

        auto deadline = std::chrono::steady_clock::now() + timeout_;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (fd_ < 0 || target != connected_to_)
            {
                if (!connect_to(target))
                {
                    result.error = "cannot connect to " + target;
                    target = endpoints_[++rotation % endpoints_.size()];
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    continue;
                }
            }

            ClientRequest request(type);
            request.message_id = next_message_id_++;
            request.key = key;
            request.value = value;

            ClientResponse response;
            if (!exchange(request, response))
            {
                disconnect();
                result.error = "connection to " + target + " failed";
                target = endpoints_[++rotation % endpoints_.size()];
                continue;
            }

            if (response.success)
            {
                result.ok = true;
                result.found = response.found;
                result.value = response.value;
                result.error.clear();
                return result;
            }

            result.error = response.error_message;
            if (!response.leader_address.empty() && response.leader_address != target)
            {
                target = response.leader_address;
            }
            else
            {
                target = endpoints_[++rotation % endpoints_.size()];
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        return result;
    }

    bool KVClient::connect_to(const std::string &endpoint)
    {
        disconnect();

        size_t colon = endpoint.rfind(':');
        if (colon == std::string::npos)
            return false;

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(std::stoi(endpoint.substr(colon + 1))));
        if (inet_pton(AF_INET, endpoint.substr(0, colon).c_str(), &addr.sin_addr) <= 0)
            return false;

        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0)
            return false;

        auto io_timeout = timeout_ + std::chrono::seconds(1);
        timeval tv{};
        tv.tv_sec = io_timeout.count() / 1000;
        tv.tv_usec = (io_timeout.count() % 1000) * 1000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
        {
            close(fd);
            return false;
        }

        fd_ = fd;
        connected_to_ = endpoint;
        return true;
    }

    void KVClient::disconnect()
    {
        if (fd_ >= 0)
            close(fd_);
        fd_ = -1;
        connected_to_.clear();
    }

    bool KVClient::exchange(const ClientRequest &request, ClientResponse &response)
    {
        std::string frame;
        return write_frame(fd_, request.serialize()) && read_frame(fd_, frame) &&
               response.deserialize(frame);
    }
}
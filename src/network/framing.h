#pragma once

#include <cstdint>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>

namespace raft
{
    inline bool write_all(int fd, const char *data, size_t len)
    {
        while (len > 0)
        {
            ssize_t n = send(fd, data, len, MSG_NOSIGNAL);
            if (n <= 0)
                return false;
            data += n;
            len -= static_cast<size_t>(n);
        }
        return true;
    }

    inline bool read_all(int fd, char *data, size_t len)
    {
        while (len > 0)
        {
            ssize_t n = recv(fd, data, len, 0);
            if (n <= 0)
                return false;
            data += n;
            len -= static_cast<size_t>(n);
        }
        return true;
    }

    inline bool write_frame(int fd, const std::string &payload)
    {
        uint32_t size = static_cast<uint32_t>(payload.size());
        std::string frame(reinterpret_cast<const char *>(&size), sizeof(size));
        frame += payload;
        return write_all(fd, frame.data(), frame.size());
    }

    inline bool read_frame(int fd, std::string &payload, uint32_t max_size = 2 * 1024 * 1024)
    {
        uint32_t size;
        if (!read_all(fd, reinterpret_cast<char *>(&size), sizeof(size)) || size > max_size)
            return false;
        payload.assign(size, '\0');
        return read_all(fd, &payload[0], size);
    }
}
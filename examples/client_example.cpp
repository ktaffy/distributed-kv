#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include "../src/client/kv_client.h"

using raft::KVClient;

static void usage(const char *prog)
{
    std::cerr << "Usage: " << prog << " [-s host:port,...] put KEY VALUE | get KEY | delete KEY\n"
              << "Default servers: 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003\n";
}

static std::vector<std::string> split(const std::string &s, char sep)
{
    std::vector<std::string> parts;
    std::istringstream iss(s);
    std::string part;
    while (std::getline(iss, part, sep))
        if (!part.empty())
            parts.push_back(part);
    return parts;
}

int main(int argc, char *argv[])
{
    std::vector<std::string> servers{"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"};
    int i = 1;
    if (argc > 2 && std::strcmp(argv[1], "-s") == 0)
    {
        servers = split(argv[2], ',');
        i = 3;
    }
    if (i >= argc)
    {
        usage(argv[0]);
        return 2;
    }

    std::string command = argv[i];
    KVClient client(servers);
    KVClient::Result result;

    if (command == "put" && argc == i + 3)
        result = client.put(argv[i + 1], argv[i + 2]);
    else if (command == "get" && argc == i + 2)
        result = client.get(argv[i + 1]);
    else if (command == "delete" && argc == i + 2)
        result = client.remove(argv[i + 1]);
    else
    {
        usage(argv[0]);
        return 2;
    }

    if (!result.ok)
    {
        std::cerr << "ERROR: " << result.error << "\n";
        return 2;
    }

    if (command == "get")
    {
        if (!result.found)
        {
            std::cerr << "(not found)\n";
            return 1;
        }
        std::cout << result.value << "\n";
    }
    else if (command == "delete" && !result.found)
    {
        std::cout << "(not found)\n";
    }
    else
    {
        std::cout << "OK\n";
    }
    return 0;
}
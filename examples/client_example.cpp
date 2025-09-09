#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <thread>
#include <sstream>
#include "../src/kvstore/kv_store.h"

using namespace raft;

void print_usage(const char *program_name)
{
    std::cout << "Usage: " << program_name << " <command> [args...]\n"
              << "Commands:\n"
              << "  put <key> <value>     Store a key-value pair\n"
              << "  get <key>             Retrieve value for key\n"
              << "  delete <key>          Remove key-value pair\n"
              << "  exists <key>          Check if key exists\n"
              << "  list                  List all keys\n"
              << "  list <prefix>         List keys with prefix\n"
              << "  benchmark <count>     Run benchmark with <count> operations\n"
              << "  interactive           Start interactive mode\n"
              << "\nExamples:\n"
              << "  " << program_name << " put user:123 '{\"name\":\"Alice\"}'\n"
              << "  " << program_name << " get user:123\n"
              << "  " << program_name << " delete user:123\n"
              << "  " << program_name << " benchmark 1000\n";
}

bool handle_put(KVStoreClient &client, const std::string &key, const std::string &value)
{
    if (client.put(key, value))
    {
        std::cout << "OK\n";
        return true;
    }
    else
    {
        std::cout << "ERROR: Failed to store key-value pair\n";
        return false;
    }
}

bool handle_get(KVStoreClient &client, const std::string &key)
{
    std::string value;
    if (client.get(key, value))
    {
        std::cout << value << "\n";
        return true;
    }
    else
    {
        std::cout << "ERROR: Key not found\n";
        return false;
    }
}

bool handle_delete(KVStoreClient &client, const std::string &key)
{
    if (client.remove(key))
    {
        std::cout << "OK\n";
        return true;
    }
    else
    {
        std::cout << "ERROR: Failed to delete key\n";
        return false;
    }
}

bool handle_exists(KVStoreClient &client, const std::string &key)
{
    if (client.exists(key))
    {
        std::cout << "true\n";
        return true;
    }
    else
    {
        std::cout << "false\n";
        return true;
    }
}

bool handle_list(KVStoreClient &client, const std::string &prefix = "")
{
    std::vector<std::string> keys;

    if (prefix.empty())
    {
        keys = client.get_keys();
    }
    else
    {
        keys = client.get_keys_with_prefix(prefix);
    }

    if (keys.empty())
    {
        std::cout << "No keys found\n";
    }
    else
    {
        for (const auto &key : keys)
        {
            std::cout << key << "\n";
        }
    }
    return true;
}

bool handle_benchmark(KVStoreClient &client, int count)
{
    std::cout << "Running benchmark with " << count << " operations...\n";

    auto start_time = std::chrono::high_resolution_clock::now();

    int successful_ops = 0;

    for (int i = 0; i < count; ++i)
    {
        std::string key = "benchmark_key_" + std::to_string(i);
        std::string value = "benchmark_value_" + std::to_string(i);

        if (client.put(key, value))
        {
            successful_ops++;
        }

        if (i % 100 == 0)
        {
            std::cout << "Progress: " << i << "/" << count << "\r" << std::flush;
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "\nBenchmark completed:\n";
    std::cout << "  Total operations: " << count << "\n";
    std::cout << "  Successful operations: " << successful_ops << "\n";
    std::cout << "  Failed operations: " << (count - successful_ops) << "\n";
    std::cout << "  Duration: " << duration.count() << " ms\n";

    if (duration.count() > 0)
    {
        double ops_per_sec = (double)successful_ops * 1000.0 / duration.count();
        std::cout << "  Throughput: " << ops_per_sec << " ops/sec\n";
    }

    return successful_ops == count;
}

void handle_interactive(KVStoreClient &client)
{
    std::cout << "Interactive mode (type 'help' for commands, 'quit' to exit)\n";

    std::string line;
    while (true)
    {
        std::cout << "kvstore> ";
        if (!std::getline(std::cin, line))
        {
            break;
        }

        if (line.empty())
        {
            continue;
        }

        if (line == "quit" || line == "exit")
        {
            break;
        }

        if (line == "help")
        {
            std::cout << "Available commands:\n"
                      << "  put <key> <value>\n"
                      << "  get <key>\n"
                      << "  delete <key>\n"
                      << "  exists <key>\n"
                      << "  list [prefix]\n"
                      << "  clear\n"
                      << "  status\n"
                      << "  quit\n";
            continue;
        }

        if (line == "status")
        {
            if (client.is_connected())
            {
                std::cout << "Connected to cluster\n";
                std::cout << "Leader: " << client.get_leader_endpoint() << "\n";
            }
            else
            {
                std::cout << "Not connected to cluster\n";
            }
            continue;
        }

        std::vector<std::string> tokens;
        std::istringstream iss(line);
        std::string token;
        while (iss >> token)
        {
            tokens.push_back(token);
        }

        if (tokens.empty())
        {
            continue;
        }

        const std::string &command = tokens[0];

        if (command == "put" && tokens.size() == 3)
        {
            handle_put(client, tokens[1], tokens[2]);
        }
        else if (command == "get" && tokens.size() == 2)
        {
            handle_get(client, tokens[1]);
        }
        else if (command == "delete" && tokens.size() == 2)
        {
            handle_delete(client, tokens[1]);
        }
        else if (command == "exists" && tokens.size() == 2)
        {
            handle_exists(client, tokens[1]);
        }
        else if (command == "list")
        {
            if (tokens.size() == 1)
            {
                handle_list(client);
            }
            else if (tokens.size() == 2)
            {
                handle_list(client, tokens[1]);
            }
            else
            {
                std::cout << "Usage: list [prefix]\n";
            }
        }
        else
        {
            std::cout << "Unknown command or invalid arguments. Type 'help' for usage.\n";
        }
    }
}

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        print_usage(argv[0]);
        return 1;
    }

    std::vector<std::string> cluster_endpoints = {
        "127.0.0.1:8001",
        "127.0.0.1:8002",
        "127.0.0.1:8003"};

    KVStoreClient client(cluster_endpoints);
    client.set_timeout(std::chrono::milliseconds(5000));
    client.set_retry_count(3);

    if (!client.connect())
    {
        std::cerr << "Error: Failed to connect to cluster\n";
        std::cerr << "Make sure the cluster is running:\n";
        std::cerr << "  ./scripts/run_cluster.sh\n";
        return 1;
    }

    std::string command = argv[1];

    try
    {
        if (command == "put" && argc == 4)
        {
            return handle_put(client, argv[2], argv[3]) ? 0 : 1;
        }
        else if (command == "get" && argc == 3)
        {
            return handle_get(client, argv[2]) ? 0 : 1;
        }
        else if (command == "delete" && argc == 3)
        {
            return handle_delete(client, argv[2]) ? 0 : 1;
        }
        else if (command == "exists" && argc == 3)
        {
            return handle_exists(client, argv[2]) ? 0 : 1;
        }
        else if (command == "list")
        {
            if (argc == 2)
            {
                return handle_list(client) ? 0 : 1;
            }
            else if (argc == 3)
            {
                return handle_list(client, argv[2]) ? 0 : 1;
            }
        }
        else if (command == "benchmark" && argc == 3)
        {
            int count = std::stoi(argv[2]);
            return handle_benchmark(client, count) ? 0 : 1;
        }
        else if (command == "interactive" && argc == 2)
        {
            handle_interactive(client);
            return 0;
        }
        else
        {
            print_usage(argv[0]);
            return 1;
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
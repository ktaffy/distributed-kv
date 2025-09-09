#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <signal.h>
#include <getopt.h>
#include <thread>
#include <chrono>

#include "raft/raft_node.h"
#include "utils/config.h"
#include "utils/logger.h"

using namespace raft;

std::unique_ptr<RaftNode> g_node;
std::shared_ptr<Logger> g_logger;
volatile bool g_shutdown = false;

void signal_handler(int signal)
{
    if (signal == SIGINT || signal == SIGTERM)
    {
        g_logger->info("Received shutdown signal");
        g_shutdown = true;
        if (g_node)
        {
            g_node->stop();
        }
    }
}

void print_usage(const char *program_name)
{
    std::cout << "Usage: " << program_name << " [OPTIONS]\n"
              << "Options:\n"
              << "  -c, --config FILE     Configuration file path\n"
              << "  -n, --node-id ID      Node ID (overrides config)\n"
              << "  -p, --port PORT       Listen port (overrides config)\n"
              << "  -d, --data-dir DIR    Data directory (overrides config)\n"
              << "  -l, --log-level LEVEL Log level (TRACE,DEBUG,INFO,WARN,ERROR,FATAL)\n"
              << "  -h, --help            Show this help message\n"
              << "  -v, --version         Show version information\n"
              << "\nExamples:\n"
              << "  " << program_name << " --config config/node1.conf\n"
              << "  " << program_name << " --node-id 1 --port 8001 --data-dir ./data/node1\n";
}

void print_version()
{
    std::cout << "Distributed Key-Value Store v1.0.0\n"
              << "Built with Raft consensus algorithm\n"
              << "Copyright (c) 2024\n";
}

bool parse_arguments(int argc, char *argv[], Config &config)
{
    static struct option long_options[] = {
        {"config", required_argument, 0, 'c'},
        {"node-id", required_argument, 0, 'n'},
        {"port", required_argument, 0, 'p'},
        {"data-dir", required_argument, 0, 'd'},
        {"log-level", required_argument, 0, 'l'},
        {"help", no_argument, 0, 'h'},
        {"version", no_argument, 0, 'v'},
        {0, 0, 0, 0}};

    std::string config_file;
    int c;
    int option_index = 0;

    while ((c = getopt_long(argc, argv, "c:n:p:d:l:hv", long_options, &option_index)) != -1)
    {
        switch (c)
        {
        case 'c':
            config_file = optarg;
            break;
        case 'n':
            config.set_node_id(std::stoul(optarg));
            break;
        case 'p':
            config.set_listen_port(std::stoul(optarg));
            break;
        case 'd':
            config.set_data_directory(optarg);
            break;
        case 'l':
            config.set_log_level(optarg);
            break;
        case 'h':
            print_usage(argv[0]);
            return false;
        case 'v':
            print_version();
            return false;
        case '?':
            print_usage(argv[0]);
            return false;
        default:
            break;
        }
    }

    if (!config_file.empty())
    {
        if (!config.load_from_file(config_file))
        {
            std::cerr << "Error: Failed to load config file: " << config_file << std::endl;
            return false;
        }
    }

    return true;
}

bool validate_config(const Config &config)
{
    if (config.get_node_id() == 0)
    {
        std::cerr << "Error: Node ID must be specified and non-zero" << std::endl;
        return false;
    }

    if (config.get_listen_port() == 0)
    {
        std::cerr << "Error: Listen port must be specified and non-zero" << std::endl;
        return false;
    }

    if (config.get_data_directory().empty())
    {
        std::cerr << "Error: Data directory must be specified" << std::endl;
        return false;
    }

    if (config.get_cluster_size() == 0)
    {
        std::cerr << "Error: Cluster configuration must include at least one node" << std::endl;
        return false;
    }

    return config.validate();
}

void setup_logging(const Config &config)
{
    LogLevel level = Logger::string_to_level(config.get_log_level());
    Logger::set_global_level(level);

    std::string log_file = config.get_data_directory() + "/node.log";
    g_logger = std::make_shared<Logger>("main", log_file, level);
    g_logger->set_console_output(true);
}

void print_startup_info(const Config &config)
{
    g_logger->info("Starting Distributed Key-Value Store");
    g_logger->info("Node ID: {}", config.get_node_id());
    g_logger->info("Listen Address: {}:{}", config.get_listen_address(), config.get_listen_port());
    g_logger->info("Data Directory: {}", config.get_data_directory());
    g_logger->info("Cluster Size: {}", config.get_cluster_size());
    g_logger->info("Log Level: {}", config.get_log_level());

    auto cluster_nodes = config.get_cluster_nodes();
    g_logger->info("Cluster Nodes:");
    for (const auto &node : cluster_nodes)
    {
        g_logger->info("  Node {}: {}", node.node_id, node.get_endpoint());
    }
}

void run_node(const Config &config)
{
    try
    {
        g_node = std::make_unique<RaftNode>(config.get_node_id(), config);

        g_logger->info("Starting Raft node...");
        g_node->start();

        g_logger->info("Node started successfully");

        while (!g_shutdown && g_node->is_running())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            if (g_node->is_leader())
            {
                static auto last_leader_log = std::chrono::steady_clock::now();
                auto now = std::chrono::steady_clock::now();
                if (now - last_leader_log > std::chrono::seconds(10))
                {
                    g_logger->info("Node {} is LEADER (term: {})",
                                   config.get_node_id(), g_node->get_current_term());
                    last_leader_log = now;
                }
            }
        }

        g_logger->info("Stopping node...");
        g_node->stop();
        g_logger->info("Node stopped");
    }
    catch (const std::exception &e)
    {
        g_logger->fatal("Fatal error: {}", e.what());
        std::exit(1);
    }
    catch (...)
    {
        g_logger->fatal("Unknown fatal error occurred");
        std::exit(1);
    }
}

int main(int argc, char *argv[])
{
    Config config = Config::create_default_config(1, 8001);

    if (!parse_arguments(argc, argv, config))
    {
        return 0;
    }

    if (!validate_config(config))
    {
        return 1;
    }

    setup_logging(config);

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    print_startup_info(config);

    run_node(config);

    g_logger->info("Shutdown complete");
    Logger::shutdown();

    return 0;
}
#include "test_framework.h"
#include "raft_test_util.h"
#include "client/kv_client.h"
#include "server/client_server.h"
#include <memory>

using namespace raft;
using kvtest::TempDir;
using kvtest::make_config;

TEST(client_reads_and_writes_through_any_node)
{
    TempDir dirs[3];
    auto ports = kvtest::free_ports(3);
    std::vector<std::unique_ptr<RaftNode>> nodes;
    std::vector<std::unique_ptr<ClientServer>> servers;
    std::vector<std::string> endpoints;

    for (uint32_t id = 1; id <= 3; ++id)
    {
        auto config = make_config(id, dirs[id - 1].path, ports);
        nodes.push_back(std::make_unique<RaftNode>(id, config));
        nodes.back()->start();

        uint16_t client_port = client_port_for(ports[id - 1]);
        servers.push_back(std::make_unique<ClientServer>(*nodes.back(), config));
        CHECK(servers.back()->start(client_port));
        endpoints.push_back("127.0.0.1:" + std::to_string(client_port));
    }

    KVClient client(endpoints);

    auto put = client.put("greeting", "hello|world");
    CHECK(put.ok);
    CHECK_EQ(put.error, std::string());

    auto get = client.get("greeting");
    CHECK(get.ok);
    CHECK(get.found);
    CHECK_EQ(get.value, std::string("hello|world"));

    auto del = client.remove("greeting");
    CHECK(del.ok);
    CHECK(del.found);

    auto missing = client.get("greeting");
    CHECK(missing.ok);
    CHECK(!missing.found);

    for (auto &n : nodes)
        n->stop();
    for (auto &s : servers)
        s->stop();
}
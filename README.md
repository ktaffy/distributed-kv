# distributed-kv

A strongly consistent key-value store built on Raft, in C++17 with no external libraries.

## Quickstart

Requires Linux, a C++17 compiler, and CMake 3.12+.

```bash
make build    # compile the server and client
make test     # run the test suite
make run      # start a 3-node cluster on localhost
```

In another terminal:

```bash
./client_example put name messi
./client_example get name
./client_example delete name
```

The client can reach any node. Followers point it to the current leader.

## Failover

```bash
grep "is LEADER" logs/*.log | tail -1   # find the leader, e.g. node2
pkill -f node2.conf                     # kill it
./client_example get name               # a new leader answers within about a second
```

## How it works

Every write is appended to the leader's log and replicated to the followers. It is applied and acknowledged once a majority has stored it. Each node applies the same log in the same order to an in-memory map.

- **Election:** randomized timeouts, one vote per term, and the vote is persisted across restarts.
- **Replication:** followers keep entries that already match and truncate only at the first conflict. Leaders back up past conflicting terms in one step.
- **Reads:** GETs go through the log, so they are linearizable.
- **Retries:** each client tags requests with an ID and sequence number, so a retried write is applied once.

## Layout

```
src/raft/         election, replication, commit
src/kvstore/      state machine and client sessions
src/network/      peer transport and message encoding
src/server/       client-facing server (Raft port + 1000)
src/client/       C++ client library
src/persistence/  term, vote, and log storage
src/config/       example 3-node configuration
tests/            unit tests and in-process cluster tests
```

## Limitations

- The log is persisted but not yet fsynced, so a power loss can lose acknowledged writes.
- There are no snapshots, so the log grows without bound.
- Reads cost a full replication round. ReadIndex is planned.
- It runs a single Raft group with a fixed membership.

# Distributed Key-Value Store

## Features
- Raft Consensus: Complete implementation with leader election and log replication
- Fault Tolerance: Handles node failures and network partitions
- Consistent Hashing: Even data distribution with configurable replication
- Persistent Storage: Crash-safe with write-ahead logging and snapshots
- High Performance: 10,000+ operations/second with sub-ms read latency

## Quick Start
### Build
```
git clone https://github.com/ktaffy/distributed-kv.git
cd distributed-kv
./scripts/build.sh
```
### Run Cluster
```
# Start 3-node cluster
./scripts/run_cluster.sh

# Test the cluster
./scripts/test_cluster.sh
```

### Usage
```
# Basic operations
./client_example put user:123 "Alice"
./client_example get user:123
./client_example delete user:123
```

## Configuration
Edit `config/node1.conf`, `config/node2.conf`, `config/node3.conf` to customize:
- Node IDs and network addresses
- Data directories and log levels
- Raft timing parameters
- Replication settings

## Architecture
- **Raft Layer**: Consensus and replication
- **Storage Engine**: Persistent key-value store
- **Network Layer**: TCP communication between nodes
- **Client Interface**: Simple GET/PUT/DELETE API

## Requirements
- C++17 compiler
- CMake 3.12+
- Linux
#!/bin/bash

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BINARY_PATH="$PROJECT_ROOT/distributed_kv"
CONFIG_DIR="$PROJECT_ROOT/config"
DATA_DIR="$PROJECT_ROOT/data"
LOGS_DIR="$PROJECT_ROOT/logs"

NODE_COUNT=3
PIDS=()

cleanup() {
    echo -e "${YELLOW}Shutting down cluster...${NC}"
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "${BLUE}Stopping node with PID $pid${NC}"
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
    
    sleep 2
    
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "${RED}Force killing node with PID $pid${NC}"
            kill -KILL "$pid" 2>/dev/null || true
        fi
    done
    
    echo -e "${GREEN}Cluster shutdown complete${NC}"
}

trap cleanup EXIT INT TERM

check_prerequisites() {
    if [ ! -f "$BINARY_PATH" ]; then
        echo -e "${RED}Error: Binary not found at $BINARY_PATH${NC}"
        echo -e "${YELLOW}Please run: ./scripts/build.sh${NC}"
        exit 1
    fi
    
    if [ ! -d "$CONFIG_DIR" ]; then
        echo -e "${RED}Error: Config directory not found at $CONFIG_DIR${NC}"
        exit 1
    fi
    
    for i in $(seq 1 $NODE_COUNT); do
        if [ ! -f "$CONFIG_DIR/node$i.conf" ]; then
            echo -e "${RED}Error: Config file not found: $CONFIG_DIR/node$i.conf${NC}"
            exit 1
        fi
    done
}

create_directories() {
    echo -e "${BLUE}Creating data directories...${NC}"
    for i in $(seq 1 $NODE_COUNT); do
        mkdir -p "$DATA_DIR/node$i"
        mkdir -p "$LOGS_DIR"
    done
}

start_node() {
    local node_id=$1
    local config_file="$CONFIG_DIR/node$node_id.conf"
    local log_file="$LOGS_DIR/node$node_id.log"
    
    echo -e "${BLUE}Starting node $node_id...${NC}"
    
    "$BINARY_PATH" --config "$config_file" > "$log_file" 2>&1 &
    local pid=$!
    PIDS+=($pid)
    
    sleep 1
    
    if kill -0 "$pid" 2>/dev/null; then
        echo -e "${GREEN}Node $node_id started successfully (PID: $pid)${NC}"
        return 0
    else
        echo -e "${RED}Failed to start node $node_id${NC}"
        return 1
    fi
}

wait_for_leader_election() {
    echo -e "${BLUE}Waiting for leader election...${NC}"
    local max_wait=30
    local wait_time=0
    
    while [ $wait_time -lt $max_wait ]; do
        for i in $(seq 1 $NODE_COUNT); do
            local log_file="$LOGS_DIR/node$i.log"
            if [ -f "$log_file" ] && grep -q "is LEADER" "$log_file"; then
                echo -e "${GREEN}Leader elected! Check logs for details.${NC}"
                return 0
            fi
        done
        
        sleep 1
        wait_time=$((wait_time + 1))
        echo -n "."
    done
    
    echo -e "${YELLOW}Leader election taking longer than expected. Cluster may still be starting up.${NC}"
}

show_cluster_status() {
    echo -e "${BLUE}Cluster Status:${NC}"
    echo "================="
    
    for i in $(seq 1 $NODE_COUNT); do
        local pid=${PIDS[$((i-1))]}
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "Node $i: ${GREEN}RUNNING${NC} (PID: $pid)"
        else
            echo -e "Node $i: ${RED}STOPPED${NC}"
        fi
    done
    
    echo ""
    echo -e "${BLUE}Endpoints:${NC}"
    for i in $(seq 1 $NODE_COUNT); do
        local port=$((8000 + i))
        echo "  Node $i: http://127.0.0.1:$port"
    done
    
    echo ""
    echo -e "${BLUE}Log files:${NC}"
    for i in $(seq 1 $NODE_COUNT); do
        echo "  Node $i: $LOGS_DIR/node$i.log"
    done
}

monitor_cluster() {
    echo -e "${BLUE}Monitoring cluster (Ctrl+C to stop)...${NC}"
    echo "Press Ctrl+C to shutdown the cluster"
    echo ""
    
    while true; do
        local running_count=0
        for pid in "${PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                running_count=$((running_count + 1))
            fi
        done
        
        echo -e "\r${BLUE}Nodes running: $running_count/$NODE_COUNT${NC} $(date '+%H:%M:%S')"
        
        if [ $running_count -eq 0 ]; then
            echo -e "${RED}All nodes have stopped!${NC}"
            break
        fi
        
        sleep 5
    done
}

main() {
    echo -e "${GREEN}Starting Distributed Key-Value Store Cluster${NC}"
    echo "=============================================="
    
    check_prerequisites
    create_directories
    
    echo -e "${BLUE}Starting $NODE_COUNT nodes...${NC}"
    for i in $(seq 1 $NODE_COUNT); do
        start_node $i
        sleep 1
    done
    
    echo ""
    wait_for_leader_election
    echo ""
    show_cluster_status
    echo ""
    
    monitor_cluster
}

if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main "$@"
fi
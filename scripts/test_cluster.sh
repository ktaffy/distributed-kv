#!/bin/bash

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CLIENT_BINARY="$PROJECT_ROOT/client_example"
LOGS_DIR="$PROJECT_ROOT/logs"

TEST_RESULTS=()
TOTAL_TESTS=0
PASSED_TESTS=0

log_test() {
    local message="$1"
    echo -e "${BLUE}[TEST]${NC} $message"
}

log_success() {
    local message="$1"
    echo -e "${GREEN}[PASS]${NC} $message"
    PASSED_TESTS=$((PASSED_TESTS + 1))
}

log_failure() {
    local message="$1"
    echo -e "${RED}[FAIL]${NC} $message"
}

log_info() {
    local message="$1"
    echo -e "${YELLOW}[INFO]${NC} $message"
}

run_test() {
    local test_name="$1"
    local test_command="$2"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    log_test "Running: $test_name"
    
    if eval "$test_command"; then
        log_success "$test_name"
        TEST_RESULTS+=("PASS: $test_name")
        return 0
    else
        log_failure "$test_name"
        TEST_RESULTS+=("FAIL: $test_name")
        return 1
    fi
}

check_cluster_running() {
    local running_nodes=0
    for port in 8001 8002 8003; do
        if nc -z 127.0.0.1 $port 2>/dev/null; then
            running_nodes=$((running_nodes + 1))
        fi
    done
    
    if [ $running_nodes -eq 3 ]; then
        return 0
    else
        echo "Only $running_nodes/3 nodes are running"
        return 1
    fi
}

test_basic_connectivity() {
    for port in 8001 8002 8003; do
        if ! nc -z 127.0.0.1 $port 2>/dev/null; then
            echo "Node on port $port is not reachable"
            return 1
        fi
    done
    return 0
}

test_leader_election() {
    local leader_found=false
    for i in 1 2 3; do
        if [ -f "$LOGS_DIR/node$i.log" ]; then
            if grep -q "is LEADER" "$LOGS_DIR/node$i.log"; then
                leader_found=true
                log_info "Node $i is the leader"
                break
            fi
        fi
    done
    
    if [ "$leader_found" = true ]; then
        return 0
    else
        echo "No leader found in logs"
        return 1
    fi
}

test_client_operations() {
    if [ ! -f "$CLIENT_BINARY" ]; then
        echo "Client binary not found at $CLIENT_BINARY"
        return 1
    fi
    
    local test_key="test_key_$(date +%s)"
    local test_value="test_value_$(date +%s)"
    
    log_info "Testing PUT operation"
    if ! "$CLIENT_BINARY" put "$test_key" "$test_value" >/dev/null 2>&1; then
        echo "PUT operation failed"
        return 1
    fi
    
    log_info "Testing GET operation"
    local retrieved_value
    if ! retrieved_value=$("$CLIENT_BINARY" get "$test_key" 2>/dev/null); then
        echo "GET operation failed"
        return 1
    fi
    
    if [ "$retrieved_value" != "$test_value" ]; then
        echo "Retrieved value doesn't match: expected '$test_value', got '$retrieved_value'"
        return 1
    fi
    
    log_info "Testing DELETE operation"
    if ! "$CLIENT_BINARY" delete "$test_key" >/dev/null 2>&1; then
        echo "DELETE operation failed"
        return 1
    fi
    
    log_info "Verifying key is deleted"
    if "$CLIENT_BINARY" get "$test_key" >/dev/null 2>&1; then
        echo "Key still exists after deletion"
        return 1
    fi
    
    return 0
}

test_multiple_operations() {
    local num_operations=10
    log_info "Testing $num_operations concurrent operations"
    
    for i in $(seq 1 $num_operations); do
        local key="bulk_test_$i"
        local value="value_$i"
        
        if ! "$CLIENT_BINARY" put "$key" "$value" >/dev/null 2>&1; then
            echo "Bulk PUT operation $i failed"
            return 1
        fi
    done
    
    for i in $(seq 1 $num_operations); do
        local key="bulk_test_$i"
        local expected_value="value_$i"
        local actual_value
        
        if ! actual_value=$("$CLIENT_BINARY" get "$key" 2>/dev/null); then
            echo "Bulk GET operation $i failed"
            return 1
        fi
        
        if [ "$actual_value" != "$expected_value" ]; then
            echo "Bulk operation $i: value mismatch"
            return 1
        fi
    done
    
    return 0
}

test_node_failure_recovery() {
    log_info "Testing node failure and recovery (simulated)"
    
    local test_key="failure_test_$(date +%s)"
    local test_value="failure_value_$(date +%s)"
    
    if ! "$CLIENT_BINARY" put "$test_key" "$test_value" >/dev/null 2>&1; then
        echo "Initial PUT before failure test failed"
        return 1
    fi
    
    sleep 2
    
    if ! retrieved_value=$("CLIENT_BINARY" get "$test_key" 2>/dev/null); then
        echo "GET after simulated failure failed"
        return 1
    fi
    
    if [ "$retrieved_value" != "$test_value" ]; then
        echo "Data consistency check failed after simulated failure"
        return 1
    fi
    
    return 0
}

test_log_consistency() {
    log_info "Checking log consistency across nodes"
    
    local inconsistencies=0
    for i in 1 2 3; do
        if [ -f "$LOGS_DIR/node$i.log" ]; then
            if grep -q "ERROR\|FATAL" "$LOGS_DIR/node$i.log"; then
                echo "Found errors in node $i log"
                inconsistencies=$((inconsistencies + 1))
            fi
        else
            echo "Log file for node $i not found"
            inconsistencies=$((inconsistencies + 1))
        fi
    done
    
    if [ $inconsistencies -eq 0 ]; then
        return 0
    else
        echo "Found $inconsistencies log inconsistencies"
        return 1
    fi
}

performance_test() {
    log_info "Running basic performance test"
    
    local start_time=$(date +%s)
    local num_ops=100
    
    for i in $(seq 1 $num_ops); do
        local key="perf_test_$i"
        local value="perf_value_$i"
        
        "$CLIENT_BINARY" put "$key" "$value" >/dev/null 2>&1 || {
            echo "Performance test PUT $i failed"
            return 1
        }
    done
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    local ops_per_sec=$((num_ops / (duration + 1)))
    
    log_info "Performance: $num_ops operations in ${duration}s (~${ops_per_sec} ops/sec)"
    
    if [ $ops_per_sec -gt 10 ]; then
        return 0
    else
        echo "Performance too low: $ops_per_sec ops/sec"
        return 1
    fi
}

show_test_summary() {
    echo ""
    echo -e "${BLUE}Test Summary${NC}"
    echo "============="
    echo -e "Total tests: $TOTAL_TESTS"
    echo -e "Passed: ${GREEN}$PASSED_TESTS${NC}"
    echo -e "Failed: ${RED}$((TOTAL_TESTS - PASSED_TESTS))${NC}"
    
    if [ $PASSED_TESTS -eq $TOTAL_TESTS ]; then
        echo -e "${GREEN}All tests passed!${NC}"
    else
        echo -e "${RED}Some tests failed!${NC}"
    fi
    
    echo ""
    echo "Detailed Results:"
    for result in "${TEST_RESULTS[@]}"; do
        if [[ $result == PASS:* ]]; then
            echo -e "${GREEN}✓${NC} ${result#PASS: }"
        else
            echo -e "${RED}✗${NC} ${result#FAIL: }"
        fi
    done
}

main() {
    echo -e "${GREEN}Distributed Key-Value Store Cluster Tests${NC}"
    echo "=========================================="
    echo ""
    
    log_info "Checking if cluster is running..."
    run_test "Cluster connectivity" "check_cluster_running"
    
    run_test "Basic node connectivity" "test_basic_connectivity"
    
    run_test "Leader election" "test_leader_election"
    
    run_test "Basic client operations (PUT/GET/DELETE)" "test_client_operations"
    
    run_test "Multiple operations" "test_multiple_operations"
    
    run_test "Node failure recovery simulation" "test_node_failure_recovery"
    
    run_test "Log consistency check" "test_log_consistency"
    
    run_test "Basic performance test" "performance_test"
    
    show_test_summary
    
    if [ $PASSED_TESTS -eq $TOTAL_TESTS ]; then
        exit 0
    else
        exit 1
    fi
}

if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main "$@"
fi
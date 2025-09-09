#!/bin/bash

# Build script for distributed-kv project
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
BUILD_TYPE=${1:-Release}
BUILD_DIR="build"
INSTALL_DIR="install"
NUM_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

echo -e "${BLUE}Building distributed-kv project...${NC}"
echo -e "${YELLOW}Build type: ${BUILD_TYPE}${NC}"
echo -e "${YELLOW}Using ${NUM_CORES} cores${NC}"

# Create build directory
if [ -d "$BUILD_DIR" ]; then
    echo -e "${YELLOW}Cleaning existing build directory...${NC}"
    rm -rf "$BUILD_DIR"
fi

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Configure with CMake
echo -e "${BLUE}Configuring with CMake...${NC}"
cmake .. \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    -DCMAKE_INSTALL_PREFIX="../$INSTALL_DIR" \
    -DBUILD_TESTS=ON

# Build
echo -e "${BLUE}Building project...${NC}"
make -j"$NUM_CORES"

# Run tests
echo -e "${BLUE}Running tests...${NC}"
if make test; then
    echo -e "${GREEN}All tests passed!${NC}"
else
    echo -e "${RED}Some tests failed!${NC}"
    exit 1
fi

# Install
echo -e "${BLUE}Installing...${NC}"
make install

cd ..

# Create symbolic links for easy access
echo -e "${BLUE}Creating symbolic links...${NC}"
ln -sf "$BUILD_DIR/distributed_kv" distributed_kv
ln -sf "$BUILD_DIR/client_example" client_example

# Create data directories
echo -e "${BLUE}Creating data directories...${NC}"
mkdir -p data/{node1,node2,node3}
mkdir -p logs

# Set permissions for scripts
chmod +x scripts/*.sh

echo -e "${GREEN}Build completed successfully!${NC}"
echo -e "${YELLOW}Executables:${NC}"
echo -e "  ./distributed_kv - Main server"
echo -e "  ./client_example - Example client"
echo -e ""
echo -e "${YELLOW}Quick start:${NC}"
echo -e "  ./scripts/run_cluster.sh   # Start a 3-node cluster"
echo -e "  ./scripts/test_cluster.sh  # Test the cluster"
echo -e ""
echo -e "${YELLOW}Single node:${NC}"
echo -e "  ./distributed_kv --config config/node1.conf --node-id 1"
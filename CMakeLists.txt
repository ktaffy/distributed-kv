cmake_minimum_required(VERSION 3.12)
project(distributed-kv VERSION 1.0.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Compiler flags
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -Wall -Wextra -Wpedantic")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

# Default to Release build
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Find required packages
find_package(Threads REQUIRED)

# Include directories
include_directories(src)

# Source files
set(RAFT_SOURCES
    src/raft/raft_node.cpp
    src/raft/raft_state.cpp
    src/raft/log_entry.cpp
)

set(KVSTORE_SOURCES
    src/kvstore/kv_store.cpp
    src/kvstore/consistent_hash.cpp
)

set(NETWORK_SOURCES
    src/network/network_manager.cpp
    src/network/message.cpp
)

set(PERSISTENCE_SOURCES
    src/persistence/persistent_state.cpp
    src/persistence/log_storage.cpp
)

set(UTILS_SOURCES
    src/utils/logger.cpp
    src/utils/config.cpp
)

# Main executable
add_executable(distributed_kv
    src/main.cpp
    ${RAFT_SOURCES}
    ${KVSTORE_SOURCES}
    ${NETWORK_SOURCES}
    ${PERSISTENCE_SOURCES}
    ${UTILS_SOURCES}
)

# Link libraries
target_link_libraries(distributed_kv 
    Threads::Threads
)

# Client library (for examples and external usage)
add_library(kvstore_client STATIC
    ${KVSTORE_SOURCES}
    ${NETWORK_SOURCES}
    ${UTILS_SOURCES}
)

target_link_libraries(kvstore_client
    Threads::Threads
)

# Example client
add_executable(client_example
    examples/client_example.cpp
)

target_link_libraries(client_example
    kvstore_client
    Threads::Threads
)

# Testing
option(BUILD_TESTS "Build tests" ON)

if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# Installation
install(TARGETS distributed_kv RUNTIME DESTINATION bin)
install(TARGETS kvstore_client ARCHIVE DESTINATION lib)
install(DIRECTORY src/ DESTINATION include FILES_MATCHING PATTERN "*.h")

# Configuration files
install(DIRECTORY config/ DESTINATION etc/distributed-kv)

# Scripts
install(PROGRAMS 
    scripts/run_cluster.sh
    scripts/test_cluster.sh
    DESTINATION bin
)

# Create data directories
install(DIRECTORY DESTINATION var/lib/distributed-kv/node1)
install(DIRECTORY DESTINATION var/lib/distributed-kv/node2)
install(DIRECTORY DESTINATION var/lib/distributed-kv/node3)

# CPack configuration for packaging
set(CPACK_PACKAGE_NAME "distributed-kv")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Distributed Key-Value Store with Raft Consensus")
set(CPACK_PACKAGE_VENDOR "Your Name")
set(CPACK_PACKAGE_CONTACT "your.email@example.com")

# Platform-specific packaging
if(WIN32)
    set(CPACK_GENERATOR "ZIP")
elseif(APPLE)
    set(CPACK_GENERATOR "TGZ")
else()
    set(CPACK_GENERATOR "TGZ;DEB;RPM")
    set(CPACK_DEBIAN_PACKAGE_DEPENDS "libc6 (>= 2.27)")
    set(CPACK_RPM_PACKAGE_REQUIRES "glibc >= 2.27")
endif()

include(CPack)
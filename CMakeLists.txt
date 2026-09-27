cmake_minimum_required(VERSION 3.12)
project(distributed-kv VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra -Wpedantic)

find_package(Threads REQUIRED)
include_directories(src)

add_library(kv_proto STATIC
    src/raft/log_entry.cpp
    src/network/message.cpp
)

add_library(kv_core STATIC
    src/raft/raft_node.cpp
    src/raft/raft_state.cpp
    src/network/network_manager.cpp
    src/persistence/persistent_state.cpp
    src/persistence/log_storage.cpp
    src/server/client_server.cpp
    src/utils/logger.cpp
    src/utils/config.cpp
)
target_link_libraries(kv_core PUBLIC kv_proto Threads::Threads)

add_library(kv_client STATIC
    src/client/kv_client.cpp
)
target_link_libraries(kv_client PUBLIC kv_proto Threads::Threads)

add_executable(distributed_kv src/main.cpp)
target_link_libraries(distributed_kv PRIVATE kv_core)

add_executable(client_example examples/client_example.cpp)
target_link_libraries(client_example PRIVATE kv_client)

option(BUILD_TESTS "Build tests" ON)
if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
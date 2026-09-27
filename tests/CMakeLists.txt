add_executable(unit_tests
    test_main.cpp
    test_log_entry.cpp
    test_raft_state.cpp
    test_raft_node.cpp
    test_serialization.cpp
    test_client.cpp
    test_state_machine.cpp
    ${PROJECT_SOURCE_DIR}/src/raft/log_entry.cpp
    ${PROJECT_SOURCE_DIR}/src/raft/raft_state.cpp
    ${PROJECT_SOURCE_DIR}/src/raft/raft_node.cpp
    ${PROJECT_SOURCE_DIR}/src/network/network_manager.cpp
    ${PROJECT_SOURCE_DIR}/src/network/message.cpp
    ${PROJECT_SOURCE_DIR}/src/persistence/persistent_state.cpp
    ${PROJECT_SOURCE_DIR}/src/persistence/log_storage.cpp
    ${PROJECT_SOURCE_DIR}/src/utils/logger.cpp
    ${PROJECT_SOURCE_DIR}/src/utils/config.cpp
    ${PROJECT_SOURCE_DIR}/src/server/client_server.cpp
    ${PROJECT_SOURCE_DIR}/src/client/kv_client.cpp
)

target_link_libraries(unit_tests Threads::Threads)
add_test(NAME unit_tests COMMAND unit_tests)
set_tests_properties(unit_tests PROPERTIES TIMEOUT 30)
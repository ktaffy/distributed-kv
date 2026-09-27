add_executable(unit_tests
    test_main.cpp
    test_log_entry.cpp
    test_raft_state.cpp
    test_raft_node.cpp
    test_serialization.cpp
    test_client.cpp
    test_state_machine.cpp
)

target_link_libraries(unit_tests PRIVATE kv_core kv_client)
add_test(NAME unit_tests COMMAND unit_tests)
set_tests_properties(unit_tests PROPERTIES TIMEOUT 30)
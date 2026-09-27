add_executable(unit_tests
    test_main.cpp
    test_log_entry.cpp
    test_raft_state.cpp
    ${PROJECT_SOURCE_DIR}/src/raft/log_entry.cpp
    ${PROJECT_SOURCE_DIR}/src/raft/raft_state.cpp
)

target_link_libraries(unit_tests Threads::Threads)
add_test(NAME unit_tests COMMAND unit_tests)
set_tests_properties(unit_tests PROPERTIES TIMEOUT 10)
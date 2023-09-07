set(PROTOCOL_TEST_DEPS
    trino_protocol
    gluten_trino_functions
    presto_type_converter
    gtest
    velox_core
    velox_exec
    velox_tpch_connector
    velox_hive_connector
    velox_type_fbhive)

add_executable(TpchQ6PlanTest TpchQ6PlanTest.cpp)
add_executable(SplitTest SplitTest.cpp)

target_link_libraries(TpchQ6PlanTest ${PROTOCOL_TEST_DEPS})
target_link_libraries(SplitTest ${PROTOCOL_TEST_DEPS})

add_test(TpchQ6PlanTest TpchQ6PlanTest ${TEST_ARGS})
add_test(SplitTest SplitTest ${TEST_ARGS})

set_tests_properties(
  SplitTest PROPERTIES ENVIRONMENT DATA_DIR=${CMAKE_SOURCE_DIR}/src/testJson)

set_tests_properties(
  TpchQ6PlanTest PROPERTIES ENVIRONMENT
                            DATA_DIR=${CMAKE_SOURCE_DIR}/src/testJson)

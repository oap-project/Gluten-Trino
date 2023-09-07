set(TEST_LIBS
    gluten_trino
    gtest
    Folly::folly
    velox_core
    velox_exec
    velox_vector_test_lib
    velox_exec_test_lib)

add_executable(SerdeTest SerdeTest.cpp)
target_link_libraries(SerdeTest ${TEST_LIBS})
add_test(SerdeTest ${EXECUTABLE_OUTPUT_PATH}/SerdeTest ${TEST_ARGS})

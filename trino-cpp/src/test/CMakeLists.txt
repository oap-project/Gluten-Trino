# find_package(GTest REQUIRED)

set(TEST_LIBS 
    trino_bridge
    gtest
    Folly::folly
)
# older versions of GCC need it to allow std::filesystem
if(CMAKE_COMPILER_IS_GNUCC AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9)
    list(APPEND TEST_LIBS "stdc++fs")
endif()

add_executable(SerdeTest serde_test.cpp)
target_link_libraries(SerdeTest ${TEST_LIBS})
add_test(SerdeTest ${EXECUTABLE_OUTPUT_PATH}/SerdeTest ${TEST_ARGS})

add_executable(WorkflowTest workflow_test.cpp)
target_link_libraries(WorkflowTest ${TEST_LIBS})
add_test(WorkflowTest ${EXECUTABLE_OUTPUT_PATH}/WorkflowTest ${TEST_ARGS})
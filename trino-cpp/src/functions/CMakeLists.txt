add_library(trino_bridge_functions STATIC TpchPartitionFunction.cpp TrinoSumAggregate.cpp)
target_link_libraries(trino_bridge_functions velox_core velox_exec velox_vector)
target_include_directories(trino_bridge_functions SYSTEM PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/src)
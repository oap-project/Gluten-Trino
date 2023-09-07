add_library(gluten_trino_functions STATIC TpchPartitionFunction.cpp
                                          TrinoSumAggregate.cpp)
target_link_libraries(gluten_trino_functions velox_core velox_exec velox_vector)
target_include_directories(
  gluten_trino_functions SYSTEM PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
                                       ${PROJECT_SOURCE_DIR}/src)

if(GLUTEN_TRINO_BUILDING_TESTS)
  add_subdirectory(tests)
endif()

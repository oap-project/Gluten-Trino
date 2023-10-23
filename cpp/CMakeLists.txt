cmake_minimum_required(VERSION 3.15)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

project(TrinoBridge)

include(CTest)

execute_process(
  COMMAND
    bash -c
    "( source ${CMAKE_CURRENT_SOURCE_DIR}/velox/scripts/setup-helper-functions.sh && echo -n $(get_cxx_flags $ENV{CPU_TARGET}))"
  OUTPUT_VARIABLE SCRIPT_CXX_FLAGS
  RESULT_VARIABLE COMMAND_STATUS)

# Compiler options
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS}")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Gluten-Trino options
option(GLUTEN_TRINO_ENABLE_SANITIZER
       "Enable AddressSanitizer for Gluten-Trino." OFF)
option(GLUTEN_TRINO_BUILDING_TESTS "Building tests for Gluten-Trino." ON)
option(GLUTEN_TRINO_ENABLE_S3 "Enable S3 storage for Gluten-Trino." OFF)

# Velox options
add_compile_definitions(FOLLY_HAVE_INT128_T=1)
set(VELOX_BUILD_TESTING OFF)
set(VELOX_ENABLE_SPARK_FUNCTIONS OFF)
set(VELOX_ENABLE_EXAMPLES OFF)
set(VELOX_ENABLE_PARSE ON)
set(VELOX_BUILD_TEST_UTILS ON)
set(VELOX_ENABLE_PARQUET ON)
set(VELOX_ENABLE_HDFS ON)

if(GLUTEN_TRINO_ENABLE_S3)
  set(VELOX_ENABLE_S3 ON)
  add_definitions(-DENABLE_GLUTEN_TRINO_S3)
else()
  set(VELOX_ENABLE_S3 OFF)
endif(GLUTEN_TRINO_ENABLE_S3)

set(XSIMD_INCLUDE_DIRS ${CMAKE_BINARY_DIR}/_deps/xsimd-src/include/)
include_directories(SYSTEM ${XSIMD_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/velox)
include_directories(${CMAKE_BINARY_DIR})

add_subdirectory(velox)
add_subdirectory(src)

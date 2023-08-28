cmake_minimum_required(VERSION 3.15)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

project(TrinoBridge)

set(VELOX_BINARY_ROOT "${CMAKE_BINARY_DIR}/velox")
set(VELOX_HOME "${CMAKE_CURRENT_SOURCE_DIR}/velox")

execute_process(
  COMMAND
    bash -c
    "( source ${CMAKE_CURRENT_SOURCE_DIR}/velox/scripts/setup-helper-functions.sh && echo -n $(get_cxx_flags $ENV{CPU_TARGET}))"
  OUTPUT_VARIABLE SCRIPT_CXX_FLAGS
  RESULT_VARIABLE COMMAND_STATUS)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS}")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set all Velox options below
add_compile_definitions(FOLLY_HAVE_INT128_T=1)

set(VELOX_BUILD_TESTING OFF)
set(VELOX_ENABLE_SPARK_FUNCTIONS OFF)
set(VELOX_ENABLE_EXAMPLES OFF)
set(VELOX_ENABLE_PARSE ON)
set(VELOX_BUILD_TEST_UTILS ON)
# TBD: Whether need to support Parquet, it is disabled in Presto-cpp by default.
set(VELOX_ENABLE_PARQUET ON)
set(VELOX_ENABLE_HDFS ON)
set(VELOX_ENABLE_S3 OFF)

include_directories(.)
include_directories(src)
include_directories(velox)
include_directories(velox/velox/external/xxhash)
include_directories(${VELOX_BINARY_ROOT})
include_directories(${CMAKE_BINARY_DIR})

add_subdirectory(velox)
add_subdirectory(src)

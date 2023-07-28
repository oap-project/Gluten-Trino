find_library(FMT fmt)
find_library(RE2 re2 REQUIRED)
find_package(JNI 17 REQUIRED)
find_package(Folly REQUIRED CONFIG)
find_package(gflags REQUIRED COMPONENTS shared CONFIG)
find_package(antlr4-runtime REQUIRED)
find_path(
  ANTLR4_RUNTIME_INCLUDE_DIR
  NAMES antlr4-runtime.h
  PATH_SUFFIXES antlr4-runtime)

set(ENABLE_TRINO_EXCHANGE ON)

if(ENABLE_TRINO_EXCHANGE)
  add_definitions(-DENABLE_TRINO_EXCHANGE)
  find_library(PROXYGEN proxygen REQUIRED)
  find_library(PROXYGEN_HTTP_SERVER proxygenhttpserver REQUIRED)
  find_library(WANGLE wangle REQUIRED)
  find_library(FIZZ fizz REQUIRED)
  set(PROXYGEN_LIBRARIES ${PROXYGEN_HTTP_SERVER} ${PROXYGEN} ${WANGLE} ${FIZZ})
  find_path(PROXYGEN_DIR NAMES include/proxygen)
  set(PROXYGEN_INCLUDE_DIR "${PROXYGEN_DIR}/include/proxygen")
endif()

include_directories(SYSTEM ${ANTLR4_RUNTIME_INCLUDE_DIR})
set(Protobuf_INCLUDE_DIRS ${CMAKE_BINARY_DIR}/_deps/protobuf-src/src/)
set(Xsimd_INCLUDE_DIRS ${CMAKE_BINARY_DIR}/_deps/xsimd-src/include/)
include_directories(${JNI_INCLUDE_DIRS})

option(TRINO_BRIDGE_ENABLE_SANITIZER
       "Enable AddressSanitizer for Trino Bridge." OFF)
option(TRINO_BRIDGE_BUILDING_TESTS "Building Tests for Trino Bridge." OFF)

if(TRINO_BRIDGE_ENABLE_SANITIZER)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
else()
  # set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()

add_subdirectory(functions)
add_subdirectory(types)
add_subdirectory(protocol)

set(TRINO_CPP_UTILS_SOURCES utils/JniUtils.cpp utils/NativeListenableFuture.cpp)

set(TRINO_EXCHANGE_SOURCES TrinoExchangeSource.cpp utils/HttpClient.cpp
                           utils/Configs.cpp)

set(SOURCES trino_bridge.cpp utils.cpp test_utils.cpp NativeConfigs.cpp
        ${TRINO_CPP_UTILS_SOURCES})

if(ENABLE_TRINO_EXCHANGE)
  set(SOURCES ${SOURCES} ${TRINO_EXCHANGE_SOURCES})
endif()

add_library(trino_bridge SHARED ${SOURCES})

target_include_directories(
  trino_bridge
  PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH}
         ${JNI_INCLUDE_DIRS}
         ${CMAKE_CURRENT_SOURCE_DIR}
         ${root_directory}/src
         ${VELOX_HOME}
         ${VELOX_BUILD_PATH}
         ${VELOX_HOME}/velox/vector
         ${Xsimd_INCLUDE_DIRS}
         ${Protobuf_INCLUDE_DIRS})

set(VELOX_LIBS
    velox_core
    velox_vector
    velox_exec
    velox_time
    velox_type
    velox_type_tz
    velox_serialization
    velox_encode
    velox_dwio_common_exception
    velox_functions_lib
    velox_hive_partition_function
    velox_window
    velox_dwio_dwrf_reader
    velox_dwio_parquet_reader
    velox_functions_prestosql
    velox_aggregates
    velox_hive_connector
    velox_tpch_connector
    velox_hdfs
    velox_exec_test_lib
    velox_parse_parser)

if(VELOX_ENABLE_S3)
  add_definitions(-DENABLE_TRINO_S3)
  list(APPEND VELOX_LIBS velox_s3fs)
endif()

add_subdirectory(serialization)

target_link_libraries(trino_bridge PUBLIC
        trino_bridge_functions
        trino_bridge_serialization
        ${VELOX_LIBS}
        trino_protocol
        velox_vector_test_lib)

if(ENABLE_TRINO_EXCHANGE)
  target_link_libraries(trino_bridge PUBLIC ${PROXYGEN_LIBRARIES})
endif()

target_link_libraries(trino_bridge PUBLIC ${FMT} Folly::folly ${RE2}
                                          presto_type_converter)

if(TRINO_BRIDGE_BUILDING_TESTS)
  add_subdirectory(test)
endif()

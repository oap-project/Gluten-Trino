# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

find_library(FMT fmt)
find_library(RE2 re2 REQUIRED)
find_package(JNI 17 REQUIRED)
find_package(Folly REQUIRED CONFIG)
find_package(gflags REQUIRED COMPONENTS shared CONFIG)
find_package(antlr4-runtime REQUIRED)
find_path(
  ANTLR4_RUNTIME_INCLUDE_DIRS
  NAMES antlr4-runtime.h
  PATH_SUFFIXES antlr4-runtime)
find_library(PROXYGEN proxygen REQUIRED)
find_library(PROXYGEN_HTTP_SERVER proxygenhttpserver REQUIRED)
find_library(WANGLE wangle REQUIRED)
find_library(FIZZ fizz REQUIRED)
set(PROXYGEN_LIBRARIES ${PROXYGEN_HTTP_SERVER} ${PROXYGEN} ${WANGLE} ${FIZZ})
find_path(PROXYGEN_DIR NAMES include/proxygen)
set(PROXYGEN_INCLUDE_DIRS "${PROXYGEN_DIR}/include/proxygen")

if(GLUTEN_TRINO_ENABLE_SANITIZER)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
endif()

include_directories(SYSTEM ${ANTLR4_RUNTIME_INCLUDE_DIRS}
                    ${PROXYGEN_INCLUDE_DIRS} ${JNI_INCLUDE_DIRS})

add_subdirectory(functions)
add_subdirectory(types)
add_subdirectory(protocol)
add_subdirectory(serialization)

set(TRINO_CPP_UTILS_SOURCES utils/JniUtils.cpp)
set(TRINO_EXCHANGE_SOURCES TrinoExchangeSource.cpp utils/HttpClient.cpp
                           utils/ThreadUtils.cpp)
set(SOURCES
    TrinoBridge.cpp
    NativeConfigs.cpp
    JniHandle.cpp
    TaskHandle.cpp
    PartitionOutputData.cpp
    ${TRINO_CPP_UTILS_SOURCES}
    ${TRINO_EXCHANGE_SOURCES})

add_library(gluten_trino SHARED ${SOURCES})

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
    velox_type_fbhive
    velox_hdfs
    velox_exec_test_lib
    velox_parse_parser
    velox_parse_utils
    velox_parse_expression)

if(GLUTEN_TRINO_ENABLE_S3)
  list(APPEND VELOX_LIBS velox_s3fs)
endif()

target_link_libraries(
  gluten_trino
  PUBLIC gluten_trino_functions
         gluten_trino_serialization
         ${VELOX_LIBS}
         trino_protocol
         velox_vector_test_lib
         presto_type_converter
         ${FMT}
         Folly::folly
         ${RE2}
         ${PROXYGEN_LIBRARIES})

if(GLUTEN_TRINO_BUILDING_TESTS)
  add_subdirectory(tests)
endif()

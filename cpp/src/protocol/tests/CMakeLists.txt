# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

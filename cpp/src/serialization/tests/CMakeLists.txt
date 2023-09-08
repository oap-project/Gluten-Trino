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

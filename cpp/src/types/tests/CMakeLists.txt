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

add_executable(type_convertor_test TypeSignatureTest.cpp RowExpressionTest.cpp
                                   VeloxSplitConvertorTest.cpp)

add_test(
  NAME type_convertor_test
  COMMAND type_convertor_test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_dependencies(type_convertor_test gluten_trino)

target_link_libraries(
  type_convertor_test
  gtest
  gtest_main
  gluten_trino
  ${GLOG}
  ${GFLAGS_LIBRARIES}
  pthread)

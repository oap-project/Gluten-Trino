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

set(SERIALIZATION_SOURCE TrinoSerializer.cpp TrinoBufferStream.cpp
                         TrinoByteStream.cpp)

add_library(gluten_trino_serialization STATIC ${SERIALIZATION_SOURCE})

target_include_directories(
  gluten_trino_serialization
  PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH}
         ${JNI_INCLUDE_DIRS}
         ${CMAKE_CURRENT_SOURCE_DIR}
         ${root_directory}/src
         ${VELOX_HOME}
         ${VELOX_BUILD_PATH}
         ${VELOX_HOME}/velox/vector
         ${VELOX_HOME}/third_party/xsimd/include/)

# link velox_vector_test_lib for VectorMaker
target_link_libraries(gluten_trino_serialization PUBLIC ${VELOX_LIBS} ${FMT}
                                                        Folly::folly ${RE2})

if(GLUTEN_TRINO_BUILDING_TESTS)
  add_subdirectory(tests)
endif()

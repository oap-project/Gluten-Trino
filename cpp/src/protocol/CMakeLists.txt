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

get_filename_component(TRINO_CPP_PLUGIN_DIR "${PROJECT_SOURCE_DIR}/../java" ABSOLUTE)
if(DEFINED ENV{TRINO_HOME})
  set(TRINO_ABSOLUTE_DIR $ENV{TRINO_HOME})
else()
  get_filename_component(TRINO_ABSOLUTE_DIR "$ENV{HOME}/trino" ABSOLUTE)
endif()

set(TRINO_PROTOCOL_SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/trino_protocol.cpp)
set(TRINO_PROTOCOL_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/trino_protocol.h)

foreach (file ${TRINO_PROTOCOL_HEADER} ${TRINO_PROTOCOL_SOURCE})
  if (EXISTS ${file})
    message(STATUS "Deleting generated protocol file ${file}")
    file(REMOVE ${file})
  endif ()
endforeach ()

add_custom_command(
        OUTPUT ${TRINO_PROTOCOL_SOURCE} ${TRINO_PROTOCOL_HEADER}
        COMMAND TRINO_HOME=${TRINO_ABSOLUTE_DIR} TRINO_CPP_PLUGIN_HOME=${TRINO_CPP_PLUGIN_DIR} make
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_custom_target(internal_trino_protocol ALL DEPENDS ${TRINO_PROTOCOL_SOURCE} ${TRINO_PROTOCOL_HEADER})

add_library(trino_protocol OBJECT ${TRINO_PROTOCOL_SOURCE} DataSize.cpp Duration.cpp Base64Util.cpp)
add_dependencies(trino_protocol internal_trino_protocol)

target_link_libraries(trino_protocol ${RE2} ${FMT} Folly::folly velox_vector)
target_include_directories(trino_protocol SYSTEM PRIVATE ${Xsimd_INCLUDE_DIRS})

if (GLUTEN_TRINO_BUILDING_TESTS)
  add_subdirectory(tests)
endif ()
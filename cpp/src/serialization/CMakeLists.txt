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

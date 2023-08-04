

set(SERIALIZATION_SOURCE TrinoSerializer.cpp TrinoBufferStream.cpp TrinoByteStream.cpp)

add_library(trino_bridge_serialization STATIC ${SERIALIZATION_SOURCE})

target_include_directories(trino_bridge_serialization PUBLIC
${CMAKE_SYSTEM_INCLUDE_PATH}
${JNI_INCLUDE_DIRS}
${CMAKE_CURRENT_SOURCE_DIR}
${root_directory}/src
${VELOX_HOME}
${VELOX_BUILD_PATH}
${VELOX_HOME}/velox/vector
${VELOX_HOME}/third_party/xsimd/include/)

# link velox_vector_test_lib for VectorMaker
target_link_libraries(trino_bridge_serialization PUBLIC ${VELOX_LIBS} ${FMT} Folly::folly ${RE2})
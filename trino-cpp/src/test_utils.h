#pragma once

#include <jni.h>
#include <string>

#include "trino_buffer.h"
#include "trino_buffer_stream.h"
#include "trino_serializer.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/vector/tests/utils/VectorMaker.h"

#include "serialization/trino_buffer.h"
#include "serialization/trino_buffer_stream.h"
#include "serialization/trino_serializer.h"

namespace io::trino::bridge::test {

class TestUtils {
 public:
  static void writeToFile(const std::string& filePath,
                          const std::vector<facebook::velox::RowVectorPtr>& vectors,
                          std::shared_ptr<facebook::velox::dwrf::Config> config =
                              std::make_shared<facebook::velox::dwrf::Config>());

  static facebook::velox::exec::Split makeHiveSplit(std::string path) {
    return facebook::velox::exec::Split(
        facebook::velox::exec::test::HiveConnectorTestBase::makeHiveConnectorSplit(
            std::move(path)));
  }

  static facebook::velox::RowVectorPtr makeTestVector();
  static facebook::velox::exec::Split createTestHiveSplit();

  static std::shared_ptr<facebook::velox::exec::Task> makeTask(
      const std::string& taskId, facebook::velox::core::PlanFragment&& planFragment,
      int destination, facebook::velox::exec::Consumer consumer = nullptr,
      int64_t maxMemory = facebook::velox::memory::kMaxMemory);

  static io::trino::bridge::TrinoBuffer test_serialize(
      const facebook::velox::RowVectorPtr& page,
      std::shared_ptr<const facebook::velox::RowType>& rowType);

  static facebook::velox::IOBufOutputStream test_serialize_folly_buffer(
      const facebook::velox::RowVectorPtr& page,
      std::shared_ptr<const facebook::velox::RowType>& rowType);
  static facebook::velox::RowVectorPtr createEmptyPage(
      const std::shared_ptr<const facebook::velox::RowType>& rowType);
  static facebook::velox::RowVectorPtr createPage1();
  static facebook::velox::RowVectorPtr createPage2();

  static void test_serialization(JNIEnv* env, jobject obj, const std::string& bufferId,
                                 jlongArray jAddress, jintArray jLength);
};

}  // namespace io::trino::bridge::test

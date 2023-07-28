#include "test_utils.h"

#include "folly/executors/CPUThreadPoolExecutor.h"

using namespace facebook;
using namespace facebook::velox;

namespace io::trino::bridge::test {
static std::shared_ptr<memory::MemoryPool> pool{memory::addDefaultLeafMemoryPool()};
static std::shared_ptr<folly::CPUThreadPoolExecutor> executor{
    std::make_shared<folly::CPUThreadPoolExecutor>(1)};

void TestUtils::writeToFile(const std::string& filePath,
                            const std::vector<facebook::velox::RowVectorPtr>& vectors,
                            std::shared_ptr<facebook::velox::dwrf::Config> config) {
  facebook::velox::dwrf::WriterOptions options;
  options.config = config;
  options.schema = vectors[0]->type();
  auto sink = std::make_unique<facebook::velox::dwio::common::LocalFileSink>(filePath);
  auto rootPool = facebook::velox::memory::defaultMemoryManager().addRootPool();
  auto childPool = rootPool->addAggregateChild("HiveConnectorTestBase.Writer");
  facebook::velox::dwrf::Writer writer{options, std::move(sink), *childPool};
  for (size_t i = 0; i < vectors.size(); ++i) {
    writer.write(vectors[i]);
  }
  writer.close();
}

facebook::velox::RowVectorPtr TestUtils::makeTestVector() {
  velox::RowTypePtr rowType{ROW({"a", "b"}, {INTEGER(), DOUBLE()})};
  velox::test::VectorMaker vectorMaker(pool.get());
  auto flatVec1 = vectorMaker.flatVector({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto flatVec2 = vectorMaker.flatVector<double>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

  auto vector = vectorMaker.rowVector({flatVec1, flatVec2});
  return vector;
}

facebook::velox::exec::Split TestUtils::createTestHiveSplit() {
  auto vector = makeTestVector();
  std::string path = "/tmp/aaa";
  writeToFile(path, {vector});
  auto split = makeHiveSplit(path);
  return split;
}

std::shared_ptr<facebook::velox::exec::Task> TestUtils::makeTask(
    const std::string& taskId, velox::core::PlanFragment&& planFragment, int destination,
    facebook::velox::exec::Consumer consumer, int64_t maxMemory) {
  std::unordered_map<std::string, std::string> configSettings;
  auto queryCtx = std::make_shared<velox::core::QueryCtx>(executor.get());
  // queryCtx->testingOverrideMemoryPool(velox::memory::addDefaultLeafMemoryPool().getPool(
  //     queryCtx->queryId(), velox::memory::MemoryPool::Kind::kAggregate, maxMemory));
  // velox::core::PlanFragment planFragment{planNode};
  return facebook::velox::exec::Task::create(taskId, std::move(planFragment), destination, std::move(queryCtx),
                      std::move(consumer));
}

TrinoBuffer TestUtils::test_serialize(const RowVectorPtr& page,
                                      std::shared_ptr<const RowType>& rowType) {
  std::shared_ptr<MemoryManager> memoryManager = std::make_shared<MemoryManager>();

  std::shared_ptr<StreamArena> arena = std::make_shared<StreamArena>(pool.get());
  TrinoVectorSerde serde;
  auto serializer = serde.createSerializer(rowType, page->size(), arena.get(), nullptr);
  IndexRange range{0, page->size()};
  serializer->append(page, folly::Range<const IndexRange*>(&range, 1));

  TrinoBufOutputStream outputStream(*pool, nullptr);
  serializer->flush(&outputStream);

  TrinoBuffer trinoBuffer = outputStream.getTrinoBuffer(memoryManager);
  return trinoBuffer;
}

IOBufOutputStream TestUtils::test_serialize_folly_buffer(
    const facebook::velox::RowVectorPtr& page,
    std::shared_ptr<const facebook::velox::RowType>& rowType) {
  std::shared_ptr<StreamArena> arena = std::make_shared<StreamArena>(pool.get());
  TrinoVectorSerde serde;
  auto serializer = serde.createSerializer(rowType, page->size(), arena.get(), nullptr);
  IndexRange range{0, page->size()};
  serializer->append(page, folly::Range<const IndexRange*>(&range, 1));
  IOBufOutputStream outputStream(*pool);
  serializer->flush(&outputStream);
  return outputStream;
}

RowVectorPtr TestUtils::createEmptyPage(const std::shared_ptr<const RowType>& rowType) {
  velox::test::VectorMaker vectorMaker(pool.get());

  RowVectorPtr page = vectorMaker.rowVector(rowType, 0);
  return page;
}

RowVectorPtr TestUtils::createPage1() {
  velox::test::VectorMaker vectorMaker(pool.get());

  RowVectorPtr page = vectorMaker.rowVector({vectorMaker.flatVector({0, 1, 2})});
  return page;
}

RowVectorPtr TestUtils::createPage2() {
  velox::test::VectorMaker vectorMaker(pool.get());
  auto flatVec1 = vectorMaker.flatVector({0, 1, 2, 3, 4});
  flatVec1->setNull(3, true);
  flatVec1->setNullCount(1);

  auto flatVec2 = vectorMaker.flatVector<int64_t>({0, 1, 2, 3, 4});
  flatVec2->setNull(1, true);
  flatVec2->setNullCount(1);

  RowVectorPtr page = vectorMaker.rowVector({flatVec1, flatVec2});
  return page;
}

void TestUtils::test_serialization(JNIEnv* env, jobject obj, const std::string& bufferId,
                                   jlongArray jAddress, jintArray jLength) {
  if (!bufferId.compare("test_empty_page")) {
    std::vector<std::string> names{};
    std::vector<TypePtr> types{};
    std::shared_ptr<const RowType> rowType =
        std::make_shared<const RowType>(std::move(names), std::move(types));
    auto page = createEmptyPage(rowType);
    auto trinoBuffer = test_serialize(page, rowType);

    char* buf = trinoBuffer.getAddress();
    int length = static_cast<int>(trinoBuffer.getLength());
    long buf_address = (long)buf;
    env->SetLongArrayRegion(jAddress, 0, 1, &buf_address);
    env->SetIntArrayRegion(jLength, 0, 1, &length);
  } else if (!bufferId.compare("test_row_page_1")) {
    std::vector<std::string> names{"int"};
    std::vector<TypePtr> types{std::make_shared<IntegerType>()};
    std::shared_ptr<const RowType> rowType =
        std::make_shared<const RowType>(std::move(names), std::move(types));
    auto page = createPage1();
    auto trinoBuffer = test_serialize(page, rowType);

    char* buf = trinoBuffer.getAddress();
    int length = static_cast<int>(trinoBuffer.getLength());
    long buf_address = (long)buf;
    env->SetLongArrayRegion(jAddress, 0, 1, &buf_address);
    env->SetIntArrayRegion(jLength, 0, 1, &length);
    // } else if (!bufferId.compare("test_row_page_2")) {
    //   std::vector<std::string> names{"int", "long"};
    //   std::vector<TypePtr> types{std::make_shared<IntegerType>(),
    //                              std::make_shared<BigintType>()};
    //   std::shared_ptr<const RowType> rowType =
    //       std::make_shared<const RowType>(std::move(names), std::move(types));
    //   auto page = createPage2();
    //   auto trinoBuffer = test_serialize(page, rowType);

    //   char* buf = trinoBuffer.getAddress();
    //   int length = static_cast<int>(trinoBuffer.getLength());
    //   long buf_address = (long)buf;
    //   env->SetLongArrayRegion(jAddress, 0, 1, &buf_address);
    //   env->SetIntArrayRegion(jLength, 0, 1, &length);
  } else if (!bufferId.compare("test_row_page_2")) {
    std::vector<std::string> names{"int", "long"};
    std::vector<TypePtr> types{std::make_shared<IntegerType>(),
                               std::make_shared<BigintType>()};
    std::shared_ptr<const RowType> rowType =
        std::make_shared<const RowType>(std::move(names), std::move(types));
    auto page = createPage2();
    auto trinoBuffer = test_serialize_folly_buffer(page, rowType);
    auto ioBuf = trinoBuffer.getIOBuf();

    const char* buf = (const char*)ioBuf->data();
    int length = static_cast<int>(ioBuf->length());
    long buf_address = (long)buf;
    env->SetLongArrayRegion(jAddress, 0, 1, &buf_address);
    env->SetIntArrayRegion(jLength, 0, 1, &length);
  }
}

}  // namespace io::trino::bridge::test
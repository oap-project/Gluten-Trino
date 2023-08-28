#include <gtest/gtest.h>
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

#include "TestUtils.h"

#include "src/protocol/external/json/json.hpp"
#include "src/protocol/trino_protocol.h"
#include "src/types/PrestoToVeloxQueryPlan.h"
#include "src/types/PrestoToVeloxSplit.h"

#include "velox/common/memory/Memory.h"
#include "velox/core/PlanFragment.h"
#include "velox/exec/Driver.h"
#include "velox/exec/PartitionedOutputBufferManager.h"
#include "velox/exec/Task.h"
#include "velox/type/Type.h"

#include <folly/init/Init.h>
#include "folly/executors/IOThreadPoolExecutor.h"

#include "src/utils.h"

using namespace facebook;
using namespace facebook::velox;
using namespace io::trino::bridge;
using namespace io::trino::bridge::test;
using namespace io::trino::protocol;

TEST(workflow, test) {
  std::ifstream planFile("./Q6S1.json");
  nlohmann::json json = nlohmann::json::parse(planFile);

  std::shared_ptr<PlanFragment> mockPlanFragment;
  from_json(json, mockPlanFragment);

  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  io::trino::VeloxInteractiveQueryPlanConverter convertor(pool.get());
  auto fragment = convertor.toVeloxQueryPlan(*mockPlanFragment, "Test");
  std::cout << "fragment" << fragment.planNode->toString(true, true);

  std::ifstream splitFile("./Split.json");
  nlohmann::json jsonSplit = nlohmann::json::parse(splitFile);
  std::shared_ptr<SplitAssignmentsMessage> splitPtr;
  from_json(jsonSplit, splitPtr);

  static auto veloxInitializer = std::make_shared<VeloxInitializer>();

  std::string taskId{"20230523_160506_00000_38ttd.1.1.0"};

  const int kQueryThreads = 10;
  auto driverExecutor = std::make_shared<folly::CPUThreadPoolExecutor>(
      kQueryThreads, std::make_shared<folly::NamedThreadFactory>("Driver"));
  auto queryCtx = std::make_shared<velox::core::QueryCtx>(driverExecutor.get());

  auto task =
      facebook::velox::exec::Task::create(taskId, std::move(fragment), 0, queryCtx);
  uint32_t maxDrivers = 20;
  uint32_t concurrentLifespans = 10;
  facebook::velox::exec::Task::start(task, maxDrivers, concurrentLifespans);

  std::vector<io::trino::protocol::ScheduledSplit>& splits =
      splitPtr->splitAssignments[0].splits;
  for (auto& split : splits) {
    velox::exec::Split veloxSplit = io::trino::toVeloxSplit(split);
    task->addSplit(split.planNodeId, std::move(veloxSplit));
  }
  std::string planNodeId{"0"};
  task->noMoreSplits(planNodeId);

  std::this_thread::sleep_for(std::chrono::seconds(1));
  std::cout << "is running " << task->isRunning() << ". is finished "
            << task->isFinished() << std::endl;

  auto manager = velox::exec::PartitionedOutputBufferManager::getInstance().lock();
  uint64_t maxBytes = 1024 * 1024 * 10;
  std::vector<std::unique_ptr<folly::IOBuf>> pagesResult;
  bool dataExist =
      manager->getData(taskId, 0, maxBytes, 0,
                       [&pagesResult](std::vector<std::unique_ptr<folly::IOBuf>> pages,
                                      int64_t sequence) mutable {
                         for (int i = 0; i < pages.size(); i++) {
                           if (pages[i]) {
                             std::cout << " pages " << i << " is not null, size "
                                       << pages[i]->length() << std::endl;
                           } else {
                             std::cout << " pages " << i << " is null" << std::endl;
                           }
                         }
                         pagesResult = std::move(pages);
                       });
  std::cout << "result exist:" << dataExist << std::endl;
  std::cout << "num: " << pagesResult.size() << std::endl;
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv);

  int err{0};
  err = RUN_ALL_TESTS();

  return err;
}

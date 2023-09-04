
#include <gtest/gtest.h>
#include <fstream>
#include <iostream>

#include "src/protocol/external/json/json.hpp"
#include "src/protocol/trino_protocol.h"
#include "src/types/PrestoToVeloxQueryPlan.h"
#include "velox/common/memory/Memory.h"

using namespace io::trino::protocol;

class ProtocolTest : public testing::Test {};

TEST_F(ProtocolTest, plan) {
  std::ifstream file("./plan.json");
  nlohmann::json json = nlohmann::json::parse(file);

  std::shared_ptr<PlanFragment> mockPlanFragment;
  from_json(json, mockPlanFragment);

  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  io::trino::VeloxInteractiveQueryPlanConverter convertor(pool.get());
  auto veloxPlan = convertor.toVeloxQueryPlan(*mockPlanFragment, "Test");
  std::cout << veloxPlan.planNode->toString(true, true);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  err = RUN_ALL_TESTS();

  return err;
}


#include <gtest/gtest.h>
#include <fstream>
#include <iostream>

#include "src/protocol/external/json/json.hpp"
#include "src/protocol/trino_protocol.h"
#include "src/types/PrestoToVeloxQueryPlan.h"
#include "velox/common/memory/Memory.h"

using namespace io::trino::protocol;

class Q6PlanTest : public testing::Test {
 public:
  Q6PlanTest() { data_dir_ = getenv("DATA_DIR"); }

 protected:
  std::string data_dir_;
};

TEST_F(Q6PlanTest, Q6S1) {
  std::ifstream file(data_dir_ + "/Q6S1.json");
  nlohmann::json json = nlohmann::json::parse(file);

  std::shared_ptr<PlanFragment> mockPlanFragment;
  from_json(json, mockPlanFragment);

  auto pool = facebook::velox::memory::addDefaultLeafMemoryPool();
  io::trino::VeloxInteractiveQueryPlanConverter convertor(pool.get());
  auto veloxPlan = convertor.toVeloxQueryPlan(*mockPlanFragment, "Test");
  std::cout << veloxPlan.planNode->toString(true, true);
}

TEST_F(Q6PlanTest, Q6S2) {
  std::ifstream file(data_dir_ + "/Q6S2.json");
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

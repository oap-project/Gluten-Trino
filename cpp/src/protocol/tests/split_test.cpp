
#include <gtest/gtest.h>
#include <fstream>
#include <iostream>

#include "src/protocol/external/json/json.hpp"
#include "src/protocol/trino_protocol.h"
#include "src/types/PrestoToVeloxQueryPlan.h"
#include "velox/common/memory/Memory.h"

using namespace io::trino::protocol;

class ProtocolTest : public testing::Test {};

TEST_F(ProtocolTest, TPCHSplit) {
  std::ifstream file("./Split.json");
  nlohmann::json json = nlohmann::json::parse(file);

  SplitAssignmentsMessage splits;
  from_json(json, splits);
  EXPECT_EQ(splits.taskId, "20230523_160506_00000_38ttd.1.1.0");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  err = RUN_ALL_TESTS();

  return err;
}

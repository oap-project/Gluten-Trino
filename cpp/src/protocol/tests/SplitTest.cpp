/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <gtest/gtest.h>
#include <fstream>
#include <iostream>

#include "src/protocol/external/json/json.hpp"
#include "src/protocol/trino_protocol.h"
#include "src/types/PrestoToVeloxQueryPlan.h"
#include "types/PrestoToVeloxSplit.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/Split.h"

using namespace io::trino::protocol;

class ProtocolTest : public testing::Test {
 public:
  ProtocolTest() { data_dir_ = getenv("DATA_DIR"); }
  SplitAssignmentsMessage getSplitsFromFile(const std::string& file_name) {
    std::ifstream file(data_dir_ + "/" + file_name);
    nlohmann::json json = nlohmann::json::parse(file);
    SplitAssignmentsMessage splits;
    from_json(json, splits);
    return splits;
  }

 protected:
  std::string data_dir_;
};

TEST_F(ProtocolTest, TPCHConnectorSplit) {
  SplitAssignmentsMessage splits = getSplitsFromFile("TpchSplit.json");
  EXPECT_EQ(splits.taskId, "20230906_064526_00000_nqj9b.1.2.0");
  for (auto&& splitAssignment : splits.splitAssignments) {
    for (auto& split : splitAssignment.splits) {
      facebook::velox::exec::Split veloxSplit = io::trino::toVeloxSplit(split);
      EXPECT_TRUE(veloxSplit.hasConnectorSplit());
      auto ptr =
          std::dynamic_pointer_cast<facebook::velox::connector::tpch::TpchConnectorSplit>(
              veloxSplit.connectorSplit);
      EXPECT_TRUE(ptr != nullptr);
    }
  }
}

TEST_F(ProtocolTest, RemoteConnectorSplit) {
  SplitAssignmentsMessage splits = getSplitsFromFile("RemoteSplit.json");
  EXPECT_EQ(splits.taskId, "20230906_073406_00001_nqj9b.2.1.0");
  for (auto&& splitAssignment : splits.splitAssignments) {
    for (auto& split : splitAssignment.splits) {
      facebook::velox::exec::Split veloxSplit = io::trino::toVeloxSplit(split);
      EXPECT_TRUE(veloxSplit.hasConnectorSplit());
      auto ptr = std::dynamic_pointer_cast<facebook::velox::exec::RemoteConnectorSplit>(
          veloxSplit.connectorSplit);
      EXPECT_TRUE(ptr != nullptr);
    }
  }
}

TEST_F(ProtocolTest, HiveConnectorSplit) {
  SplitAssignmentsMessage splits = getSplitsFromFile("HiveSplit.json");
  EXPECT_EQ(splits.taskId, "20230906_084930_00000_9cuyd.14.0.0");
  for (auto&& splitAssignment : splits.splitAssignments) {
    for (auto& split : splitAssignment.splits) {
      facebook::velox::exec::Split veloxSplit = io::trino::toVeloxSplit(split);
      EXPECT_TRUE(veloxSplit.hasConnectorSplit());
      auto ptr =
          std::dynamic_pointer_cast<facebook::velox::connector::hive::HiveConnectorSplit>(
              veloxSplit.connectorSplit);
    }
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  int err{0};
  err = RUN_ALL_TESTS();

  return err;
}

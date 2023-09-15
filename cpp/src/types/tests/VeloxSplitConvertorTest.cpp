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

#include "src/protocol/external/json/json.hpp"
#include "src/types/PrestoToVeloxSplit.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/exec/Exchange.h"
#include "velox/velox/common/encode/Base64.h"

using namespace facebook::velox;
using namespace io::trino;

class PrestoToVeloxSplitTest : public ::testing::Test {};

TEST_F(PrestoToVeloxSplitTest, RemoteConnectorSplit) {
  auto json_str =
      R"({ "sequenceId": 0, "planNodeId": "973", "split": { "connectorId": { "catalogName": "$remote" }, "connectorSplit": { "@type": "system:io.trino.velox.protocol.RemoteSplit", "location": { "location": "http://127.0.0.1:8080/v1/task/20230914_184430_00000_mkmnx.1.0.0/results/0" }, "remoteSourceTaskId": "20230914_184430_00000_mkmnx.1.0.0" } } })";

  nlohmann::json json = nlohmann::json::parse(json_str);
  io::trino::protocol::ScheduledSplit split;
  protocol::from_json(json, split);

  facebook::velox::exec::Split veloxSplit = io::trino::toVeloxSplit(split);
  EXPECT_TRUE(veloxSplit.hasConnectorSplit());
  auto ptr = std::dynamic_pointer_cast<facebook::velox::exec::RemoteConnectorSplit>(
      veloxSplit.connectorSplit);
  EXPECT_TRUE(ptr != nullptr);
}

TEST_F(PrestoToVeloxSplitTest, TPCHConnectorSplit) {
  auto json_str =
      R"({"sequenceId":0,"planNodeId":"0","split":{"connectorId":{"catalogName":"tpch"},"connectorSplit":{"@type":"system:io.trino.plugin.tpch.protocol.TpchSplit","partNumber":257,"totalParts":288,"addresses":["127.0.0.1:8080"]}}})";

  nlohmann::json json = nlohmann::json::parse(json_str);
  io::trino::protocol::ScheduledSplit split;
  protocol::from_json(json, split);

  facebook::velox::exec::Split veloxSplit = io::trino::toVeloxSplit(split);
  EXPECT_TRUE(veloxSplit.hasConnectorSplit());
  auto ptr =
      std::dynamic_pointer_cast<facebook::velox::connector::tpch::TpchConnectorSplit>(
          veloxSplit.connectorSplit);
  EXPECT_TRUE(ptr != nullptr);
}

TEST_F(PrestoToVeloxSplitTest, HiveConnectorSplit) {
  auto json_str =
      R"({"sequenceId":0,"planNodeId":"9","split":{"connectorId":{"catalogName":"hive"},"connectorSplit":{"@type":"system:io.trino.plugin.hive.protocol.HiveSplit","fileSplit":{"path":"hdfs://sr228:9000/tpcds_2g/date_dim/part-00000-0423ec6a-3fcd-44b8-9f76-af058f672a8e-c000.snappy.parquet","start":0,"length":1856872,"fileSize":1856872,"fileModifiedTime":1693493010735,"customSplitInfo":{}},"database":"tpcds_2g","table":"date_dim","partitionName":"<UNPARTITIONED>","storage":{"storageFormat":{"serde":"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe","inputFormat":"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat","outputFormat":"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"},"location":"hdfs://sr228:9000/tpcds_2g/date_dim","skewed":false,"serdeParameters":{},"parameters":{}},"partitionKeys":[],"addresses":["sr228"],"nodeSelectionStrategy":"NO_PREFERENCE","partitionDataColumnCount":28,"tableToPartitionMapping":{"partitionSchemaDifference":{}},"s3SelectPushdownEnabled":false,"cacheQuota":{"cacheQuotaScope":"GLOBAL"},"redundantColumnDomains":[],"splitWeight":5}}})";

  nlohmann::json json = nlohmann::json::parse(json_str);
  io::trino::protocol::ScheduledSplit split;
  protocol::from_json(json, split);

  facebook::velox::exec::Split veloxSplit = io::trino::toVeloxSplit(split);
  EXPECT_TRUE(veloxSplit.hasConnectorSplit());
  auto ptr =
      std::dynamic_pointer_cast<facebook::velox::connector::hive::HiveConnectorSplit>(
          veloxSplit.connectorSplit);
}
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

#include "src/NativeConfigs.h"

class NativeConfigsTests : public ::testing::Test {};

TEST(NativeConfigsTests, basic) {
  auto& config = io::trino::bridge::NativeConfigs::instance();
  config.initialize(
      "{"
      "\"maxOutputPageBytes\": 1,"
      "\"maxWorkerThreads\": 2,"
      "\"maxDriversPerTask\": 3,"
      "\"taskConcurrency\": 4,"
      "\"exchangeClientThreads\": 5,"
      "\"queryMaxMemoryPerNode\": 6,"
      "\"logVerboseModules\": \"a=1,b=2,c=3\","
      "\"maxNodeMemory\": 8,"
      "\"useMmapAllocator\": false,"
      "\"useMmapArena\": true,"
      "\"mmapArenaCapacityRatio\": 11,"
      "\"asyncDataCacheEnabled\": false,"
      "\"asyncCacheSsdSize\": 13,"
      "\"asyncCacheSsdCheckpointSize\": 14,"
      "\"asyncCacheSsdDisableFileCow\": true,"
      "\"asyncCacheSsdPath\": \"16\","
      "\"enableMemoryLeakCheck\": false,"
      "\"enableMemoryArbitration\": true,"
      "\"memoryArbitratorKind\": \"19\","
      "\"reservedMemoryPoolCapacityPercentage\": 20,"
      "\"initMemoryPoolCapacity\": 21,"
      "\"minMemoryPoolTransferCapacity\": 22,"
      "\"maxHttpSessionReadBufferSize\": 23,"
      "\"spillEnabled\": false,"
      "\"joinSpillEnabled\": true,"
      "\"aggSpillEnabled\": false,"
      "\"orderBySpillEnabled\": true,"
      "\"spillDir\": \"28\","
      "\"joinSpillMemoryThreshold\": 29,"
      "\"aggregationSpillMemoryThreshold\": 30,"
      "\"orderBySpillMemoryThreshold\": 31,"
      "\"concurrentLifespans\": 32,"
      "\"baseUrl\": \"33\","
      "\"instanceId\": \"34\","
      "\"httpMaxAllocateBytes\": 35,"
      "\"httpsClientCertAndKeyPath\": \"36\","
      "\"httpsSupportedCiphers\": \"37\""
      "}");
  ASSERT_EQ(config.getMaxOutputPageBytes(), 1);
  ASSERT_EQ(config.getMaxWorkerThreads(), 2);
  ASSERT_EQ(config.getMaxDriversPerTask(), 3);
  ASSERT_EQ(config.getTaskConcurrency(), 4);
  ASSERT_EQ(config.getExchangeClientThreads(), 5);
  ASSERT_EQ(config.getQueryMaxMemoryPerNode(), 6);
  std::unordered_map<std::string, int32_t> logVerbose{{"a", 1}, {"b", 2}, {"c", 3}};
  ASSERT_EQ(config.getLogVerboseModules(), logVerbose);
  ASSERT_EQ(config.getMaxNodeMemory(), 8);
  ASSERT_EQ(config.getUseMmapAllocator(), false);
  ASSERT_EQ(config.getUseMmapArena(), true);
  ASSERT_EQ(config.getMmapArenaCapacityRatio(), 11);
  ASSERT_EQ(config.getAsyncDataCacheEnabled(), false);
  ASSERT_EQ(config.getAsyncCacheSsdSize(), 13);
  ASSERT_EQ(config.getAsyncCacheSsdCheckpointSize(), 14);
  ASSERT_EQ(config.getAsyncCacheSsdDisableFileCow(), true);
  ASSERT_EQ(config.getAsyncCacheSsdPath(), "16");
  ASSERT_EQ(config.getEnableMemoryLeakCheck(), false);
  ASSERT_EQ(config.getEnableMemoryArbitration(), true);
  ASSERT_EQ(config.getMemoryArbitratorKind(), "19");
  ASSERT_EQ(config.getReservedMemoryPoolCapacityPercentage(), 20);
  ASSERT_EQ(config.getInitMemoryPoolCapacity(), 21);
  ASSERT_EQ(config.getMinMemoryPoolTransferCapacity(), 22);
  ASSERT_EQ(config.getMaxHttpSessionReadBufferSize(), 23);
  ASSERT_EQ(config.getSpillEnabled(), false);
  ASSERT_EQ(config.getJoinSpillEnabled(), true);
  ASSERT_EQ(config.getAggSpillEnabled(), false);
  ASSERT_EQ(config.getOrderBySpillEnabled(), true);
  ASSERT_EQ(config.getSpillDir(), "28");
  ASSERT_EQ(config.getJoinSpillMemoryThreshold(), 29);
  ASSERT_EQ(config.getAggregationSpillMemoryThreshold(), 30);
  ASSERT_EQ(config.getOrderBySpillMemoryThreshold(), 31);
  ASSERT_EQ(config.getConcurrentLifespans(), 32);
  ASSERT_EQ(config.getBaseUrl(), "33");
  ASSERT_EQ(config.getInstanceId(), "34");
  ASSERT_EQ(config.getHttpMaxAllocateBytes(), 35);
  ASSERT_EQ(config.getHttpsClientCertAndKeyPath(), "36");
  ASSERT_EQ(config.getHttpsSupportedCiphers(), "37");
}

TEST(NativeConfigsTests, singleton) {
  auto& config = io::trino::bridge::NativeConfigs::instance();
  config.initialize("{\"maxOutputPageBytes\": 1}");
  ASSERT_EQ(config.getMaxOutputPageBytes(), 1);
  config.initialize("{\"maxOutputPageBytes\": 22}");
  ASSERT_EQ(config.getMaxOutputPageBytes(), 1);
}

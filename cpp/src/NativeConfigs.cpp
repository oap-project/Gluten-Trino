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
#include "src/NativeConfigs.h"

#include "src/protocol/external/json/json.hpp"
#include "velox/core/Config.h"
#include "velox/core/QueryConfig.h"

using namespace facebook::velox;

namespace io::trino::bridge {

#define GET_KEY_FROM_JSON(KEY, JSON) \
  if (JSON.contains(#KEY)) {         \
    JSON.at(#KEY).get_to(KEY);       \
  }

NativeConfigs& NativeConfigs::instance() {
  static NativeConfigs configs;
  return configs;
}

void NativeConfigs::initialize(const std::string& configJsonString) {
  if (initialized) {
    return;
  }
  VLOG(2) << "Initializing Native Configs with config json: " << configJsonString;

  nlohmann::json configJson = nlohmann::json::parse(configJsonString);
  GET_KEY_FROM_JSON(maxOutputPageBytes, configJson);
  GET_KEY_FROM_JSON(maxWorkerThreads, configJson);
  GET_KEY_FROM_JSON(maxDriversPerTask, configJson);
  GET_KEY_FROM_JSON(taskConcurrency, configJson);
  GET_KEY_FROM_JSON(exchangeClientThreads, configJson);
  GET_KEY_FROM_JSON(queryMaxMemoryPerNode, configJson);
  GET_KEY_FROM_JSON(maxNodeMemory, configJson);
  GET_KEY_FROM_JSON(useMmapAllocator, configJson);
  GET_KEY_FROM_JSON(useMmapArena, configJson);
  GET_KEY_FROM_JSON(mmapArenaCapacityRatio, configJson);
  GET_KEY_FROM_JSON(asyncDataCacheEnabled, configJson);
  GET_KEY_FROM_JSON(asyncCacheSsdSize, configJson);
  GET_KEY_FROM_JSON(asyncCacheSsdCheckpointSize, configJson);
  GET_KEY_FROM_JSON(asyncCacheSsdDisableFileCow, configJson);
  GET_KEY_FROM_JSON(asyncCacheSsdPath, configJson);
  GET_KEY_FROM_JSON(enableMemoryLeakCheck, configJson);
  GET_KEY_FROM_JSON(enableMemoryArbitration, configJson);
  GET_KEY_FROM_JSON(memoryArbitratorKind, configJson);
  GET_KEY_FROM_JSON(reservedMemoryPoolCapacityPercentage, configJson);
  GET_KEY_FROM_JSON(initMemoryPoolCapacity, configJson);
  GET_KEY_FROM_JSON(minMemoryPoolTransferCapacity, configJson);
  GET_KEY_FROM_JSON(maxHttpSessionReadBufferSize, configJson);
  GET_KEY_FROM_JSON(spillEnabled, configJson);
  GET_KEY_FROM_JSON(joinSpillEnabled, configJson);
  GET_KEY_FROM_JSON(aggSpillEnabled, configJson);
  GET_KEY_FROM_JSON(orderBySpillEnabled, configJson);
  GET_KEY_FROM_JSON(spillDir, configJson);
  GET_KEY_FROM_JSON(joinSpillMemoryThreshold, configJson);
  GET_KEY_FROM_JSON(aggregationSpillMemoryThreshold, configJson);
  GET_KEY_FROM_JSON(orderBySpillMemoryThreshold, configJson);
  GET_KEY_FROM_JSON(concurrentLifespans, configJson);
  GET_KEY_FROM_JSON(baseUrl, configJson);
  GET_KEY_FROM_JSON(instanceId, configJson);
  GET_KEY_FROM_JSON(httpMaxAllocateBytes, configJson);
  GET_KEY_FROM_JSON(httpsClientCertAndKeyPath, configJson);
  GET_KEY_FROM_JSON(httpsSupportedCiphers, configJson);

  if (configJson.contains("logVerboseModules")) {
    std::string _logVerboseModules;
    configJson.at("logVerboseModules").get_to(_logVerboseModules);

    if (_logVerboseModules.back() != ',') {
      _logVerboseModules.push_back(',');
    }

    size_t start = 0;
    size_t index = 0;
    while (index < _logVerboseModules.size()) {
      while (index < _logVerboseModules.size() && _logVerboseModules[index] != ',')
        ++index;

      if (index >= _logVerboseModules.size()) break;

      if (index - start < 3) {
        // There are at least "m=l" for 3 characters.
        start = ++index;
        continue;
      }

      const std::string& logVerboseModule =
          _logVerboseModules.substr(start, index - start);
      size_t pos = logVerboseModule.find('=');
      if (pos == start || pos + 1 == index) {
        start = ++index;
        continue;
      }

      const std::string& moduleName = logVerboseModule.substr(0, pos);
      int level =
          std::stoi(logVerboseModule.substr(pos + 1, logVerboseModule.size() - pos - 1));

      logVerboseModules[moduleName] = level;

      start = ++index;
    }
  }

  initialized = true;
}

std::unordered_map<std::string, std::string> NativeConfigs::getQueryConfigs() const {
  return {
      {core::QueryConfig::kMaxPartitionedOutputBufferSize,
       std::to_string(getMaxOutputPageBytes())},
      {core::QueryConfig::kSpillEnabled, std::to_string(getSpillEnabled())},
      {core::QueryConfig::kJoinSpillEnabled, std::to_string(getJoinSpillEnabled())},
      {core::QueryConfig::kAggregationSpillEnabled, std::to_string(getAggSpillEnabled())},
      {core::QueryConfig::kOrderBySpillEnabled, std::to_string(getOrderBySpillEnabled())},
      {core::QueryConfig::kJoinSpillMemoryThreshold,
       std::to_string(getJoinSpillMemoryThreshold())},
      {core::QueryConfig::kAggregationSpillMemoryThreshold,
       std::to_string(getAggregationSpillMemoryThreshold())},
      {core::QueryConfig::kOrderBySpillMemoryThreshold,
       std::to_string(getJoinSpillMemoryThreshold())}};
}

std::unordered_map<std::string, std::shared_ptr<Config>>
NativeConfigs::getConnectorConfigs() {
  return {};
}

}  // namespace io::trino::bridge

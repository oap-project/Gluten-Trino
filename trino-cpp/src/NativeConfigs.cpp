#include "NativeConfigs.h"

#include "protocol/external/json/json.hpp"
#include "velox/core/Config.h"
#include "velox/core/QueryConfig.h"

using namespace facebook::velox;

namespace io::trino::bridge {

#define GET_KEY_FROM_JSON(KEY, JSON) \
  if (JSON.contains(#KEY)) {         \
    JSON.at(#KEY).get_to(KEY);       \
  }

NativeConfigs::NativeConfigs(const std::string& configJsonString) {
  LOG(INFO) << "Get configJsonString is " << configJsonString;

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
}

std::unordered_map<std::string, std::string> NativeConfigs::getQueryConfigs() const {
  return {{core::QueryConfig::kMaxPartitionedOutputBufferSize,
           std::to_string(getMaxOutputPageBytes())}};
}

std::unordered_map<std::string, std::shared_ptr<Config>>
NativeConfigs::getConnectorConfigs() {
  return {};
}

}  // namespace io::trino::bridge

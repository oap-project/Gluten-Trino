#include "NativeConfigs.h"

#include "protocol/external/json/json.hpp"
#include "velox/core/Config.h"
#include "velox/core/QueryConfig.h"

using namespace facebook::velox;

namespace io::trino::bridge {

NativeConfigs::NativeConfigs(const std::string& configJsonString) {
  LOG(INFO) << "Get configJsonString is " << configJsonString;
  nlohmann::json configJson = nlohmann::json::parse(configJsonString);
  if (configJson.contains("maxOutputPageBytes")) {
    configJson.at("maxOutputPageBytes").get_to(maxOutputPageBytes);
  }
  if (configJson.contains("maxWorkerThreads")) {
    configJson.at("maxWorkerThreads").get_to(maxWorkerThreads);
  }
  if (configJson.contains("maxDriversPerTask")) {
    configJson.at("maxDriversPerTask").get_to(maxDriversPerTask);
  }
  if (configJson.contains("taskConcurrency")) {
    configJson.at("taskConcurrency").get_to(taskConcurrency);
  }
  if (configJson.contains("exchangeClientThreads")) {
    configJson.at("exchangeClientThreads").get_to(exchangeClientThreads);
  }
  if (configJson.contains("queryMaxMemory")) {
    configJson.at("queryMaxMemory").get_to(queryMaxMemory);
  }
  if (configJson.contains("queryMaxMemoryPerNode")) {
    configJson.at("queryMaxMemoryPerNode").get_to(queryMaxMemoryPerNode);
  }
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

#pragma once

#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include "velox/common/base/BitUtil.h"

namespace facebook::velox {
class Config;
}

using namespace facebook::velox;

namespace io::trino::bridge {

class NativeConfigs {
 public:
  explicit NativeConfigs(const std::string& configJsonString);

  std::unordered_map<std::string, std::string> getQueryConfigs() const;

  std::unordered_map<std::string, std::shared_ptr<facebook::velox::Config>>
  getConnectorConfigs();

  inline const int64_t& getMaxOutputPageBytes() const { return maxOutputPageBytes; }
  inline const int32_t& getMaxWorkerThreads() const { return maxWorkerThreads; }
  inline const int32_t& getMaxDriversPerTask() const { return maxDriversPerTask; }
  inline const int32_t& getTaskConcurrency() const { return taskConcurrency; }
  inline const int32_t& getExchangeClientThreads() const { return exchangeClientThreads; }
  inline const int64_t& getQueryMaxMemory() const { return queryMaxMemory; }
  inline const int64_t& getQueryMaxMemoryPerNode() const { return queryMaxMemoryPerNode; }
  inline const std::unordered_map<std::string, int32_t> getLogVerboseModules() const {
    return logVerboseModules;
  }

 private:
  // refer to io.trino.operator.DirectExchangeClientConfig#maxResponseSize
  int64_t maxOutputPageBytes = 16 << 20;
  // refer to io.trino.execution.TaskManagerConfig#maxWorkerThreads
  int32_t maxWorkerThreads = 2 * std::thread::hardware_concurrency();
  // refer to io.trino.execution.TaskManagerConfig#maxDriversPerTask
  int32_t maxDriversPerTask = std::numeric_limits<int32_t>::max();
  // refer to io.trino.execution.TaskManagerConfig#taskConcurrency
  int32_t taskConcurrency = std::min(
      std::max(
          static_cast<int32_t>(bits::nextPowerOfTwo(std::thread::hardware_concurrency())),
          2),
      32);
  // refer to io.trino.operator.DirectExchangeClientConfig#clientThreads
  int32_t exchangeClientThreads = 25;
  // refer to io.trino.memory.MemoryManagerConfig#maxQueryMemory
  int64_t queryMaxMemory = 20l << 30;
  int64_t queryMaxMemoryPerNode = std::numeric_limits<int64_t>::max();
  std::unordered_map<std::string, int32_t> logVerboseModules;
};

using NativeConfigsPtr = std::shared_ptr<NativeConfigs>;

}  // namespace io::trino::bridge

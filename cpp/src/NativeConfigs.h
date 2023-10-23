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
#pragma once

#include <atomic>
#include <boost/noncopyable.hpp>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include "velox/common/base/BitUtil.h"
#include "velox/core/QueryConfig.h"

namespace facebook::velox {
class Config;
}

using namespace facebook::velox;

namespace io::trino::bridge {

class NativeConfigs : public boost::noncopyable {
 public:
  static NativeConfigs& instance();

  void initialize(const std::string& configJsonString);

  std::unordered_map<std::string, std::string> getQueryConfigs() const;

  std::unordered_map<std::string, std::shared_ptr<facebook::velox::Config>>
  getConnectorConfigs();

  inline const int64_t& getMaxOutputPageBytes() const { return maxOutputPageBytes; }
  inline const int32_t& getMaxWorkerThreads() const { return maxWorkerThreads; }
  inline const int32_t& getMaxDriversPerTask() const { return maxDriversPerTask; }
  inline const int32_t& getTaskConcurrency() const { return taskConcurrency; }
  inline const int32_t& getExchangeClientThreads() const { return exchangeClientThreads; }
  inline const int64_t& getQueryMaxMemoryPerNode() const { return queryMaxMemoryPerNode; }
  inline const std::unordered_map<std::string, int32_t> getLogVerboseModules() const {
    return logVerboseModules;
  }
  inline const int64_t& getMaxNodeMemory() const { return maxNodeMemory; }
  inline const bool& getUseMmapAllocator() const { return useMmapAllocator; }
  inline const bool& getUseMmapArena() const { return useMmapArena; }
  inline const int32_t& getMmapArenaCapacityRatio() const {
    return mmapArenaCapacityRatio;
  }
  inline const bool& getAsyncDataCacheEnabled() const { return asyncDataCacheEnabled; }
  inline const int64_t& getAsyncCacheSsdSize() const { return asyncCacheSsdSize; }
  inline const int64_t& getAsyncCacheSsdCheckpointSize() const {
    return asyncCacheSsdCheckpointSize;
  }
  inline const bool& getAsyncCacheSsdDisableFileCow() const {
    return asyncCacheSsdDisableFileCow;
  }
  inline const std::string& getAsyncCacheSsdPath() const { return asyncCacheSsdPath; }
  inline const bool& getEnableMemoryLeakCheck() const { return enableMemoryLeakCheck; }
  inline const bool& getEnableMemoryArbitration() const {
    return enableMemoryArbitration;
  }
  inline const std::string& getMemoryArbitratorKind() const {
    return memoryArbitratorKind;
  }
  inline const int32_t& getReservedMemoryPoolCapacityPercentage() const {
    return reservedMemoryPoolCapacityPercentage;
  }
  inline const uint64_t& getInitMemoryPoolCapacity() const {
    return initMemoryPoolCapacity;
  }
  inline const uint64_t& getMinMemoryPoolTransferCapacity() const {
    return minMemoryPoolTransferCapacity;
  }
  inline const uint32_t& getMaxHttpSessionReadBufferSize() const {
    return maxHttpSessionReadBufferSize;
  }
  inline const bool& getSpillEnabled() const { return spillEnabled; }
  inline const bool& getJoinSpillEnabled() const { return joinSpillEnabled; }
  inline const bool& getAggSpillEnabled() const { return aggSpillEnabled; }
  inline const bool& getOrderBySpillEnabled() const { return orderBySpillEnabled; }
  inline const std::string& getSpillDir() const { return spillDir; }
  inline const uint64_t& getJoinSpillMemoryThreshold() const {
    return joinSpillMemoryThreshold;
  }
  inline const uint64_t& getAggregationSpillMemoryThreshold() const {
    return aggregationSpillMemoryThreshold;
  }
  inline const uint64_t& getOrderBySpillMemoryThreshold() const {
    return orderBySpillMemoryThreshold;
  }
  inline const int32_t& getConcurrentLifespans() const { return concurrentLifespans; }
  inline const std::string& getBaseUrl() const { return baseUrl; }
  inline const std::string& getInstanceId() const { return instanceId; }
  inline const uint64_t& getHttpMaxAllocateBytes() const { return httpMaxAllocateBytes; }
  inline const std::string& getHttpsClientCertAndKeyPath() const {
    return httpsClientCertAndKeyPath;
  }
  inline const std::string& getHttpsSupportedCiphers() const {
    return httpsSupportedCiphers;
  }
  inline const std::chrono::duration<double> getExchangeMaxErrorDuration() const {
    return facebook::velox::core::toDuration(exchangeMaxErrorDuration);
  }
  inline const std::chrono::duration<double> getExchangeRequestTimeout() const {
    return facebook::velox::core::toDuration(exchangeRequestTimeout);
  }
  inline const bool& getExchangeImmediateBufferTransfer() const {
    return exchangeImmediateBufferTransfer;
  }

 private:
  std::atomic_bool initialized{false};

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
  int64_t queryMaxMemoryPerNode = std::numeric_limits<int64_t>::max();
  std::unordered_map<std::string, int32_t> logVerboseModules;
  int64_t maxNodeMemory = std::numeric_limits<int64_t>::max();
  bool useMmapAllocator = false;
  bool useMmapArena = false;
  int32_t mmapArenaCapacityRatio = 10;
  bool asyncDataCacheEnabled = true;
  int64_t asyncCacheSsdSize = 0;
  int64_t asyncCacheSsdCheckpointSize = 0;
  bool asyncCacheSsdDisableFileCow = false;
  std::string asyncCacheSsdPath = "/tmp/gluten_trino/cache";
  bool enableMemoryLeakCheck = true;
  bool enableMemoryArbitration = false;
  std::string memoryArbitratorKind = "";
  int32_t reservedMemoryPoolCapacityPercentage = 10;
  uint64_t initMemoryPoolCapacity = 120 << 20;
  uint64_t minMemoryPoolTransferCapacity = 32 << 20;
  uint32_t maxHttpSessionReadBufferSize = 65536;
  bool spillEnabled = false;
  bool joinSpillEnabled = false;
  bool aggSpillEnabled = false;
  bool orderBySpillEnabled = false;
  std::string spillDir = "";
  uint64_t joinSpillMemoryThreshold = 0;
  uint64_t aggregationSpillMemoryThreshold = 0;
  uint64_t orderBySpillMemoryThreshold = 0;
  int32_t concurrentLifespans = 10;
  std::string baseUrl = "http://localhost:9090";
  std::string instanceId = "";
  uint64_t httpMaxAllocateBytes = 64 << 10;
  std::string httpsClientCertAndKeyPath = "";
  std::string httpsSupportedCiphers = "ECDHE-ECDSA-AES256-GCM-SHA384,AES256-GCM-SHA384";
  std::string exchangeMaxErrorDuration = "30s";
  std::string exchangeRequestTimeout = "10s";
  bool exchangeImmediateBufferTransfer = false;
};

}  // namespace io::trino::bridge

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

#include "JniHandle.h"
#include "NativeConfigs.h"
#include "TaskHandle.h"
#include "protocol/trino_protocol.h"
#include "serialization/TrinoSerializer.h"
#include "types/PrestoToVeloxQueryPlan.h"
#include "utils/JniUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/core/QueryCtx.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/Task.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/parse/TypeResolver.h"

#ifdef ENABLE_TRINO_S3
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#endif

#ifdef ENABLE_TRINO_EXCHANGE
#include "TrinoExchangeSource.h"
#include "velox/exec/Exchange.h"
#endif

using namespace facebook::velox;

namespace io::trino::bridge {

const std::string JniHandle::kGlutenTrinoFunctionPrefix("trino.bridge.");
const std::string JniHandle::kHiveConnectorId("hive");
const std::string JniHandle::kTpchConnectorId("tpch");

extern void registerTrinoSumAggregate(const std::string& prefix);

JniHandle::JniHandle(const NativeSqlTaskExecutionManagerPtr& javaManager)
    : javaManager_(javaManager) {
  auto& config = NativeConfigs::instance();
  driverExecutor_ = getDriverCPUExecutor(config.getMaxWorkerThreads());
  exchangeIOExecutor_ = getExchangeIOCPUExecutor(config.getExchangeClientThreads());
  if (config.getSpillEnabled()) {
    spillExecutor_ = getSpillExecutor();
  }

  initializeVeloxMemory();
}

void JniHandle::initializeVelox() {
  // Setup and register.
  filesystems::registerLocalFileSystem();

  std::unordered_map<std::string, std::string> configurationValues;

  auto properties = std::make_shared<const velox::core::MemConfig>(configurationValues);
  auto hiveConnectorExecutor = getConnectorIOExecutor(30, "HiveConnectorIO");
  auto hiveConnector =
      velox::connector::getConnectorFactory(
          velox::connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, properties, hiveConnectorExecutor.get());
  velox::connector::registerConnector(hiveConnector);

  auto tpchConnector = connector::getConnectorFactory(
                           connector::tpch::TpchConnectorFactory::kTpchConnectorName)
                           ->newConnector(kTpchConnectorId, nullptr);
  velox::connector::registerConnector(tpchConnector);

  velox::parquet::registerParquetReaderFactory();

  velox::filesystems::registerHdfsFileSystem();

#ifdef ENABLE_TRINO_S3
  velox::filesystems::registerS3FileSystem();
#endif

  velox::dwrf::registerDwrfReaderFactory();
  // Register Velox functions
  static const std::string kPrestoDefaultPrefix{"presto.default."};
  velox::functions::prestosql::registerAllScalarFunctions(kPrestoDefaultPrefix);
  velox::aggregate::prestosql::registerAllAggregateFunctions(kPrestoDefaultPrefix);
  velox::window::prestosql::registerAllWindowFunctions(kPrestoDefaultPrefix);

  velox::parse::registerTypeResolver();

  TrinoVectorSerde::registerVectorSerde();

#ifdef ENABLE_TRINO_EXCHANGE
  facebook::velox::exec::ExchangeSource::registerFactory(
      TrinoExchangeSource::createExchangeSource);
#endif

  registerTrinoSumAggregate(kGlutenTrinoFunctionPrefix + "sum");
}

void JniHandle::initializeVeloxMemory() {
  auto& config = NativeConfigs::instance();
  const int64_t memoryBytes = config.getMaxNodeMemory();
  LOG(INFO) << "Starting with node memory " << (memoryBytes >> 30) << "GB";

  if (config.getUseMmapAllocator()) {
    memory::MmapAllocator::Options options;
    options.capacity = memoryBytes;
    options.useMmapArena = config.getUseMmapArena();
    options.mmapArenaCapacityRatio = config.getMmapArenaCapacityRatio();
    allocator_ = std::make_shared<memory::MmapAllocator>(options);
  } else {
    allocator_ = memory::MemoryAllocator::createDefaultInstance();
  }
  memory::MemoryAllocator::setDefaultInstance(allocator_.get());

  if (config.getAsyncDataCacheEnabled()) {
    std::unique_ptr<cache::SsdCache> ssd;
    const auto asyncCacheSsdSize = config.getAsyncCacheSsdSize();
    if (asyncCacheSsdSize > 0) {
      constexpr int32_t kNumSsdShards = 16;
      cacheExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(kNumSsdShards);
      auto asyncCacheSsdCheckpointSize = config.getAsyncCacheSsdCheckpointSize();
      auto asyncCacheSsdDisableFileCow = config.getAsyncCacheSsdDisableFileCow();
      LOG(INFO) << "Initializing SSD cache with capacity " << (asyncCacheSsdSize >> 30)
                << "GB, checkpoint size " << (asyncCacheSsdCheckpointSize >> 30)
                << "GB, file cow "
                << (asyncCacheSsdDisableFileCow ? "DISABLED" : "ENABLED");
      ssd = std::make_unique<velox::cache::SsdCache>(
          config.getAsyncCacheSsdPath(), asyncCacheSsdSize, kNumSsdShards,
          cacheExecutor_.get(), asyncCacheSsdCheckpointSize, asyncCacheSsdDisableFileCow);
    }
    cache_ = velox::cache::AsyncDataCache::create(allocator_.get(), std::move(ssd));
  } else {
    VELOX_CHECK_EQ(config.getAsyncCacheSsdSize(), 0,
                   "Async data cache cannot be disabled if ssd cache is enabled");
  }

  // Set up velox memory manager.
  memory::MemoryManagerOptions options;
  options.capacity = memoryBytes;
  options.checkUsageLeak = config.getEnableMemoryLeakCheck();
  if (config.getEnableMemoryArbitration()) {
    options.arbitratorKind = config.getMemoryArbitratorKind();
    options.capacity =
        memoryBytes * 100 / config.getReservedMemoryPoolCapacityPercentage();
    options.memoryPoolInitCapacity = config.getInitMemoryPoolCapacity();
    options.memoryPoolTransferCapacity = config.getMinMemoryPoolTransferCapacity();
  }
  const auto& manager = memory::MemoryManager::getInstance(options);
  LOG(INFO) << "Memory manager has been setup: " << manager.toString();
}

TaskHandlePtr JniHandle::createTaskHandle(const io::trino::TrinoTaskId& id,
                                          const protocol::PlanFragment& plan) {
  return withWLock([&id, &plan, this]() {
    if (auto iter = taskMap_.find(id.fullId()); iter != taskMap_.end()) {
      return iter->second.get();
    }
    size_t numPartitions = 0;
    if (plan.partitioningScheme.bucketToPartition) {
      numPartitions =
          *std::max_element(plan.partitioningScheme.bucketToPartition->begin(),
                            plan.partitioningScheme.bucketToPartition->end()) +
          1;
    } else {
      LOG(INFO) << fmt::format("No partition buffer number in task {}.", id.fullId());
    }

    bool isBroadcast = false;
    if (auto handle =
            std::dynamic_pointer_cast<io::trino::protocol::SystemPartitioningHandle>(
                plan.partitioningScheme.partitioning.handle.connectorHandle)) {
      if (handle->function == io::trino::protocol::SystemPartitionFunction::BROADCAST) {
        LOG(INFO) << fmt::format("Task {} contains broadcast output buffer.",
                                 id.fullId());
        isBroadcast = true;
        numPartitions = 1;
      }
    }

    LOG(INFO) << fmt::format("Task {} contains {} output buffer.", id.fullId(),
                             numPartitions);

    auto& config = NativeConfigs::instance();
    auto queryCtx = std::make_shared<core::QueryCtx>(
        driverExecutor_.get(), std::move(config.getQueryConfigs()),
        std::move(config.getConnectorConfigs()), cache::AsyncDataCache::getInstance(),
        memory::defaultMemoryManager().addRootPool(id.fullId(),
                                                   config.getQueryMaxMemoryPerNode()));

    VeloxInteractiveQueryPlanConverter convertor(getPlanConvertorMemPool().get());
    core::PlanFragment fragment = convertor.toVeloxQueryPlan(plan, nullptr, id.fullId());

    LOG(INFO) << fmt::format("Task {},\n PlanFragment: {}", id.fullId(),
                             fragment.planNode->toString(true, true));

    auto task = exec::Task::create(id.fullId(), std::move(fragment), id.id(), queryCtx);
    std::string parentPath = config.getSpillDir();
    if (!parentPath.empty()) {
      std::string fullPath = parentPath + "/spill-" + id.fullId();
      bool ret = std::filesystem::create_directories(fullPath);
      if (!ret) {
        LOG(WARNING) << "Create directory " << fullPath << " failed!";
      }
      task->setSpillDirectory(fullPath);
    }
    auto iter = taskMap_.insert({id.fullId(), TaskHandle::createTaskHandle(
                                                  id, task, numPartitions, isBroadcast)});
    return iter.first->second.get();
  });
}

TaskHandlePtr JniHandle::getTaskHandle(const io::trino::TrinoTaskId& id) {
  return withRLock([&id, this]() -> TaskHandle* {
    if (auto iter = taskMap_.find(id.fullId()); iter != taskMap_.end()) {
      return iter->second.get();
    } else {
      return nullptr;
    }
  });
}

bool JniHandle::removeTask(const io::trino::TrinoTaskId& id) {
  return withWLock([this, &id]() {
    if (auto taskIter = taskMap_.find(id.fullId()); taskIter != taskMap_.end()) {
      auto&& task = taskIter->second->getTask();

      printTaskStatus(id, task);

      taskMap_.erase(taskIter);
      return true;
    } else {
      return false;
    }
  });
}

void JniHandle::terminateTask(const io::trino::TrinoTaskId& id, exec::TaskState state) {
  TaskHandlePtr taskHandle;
  withRLock([this, &id, &taskHandle]() {
    if (auto taskIter = taskMap_.find(id.fullId()); taskIter != taskMap_.end()) {
      TaskHandlePtr newPtr(taskIter->second);
      taskHandle.swap(newPtr);
    } else {
      LOG(WARNING) << fmt::format("Attempt to terminate a removed task {}", id.fullId());
    }
  });
  if (taskHandle) {
    switch (state) {
      case exec::TaskState::kCanceled:
        taskHandle->getTask()->requestCancel().wait();
        break;
      case exec::TaskState::kAborted:
        taskHandle->getTask()->requestAbort().wait();
        break;
      default:
        break;
    }
  }
}

std::shared_ptr<memory::MemoryPool> JniHandle::getPlanConvertorMemPool() {
  static std::shared_ptr<memory::MemoryPool> pool =
      velox::memory::addDefaultLeafMemoryPool("PlanConvertor");
  return pool;
}

void JniHandle::printTaskStatus(const io::trino::TrinoTaskId& id,
                                const std::shared_ptr<exec::Task>& task) {
  std::stringstream ss;
  ss << fmt::format("Task {} status:\n", id.fullId());

  auto&& taskStatus = task->taskStats();
  ss << fmt::format(
      "\tCreateTime: {} ms, FirstSplitStart: {} ms, LastSplitStart: {} ms, "
      "LastSplitEnd: {} ms, FinishingTime: {} ms\n",
      taskStatus.executionStartTimeMs, taskStatus.firstSplitStartTimeMs,
      taskStatus.lastSplitStartTimeMs, taskStatus.executionEndTimeMs,
      taskStatus.endTimeMs);
  ss << fmt::format("\tSplitProcessingTime: {} ms, TaskExecutionTime: {} ms\n",
                    taskStatus.executionEndTimeMs - taskStatus.firstSplitStartTimeMs,
                    taskStatus.endTimeMs - taskStatus.executionStartTimeMs)
     << fmt::format("\tSplits: {}, Drivers: {}\n", taskStatus.numTotalSplits,
                    taskStatus.numTotalDrivers);
  ss << "\tPipeline status:\n";

  for (size_t pipelineId = 0; pipelineId < taskStatus.pipelineStats.size();
       ++pipelineId) {
    auto&& pipelineStatus = taskStatus.pipelineStats[pipelineId];
    ss << fmt::format("\t\tPipeline {}: {} {}\n", pipelineId,
                      pipelineStatus.inputPipeline ? "input" : "",
                      pipelineStatus.outputPipeline ? "output" : "");
    for (size_t opId = 0; opId < pipelineStatus.operatorStats.size(); ++opId) {
      auto&& opStatus = pipelineStatus.operatorStats[opId];
      ss << fmt::format("\t\t\tOp {}, {}: ", opStatus.operatorId, opStatus.operatorType)
         << "\n";

      if (opId == 0 && pipelineStatus.inputPipeline) {
        ss << fmt::format("\t\t\t\tRaw Input: {} rows, {} bytes\n",
                          opStatus.rawInputPositions, opStatus.rawInputBytes);
      }

      ss << fmt::format("\t\t\t\tInput: {} vectors, {} rows, {} bytes\n",
                        opStatus.inputVectors, opStatus.inputPositions,
                        opStatus.inputBytes);

      ss << fmt::format("\t\t\t\tOutput: {} vectors, {} rows, {} bytes\n",
                        opStatus.outputVectors, opStatus.outputPositions,
                        opStatus.outputBytes);

      for (auto&& metric : opStatus.runtimeStats) {
        ss << "\t\t\t\t" << metric.first << ":";
        metric.second.printMetric(ss);
        ss << "\n";
      }
    }
  }
  LOG(INFO) << ss.str();
}

}  // namespace io::trino::bridge
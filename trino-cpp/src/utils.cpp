
#include "utils.h"

#include "velox/common/file/FileSystems.h"
#include "velox/common/time/Timer.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"
#ifdef ENABLE_TRINO_S3
#include "velox/connectors/hive/storage_adapters/s3fs/RegisterS3FileSystem.h"
#endif
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Task.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/parse/TypeResolver.h"

#ifdef ENABLE_TRINO_EXCHANGE
#include "TrinoExchangeSource.h"
#include "velox/exec/Exchange.h"
#endif

#include "serialization/TrinoSerializer.h"

using namespace facebook;
using namespace facebook::velox;
using namespace io::trino::protocol;
using namespace io::trino;

namespace io::trino::bridge {

const std::string baseUri = "http:://localhost:12345";  // TODO: pass via jni
const std::string trinoBridgePrefix("trino.bridge.");

extern void registerTrinoSumAggregate(const std::string& prefix);

io::trino::protocol::DateTime toISOTimestamp(uint64_t timeMilli) {
  char buf[80];
  time_t timeSecond = timeMilli / 1000;
  tm gmtTime;
  gmtime_r(&timeSecond, &gmtTime);
  strftime(buf, sizeof buf, "%FT%T", &gmtTime);
  return fmt::format("{}.{:03d}Z", buf, timeMilli % 1000);
}

io::trino::protocol::TaskStatus getTaskStatus(
    std::shared_ptr<facebook::velox::exec::Task>& task, TaskId taskId) {
  const facebook::velox::exec::TaskStats taskStats = task->taskStats();

  io::trino::protocol::TaskStatus taskStatus{};
  taskStatus.taskId = taskId;
  taskStatus.taskInstanceId = "";  // FIXME
  taskStatus.queuedPartitionedDrivers = taskStats.numQueuedSplits;
  taskStatus.runningPartitionedDrivers = taskStats.numRunningSplits;
  taskStatus.state = toTrinoTaskState(task->state());
  taskStatus.memoryReservation = {static_cast<double>(task->pool()->currentBytes()),
                                  io::trino::protocol::DataUnit::BYTE};
  taskStatus.self = fmt::format("{}/v1/task/{}", baseUri, taskId);
  // taskStatus.version =
  // taskStatus.nodeId =
  // taskStatus.ExecutionFailureInfo =
  // taskStatus.outputBufferStatus =
  // taskStatus.outputDataSize =
  // taskStatus.physicalWrittenDataSize =
  // taskStatus.peakMemoryReservation =
  // taskStatus.revocableMemoryReservation =
  // taskStatus.dynamicFiltersVersion =
  // taskStatus.queuedPartitionedSplitsWeight =
  // taskStatus.runningPartitionedSplitsWeight =

  return taskStatus;
}

io::trino::protocol::TaskStats getTaskStats(
    const std::shared_ptr<facebook::velox::exec::Task>& task, const TrinoTaskId& taskId) {
  io::trino::protocol::TaskStats stats;
  auto&& veloxStats = task->taskStats();

  stats.firstStartTime = toISOTimestamp(veloxStats.firstSplitStartTimeMs);
  stats.lastStartTime = toISOTimestamp(veloxStats.lastSplitStartTimeMs);
  stats.lastEndTime =
      toISOTimestamp(veloxStats.endTimeMs);  // Note: same as the Presto-CPP

  stats.createTime = toISOTimestamp(0);  // TODO: Padding, remove in the future
  stats.endTime = toISOTimestamp(0);     // TODO: Padding, remove in the future

  stats.totalDrivers = veloxStats.numTotalSplits;
  stats.queuedDrivers = veloxStats.numQueuedSplits;
  stats.runningDrivers = veloxStats.numRunningSplits;
  stats.completedDrivers = veloxStats.numFinishedSplits;

  int blockedDrivers = 0;
  // List<BlockedReason> blockedReasons; // TODO: No similar semantic in Trino
  for (auto&& blocked : veloxStats.numBlockedDrivers) {
    blockedDrivers += blocked.second;
  }
  stats.blockedDrivers = blockedDrivers;

  stats.totalScheduledTime = Duration(0, protocol::TimeUnit::NANOSECONDS);
  stats.totalBlockedTime = Duration(0, protocol::TimeUnit::NANOSECONDS);
  stats.totalCpuTime = Duration(0, protocol::TimeUnit::NANOSECONDS);

  stats.pipelines.resize(veloxStats.pipelineStats.size());
  for (int i = 0; i < veloxStats.pipelineStats.size(); ++i) {
    auto& pipelineStats = stats.pipelines[i];
    auto& veloxPipelineStats = veloxStats.pipelineStats[i];
    pipelineStats.inputPipeline = veloxPipelineStats.inputPipeline;
    pipelineStats.outputPipeline = veloxPipelineStats.outputPipeline;
    pipelineStats.firstStartTime = stats.createTime;
    pipelineStats.lastStartTime = stats.endTime;
    pipelineStats.lastEndTime = stats.endTime;

    pipelineStats.operatorSummaries.resize(veloxPipelineStats.operatorStats.size());

    pipelineStats.totalScheduledTime = Duration(0, protocol::TimeUnit::NANOSECONDS);
    pipelineStats.totalBlockedTime = Duration(0, protocol::TimeUnit::NANOSECONDS);
    pipelineStats.totalCpuTime = Duration(0, protocol::TimeUnit::NANOSECONDS);

    // tasks may fail before any operators are created;
    // collect stats only when we have operators
    if (!veloxPipelineStats.operatorStats.empty()) {
      const auto& firstOperatorStats = veloxPipelineStats.operatorStats[0];
      const auto& lastOperatorStats = veloxPipelineStats.operatorStats.back();

      pipelineStats.pipelineId = firstOperatorStats.pipelineId;
      pipelineStats.totalDrivers = firstOperatorStats.numDrivers;  // Why?
      pipelineStats.rawInputPositions = firstOperatorStats.rawInputPositions;
      pipelineStats.rawInputDataSize = {
          static_cast<double>(firstOperatorStats.rawInputBytes),
          protocol::DataUnit::BYTE};
      pipelineStats.processedInputPositions = firstOperatorStats.inputPositions;
      pipelineStats.processedInputDataSize = {
          static_cast<double>(firstOperatorStats.inputBytes), protocol::DataUnit::BYTE};
      pipelineStats.outputPositions = lastOperatorStats.outputPositions;
      pipelineStats.outputDataSize = {static_cast<double>(lastOperatorStats.outputBytes),
                                      protocol::DataUnit::BYTE};
    }

    if (pipelineStats.inputPipeline) {
      stats.rawInputPositions += pipelineStats.rawInputPositions;
      stats.rawInputDataSize += pipelineStats.rawInputDataSize;
      stats.processedInputPositions += pipelineStats.processedInputPositions;
      stats.processedInputDataSize += pipelineStats.processedInputDataSize;
    }

    if (pipelineStats.outputPipeline) {
      stats.outputPositions += pipelineStats.outputPositions;
      stats.outputDataSize += pipelineStats.outputDataSize;
    }

    for (auto j = 0; j < veloxPipelineStats.operatorStats.size(); ++j) {
      auto& opOut = pipelineStats.operatorSummaries[j];
      auto& op = veloxPipelineStats.operatorStats[j];

      opOut.stageId = taskId.stageId();
      opOut.pipelineId = i;
      opOut.planNodeId = op.planNodeId;
      opOut.operatorId = op.operatorId;
      opOut.operatorType = toTrinoOperatorType(op.operatorType);

      opOut.totalDrivers = op.numDrivers;
      opOut.inputPositions = op.inputPositions;
      opOut.sumSquaredInputPositions =
          ((double)op.inputPositions) * op.inputPositions;  // Purpose?
      opOut.inputDataSize = protocol::DataSize(op.inputBytes, protocol::DataUnit::BYTE);

      // Report raw input statistics on the Project node following TableScan, if
      // exists.
      if (j == 1 && op.operatorType == "FilterProject" &&
          veloxPipelineStats.operatorStats[0].operatorType == "TableScan") {
        const auto& scanOp = veloxPipelineStats.operatorStats[0];
        opOut.rawInputDataSize =
            protocol::DataSize(scanOp.rawInputBytes, protocol::DataUnit::BYTE);
        // TBD: There is no rawInputPositions in Trino, we use physicalInputPositions to
        // represent.
        opOut.physicalInputPositions = scanOp.rawInputPositions;
        opOut.physicalInputDataSize = opOut.rawInputDataSize;
      }

      opOut.outputPositions = op.outputPositions;
      opOut.outputDataSize = protocol::DataSize(op.outputBytes, protocol::DataUnit::BYTE);

      setTiming(op.addInputTiming, opOut.addInputCalls, opOut.addInputWall,
                opOut.addInputCpu);
      setTiming(op.getOutputTiming, opOut.getOutputCalls, opOut.getOutputWall,
                opOut.getOutputCpu);
      setTiming(op.finishTiming, opOut.finishCalls, opOut.finishWall, opOut.finishCpu);

      opOut.blockedWall =
          protocol::Duration(op.blockedWallNanos, protocol::TimeUnit::NANOSECONDS);

      // TODO: Align the memory management semantic between Velox and Trino

      // opOut.userMemoryReservation = protocol::DataSize(
      //     op.memoryStats.userMemoryReservation, protocol::DataUnit::BYTE);
      // opOut.revocableMemoryReservation = protocol::DataSize(
      //     op.memoryStats.revocableMemoryReservation, protocol::DataUnit::BYTE);
      // opOut.systemMemoryReservation = protocol::DataSize(
      //     op.memoryStats.systemMemoryReservation, protocol::DataUnit::BYTE);
      // opOut.peakUserMemoryReservation = protocol::DataSize(
      //     op.memoryStats.peakUserMemoryReservation, protocol::DataUnit::BYTE);
      // opOut.peakSystemMemoryReservation = protocol::DataSize(
      //     op.memoryStats.peakSystemMemoryReservation, protocol::DataUnit::BYTE);
      // opOut.peakTotalMemoryReservation = protocol::DataSize(
      //     op.memoryStats.peakTotalMemoryReservation, protocol::DataUnit::BYTE);

      opOut.spilledDataSize =
          protocol::DataSize(op.spilledBytes, protocol::DataUnit::BYTE);

      auto wallNanos = op.addInputTiming.wallNanos + op.getOutputTiming.wallNanos +
                       op.finishTiming.wallNanos;
      auto cpuNanos = op.addInputTiming.cpuNanos + op.getOutputTiming.cpuNanos +
                      op.finishTiming.cpuNanos;

      auto totalScheduledTime = Duration(wallNanos, protocol::TimeUnit::NANOSECONDS);
      auto totalBlockedTime = Duration(op.blockedWallNanos, protocol::TimeUnit::NANOSECONDS);
      auto totalCpuTime = Duration(cpuNanos, protocol::TimeUnit::NANOSECONDS);

      pipelineStats.totalScheduledTime += totalScheduledTime;
      pipelineStats.totalCpuTime += totalCpuTime;
      pipelineStats.totalBlockedTime += totalBlockedTime;

      // TODO: Align the memory management semantic between Velox and Trino

      // pipelineStats.userMemoryReservationInBytes +=
      // op.memoryStats.userMemoryReservation;
      // pipelineStats.revocableMemoryReservationInBytes +=
      //     op.memoryStats.revocableMemoryReservation;
      // pipelineStats.systemMemoryReservationInBytes +=
      //     op.memoryStats.systemMemoryReservation;

      stats.totalScheduledTime += totalScheduledTime;
      stats.totalCpuTime += totalCpuTime;
      stats.totalBlockedTime += totalBlockedTime;
    }  // pipeline's operators loop
  }    // task's pipelines loop

  return stats;
}

io::trino::protocol::TaskInfo getTaskInfo(
    std::shared_ptr<facebook::velox::exec::Task>& task, TaskId taskId) {
  io::trino::protocol::TaskInfo taskInfo;

  size_t lastHeartbeatMs = facebook::velox::getCurrentTimeMs();
  taskInfo.lastHeartbeat = toISOTimestamp(lastHeartbeatMs);

  const facebook::velox::exec::TaskStats taskStats = task->taskStats();

  taskInfo.taskStatus = getTaskStatus(task, taskId);

  io::trino::protocol::TaskStats& trinoTaskStats = taskInfo.stats;
  trinoTaskStats.totalScheduledTime = {};
  trinoTaskStats.totalCpuTime = {};
  trinoTaskStats.totalBlockedTime = {};

  trinoTaskStats.createTime = toISOTimestamp(taskStats.executionStartTimeMs);
  trinoTaskStats.firstStartTime = toISOTimestamp(taskStats.firstSplitStartTimeMs);
  trinoTaskStats.lastStartTime = toISOTimestamp(taskStats.lastSplitStartTimeMs);
  trinoTaskStats.lastEndTime = toISOTimestamp(taskStats.executionEndTimeMs);
  trinoTaskStats.endTime = toISOTimestamp(taskStats.executionEndTimeMs);

  trinoTaskStats.userMemoryReservation = {
      static_cast<double>(task->pool()->currentBytes()),
      io::trino::protocol::DataUnit::BYTE};
  trinoTaskStats.peakUserMemoryReservation = {
      static_cast<double>(task->pool()->peakBytes()),
      io::trino::protocol::DataUnit::BYTE};
  trinoTaskStats.revocableMemoryReservation = {};
  trinoTaskStats.cumulativeUserMemory = {};

  trinoTaskStats.rawInputPositions = 0;
  trinoTaskStats.rawInputDataSize = {};
  trinoTaskStats.processedInputPositions = 0;
  trinoTaskStats.processedInputDataSize = {};
  trinoTaskStats.outputPositions = 0;
  trinoTaskStats.outputDataSize = {};

  trinoTaskStats.totalDrivers = taskStats.numTotalSplits;
  trinoTaskStats.queuedDrivers = taskStats.numQueuedSplits;
  trinoTaskStats.runningDrivers = taskStats.numRunningSplits;
  trinoTaskStats.completedDrivers = taskStats.numFinishedSplits;

  trinoTaskStats.pipelines.resize(taskStats.pipelineStats.size());

  for (int i = 0; i < taskStats.pipelineStats.size(); ++i) {
    auto& pipelineOut = taskInfo.stats.pipelines[i];
    auto& pipeline = taskStats.pipelineStats[i];
    pipelineOut.inputPipeline = pipeline.inputPipeline;
    pipelineOut.outputPipeline = pipeline.outputPipeline;
    pipelineOut.firstStartTime = trinoTaskStats.createTime;
    pipelineOut.lastStartTime = trinoTaskStats.endTime;
    pipelineOut.lastEndTime = trinoTaskStats.endTime;

    pipelineOut.operatorSummaries.resize(pipeline.operatorStats.size());
    pipelineOut.totalScheduledTime = {};
    pipelineOut.totalCpuTime = {};
    pipelineOut.totalBlockedTime = {};
    pipelineOut.userMemoryReservation = {};
    pipelineOut.revocableMemoryReservation = {};

    // tasks may fail before any operators are created;
    // collect stats only when we have operators
    if (!pipeline.operatorStats.empty()) {
      const auto& firstOperatorStats = pipeline.operatorStats[0];
      const auto& lastOperatorStats = pipeline.operatorStats.back();

      pipelineOut.pipelineId = firstOperatorStats.pipelineId;
      pipelineOut.totalDrivers = firstOperatorStats.numDrivers;
      pipelineOut.rawInputPositions = firstOperatorStats.rawInputPositions;
      pipelineOut.rawInputDataSize = {
          static_cast<double>(firstOperatorStats.rawInputBytes),
          protocol::DataUnit::BYTE};
      pipelineOut.processedInputPositions = firstOperatorStats.inputPositions;
      pipelineOut.processedInputDataSize = {
          static_cast<double>(firstOperatorStats.inputBytes), protocol::DataUnit::BYTE};
      pipelineOut.outputPositions = lastOperatorStats.outputPositions;
      pipelineOut.outputDataSize = {static_cast<double>(lastOperatorStats.outputBytes),
                                    protocol::DataUnit::BYTE};
    }

    if (pipelineOut.inputPipeline) {
      trinoTaskStats.rawInputPositions += pipelineOut.rawInputPositions;
      //   trinoTaskStats.rawInputDataSize += pipelineOut.rawInputDataSize;
      trinoTaskStats.processedInputPositions += pipelineOut.processedInputPositions;
      //   trinoTaskStats.processedInputDataSize += pipelineOut.processedInputDataSize;
    }
    if (pipelineOut.outputPipeline) {
      trinoTaskStats.outputPositions += pipelineOut.outputPositions;
      //   trinoTaskStats.outputDataSize += pipelineOut.outputDataSize;
    }

    for (auto j = 0; j < pipeline.operatorStats.size(); ++j) {
      auto& opOut = pipelineOut.operatorSummaries[j];
      auto& op = pipeline.operatorStats[j];

      //   opOut.stageId = id.stageId();
      //   opOut.stageExecutionId = id.stageExecutionId();
      opOut.pipelineId = i;
      opOut.planNodeId = op.planNodeId;
      opOut.operatorId = op.operatorId;
      opOut.operatorType = toTrinoOperatorType(op.operatorType);

      opOut.totalDrivers = op.numDrivers;
      opOut.inputPositions = op.inputPositions;
      opOut.sumSquaredInputPositions = ((double)op.inputPositions) * op.inputPositions;
      opOut.inputDataSize = protocol::DataSize(op.inputBytes, protocol::DataUnit::BYTE);
      //   opOut.rawInputPositions = op.rawInputPositions;
      opOut.rawInputDataSize =
          protocol::DataSize(op.rawInputBytes, protocol::DataUnit::BYTE);

      // Report raw input statistics on the Project node following TableScan, if
      // exists.
      if (j == 1 && op.operatorType == "FilterProject" &&
          pipeline.operatorStats[0].operatorType == "TableScan") {
        const auto& scanOp = pipeline.operatorStats[0];
        // opOut.rawInputPositions = scanOp.rawInputPositions;
        opOut.rawInputDataSize =
            protocol::DataSize(scanOp.rawInputBytes, protocol::DataUnit::BYTE);
      }

      opOut.outputPositions = op.outputPositions;
      opOut.outputDataSize = protocol::DataSize(op.outputBytes, protocol::DataUnit::BYTE);

      setTiming(op.addInputTiming, opOut.addInputCalls, opOut.addInputWall,
                opOut.addInputCpu);
      setTiming(op.getOutputTiming, opOut.getOutputCalls, opOut.getOutputWall,
                opOut.getOutputCpu);
      setTiming(op.finishTiming, opOut.finishCalls, opOut.finishWall, opOut.finishCpu);

      opOut.blockedWall =
          protocol::Duration(op.blockedWallNanos, protocol::TimeUnit::NANOSECONDS);

      opOut.userMemoryReservation = protocol::DataSize(
          op.memoryStats.userMemoryReservation, protocol::DataUnit::BYTE);
      opOut.revocableMemoryReservation = protocol::DataSize(
          op.memoryStats.revocableMemoryReservation, protocol::DataUnit::BYTE);
      //   opOut.systemMemoryReservation = protocol::DataSize(
      //       op.memoryStats.systemMemoryReservation, protocol::DataUnit::BYTE);
      opOut.peakUserMemoryReservation = protocol::DataSize(
          op.memoryStats.peakUserMemoryReservation, protocol::DataUnit::BYTE);
      //   opOut.peakSystemMemoryReservation = protocol::DataSize(
      //       op.memoryStats.peakSystemMemoryReservation, protocol::DataUnit::BYTE);
      opOut.peakTotalMemoryReservation = protocol::DataSize(
          op.memoryStats.peakTotalMemoryReservation, protocol::DataUnit::BYTE);

      opOut.spilledDataSize =
          protocol::DataSize(op.spilledBytes, protocol::DataUnit::BYTE);

      for (const auto& stat : op.runtimeStats) {
        // auto statName =
        //     fmt::format("{}.{}.{}", op.operatorType, op.planNodeId, stat.first);
        // opOut.runtimeStats[statName] = toRuntimeMetric(statName, stat.second);
        // if (taskRuntimeStats.count(statName)) {
        //   taskRuntimeStats[statName].merge(stat.second);
        // } else {
        //   taskRuntimeStats[statName] = stat.second;
        // }
      }
      if (op.numSplits != 0) {
        // const auto statName =
        //     fmt::format("{}.{}.{}", op.operatorType, op.planNodeId, "numSplits");
        // opOut.runtimeStats.emplace(
        //     statName,
        //     protocol::RuntimeMetric{statName, protocol::RuntimeUnit::NONE,
        //     op.numSplits,
        //                             1, op.numSplits, op.numSplits});
      }

      // If Velox's operator has spilling stats, then add them to the protocol
      // operator stats and the task stats as runtime stats.
      // if (op.spilledBytes > 0) {
      //   std::string statName =
      //       fmt::format("{}.{}.spilledBytes", op.operatorType, op.planNodeId);
      //   auto protocolMetric = createProtocolRuntimeMetric(statName, op.spilledBytes,
      //                                                     protocol::RuntimeUnit::BYTE);
      //   opOut.runtimeStats.emplace(statName, protocolMetric);
      //   trinoTaskStats.runtimeStats[statName] = protocolMetric;

      //   statName = fmt::format("{}.{}.spilledRows", op.operatorType, op.planNodeId);
      //   protocolMetric = createProtocolRuntimeMetric(statName, op.spilledRows,
      //                                                protocol::RuntimeUnit::NONE);
      //   opOut.runtimeStats.emplace(statName, protocolMetric);
      //   trinoTaskStats.runtimeStats[statName] = protocolMetric;

      //   statName = fmt::format("{}.{}.spilledPartitions", op.operatorType,
      //   op.planNodeId); protocolMetric = createProtocolRuntimeMetric(statName,
      //   op.spilledPartitions,
      //                                                protocol::RuntimeUnit::NONE);
      //   opOut.runtimeStats.emplace(statName, protocolMetric);
      //   trinoTaskStats.runtimeStats[statName] = protocolMetric;

      //   statName = fmt::format("{}.{}.spilledFiles", op.operatorType, op.planNodeId);
      //   protocolMetric = createProtocolRuntimeMetric(statName, op.spilledFiles,
      //                                                protocol::RuntimeUnit::NONE);
      //   opOut.runtimeStats.emplace(statName, protocolMetric);
      //   trinoTaskStats.runtimeStats[statName] = protocolMetric;
      // }

      auto wallNanos = op.addInputTiming.wallNanos + op.getOutputTiming.wallNanos +
                       op.finishTiming.wallNanos;
      auto cpuNanos = op.addInputTiming.cpuNanos + op.getOutputTiming.cpuNanos +
                      op.finishTiming.cpuNanos;

      pipelineOut.totalScheduledTime +=
          {static_cast<double>(wallNanos), TimeUnit::NANOSECONDS};
      pipelineOut.totalCpuTime += {static_cast<double>(cpuNanos), TimeUnit::NANOSECONDS};
      pipelineOut.totalBlockedTime +=
          {static_cast<double>(op.blockedWallNanos), TimeUnit::NANOSECONDS};
      pipelineOut.userMemoryReservation +=
          {static_cast<double>(op.memoryStats.userMemoryReservation), DataUnit::BYTE};
      pipelineOut.revocableMemoryReservation += {
          static_cast<double>(op.memoryStats.revocableMemoryReservation), DataUnit::BYTE};
      // pipelineOut.systemMemoryReservation +=
      //     {op.memoryStats.systemMemoryReservation, DataUnit::BYTE};

      trinoTaskStats.totalScheduledTime +=
          {static_cast<double>(wallNanos), TimeUnit::NANOSECONDS};
      trinoTaskStats.totalCpuTime +=
          {static_cast<double>(cpuNanos), TimeUnit::NANOSECONDS};
      trinoTaskStats.totalBlockedTime +=
          {static_cast<double>(op.blockedWallNanos), TimeUnit::NANOSECONDS};
    }  // pipeline's operators loop
  }    // task's pipelines loop

  return taskInfo;
}

facebook::velox::exec::Split toVeloxSplit(TrinoSplit& trinoSplit) {
  const int splitGroupId = 0;
  return facebook::velox::exec::Split(nullptr, splitGroupId);
}

io::trino::protocol::TaskState toTrinoTaskState(facebook::velox::exec::TaskState state) {
  switch (state) {
    case exec::kRunning:
      return io::trino::protocol::TaskState::RUNNING;
    case exec::kFinished:
      return io::trino::protocol::TaskState::FINISHED;
    case exec::kCanceled:
      return io::trino::protocol::TaskState::CANCELED;
    case exec::kFailed:
      return io::trino::protocol::TaskState::FAILED;
    case exec::kAborted:
    default:
      return io::trino::protocol::TaskState::ABORTED;
  }
}

std::string toTrinoOperatorType(const std::string& operatorType) {
  if (operatorType == "MergeExchange") {
    return "MergeOperator";
  }
  if (operatorType == "Exchange") {
    return "ExchangeOperator";
  }
  if (operatorType == "TableScan") {
    return "TableScanOperator";
  }
  return operatorType;
}

void setTiming(const CpuWallTiming& timing, int64_t& count, protocol::Duration& wall,
               protocol::Duration& cpu) {
  count = timing.count;
  wall = protocol::Duration(timing.wallNanos, protocol::TimeUnit::NANOSECONDS);
  cpu = protocol::Duration(timing.cpuNanos, protocol::TimeUnit::NANOSECONDS);
}

// class PlanConvertor {
//  public:
//   PlanConvertor();
//   velox::core::PlanFragment toVeloxPlan(protocol::PlanFragment& trinoPlan);
// };

// velox::core::PlanFragment toVeloxQueryPlan(std::string plan) {
//   protocol::PlanFragment trinoPlan = json::parse(plan);
//   PlanConvertor planConvertor;
//   velox::core::PlanFragment planFragment = planConvertor->toVeloxPlan(trinoPlan);
//   return planFragment;
// }

void VeloxInitializer::init() {
  // Setup and register.
  velox::filesystems::registerLocalFileSystem();

  std::unordered_map<std::string, std::string> configurationValues;

  auto properties = std::make_shared<const velox::core::MemConfig>(configurationValues);
  auto hiveConnectorExecutor = getConnectorIOExecutor(30, "HiveConnectorIO");
  auto hiveConnector =
      velox::connector::getConnectorFactory(
          velox::connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, properties, hiveConnectorExecutor.get());

  velox::connector::registerConnector(hiveConnector);

  const std::string kTpchConnectorId_{"tpch"};

  auto tpchConnector = connector::getConnectorFactory(
                           connector::tpch::TpchConnectorFactory::kTpchConnectorName)
                           ->newConnector(kTpchConnectorId_, nullptr);
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

  registerTrinoSumAggregate(trinoBridgePrefix + "sum");
}
}  // namespace io::trino::bridge
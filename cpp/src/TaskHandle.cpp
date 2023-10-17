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

#include "src/TaskHandle.h"
#include "src/NativeConfigs.h"
#include "src/PartitionOutputData.h"
#include "src/protocol/trino_protocol.h"
#include "src/utils/ThreadUtils.h"
#include "velox/exec/Task.h"

namespace io::trino::bridge {

TaskHandle::TaskHandle(const io::trino::TrinoTaskId& id,
                       io::trino::bridge::TaskHandle::TaskPtr task_ptr,
                       size_t numPartitions, bool broadcast)
    : driverExecutor_(getDriverCPUExecutor()),
      taskId(id),
      task(std::move(task_ptr)),
      outputs(numPartitions),
      isBroadcast(broadcast),
      ref_count_(0) {
  for (size_t partitionId = 0; partitionId < numPartitions; ++partitionId) {
    outputs[partitionId] = std::make_unique<PartitionOutputData>();
  }
}

TaskHandlePtr TaskHandle::createTaskHandle(
    const io::trino::TrinoTaskId& id, io::trino::bridge::TaskHandle::TaskPtr task_ptr,
    size_t numPartitions, bool broadcast) {
  return {new TaskHandle(id, std::move(task_ptr), numPartitions, broadcast)};
}

void TaskHandle::start(std::function<void(folly::Unit)> stateChangeListener) {
  task->stateChangeFuture(0).via(driverExecutor_.get()).thenValue(stateChangeListener);

  auto& config = NativeConfigs::instance();

  int32_t maxDriverPerTask =
      std::max(1, std::min(config.getMaxDriversPerTask(), config.getTaskConcurrency()));
  int32_t concurrentLifespans = config.getConcurrentLifespans();

  exec::Task::start(task, maxDriverPerTask, concurrentLifespans);

  VLOG(1) << "Task " << taskId.fullId()
          << " is started, maxDriverPerTask=" << std::to_string(maxDriverPerTask);
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

io::trino::protocol::DateTime toISOTimestamp(uint64_t timeMilli) {
  char buf[80];
  time_t timeSecond = timeMilli / 1000;
  tm gmtTime;
  gmtime_r(&timeSecond, &gmtTime);
  strftime(buf, sizeof buf, "%FT%T", &gmtTime);
  return fmt::format("{}.{:03d}Z", buf, timeMilli % 1000);
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

io::trino::protocol::TaskStatus TaskHandle::getTaskStatus() {
  const facebook::velox::exec::TaskStats taskStats = task->taskStats();

  io::trino::protocol::TaskStatus taskStatus{};
  taskStatus.taskId = taskId.fullId();
  taskStatus.taskInstanceId = NativeConfigs::instance().getInstanceId();
  taskStatus.queuedPartitionedDrivers = taskStats.numQueuedSplits;
  taskStatus.runningPartitionedDrivers = taskStats.numRunningSplits;
  taskStatus.state = toTrinoTaskState(task->state());
  taskStatus.memoryReservation = {static_cast<double>(task->pool()->currentBytes()),
                                  io::trino::protocol::DataUnit::BYTE};
  taskStatus.self = fmt::format("{}/v1/task/{}", NativeConfigs::instance().getBaseUrl(),
                                taskId.fullId());

  return taskStatus;
}

io::trino::protocol::TaskStats TaskHandle::getTaskStats() {
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

  stats.totalScheduledTime = protocol::Duration(0, protocol::TimeUnit::NANOSECONDS);
  stats.totalBlockedTime = protocol::Duration(0, protocol::TimeUnit::NANOSECONDS);
  stats.totalCpuTime = protocol::Duration(0, protocol::TimeUnit::NANOSECONDS);

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

    pipelineStats.totalScheduledTime =
        protocol::Duration(0, protocol::TimeUnit::NANOSECONDS);
    pipelineStats.totalBlockedTime =
        protocol::Duration(0, protocol::TimeUnit::NANOSECONDS);
    pipelineStats.totalCpuTime = protocol::Duration(0, protocol::TimeUnit::NANOSECONDS);

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

      if (op.operatorType == "Exchange") {
        // For Exchange:
        //  input / output size: deserialized vector size.
        //  raw input size: serialized page size, network traffic.
        // For PartitionedOutput:
        //  input / output size: output vector size, before serialization.
        //  No stats about serialized page size.
        opOut.rawInputDataSize =
            protocol::DataSize(op.rawInputBytes, protocol::DataUnit::BYTE);
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

      auto totalScheduledTime =
          protocol::Duration(wallNanos, protocol::TimeUnit::NANOSECONDS);
      auto totalBlockedTime =
          protocol::Duration(op.blockedWallNanos, protocol::TimeUnit::NANOSECONDS);
      auto totalCpuTime = protocol::Duration(cpuNanos, protocol::TimeUnit::NANOSECONDS);

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

  }  // task's pipelines loop

  auto&& outPipeline = stats.pipelines.front();
  auto&& outOp = outPipeline.operatorSummaries.back();
  if (outOp.stageId > 0) {
    size_t opNum = outPipeline.operatorSummaries.size();
    VELOX_CHECK_GT(opNum, 1);
    outOp.planNodeId = outPipeline.operatorSummaries[opNum - 2].planNodeId;
  }

  return stats;
}

std::unique_ptr<PartitionOutputData>& TaskHandle::getPartitionOutputData(
    size_t destination) {
  return outputs[destination];
}

void TaskHandle::addRef() { ref_count_.fetch_add(1); }

void TaskHandle::release() {
  if (ref_count_.fetch_sub(1) == 1) {
    delete this;
  }
}

void intrusive_ptr_add_ref(TaskHandle* handle) { handle->addRef(); }
void intrusive_ptr_release(TaskHandle* handle) { handle->release(); }

}  // namespace io::trino::bridge

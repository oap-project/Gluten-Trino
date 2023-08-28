#pragma once

#include "velox/common/time/CpuWallTimer.h"
#include "velox/exec/Task.h"

#include "protocol/trino_protocol.h"
#include "types/TrinoTaskId.h"

#include <fmt/format.h>
#include <memory>
#include <string>

using PlanNodeId = std::string;
using TaskId = std::string;

namespace io::trino::bridge {

static const std::string kHiveConnectorId = "test-hive";

io::trino::protocol::DateTime toISOTimestamp(uint64_t timeMilli);

io::trino::protocol::TaskStatus getTaskStatus(
    std::shared_ptr<facebook::velox::exec::Task>& task, TaskId taskId);

io::trino::protocol::TaskStats getTaskStats(
    const std::shared_ptr<facebook::velox::exec::Task>& task, const TrinoTaskId& taskId);

io::trino::protocol::TaskInfo getTaskInfo(
    std::shared_ptr<facebook::velox::exec::Task>& task, TaskId taskId);

struct TrinoSplit {
  long sequenceId = {};
  PlanNodeId planNodeId = {};
};

facebook::velox::exec::Split toVeloxSplit(TrinoSplit& trinoSplit);

io::trino::protocol::TaskState toTrinoTaskState(facebook::velox::exec::TaskState state);

std::string toTrinoOperatorType(const std::string& operatorType);

void setTiming(const facebook::velox::CpuWallTiming& timing, int64_t& count,
               io::trino::protocol::Duration& wall, io::trino::protocol::Duration& cpu);

facebook::velox::core::PlanFragment toVeloxQueryPlan();

class VeloxInitializer {
 public:
  explicit VeloxInitializer() { init(); }

  ~VeloxInitializer() {}

  void init();
};
}  // namespace io::trino::bridge
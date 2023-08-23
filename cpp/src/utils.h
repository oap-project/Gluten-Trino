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
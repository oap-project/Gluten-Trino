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

#include <memory>
#include "boost/intrusive_ptr.hpp"
#include "types/TrinoTaskId.h"

namespace facebook::velox {
namespace exec {
class Task;
}
}  // namespace facebook::velox

namespace io::trino::protocol {
struct TaskStats;
struct TaskStatus;
}  // namespace io::trino::protocol

namespace folly {
class CPUThreadPoolExecutor;
class Unit;
}  // namespace folly

using namespace facebook::velox;

namespace io::trino::bridge {

class TaskHandle;
using TaskHandlePtr = boost::intrusive_ptr<TaskHandle>;

class PartitionOutputData;

class TaskHandle {
 public:
  using TaskPtr = std::shared_ptr<exec::Task>;

  static TaskHandlePtr createTaskHandle(const TrinoTaskId& id, TaskPtr task_ptr,
                                        size_t numPartitions = 1, bool broadcast = false);

  io::trino::protocol::TaskStats getTaskStats();

  io::trino::protocol::TaskStatus getTaskStatus();

  inline TrinoTaskId& getTaskId() { return taskId; }
  inline TaskPtr getTask() { return task; }
  inline bool getIsBroadcast() { return isBroadcast; }
  std::unique_ptr<PartitionOutputData>& getPartitionOutputData(size_t destination);

  void start(std::function<void(folly::Unit)> stateChangeListener);

 private:
  TaskHandle(const TrinoTaskId& id, TaskPtr task_ptr, size_t numPartitions,
             bool broadcast);

  friend void intrusive_ptr_add_ref(TaskHandle*);
  friend void intrusive_ptr_release(TaskHandle*);

  void addRef();
  void release();

  std::shared_ptr<folly::CPUThreadPoolExecutor> driverExecutor_;
  TrinoTaskId taskId;
  TaskPtr task;
  std::vector<std::unique_ptr<PartitionOutputData>> outputs;
  bool isBroadcast;
  std::atomic<int32_t> ref_count_;
};
using TaskHandlePtr = boost::intrusive_ptr<TaskHandle>;

void intrusive_ptr_add_ref(TaskHandle* handle) { handle->addRef(); }
void intrusive_ptr_release(TaskHandle* handle) { handle->release(); }

}  // namespace io::trino::bridge

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

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <boost/intrusive_ptr.hpp>
#include <memory>

#include "velox/exec/Task.h"

namespace facebook::velox {
namespace memory {
class MemoryPool;
class MemoryAllocator;
}  // namespace memory
namespace cache {
class AsyncDataCache;
}
}  // namespace facebook::velox

using namespace facebook::velox;

namespace io::trino {
class TrinoTaskId;

namespace protocol {
class PlanFragment;
}

namespace bridge {

class NativeSqlTaskExecutionManager;
using NativeSqlTaskExecutionManagerPtr = std::shared_ptr<NativeSqlTaskExecutionManager>;

struct TaskHandle;
using TaskHandlePtr = boost::intrusive_ptr<TaskHandle>;

class JniHandle {
 public:
  explicit JniHandle(const NativeSqlTaskExecutionManagerPtr& javaManager);

  void initializeVelox();

  TaskHandlePtr createTaskHandle(const TrinoTaskId& id,
                                 const protocol::PlanFragment& plan);

  TaskHandlePtr getTaskHandle(const TrinoTaskId& id);

  bool removeTask(const TrinoTaskId& id);

  void terminateTask(const TrinoTaskId& id, exec::TaskState state);

  inline NativeSqlTaskExecutionManager* getNativeSqlTaskExecutionManager() {
    return javaManager_.get();
  }

 private:
  static std::shared_ptr<memory::MemoryPool> getPlanConvertorMemPool();

  void initializeVeloxMemory();

  void printTaskStatus(const TrinoTaskId& id, const std::shared_ptr<exec::Task>& task);

  template <typename F, typename... Args>
  std::invoke_result_t<F, Args&&...> withWLock(F&& func, Args&&... args) const {
    taskMapLock_.lock();
    auto guard = folly::makeGuard([this]() { taskMapLock_.unlock(); });
    return func(args...);
  }

  template <typename F, typename... Args>
  std::invoke_result_t<F, Args&&...> withRLock(F&& func, Args&&... args) const {
    taskMapLock_.lock_shared();
    auto guard = folly::makeGuard([this]() { taskMapLock_.unlock_shared(); });
    return func(args...);
  }

 private:
  mutable std::shared_mutex taskMapLock_;
  NativeSqlTaskExecutionManagerPtr javaManager_;
  std::unordered_map<std::string, TaskHandlePtr> taskMap_;
  std::shared_ptr<folly::CPUThreadPoolExecutor> driverExecutor_;
  std::shared_ptr<folly::IOThreadPoolExecutor> exchangeIOExecutor_;
  std::shared_ptr<folly::IOThreadPoolExecutor> spillExecutor_;

  std::shared_ptr<cache::AsyncDataCache> cache_;
  std::unique_ptr<folly::IOThreadPoolExecutor> cacheExecutor_;
  std::shared_ptr<memory::MemoryAllocator> allocator_;

  static const std::string kGlutenTrinoFunctionPrefix;
};
}  // namespace bridge
}  // namespace io::trino

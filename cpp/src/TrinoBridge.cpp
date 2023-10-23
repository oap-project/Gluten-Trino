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

#include "src/TrinoBridge.h"

#include <proxygen/lib/http/session/HTTPSessionBase.h>

#include "src/JniHandle.h"
#include "src/NativeConfigs.h"
#include "src/PartitionOutputData.h"
#include "src/TaskHandle.h"
#include "src/protocol/trino_protocol.h"
#include "src/types/PrestoToVeloxSplit.h"
#include "src/utils/JniUtils.h"
#include "velox/exec/PartitionedOutputBufferManager.h"
#include "velox/exec/Task.h"

using namespace io::trino::bridge;
using namespace facebook::velox;

DECLARE_bool(velox_exception_user_stacktrace_enabled);
DECLARE_bool(velox_memory_leak_check_enabled);

template <typename F, typename... Args>
void tryLogException(F&& func, Args&&... args) {
  using tuple_type = std::tuple<std::decay_t<Args>...>;
  tuple_type t{std::forward<Args>(args)...};
  try {
    std::apply(std::forward<F>(func), t);
  } catch (const std::exception& e) {
    JniUtils::logError(JniUtils::getJNIEnv(), __FILE__, __LINE__, e.what());
    JniUtils::throwJavaRuntimeException(JniUtils::getJNIEnv(), e.what());
  }
}

template <typename F, typename T, typename... Args>
T tryLogExceptionWithReturnValue(F&& func, const T& returnValueOnError, Args&&... args) {
  using tuple_type = std::tuple<std::decay_t<Args>...>;
  tuple_type t{std::forward<Args>(args)...};
  try {
    return std::apply(std::forward<F>(func), t);
  } catch (const std::exception& e) {
    JniUtils::logError(JniUtils::getJNIEnv(), __FILE__, __LINE__, e.what());
    JniUtils::throwJavaRuntimeException(JniUtils::getJNIEnv(), e.what());
  }
  return returnValueOnError;
}

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }
  GLOBAL_JAVA_VM = vm;

  io::trino::bridge::Unsafe::instance().initialize(env);
  FLAGS_velox_exception_user_stacktrace_enabled = true;
  // See folly CPUThreadPoolExecutor.h
  // It's better to close dynamic cpu thread pool executor since the thread pool in Java
  // side is increased only if it needs more but never get decreased.
  FLAGS_dynamic_cputhreadpoolexecutor = false;

  return JNI_VERSION;
}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM* vm, void* reserved) {
  GLOBAL_JAVA_VM = nullptr;
}

// return value:
// 0 : create successful
// 1 : task already exists
// others : failed
JNIEXPORT jlong JNICALL Java_io_trino_jni_TrinoBridge_createTask(JNIEnv* env, jobject obj,
                                                                 jlong handlePtr,
                                                                 jstring jTaskId,
                                                                 jstring jPlanFragment) {
  return tryLogExceptionWithReturnValue(
      [env, handlePtr, jTaskId, jPlanFragment]() {
        io::trino::TrinoTaskId taskId(JniUtils::jstringToString(env, jTaskId));

        JniHandle* handle = reinterpret_cast<JniHandle*>(handlePtr);
        if (!handle) {
          JniUtils::throwJavaRuntimeException(env, "Empty JniHandle!!!");
          return -1;
        }

        std::string planFragment(JniUtils::jstringToString(env, jPlanFragment));
        JniUtils::logDebug(
            env, __FILE__, __LINE__,
            "Task " + taskId.fullId() + " gets PlanFragment Json: " + planFragment);
        nlohmann::json json = nlohmann::json::parse(planFragment);
        std::shared_ptr<io::trino::protocol::PlanFragment> glutenPlanFragment;
        from_json(json, glutenPlanFragment);
        if (!glutenPlanFragment) {
          JniUtils::throwJavaRuntimeException(
              env,
              "Failed to parse Json string into Gluten plan fragment" + to_string(json));
          return -1;
        }

        TaskHandlePtr taskHandle = handle->createTaskHandle(taskId, *glutenPlanFragment);
        taskHandle->start([handle, taskId](folly::Unit unit) {
          handle->getNativeSqlTaskExecutionManager()->requestUpdateNativeTaskStatus(
              taskId);
        });
        return 0;
      },
      -1);
}

JNIEXPORT void JNICALL Java_io_trino_jni_TrinoBridge_failedTask(JNIEnv* env, jobject obj,
                                                                jlong handlePtr,
                                                                jstring jTaskId,
                                                                jstring failedReason) {
  tryLogException([&env, &handlePtr, &jTaskId, &failedReason]() {
    io::trino::TrinoTaskId taskId(JniUtils::jstringToString(env, jTaskId));

    JniHandle* handle = reinterpret_cast<JniHandle*>(handlePtr);
    if (!handle) {
      JniUtils::throwJavaRuntimeException(env, "Empty JniHandle!!!");
      return;
    }
    TaskHandlePtr taskHandle = handle->getTaskHandle(taskId);
    if (!taskHandle) {
      JniUtils::throwJavaRuntimeException(
          env, "TaskHandle for task " + taskId.fullId() + " didn't exist.");
      return;
    }
    taskHandle->getTask()->setError(JniUtils::jstringToString(env, failedReason));
  });
}

JNIEXPORT void JNICALL Java_io_trino_jni_TrinoBridge_removeTask(JNIEnv* env, jobject obj,
                                                                jlong handlePtr,
                                                                jstring jTaskId) {
  JniHandle* handle = reinterpret_cast<JniHandle*>(handlePtr);
  io::trino::TrinoTaskId taskId(JniUtils::jstringToString(env, jTaskId));
  handle->removeTask(taskId);
}

JNIEXPORT void JNICALL Java_io_trino_jni_TrinoBridge_addSplits(JNIEnv* env, jobject obj,
                                                               jlong handlePtr,
                                                               jstring jTaskId,
                                                               jstring jSplitInfo) {
  return tryLogException([env, handlePtr, jTaskId, jSplitInfo]() {
    io::trino::TrinoTaskId taskId(JniUtils::jstringToString(env, jTaskId));
    JniHandle* handle = reinterpret_cast<JniHandle*>(handlePtr);
    TaskHandlePtr taskHandle = handle->getTaskHandle(taskId);
    if (taskHandle) {
      std::shared_ptr<exec::Task> task = taskHandle->getTask();
      std::string splitInfo = JniUtils::jstringToString(env, jSplitInfo);
      nlohmann::json jsonSplits = nlohmann::json::parse(splitInfo);
      std::shared_ptr<io::trino::protocol::SplitAssignmentsMessage> splitsPtr;
      from_json(jsonSplits, splitsPtr);

      for (auto&& splitAssignment : splitsPtr->splitAssignments) {
        long maxSplitSequenceId = -1;
        for (auto& split : splitAssignment.splits) {
          exec::Split veloxSplit = io::trino::toVeloxSplit(split);
          if (veloxSplit.hasConnectorSplit()) {
            maxSplitSequenceId = std::max(maxSplitSequenceId, split.sequenceId);
            task->addSplitWithSequence(split.planNodeId, std::move(veloxSplit),
                                       split.sequenceId);
          }
        }
        task->setMaxSplitSequenceId(splitAssignment.planNodeId, maxSplitSequenceId);

        if (splitAssignment.noMoreSplits) {
          task->noMoreSplits(splitAssignment.planNodeId);
        }
      }
    } else {
      std::cerr << "Not found task id " << taskId.fullId() << " when call addSplits."
                << std::endl;
    }
  });
}

JNIEXPORT jlong JNICALL Java_io_trino_jni_TrinoBridge_init(JNIEnv* env, jobject obj,
                                                           jstring configJson,
                                                           jobject manager) {
  // init JniHandle and return to Java
  static std::vector<std::unique_ptr<JniHandle>> jniHandleHolder;
  FLAGS_velox_memory_leak_check_enabled = true;

  auto& config = NativeConfigs::instance();
  config.initialize(JniUtils::jstringToString(env, configJson));

  proxygen::HTTPSessionBase::setMaxReadBufferSize(
      config.getMaxHttpSessionReadBufferSize());

  for (auto& [moduleName, level] : config.getLogVerboseModules()) {
    google::SetVLOGLevel(moduleName.c_str(), level);
  }

  JniHandle* handle =
      new JniHandle(std::make_shared<NativeSqlTaskExecutionManager>(manager));
  handle->initializeVelox();
  jniHandleHolder.emplace_back(handle);
  return reinterpret_cast<int64_t>(handle);
}

JNIEXPORT jlong JNICALL Java_io_trino_jni_TrinoBridge_close(JNIEnv* env, jobject obj,
                                                            jlong handlePtr) {
  return tryLogExceptionWithReturnValue(
      [handlePtr]() {
        JniHandle* handle = reinterpret_cast<JniHandle*>(handlePtr);
        VLOG(google::INFO) << "JNIHandle closed";
        delete handle;
        return 0;
      },
      -1);
}

JNIEXPORT void JNICALL Java_io_trino_jni_TrinoBridge_noMoreBuffers(
    JNIEnv* env, jobject obj, jlong handlePtr, jstring jTaskId, jint jNumPartitions) {
  tryLogException([env, handlePtr, jTaskId, jNumPartitions]() -> void {
    io::trino::TrinoTaskId taskId(JniUtils::jstringToString(env, jTaskId));
    JniHandle* handle = reinterpret_cast<JniHandle*>(handlePtr);
    if (!handle) {
      JniUtils::throwJavaRuntimeException(env, "Empty handle!!!");
      return;
    }
    TaskHandlePtr taskHandle = handle->getTaskHandle(taskId);
    if (!taskHandle || !taskHandle->getTask()) {
      JniUtils::throwJavaRuntimeException(
          env, "Task " + taskId.fullId() + " has already finished.");
      return;
    }
    if (taskHandle->getIsBroadcast()) {
      for (int destination = 0; destination < jNumPartitions; ++destination) {
        taskHandle->getTask()->updateOutputBuffers(destination, true);
      }
    }
  });
}

JNIEXPORT void JNICALL Java_io_trino_jni_TrinoBridge_registerOutputPartitionListener(
    JNIEnv* env, jobject obj, jlong handlePtr, jstring jTaskId, jint jPartitionId,
    jlong jSequence, jlong maxBytes) {
  tryLogException([env, handlePtr, jTaskId, jPartitionId, jSequence, maxBytes]() {
    io::trino::TrinoTaskId taskId(JniUtils::jstringToString(env, jTaskId));
    JniHandle* handle = reinterpret_cast<JniHandle*>(handlePtr);
    TaskHandlePtr taskHandle = handle->getTaskHandle(taskId);
    if (!handle) {
      JniUtils::logError(env, __FILE__, __LINE__, "Empty handle!!!");
      return;
    }
    if (taskHandle) {
      auto manager = exec::OutputBufferManager::getInstance().lock();
      int destination = jPartitionId;

      auto& output = taskHandle->getPartitionOutputData(destination);
      VELOX_CHECK(!output->getListenerRegistered())
      output->registerListener();

      auto& taskId = taskHandle->getTaskId();
      bool exist = manager->getData(
          taskId.fullId(), destination, maxBytes, jSequence,
          [&output, &taskId, handle, destination](
              std::vector<std::unique_ptr<folly::IOBuf>> pages,
              int64_t sequence) mutable {
            output->enqueueWithLock(sequence, pages);
            output->consumeListener();
            handle->getNativeSqlTaskExecutionManager()->requestFetchNativeOutput(
                taskId, destination);
          });

      if (!exist) {
        // For the case that the result in the buffer manager is removed, but java side is
        // not notified.
        handle->getNativeSqlTaskExecutionManager()->requestFetchNativeOutput(taskId,
                                                                             destination);
      }
    }
  });
}

JNIEXPORT jint JNICALL Java_io_trino_jni_TrinoBridge_getBufferStep1(
    JNIEnv* env, jobject obj, jlong handlePtr, jstring jTaskId, jint jPartitionId) {
  return tryLogExceptionWithReturnValue(
      [env, handlePtr, jTaskId, jPartitionId]() {
        io::trino::TrinoTaskId taskId(JniUtils::jstringToString(env, jTaskId));
        JniHandle* handle = reinterpret_cast<JniHandle*>(handlePtr);
        TaskHandlePtr taskHandle = handle->getTaskHandle(taskId);
        if (!handle) {
          JniUtils::logWarning(env, __FILE__, __LINE__, "Empty handle!!!");
          return 0;
        }
        if (taskHandle) {
          int destination = jPartitionId;
          auto& output = taskHandle->getPartitionOutputData(destination);
          size_t data_num = output->withLock(
              [&taskId, &destination, &taskHandle](PartitionOutputData& data) {
                if (data.noMoreData() && data.getOutputDataNum() == 0) {
                  auto manager =
                      exec::OutputBufferManager::getInstance().lock();
                  manager->deleteResults(taskId.fullId(), destination);
                }
                return data.getOutputDataNum();
              },
              *output);

          return static_cast<jint>(data_num);
        } else {
          JniUtils::logError(env, __FILE__, __LINE__, "Task does not exist!");
          return 0;
        }
      },
      0);
}

JNIEXPORT void JNICALL Java_io_trino_jni_TrinoBridge_getBufferStep2(
    JNIEnv* env, jobject obj, jlong handlePtr, jstring jTaskId, jint jPartitionId,
    jint results_num, jintArray jLengthArray) {
  return tryLogException(
      [env, handlePtr, jTaskId, jPartitionId, results_num, jLengthArray]() {
        io::trino::TrinoTaskId taskId(JniUtils::jstringToString(env, jTaskId));
        JniHandle* handle = reinterpret_cast<JniHandle*>(handlePtr);
        TaskHandlePtr taskHandle = handle->getTaskHandle(taskId);
        size_t destination = jPartitionId;
        auto& output = taskHandle->getPartitionOutputData(destination);
        output->withLock([env, &jLengthArray, &output, results_num]() {
          for (auto index = 0; index < results_num; index++) {
            int32_t size = output->getDataSize(index);
            env->SetIntArrayRegion(jLengthArray, index, 1, &size);
          }
        });
        return;
      });
}

JNIEXPORT void JNICALL Java_io_trino_jni_TrinoBridge_getBufferStep3(
    JNIEnv* env, jobject obj, jlong handlePtr, jstring jTaskId, jint jPartitionId,
    jint results_num, jlongArray jAddressArray) {
  tryLogException([env, handlePtr, jTaskId, jPartitionId, results_num, jAddressArray]() {
    io::trino::TrinoTaskId taskId(JniUtils::jstringToString(env, jTaskId));
    JniHandle* handle = reinterpret_cast<JniHandle*>(handlePtr);
    TaskHandlePtr taskHandle = handle->getTaskHandle(taskId);
    jboolean isCopy{false};
    auto* addressArray =
        reinterpret_cast<int64_t*>(env->GetLongArrayElements(jAddressArray, &isCopy));
    size_t destination = jPartitionId;
    auto& output = taskHandle->getPartitionOutputData(destination);

    auto pages = output->popWithLock(results_num);
    for (size_t index = 0; index < results_num; index++) {
      uint8_t* dst_addr = (uint8_t*)(addressArray[index]);
      const auto buf = pages[index].get();
      auto curBuf = buf;
      size_t start_offset = 0;
      do {
        std::memcpy(dst_addr + start_offset, curBuf->data(), curBuf->length());
        start_offset += curBuf->length();
        curBuf = curBuf->next();
      } while (buf != curBuf);
    }
  });
}

JNIEXPORT jstring JNICALL Java_io_trino_jni_TrinoBridge_getTaskStatus(JNIEnv* env,
                                                                      jobject obj,
                                                                      jlong handlePtr,
                                                                      jstring jTaskId) {
  return tryLogExceptionWithReturnValue(
      [env, handlePtr, jTaskId]() {
        io::trino::TrinoTaskId taskId(JniUtils::jstringToString(env, jTaskId));
        JniHandle* handle = reinterpret_cast<JniHandle*>(handlePtr);
        TaskHandlePtr taskHandle = handle->getTaskHandle(taskId);

        if (!taskHandle) {  // not found, return empty.
          io::trino::protocol::TaskStatus emptyTaskStatus;
          nlohmann::json j;
          io::trino::protocol::to_json(j, emptyTaskStatus);
          return env->NewStringUTF(j.dump().c_str());
        } else {
          io::trino::protocol::TaskStatus taskStatus = taskHandle->getTaskStatus();
          nlohmann::json j;
          io::trino::protocol::to_json(j, taskStatus);
          return env->NewStringUTF(j.dump().c_str());
        }
      },
      static_cast<jstring>(nullptr));
}

JNIEXPORT jstring JNICALL Java_io_trino_jni_TrinoBridge_getTaskStats(JNIEnv* env,
                                                                     jobject obj,
                                                                     jlong handlePtr,
                                                                     jstring jTaskId) {
  return tryLogExceptionWithReturnValue(
      [env, handlePtr, jTaskId]() {
        io::trino::TrinoTaskId taskId(JniUtils::jstringToString(env, jTaskId));
        JniHandle* handle = reinterpret_cast<JniHandle*>(handlePtr);
        TaskHandlePtr taskHandle = handle->getTaskHandle(taskId);

        if (!taskHandle) {
          VLOG(google::ERROR) << "Attempt to get a removed task stats, id="
                              << taskId.fullId();
          return static_cast<jstring>(nullptr);
        } else {
          io::trino::protocol::TaskStats taskStats = taskHandle->getTaskStats();
          nlohmann::json j;
          io::trino::protocol::to_json(j, taskStats);
          return env->NewStringUTF(j.dump().c_str());
        }
      },
      static_cast<jstring>(nullptr));
}

JNIEXPORT void JNICALL Java_io_trino_jni_TrinoBridge_registerConnector(
    JNIEnv* jniEnv, jobject cls, jstring jCatalogProperties) {
  return tryLogException([jniEnv, jCatalogProperties]() {
    const auto& registeredConnectors = connector::getAllConnectors();

    std::string catalogProperties = JniUtils::jstringToString(jniEnv, jCatalogProperties);
    nlohmann::json j = nlohmann::json::parse(catalogProperties);
    std::shared_ptr<io::trino::protocol::CatalogProperties> catalogPropertiesPtr;
    from_json(j, catalogPropertiesPtr);
    std::string connectorName = catalogPropertiesPtr->connectorName;
    std::string catalogName = catalogPropertiesPtr->catalogName;
    std::map<std::string, std::string> propertyValues = catalogPropertiesPtr->properties;

    // Check if the connector is already registered, if not, register it.
    if (registeredConnectors.empty() || registeredConnectors.count(connectorName) == 0) {
      std::unordered_map<std::string, std::string> connectorConf(propertyValues.begin(),
                                                                 propertyValues.end());
      std::shared_ptr<const Config> properties =
          std::make_shared<const core::MemConfig>(std::move(connectorConf));
      std::shared_ptr<connector::Connector> connector =
          connector::getConnectorFactory(connectorName)
              ->newConnector(catalogName, std::move(properties));
      connector::registerConnector(connector);
    }
  });
}

JNIEXPORT void JNICALL Java_io_trino_jni_TrinoBridge_abortTask(JNIEnv* env, jobject obj,
                                                               jlong handlePtr,
                                                               jstring jTaskId) {
  JniHandle* handle = reinterpret_cast<JniHandle*>(handlePtr);
  io::trino::TrinoTaskId taskId(JniUtils::jstringToString(env, jTaskId));
  handle->terminateTask(taskId, exec::kAborted);
}

JNIEXPORT void JNICALL Java_io_trino_jni_TrinoBridge_cancelTask(JNIEnv* env, jobject obj,
                                                                jlong handlePtr,
                                                                jstring jTaskId) {
  JniHandle* handle = reinterpret_cast<JniHandle*>(handlePtr);
  io::trino::TrinoTaskId taskId(JniUtils::jstringToString(env, jTaskId));
  handle->terminateTask(taskId, exec::kCanceled);
}

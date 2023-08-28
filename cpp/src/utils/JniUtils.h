#pragma once

#include <jni.h>
#include <memory>
#include <string>

#include "src/types/TrinoTaskId.h"

namespace io::trino::bridge {

class JniUtils {
 public:
  static std::string jstringToString(JNIEnv* jniEnv, jstring jstr);
  static void throwJavaRuntimeException(JNIEnv* jniEnv, const std::string& errorMessage);

  static void logDebug(JNIEnv* jniEnv, const char* file, int32_t line,
                       const std::string& logMessage);
  static void logInfo(JNIEnv* jniEnv, const char* file, int32_t line,
                      const std::string& logMessage);
  static void logWarning(JNIEnv* jniEnv, const char* file, int32_t line,
                         const std::string& logMessage);
  static void logError(JNIEnv* jniEnv, const char* file, int32_t line,
                       const std::string& logMessage);

  static jobject createJavaRuntimeException(JNIEnv* jniEnv,
                                            const std::string& errorMessage);

  static JNIEnv* getJNIEnv();
};

class Unsafe {
 public:
  static Unsafe& instance();

  void initialize(JNIEnv* env);

  template <typename T>
  int32_t arraySliceSize(int32_t length) const;

 private:
  explicit Unsafe() = default;

  static int32_t multiplyExact(int x, int y);
  static int32_t getUnsafeArrayIndexScale(JNIEnv* env, jclass unsafeClass,
                                          const std::string& name);

  int32_t arrayIntIndexScale;
  int32_t arrayLongIndexScale;
  int32_t arrayBooleanIndexScale;
};

class NativeSqlTaskExecutionManager {
 public:
  explicit NativeSqlTaskExecutionManager(jobject managerObj);

  void requestFetchNativeOutput(const TrinoTaskId& taskId, int partitionId);

  void requestUpdateNativeTaskStatus(const TrinoTaskId& taskId);

 private:
  jmethodID getMemberFunction(JNIEnv* env, const char* fname, const char* signture);

 private:
  jobject javaObj_;
};

using NativeSqlTaskExecutionManagerPtr = std::shared_ptr<NativeSqlTaskExecutionManager>;
}  // namespace io::trino::bridge

#include "JniUtils.h"

#include <glog/logging.h>
#include <sstream>

#include "velox/common/base/Exceptions.h"

extern int JNI_VERSION;
extern JavaVM* GLOBAL_JAVA_VM;

#define logInternal(logLevel)                                                          \
  std::stringstream ss;                                                                \
  ss << "[" << file << ":" << line << "] " << logMessage;                              \
  std::string finalLogMessage = std::move(ss.str());                                   \
  jclass javaLoggerClass = jniEnv->FindClass("io/trino/jni/NativeLogger");             \
  jmethodID methodId =                                                                 \
      jniEnv->GetStaticMethodID(javaLoggerClass, (logLevel), "(Ljava/lang/String;)V"); \
  jstring javaLogMessage = jniEnv->NewStringUTF(finalLogMessage.c_str());              \
  jniEnv->CallStaticVoidMethod(javaLoggerClass, methodId, javaLogMessage)

namespace io::trino::bridge {

std::string JniUtils::jstringToString(JNIEnv* env, jstring jstr) {
  const char* cstr = env->GetStringUTFChars(jstr, nullptr);
  std::string str(cstr);
  env->ReleaseStringUTFChars(jstr, cstr);
  return std::move(str);
}

void JniUtils::throwJavaRuntimeException(JNIEnv* jniEnv,
                                         const std::string& errorMessage) {
  try {
    jthrowable javaRuntimeException =
        static_cast<jthrowable>(createJavaRuntimeException(jniEnv, errorMessage));
    if (!javaRuntimeException) {
      throw std::runtime_error("Failed to create java object of RuntimeException.");
    }
    jniEnv->Throw(javaRuntimeException);
  } catch (const std::exception& e) {
    logInfo(jniEnv, __FILE__, __LINE__,
            std::string("Failed to throw java runtime exception, reason: ") + e.what());
  }
}

void JniUtils::logDebug(JNIEnv* jniEnv, const char* file, int32_t line,
                        const std::string& logMessage) {
  logInternal("logDebug");
}

void JniUtils::logInfo(JNIEnv* jniEnv, const char* file, int32_t line,
                       const std::string& logMessage) {
  logInternal("logInfo");
}

void JniUtils::logWarning(JNIEnv* jniEnv, const char* file, int32_t line,
                          const std::string& logMessage) {
  logInternal("logWarning");
}

void JniUtils::logError(JNIEnv* jniEnv, const char* file, int32_t line,
                        const std::string& logMessage) {
  logInternal("logError");
}

jobject JniUtils::createJavaRuntimeException(JNIEnv* jniEnv,
                                             const std::string& errorMessage) {
  jclass javaRuntimeExceptionClass = jniEnv->FindClass("java/lang/RuntimeException");
  if (!javaRuntimeExceptionClass) {
    JniUtils::logError(jniEnv, __FILE__, __LINE__,
                       "Failed to find java class of RuntimeException.");
    return nullptr;
  }
  jmethodID constructor =
      jniEnv->GetMethodID(javaRuntimeExceptionClass, "<init>", "(Ljava/lang/String;)V");
  if (!constructor) {
    JniUtils::logError(jniEnv, __FILE__, __LINE__,
                       "Failed to find the constructor of RuntimeException.");
    return nullptr;
  }
  jstring javaErrorMessage = jniEnv->NewStringUTF(errorMessage.c_str());
  if (!javaErrorMessage) {
    JniUtils::logError(jniEnv, __FILE__, __LINE__,
                       "Failed to create the error message string in JVM.");
    return nullptr;
  }
  return jniEnv->NewObject(javaRuntimeExceptionClass, constructor, javaErrorMessage);
}

JNIEnv* JniUtils::getJNIEnv() {
  if (!GLOBAL_JAVA_VM) {
    VLOG(google::ERROR) << "GLOBAL_JAVA_VM is null.";
  }

  JNIEnv* env = reinterpret_cast<JNIEnv*>(std::malloc(sizeof(JNIEnv)));
  if (GLOBAL_JAVA_VM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    if (GLOBAL_JAVA_VM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr) !=
        JNI_OK) {
      VLOG(google::ERROR) << "Can't get JNIEnv.";
    }
  }
  return env;
}

Unsafe& Unsafe::instance() {
  static Unsafe unsafe;
  return unsafe;
}

template <>
int32_t Unsafe::arraySliceSize<bool>(int32_t length) const {
  return multiplyExact(length, arrayBooleanIndexScale);
}

template <>
int32_t Unsafe::arraySliceSize<double>(int32_t length) const {
  return multiplyExact(length, sizeof(double));
}

template <>
int32_t Unsafe::arraySliceSize<short>(int32_t length) const {
  return multiplyExact(length, sizeof(short));
}

template <>
int32_t Unsafe::arraySliceSize<int8_t>(int32_t length) const {
  return multiplyExact(length, sizeof(int8_t));
}

template <>
int32_t Unsafe::arraySliceSize<float>(int32_t length) const {
  return multiplyExact(length, sizeof(float));
}

using int128_t = __int128_t;
template <>
int32_t Unsafe::arraySliceSize<int128_t>(int32_t length) const {
  return multiplyExact(length, sizeof(int128_t));
}

template <>
int32_t Unsafe::arraySliceSize<int64_t>(int32_t length) const {
  return multiplyExact(length, arrayLongIndexScale);
}

template <>
int32_t Unsafe::arraySliceSize<int32_t>(int32_t length) const {
  return multiplyExact(length, arrayIntIndexScale);
}

void Unsafe::initialize(JNIEnv* env) {
  jclass unsafeClass = env->FindClass("sun/misc/Unsafe");
  if (!unsafeClass) {
    JniUtils::throwJavaRuntimeException(env, "Cannot find the class of sun.misc.Unsafe.");
  }

  arrayIntIndexScale =
      getUnsafeArrayIndexScale(env, unsafeClass, "ARRAY_INT_INDEX_SCALE");
  arrayLongIndexScale =
      getUnsafeArrayIndexScale(env, unsafeClass, "ARRAY_LONG_INDEX_SCALE");
  arrayBooleanIndexScale =
      getUnsafeArrayIndexScale(env, unsafeClass, "ARRAY_BOOLEAN_INDEX_SCALE");
}

int32_t Unsafe::multiplyExact(int x, int y) {
  int64_t r = static_cast<int64_t>(x) * static_cast<int64_t>(y);
  if (static_cast<int32_t>(r) != r) {
    JniUtils::throwJavaRuntimeException(JniUtils::getJNIEnv(), "integer overflow");
  }
  return static_cast<int32_t>(r);
}

int32_t Unsafe::getUnsafeArrayIndexScale(JNIEnv* env, jclass unsafeClass,
                                         const std::string& name) {
  jfieldID arrayBooleanIndexScaleFieldId =
      env->GetStaticFieldID(unsafeClass, name.c_str(), "I");
  if (!arrayBooleanIndexScaleFieldId) {
    JniUtils::throwJavaRuntimeException(
        env, "Cannot find the field " + name + " in sun.misc.Unsafe");
  }
  return env->GetStaticIntField(unsafeClass, arrayBooleanIndexScaleFieldId);
}

NativeSqlTaskExecutionManager::NativeSqlTaskExecutionManager(jobject managerObj)
    : javaObj_(JniUtils::getJNIEnv()->NewGlobalRef(managerObj)) {}

jmethodID NativeSqlTaskExecutionManager::getMemberFunction(JNIEnv* env, const char* fname,
                                                           const char* signature) {
  jclass javaNativeSqlTaskExecutionManagerClass = env->GetObjectClass(javaObj_);
  VELOX_CHECK(javaNativeSqlTaskExecutionManagerClass);

  jmethodID methodId =
      env->GetMethodID(javaNativeSqlTaskExecutionManagerClass, fname, signature);
  VELOX_CHECK(methodId);

  return methodId;
}

void NativeSqlTaskExecutionManager::requestFetchNativeOutput(const TrinoTaskId& taskId,
                                                             int partitionId) {
  JNIEnv* env = JniUtils::getJNIEnv();
  VELOX_CHECK(env);

  jmethodID fetchOutputFromNativeFunc =
      getMemberFunction(env, "fetchOutputFromNative", "(Ljava/lang/String;I)V");
  env->CallVoidMethod(javaObj_, fetchOutputFromNativeFunc,
                      env->NewStringUTF(taskId.fullId().c_str()),
                      static_cast<jint>(partitionId));
}

void NativeSqlTaskExecutionManager::requestUpdateNativeTaskStatus(
    const TrinoTaskId& taskId) {
  JNIEnv* env = JniUtils::getJNIEnv();
  VELOX_CHECK(env);

  jmethodID updateNativeTaskStatusFunc =
      getMemberFunction(env, "updateNativeTaskStatus", "(Ljava/lang/String;)V");
  env->CallVoidMethod(javaObj_, updateNativeTaskStatusFunc,
                      env->NewStringUTF(taskId.fullId().c_str()));
}
}  // namespace io::trino::bridge
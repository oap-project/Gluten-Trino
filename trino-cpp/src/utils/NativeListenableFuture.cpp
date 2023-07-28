#include "NativeListenableFuture.h"

#include "utils/JniUtils.h"

#include <stdexcept>

namespace io::trino::bridge
{

NativeListenableFuture::NativeListenableFuture(folly::SemiFuture<folly::Unit> _future)
{
  future = std::move(_future)
               .via(executor().get())
               .then([ this ](const folly::Try<folly::Unit> & u) { this->onSuccess(); })
               .thenError(folly::tag_t<std::exception>{}, [ this ](const std::exception & e) { this->onError(e); });
}

void NativeListenableFuture::init()
{
  JNIEnv * jniEnv = JniUtils::getJNIEnv();
  if (jniEnv == nullptr)
  {
    throw std::runtime_error("Failed to obtain JNIEnv for current thread.");
  }

  jclass javaClass = jniEnv->FindClass("io/trino/jni/NativeListenableFuture");
  jmethodID constructor = jniEnv->GetMethodID(javaClass, "<init>", "(J)V");
  auto nativePointer = reinterpret_cast<jlong>(this);

  javaFuture = jniEnv->NewObject(javaClass, constructor, nativePointer);
}

void NativeListenableFuture::onSuccess()
{
  JNIEnv * jniEnv = JniUtils::getJNIEnv();
  if (jniEnv == nullptr)
  {
    throw std::runtime_error("Failed to obtain JNIEnv for current thread.");
  }

  jclass javaFutureClass = jniEnv->GetObjectClass(javaFuture);
  jmethodID setValueMethod = jniEnv->GetMethodID(javaFutureClass, "setValue", "()V");
  jniEnv->CallVoidMethod(javaFuture, setValueMethod);
}

void NativeListenableFuture::onError(const std::exception & e)
{
  JNIEnv * jniEnv = JniUtils::getJNIEnv();
  if (jniEnv == nullptr)
  {
    throw std::runtime_error("Failed to obtain JNIEnv for current thread.");
  }

  jclass javaFutureClass = jniEnv->GetObjectClass(javaFuture);
  jmethodID setExceptionMethod = jniEnv->GetMethodID(javaFutureClass, "setException", "(Ljava/lang/Throwable;)V");
  jobject runtimeException = JniUtils::createJavaRuntimeException(jniEnv, e.what());

  jniEnv->CallVoidMethod(javaFuture, setExceptionMethod, runtimeException);
}

bool NativeListenableFuture::cancel()
{
  bool expected = false;
  bool desired = true;
  if (isCanceled.compare_exchange_strong(expected, desired))
  {
    future.cancel();
    return true;
  }
  return false;
}

jobject NativeListenableFuture::getJavaFuture()
{
  return javaFuture;
}

}
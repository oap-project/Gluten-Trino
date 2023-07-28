#pragma once

#include <jni.h>

#include "folly/Unit.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "folly/futures/Future.h"

namespace io::trino::bridge
{

class NativeListenableFuture
{
 public:
  static std::shared_ptr<folly::CPUThreadPoolExecutor> & executor()
  {
    static auto executor
        = std::make_shared<folly::CPUThreadPoolExecutor>(1, std::make_shared<folly::NamedThreadFactory>("NativeListenableFuture"));
    return executor;
  }

  explicit NativeListenableFuture(folly::SemiFuture<folly::Unit> _future);

  void init();
  bool cancel();

  // todo: maybe need to return a global ref of java future.
  jobject getJavaFuture();

 private:
  void onSuccess();
  void onError(const std::exception & e);

  folly::SemiFuture<folly::Unit> future;
  std::atomic_bool isCanceled{false};

  jobject javaFuture;
};

}

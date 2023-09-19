#include "utils/ThreadUtils.h"

using namespace facebook;

namespace io::trino::bridge {

std::shared_ptr<folly::CPUThreadPoolExecutor> getDriverCPUExecutor(
    size_t threadNum, const std::string& name) {
  static auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(
      threadNum, std::make_shared<folly::NamedThreadFactory>(name));
  return executor;
}

std::shared_ptr<folly::IOThreadPoolExecutor> getSpillExecutor(size_t threadNum,
                                                              const std::string& name) {
  static auto executor = std::make_shared<folly::IOThreadPoolExecutor>(
      threadNum, std::make_shared<folly::NamedThreadFactory>(name));
  return executor;
}

std::shared_ptr<folly::IOThreadPoolExecutor> getExchangeIOCPUExecutor(
    size_t threadNum, const std::string& name) {
  static auto executor = std::make_shared<folly::IOThreadPoolExecutor>(
      threadNum, std::make_shared<folly::NamedThreadFactory>(name));
  return executor;
}

std::shared_ptr<folly::IOThreadPoolExecutor> getConnectorIOExecutor(
    size_t threadNum, const std::string& name) {
  static auto executor = std::make_shared<folly::IOThreadPoolExecutor>(
      threadNum, std::make_shared<folly::NamedThreadFactory>(name));
  return executor;
}

}  // namespace io::trino::bridge
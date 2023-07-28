#pragma once

#include <folly/Uri.h>

#include "utils/Configs.h"
#include "utils/HttpClient.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/Exchange.h"

namespace io::trino::bridge {
class TrinoExchangeSource : public facebook::velox::exec::ExchangeSource {
 public:
  TrinoExchangeSource(const folly::Uri& baseUri, int destination,
                      std::shared_ptr<facebook::velox::exec::ExchangeQueue> queue,
                      facebook::velox::memory::MemoryPool* pool,
                      const std::string& clientCertAndKeyPath_ = "",
                      const std::string& ciphers_ = "");

  bool shouldRequestLocked() override;

  static std::unique_ptr<ExchangeSource> createExchangeSource(
      const std::string& url, int destination,
      std::shared_ptr<facebook::velox::exec::ExchangeQueue> queue,
      facebook::velox::memory::MemoryPool* pool);

  void close() override;

  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {{"TrinoExchangeSource.numPages", numPages_}};
  }

  int testingFailedAttempts() const { return failedAttempts_; }

  /// Invoked to track the node-wise memory usage queued in
  /// TrinoExchangeSource. If 'updateBytes' > 0, then increment the usage,
  /// otherwise decrement the usage.
  static void updateMemoryUsage(int64_t updateBytes);

  /// Invoked to get the node-wise queued memory usage from
  /// TrinoExchangeSource.
  static void getMemoryUsage(int64_t& currentBytes, int64_t& peakBytes);

  /// Invoked to reset the node-wise peak memory usage back to the current
  /// memory usage in TrinoExchangeSource. Instead of getting all time peak,
  /// this can be useful when tracking the peak within some fixed time
  /// intervals.
  static void resetPeakMemoryUsage();

  /// Used by test to clear the node-wise memory usage tracking.
  static void testingClearMemoryUsage();

 private:
  void request() override;

  void doRequest();

  void processDataResponse(std::unique_ptr<http::HttpResponse> response);

  // If 'retry' is true, then retry the http request failure until reaches the
  // retry limit, otherwise just set exchange source error without retry. As
  // for now, we don't retry on the request failure which is caused by the
  // memory allocation failure for the http response data.
  void processDataError(const std::string& path, const std::string& error,
                        bool retry = true);

  void acknowledgeResults(int64_t ackSequence);

  void abortResults();

  // Returns a shared ptr owning the current object.
  std::shared_ptr<TrinoExchangeSource> getSelfPtr();

  // Tracks the currently node-wide queued memory usage in bytes.
  static std::atomic<int64_t>& currQueuedMemoryBytes() {
    static std::atomic<int64_t> currQueuedMemoryBytes{0};
    return currQueuedMemoryBytes;
  }

  // Records the node-wide peak queued memory usage in bytes.
  // Tracks the currently node-wide queued memory usage in bytes.
  static std::atomic<int64_t>& peakQueuedMemoryBytes() {
    static std::atomic<int64_t> peakQueuedMemoryBytes{0};
    return peakQueuedMemoryBytes;
  }

  const std::string basePath_;
  const std::string host_;
  const uint16_t port_;
  const std::string clientCertAndKeyPath_;
  const std::string ciphers_;

  std::unique_ptr<http::HttpClient> httpClient_;
  int failedAttempts_;
  // The number of pages received from this presto exchange source.
  uint64_t numPages_{0};
  std::atomic_bool closed_{false};
  // A boolean indicating whether abortResults() call was issued and was
  // successfully processed by the remote server.
  std::atomic_bool abortResultsSucceeded_{false};

  folly::CPUThreadPoolExecutor* driverThreadPool_{nullptr};
};
}  // namespace io::trino::bridge

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
#include "src/TrinoExchangeSource.h"

#include <fmt/core.h>
#include <folly/SocketAddress.h>
#include <re2/re2.h>
#include <sstream>

#include "src/NativeConfigs.h"
#include "src/protocol/trino_protocol.h"
#include "src/utils/Counters.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/Operator.h"

using namespace facebook::velox;

namespace io::trino::bridge {
namespace {

std::string extractTaskId(const std::string& path) {
  static const RE2 kPattern("/v1/task/([^/]*)/.*");
  std::string taskId;
  if (RE2::PartialMatch(path, kPattern, &taskId)) {
    return taskId;
  }

  VLOG(1) << "Failed to extract task ID from remote split: " << path;

  throw std::invalid_argument(
      fmt::format("Cannot extract task ID from remote split URL: {}", path));
}

void onFinalFailure(const std::string& errorMessage,
                    std::shared_ptr<exec::ExchangeQueue> queue) {
  queue->setError(errorMessage);
}

std::string bodyAsString(http::HttpResponse& response, memory::MemoryPool* pool) {
  if (response.hasError()) {
    return response.error();
  }
  std::ostringstream oss;
  auto iobufs = response.consumeBody();
  for (auto& body : iobufs) {
    oss << std::string((const char*)body->data(), body->length());
    if (pool != nullptr) {
      pool->free(body->writableData(), body->capacity());
    }
  }
  return oss.str();
}
}  // namespace

TrinoExchangeSource::TrinoExchangeSource(const folly::Uri& baseUri, int destination,
                                         std::shared_ptr<exec::ExchangeQueue> queue,
                                         memory::MemoryPool* pool,
                                         folly::CPUThreadPoolExecutor* driverExecutor,
                                         folly::IOThreadPoolExecutor* httpExecutor,
                                         const std::string& clientCertAndKeyPath,
                                         const std::string& ciphers)
    : ExchangeSource(extractTaskId(baseUri.path()), destination, queue, pool),
      basePath_(baseUri.path()),
      host_(baseUri.host()),
      port_(baseUri.port()),
      clientCertAndKeyPath_(clientCertAndKeyPath),
      ciphers_(ciphers),
      immediateBufferTransfer_(
          NativeConfigs::instance().getExchangeImmediateBufferTransfer()),
      driverExecutor_(getDriverCPUExecutor().get()),
      httpExecutor_(httpExecutor) {
  folly::SocketAddress address;
  if (folly::IPAddress::validate(host_)) {
    address = folly::SocketAddress(folly::IPAddress(host_), port_);
  } else {
    address = folly::SocketAddress(host_, port_, true);
  }
  auto timeoutMs = std::chrono::duration_cast<std::chrono::milliseconds>(
      NativeConfigs::instance().getExchangeRequestTimeout());
  VELOX_CHECK_NOT_NULL(driverExecutor_);
  VELOX_CHECK_NOT_NULL(httpExecutor_);
  VELOX_CHECK_NOT_NULL(pool_);
  auto* ioEventBase = httpExecutor_->getEventBase();
  httpClient_ = std::make_shared<http::HttpClient>(
      ioEventBase, address, timeoutMs, immediateBufferTransfer_ ? pool_ : nullptr,
      clientCertAndKeyPath_, ciphers_, [](size_t bufferBytes) {
        REPORT_ADD_STAT_VALUE(kCounterHttpClientPrestoExchangeNumOnBody);
        REPORT_ADD_HISTOGRAM_VALUE(kCounterHttpClientPrestoExchangeOnBodyBytes,
                                   bufferBytes);
      });
}

bool TrinoExchangeSource::shouldRequestLocked() {
  if (atEnd_) {
    return false;
  }

  if (!requestPending_) {
    VELOX_CHECK(!promise_.valid() || promise_.isFulfilled());
    requestPending_ = true;
    return true;
  }

  // We are still processing previous request.
  return false;
}

folly::SemiFuture<TrinoExchangeSource::Response> TrinoExchangeSource::request(
    uint32_t maxBytes, uint32_t maxWaitSeconds) {
  // Before calling 'request', the caller should have called
  // 'shouldRequestLocked' and received 'true' response. Hence, we expect
  // requestPending_ == true, atEnd_ == false.
  // This call cannot be made concurrently from multiple threads.
  VELOX_CHECK(requestPending_);
  VELOX_CHECK(!promise_.valid() || promise_.isFulfilled());

  auto promise = VeloxPromise<Response>("PrestoExchangeSource::request");
  auto future = promise.getSemiFuture();

  promise_ = std::move(promise);
  failedAttempts_ = 0;
  dataRequestRetryState_ =
      RetryState(std::chrono::duration_cast<std::chrono::milliseconds>(
                     NativeConfigs::instance().getExchangeMaxErrorDuration())
                     .count());
  doRequest(dataRequestRetryState_.nextDelayMs(), maxBytes, maxWaitSeconds);

  return future;
}

void TrinoExchangeSource::doRequest(int64_t delayMs, uint32_t maxBytes,
                                    uint32_t maxWaitSeconds) {
  if (closed_.load()) {
    queue_->setError("TrinoExchangeSource closed");
    return;
  }
  auto path = fmt::format("{}/{}", basePath_, sequence_);
  VLOG(1) << "Fetching data from " << host_ << ":" << port_ << " " << path;
  auto self = getSelfPtr();
  ++requestTimes_;
  uint64_t startMicro = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::high_resolution_clock::now().time_since_epoch())
                            .count();
  http::RequestBuilder()
      .method(proxygen::HTTPMethod::GET)
      .url(path)
      // .header(protocol::TRINO_INTERNAL_BEARER_HEADER, "32MB")
      .header(protocol::TRINO_MAX_SIZE_HTTP_HEADER, "32MB")
      .send(httpClient_.get(), "", delayMs)
      .via(driverExecutor_)
      .thenValue([path, maxBytes, maxWaitSeconds, self,
                  startMicro](std::unique_ptr<http::HttpResponse> response) {
        facebook::velox::common::testutil::TestValue::adjust(
            "io::trino::bridge::TrinoExchangeSource::doRequest", self.get());
        auto* headers = response->headers();
        if (headers->getStatusCode() != http::kHttpOk &&
            headers->getStatusCode() != http::kHttpNoContent) {
          // Ideally, not all errors are retryable - especially internal server
          // errors - which usually point to a query failure on another machine.
          // But we retry all such errors and rely on the coordinator to
          // cancel other tasks, when some tasks have failed.
          self->processDataError(
              path, maxBytes, maxWaitSeconds,
              fmt::format("Received HTTP {} {} {}", headers->getStatusCode(),
                          headers->getStatusMessage(),
                          bodyAsString(*response, self->immediateBufferTransfer_
                                                      ? self->pool_.get()
                                                      : nullptr)));
        } else if (response->hasError()) {
          self->processDataError(path, maxBytes, maxWaitSeconds, response->error());
        } else {
          self->processDataResponse(std::move(response), startMicro);
        }
      })
      .thenError(folly::tag_t<std::exception>{},
                 [path, maxBytes, maxWaitSeconds, self, this](const std::exception& e) {
                   ++errorResponseTimes_;
                   self->processDataError(path, maxBytes, maxWaitSeconds, e.what());
                 });
}

void TrinoExchangeSource::processDataResponse(
    std::unique_ptr<http::HttpResponse> response, uint64_t startMicroSeconds) {
  if (closed_.load()) {
    // If TrinoExchangeSource is already closed, just free all buffers
    // allocated without doing any processing. This can happen when a super slow
    // response comes back after its owning 'Task' gets destroyed.
    try {
      response->freeBuffers();
      return;
    } catch (const std::exception& e) {
      VLOG(1) << "Free buffer error" << e.what();
    }
  }
  auto* headers = response->headers();
  VELOX_CHECK(!headers->getIsChunked(),
              "Chunked http transferring encoding is not supported.")
  uint64_t contentLength =
      atol(headers->getHeaders()
               .getSingleOrEmpty(proxygen::HTTP_HEADER_CONTENT_LENGTH)
               .c_str());
  VLOG(1) << "Fetched data for " << basePath_ << "/" << sequence_ << ": " << contentLength
          << " bytes";

  auto complete = headers->getHeaders()
                      .getSingleOrEmpty(protocol::TRINO_BUFFER_COMPLETE_HEADER)
                      .compare("true") == 0;
  if (complete) {
    VLOG(1) << "Received buffer-complete header for " << basePath_ << "/" << sequence_;
  }

  int64_t ackSequence = atol(headers->getHeaders()
                                 .getSingleOrEmpty(protocol::TRINO_PAGE_NEXT_TOKEN_HEADER)
                                 .c_str());

  std::unique_ptr<exec::SerializedPage> page;
  const bool empty = response->empty();
  if (!empty) {
    uint64_t now = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::high_resolution_clock::now().time_since_epoch())
                       .count();
    nonEmptyRequestMicroRTTs_ += now - startMicroSeconds;

    std::vector<std::unique_ptr<folly::IOBuf>> iobufs;
    // trino have a 16 bytes header, including 4 bytes magic, 8 bytes checksum, 4 bytes
    // page size
    // Refer to io.trino.operator.HttpPageBufferClient.PageResponseHandler#handle
    // We need skip only once.
    if (immediateBufferTransfer_) {
      iobufs = response->consumeBody();
    } else {
      iobufs.emplace_back(response->consumeBody(pool_.get()));
    }
    int64_t totalBytes{0};
    std::unique_ptr<folly::IOBuf> singleChain;

    for (auto& buf : iobufs) {
      totalBytes += buf->capacity();
      if (!singleChain) {
        buf->trimStart(16);
        singleChain = std::move(buf);
      } else {
        singleChain->prev()->appendChain(std::move(buf));
      }
    }
    TrinoExchangeSource::updateMemoryUsage(totalBytes);

    page = std::make_unique<exec::SerializedPage>(
        std::move(singleChain), [pool = pool_](folly::IOBuf& iobuf) {
          int64_t freedBytes{0};
          // Free the backed memory from MemoryAllocator on page dtor
          folly::IOBuf* start = &iobuf;
          auto curr = start;
          do {
            freedBytes += curr->capacity();
            // FIXME: Jiguang
            // pool->free(curr->writableData(), curr->capacity());
            curr = curr->next();
          } while (curr != start);
          TrinoExchangeSource::updateMemoryUsage(-freedBytes);
        });
  } else {
    ++emptyResponseTimes_;
    VLOG(1) << "Received empty response for " << basePath_ << "/" << sequence_;
  }

  const int64_t pageSize = empty ? 0 : page->size();

  REPORT_ADD_HISTOGRAM_VALUE(kCounterPrestoExchangeSerializedPageSize, pageSize);

  {
    VeloxPromise<Response> requestPromise;
    std::vector<ContinuePromise> queuePromises;
    {
      std::lock_guard<std::mutex> l(queue_->mutex());
      if (page) {
        VLOG(1) << "Enqueuing page for " << basePath_ << "/" << sequence_ << ": "
                << pageSize << " bytes";
        ++numPages_;
        totalBytes_ += pageSize;
        queue_->enqueueLocked(std::move(page), queuePromises);
      }
      if (complete) {
        VLOG(1) << "Enqueuing empty page for " << basePath_ << "/" << sequence_;
        atEnd_ = true;
        queue_->enqueueLocked(nullptr, queuePromises);
      }

      sequence_ = ackSequence;
      requestPending_ = false;
      requestPromise = std::move(promise_);
    }
    for (auto& promise : queuePromises) {
      promise.setValue();
    }

    if (requestPromise.valid() && !requestPromise.isFulfilled()) {
      requestPromise.setValue(Response{pageSize, complete});
    } else {
      // The source must have been closed.
      VELOX_CHECK(closed_.load());
    }
  }

  if (complete) {
    abortResults();
  } else if (!empty) {
    // Acknowledge results for non-empty content.
    acknowledgeResults(ackSequence);
  }
}

void TrinoExchangeSource::processDataError(const std::string& path, uint32_t maxBytes,
                                           uint32_t maxWaitSeconds,
                                           const std::string& error, bool retry) {
  ++failedAttempts_;
  if (retry && !dataRequestRetryState_.isExhausted()) {
    VLOG(1) << "Failed to fetch data from " << host_ << ":" << port_ << " " << path
            << " - Retrying: " << error;

    doRequest(dataRequestRetryState_.nextDelayMs(), maxBytes, maxWaitSeconds);
    return;
  }

  onFinalFailure(
      fmt::format("Failed to fetch data from {}:{} {} - Exhausted after {} retries: {}",
                  host_, port_, path, failedAttempts_, error),
      queue_);

  if (!checkSetRequestPromise()) {
    // The source must have been closed.
    VELOX_CHECK(closed_.load());
  }
}

bool TrinoExchangeSource::checkSetRequestPromise() {
  VeloxPromise<Response> promise;
  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    promise = std::move(promise_);
  }
  if (promise.valid() && !promise.isFulfilled()) {
    promise.setValue(Response{0, false});
    return true;
  }

  return false;
}

void TrinoExchangeSource::acknowledgeResults(int64_t ackSequence) {
  auto ackPath = fmt::format("{}/{}/acknowledge", basePath_, ackSequence);
  VLOG(1) << "Sending ack " << ackPath;
  auto self = getSelfPtr();

  http::RequestBuilder()
      .method(proxygen::HTTPMethod::GET)
      .url(ackPath)
      .send(httpClient_.get())
      .via(driverExecutor_)
      .thenValue([self](std::unique_ptr<http::HttpResponse> response) {
        auto* headers = response->headers();
        uint64_t statusCode = headers->getStatusCode();
        if (statusCode != http::kHttpOk && statusCode != http::kHttpNoContent) {
          LOG(WARNING) << "AckPath: " << headers->getURL() << "code: " << statusCode
                       << ", body:\n"
                       << response->dumpBodyChain();
        }
      })
      .thenError(folly::tag_t<std::exception>{}, [self](const std::exception& e) {
        // Acks are optional. No need to fail the query.
        VLOG(1) << "Ack failed: " << e.what();
      });
}

void TrinoExchangeSource::abortResults() {
  if (abortResultsIssued_.exchange(true)) {
    return;
  }

  abortRetryState_ =
      RetryState(std::chrono::duration_cast<std::chrono::milliseconds>(
                     NativeConfigs::instance().getExchangeMaxErrorDuration())
                     .count());
  VLOG(1) << "Sending abort results " << basePath_;
  doAbortResults(abortRetryState_.nextDelayMs());
}

void TrinoExchangeSource::doAbortResults(int64_t delayMs) {
  auto queue = queue_;
  auto self = getSelfPtr();
  http::RequestBuilder()
      .method(proxygen::HTTPMethod::DELETE)
      .url(basePath_)
      .send(httpClient_.get(), "", delayMs)
      .via(driverExecutor_)
      .thenTry([queue, self](folly::Try<std::unique_ptr<http::HttpResponse>> response) {
        std::optional<std::string> error;
        if (response.hasException()) {
          error = response.exception().what();
        } else {
          auto statusCode = response.value()->headers()->getStatusCode();
          if (statusCode != http::kHttpOk && statusCode != http::kHttpNoContent) {
            error = std::to_string(statusCode);
          }
        }
        if (!error.has_value()) {
          return;
        }
        if (self->abortRetryState_.isExhausted()) {
          const std::string errMsg = fmt::format("Abort results failed: {}, path {}",
                                                 error.value(), self->basePath_);
          LOG(ERROR) << errMsg;
          return onFinalFailure(errMsg, queue);
        }
        self->doAbortResults(self->abortRetryState_.nextDelayMs());
      });
}

void TrinoExchangeSource::close() {
  closed_.store(true);
  checkSetRequestPromise();
  abortResults();
}

std::shared_ptr<TrinoExchangeSource> TrinoExchangeSource::getSelfPtr() {
  return std::dynamic_pointer_cast<TrinoExchangeSource>(shared_from_this());
}

// static
std::unique_ptr<exec::ExchangeSource> TrinoExchangeSource::create(
    const std::string& url, int destination, std::shared_ptr<exec::ExchangeQueue> queue,
    memory::MemoryPool* pool, folly::CPUThreadPoolExecutor* driverExecutor,
    folly::IOThreadPoolExecutor* httpExecutor) {
  if (strncmp(url.c_str(), "http://", 7) == 0) {
    return std::make_unique<TrinoExchangeSource>(folly::Uri(url), destination, queue,
                                                 pool, driverExecutor, httpExecutor);
  } else if (strncmp(url.c_str(), "https://", 8) == 0) {
    const auto clientCertAndKeyPath =
        NativeConfigs::instance().getHttpsClientCertAndKeyPath();
    const auto ciphers = NativeConfigs::instance().getHttpsSupportedCiphers();
    return std::make_unique<TrinoExchangeSource>(folly::Uri(url), destination, queue,
                                                 pool, driverExecutor, httpExecutor,
                                                 clientCertAndKeyPath, ciphers);
  }
  return nullptr;
}

void TrinoExchangeSource::updateMemoryUsage(int64_t updateBytes) {
  const int64_t newMemoryBytes =
      currQueuedMemoryBytes().fetch_add(updateBytes) + updateBytes;
  if (updateBytes > 0) {
    peakQueuedMemoryBytes() = std::max<int64_t>(peakQueuedMemoryBytes(), newMemoryBytes);
  } else {
    VELOX_CHECK_GE(currQueuedMemoryBytes(), 0);
  }
}

void TrinoExchangeSource::getMemoryUsage(int64_t& currentBytes, int64_t& peakBytes) {
  currentBytes = currQueuedMemoryBytes();
  peakBytes = std::max<int64_t>(currentBytes, peakQueuedMemoryBytes());
}

void TrinoExchangeSource::resetPeakMemoryUsage() {
  peakQueuedMemoryBytes() = currQueuedMemoryBytes().load();
}

void TrinoExchangeSource::testingClearMemoryUsage() {
  currQueuedMemoryBytes() = 0;
  peakQueuedMemoryBytes() = 0;
}
}  // namespace io::trino::bridge

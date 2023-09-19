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
#include "TrinoExchangeSource.h"

#include <fmt/core.h>
#include <re2/re2.h>
#include <sstream>

#include "NativeConfigs.h"
#include "folly/SocketAddress.h"
#include "protocol/trino_protocol.h"
#include "utils/Counters.h"
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
}  // namespace

TrinoExchangeSource::TrinoExchangeSource(const folly::Uri& baseUri, int destination,
                                         std::shared_ptr<exec::ExchangeQueue> queue,
                                         memory::MemoryPool* pool,
                                         const std::string& clientCertAndKeyPath,
                                         const std::string& ciphers)
    : ExchangeSource(extractTaskId(baseUri.path()), destination, queue, pool),
      basePath_(baseUri.path()),
      host_(baseUri.host()),
      port_(baseUri.port()),
      clientCertAndKeyPath_(clientCertAndKeyPath),
      ciphers_(ciphers),
      driverThreadPool_(getDriverCPUExecutor().get()) {
  folly::SocketAddress address(folly::IPAddress(host_).str(), port_, true);
  auto* eventBase = getExchangeIOCPUExecutor()->getEventBase();
  httpClient_ = std::make_unique<http::HttpClient>(
      eventBase, address, std::chrono::milliseconds(10'000), clientCertAndKeyPath_,
      ciphers_, [](size_t bufferBytes) {
        REPORT_ADD_STAT_VALUE(kCounterHttpClientPrestoExchangeNumOnBody);
        REPORT_ADD_HISTOGRAM_VALUE(kCounterHttpClientPrestoExchangeOnBodyBytes,
                                   bufferBytes);
      });
}

bool TrinoExchangeSource::shouldRequestLocked() {
  if (atEnd_) {
    return false;
  }
  bool pending = requestPending_;
  requestPending_ = true;
  return !pending;
}

void TrinoExchangeSource::request() {
  failedAttempts_ = 0;
  doRequest();
}

void TrinoExchangeSource::doRequest() {
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
      .send(httpClient_.get(), pool_.get())
      .via(driverThreadPool_)
      .thenValue([path, self, startMicro](std::unique_ptr<http::HttpResponse> response) {
        facebook::velox::common::testutil::TestValue::adjust(
            "io::trino::bridge::TrinoExchangeSource::doRequest", self.get());
        auto* headers = response->headers();
        if (headers->getStatusCode() != http::kHttpOk &&
            headers->getStatusCode() != http::kHttpNoContent) {
          self->processDataError(
              path, fmt::format("Received HTTP {} {}", headers->getStatusCode(),
                                headers->getStatusMessage()));
        } else if (response->hasError()) {
          self->processDataError(path, response->error(), false);
        } else {
          self->processDataResponse(std::move(response), startMicro);
        }
      })
      .thenError(folly::tag_t<std::exception>{},
                 [path, self, this](const std::exception& e) {
                   ++errorResponseTimes_;
                   self->processDataError(path, e.what());
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
  std::unique_ptr<folly::IOBuf> singleChain;
  const bool empty = response->empty();
  int64_t totalBytes{0};
  if (!empty) {
    uint64_t now = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::high_resolution_clock::now().time_since_epoch())
                       .count();
    nonEmptyRequestMicroRTTs_ += now - startMicroSeconds;

    auto iobufs = response->consumeBody();
    // trino have a 16 bytes header, including 4 bytes magic, 8 bytes checksum, 4 bytes
    // page size
    // Refer to io.trino.operator.HttpPageBufferClient.PageResponseHandler#handle
    // We need skip only once.
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

  REPORT_ADD_HISTOGRAM_VALUE(kCounterPrestoExchangeSerializedPageSize,
                             page ? page->size() : 0);

  {
    std::vector<ContinuePromise> promises;
    {
      std::lock_guard<std::mutex> l(queue_->mutex());
      if (page) {
        VLOG(1) << "Enqueuing page for " << basePath_ << "/" << sequence_ << ": "
                << page->size() << " bytes";
        queue_->enqueueLocked(std::move(page), promises);
      }
      if (complete) {
        VLOG(1) << "Enqueuing empty page for " << basePath_ << "/" << sequence_;
        atEnd_ = true;
        queue_->enqueueLocked(nullptr, promises);
      }

      sequence_ = ackSequence;

      // Reset requestPending_ if the response is complete or have pages.
      if (complete || !empty) {
        requestPending_ = false;
      }
    }
    for (auto& promise : promises) {
      promise.setValue();
    }
  }

  if (complete) {
    abortResults();
  } else {
    if (!empty) {
      // Acknowledge results for non-empty content.
      acknowledgeResults(ackSequence);
    } else {
      // Rerequest results for incomplete results with no pages.
      request();
    }
  }
}

void TrinoExchangeSource::processDataError(const std::string& path,
                                           const std::string& error, bool retry) {
  ++failedAttempts_;
  if (retry && failedAttempts_ < 3) {
    VLOG(1) << "Failed to fetch data from " << host_ << ":" << port_ << " " << path
            << " - Retrying: " << error;

    doRequest();
    return;
  }

  onFinalFailure(
      fmt::format("Failed to fetched data from {}:{} {} - Exhausted retries: {}", host_,
                  port_, path, error),
      queue_);
}

void TrinoExchangeSource::acknowledgeResults(int64_t ackSequence) {
  auto ackPath = fmt::format("{}/{}/acknowledge", basePath_, ackSequence);
  VLOG(1) << "Sending ack " << ackPath;
  auto self = getSelfPtr();

  http::RequestBuilder()
      .method(proxygen::HTTPMethod::GET)
      .url(ackPath)
      .send(httpClient_.get(), pool_.get())
      .via(driverThreadPool_)
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
  VLOG(1) << "Sending abort results " << basePath_;
  auto queue = queue_;
  auto self = getSelfPtr();
  http::RequestBuilder()
      .method(proxygen::HTTPMethod::DELETE)
      .url(basePath_)
      .send(httpClient_.get(), pool_.get())
      .via(driverThreadPool_)
      .thenValue([queue, self](std::unique_ptr<http::HttpResponse> response) {
        auto statusCode = response->headers()->getStatusCode();
        if (statusCode != http::kHttpOk && statusCode != http::kHttpNoContent) {
          const std::string errMsg = fmt::format("Abort results failed: {}, path {}",
                                                 statusCode, self->basePath_);
          LOG(ERROR) << errMsg;
          onFinalFailure(errMsg, queue);
        } else {
          self->abortResultsSucceeded_.store(true);
        }
      })
      .thenError(folly::tag_t<std::exception>{}, [queue, self](const std::exception& e) {
        const std::string errMsg =
            fmt::format("Abort results failed: {}, path {}", e.what(), self->basePath_);
        LOG(ERROR) << errMsg;
        // Captures 'queue' by value to ensure lifetime. Error
        // detection can be arbitrarily late, for example after cancellation
        // due to other errors.
        onFinalFailure(errMsg, queue);
      });
}

void TrinoExchangeSource::close() {
  closed_.store(true);
  if (!abortResultsSucceeded_.load()) {
    abortResults();
  }
}

std::shared_ptr<TrinoExchangeSource> TrinoExchangeSource::getSelfPtr() {
  return std::dynamic_pointer_cast<TrinoExchangeSource>(shared_from_this());
}

// static
std::unique_ptr<exec::ExchangeSource> TrinoExchangeSource::createExchangeSource(
    const std::string& url, int destination, std::shared_ptr<exec::ExchangeQueue> queue,
    memory::MemoryPool* pool) {
  if (strncmp(url.c_str(), "http://", 7) == 0) {
    return std::make_unique<TrinoExchangeSource>(folly::Uri(url), destination, queue,
                                                 pool);
  } else if (strncmp(url.c_str(), "https://", 8) == 0) {
    const auto clientCertAndKeyPath =
        NativeConfigs::instance().getHttpsClientCertAndKeyPath();
    const auto ciphers = NativeConfigs::instance().getHttpsSupportedCiphers();
    return std::make_unique<TrinoExchangeSource>(folly::Uri(url), destination, queue,
                                                 pool, clientCertAndKeyPath, ciphers);
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

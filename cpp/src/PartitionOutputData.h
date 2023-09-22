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

#include <memory>
#include <mutex>
#include <vector>
#include "folly/io/IOBuf.h"

namespace io::trino::bridge {

class PartitionOutputData {
 public:
  explicit PartitionOutputData();

  size_t getSequence() const { return sequence_; }

  bool getListenerRegistered() const { return listenerRegistered_; }

  void registerListener() { listenerRegistered_ = true; }

  void consumeListener() { listenerRegistered_ = false; }

  bool noMoreData() const { return noMore_; }

  size_t getOutputDataNum() const { return noMore_ ? data_.size() - 1 : data_.size(); }

  size_t getDataSize(size_t index) const;

  std::vector<std::unique_ptr<folly::IOBuf>> popWithLock(size_t num);

  std::vector<std::unique_ptr<folly::IOBuf>> pop(size_t num);

  void enqueueWithLock(size_t seq, std::vector<std::unique_ptr<folly::IOBuf>>& data);

  void enqueue(size_t seq, std::vector<std::unique_ptr<folly::IOBuf>>& data);

  template <typename F, typename... Args>
  std::invoke_result_t<F, Args&&...> withLock(F&& func, Args&&... args) const {
    std::lock_guard<std::recursive_mutex> lg(lock_);
    return func(args...);
  }

 private:
  mutable std::recursive_mutex lock_;
  size_t sequence_;
  std::vector<std::unique_ptr<folly::IOBuf>> data_;
  bool noMore_;
  bool listenerRegistered_;
};

}  // namespace io::trino::bridge

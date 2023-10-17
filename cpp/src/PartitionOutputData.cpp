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

#include "src/PartitionOutputData.h"

#include "velox/common/base/Exceptions.h"

namespace io::trino::bridge {

PartitionOutputData::PartitionOutputData()
    : sequence_(0), noMore_(false), listenerRegistered_(false) {}

size_t PartitionOutputData::getDataSize(size_t index) const {
  VELOX_CHECK_LT(index, data_.size());
  return data_[index]->computeChainDataLength();
}

std::vector<std::unique_ptr<folly::IOBuf>> PartitionOutputData::pop(size_t num) {
  num = std::min(num, getOutputDataNum());
  std::vector<std::unique_ptr<folly::IOBuf>> out(num);

  for (size_t i = 0; i < num; ++i) {
    out[i] = std::move(data_[i]);
  }

  data_.erase(data_.begin(), data_.begin() + num);

  return out;
}

std::vector<std::unique_ptr<folly::IOBuf>> PartitionOutputData::popWithLock(size_t num) {
  return withLock([this](size_t num) { return pop(num); }, num);
}

void PartitionOutputData::enqueueWithLock(
    size_t seq, std::vector<std::unique_ptr<folly::IOBuf>>& data) {
  withLock([this, &seq, &data]() mutable { enqueue(seq, data); });
}

void PartitionOutputData::enqueue(size_t seq,
                                  std::vector<std::unique_ptr<folly::IOBuf>>& data) {
  size_t current_seq = getSequence();
  VELOX_CHECK_LE(seq, current_seq, "Unsupported skipping output data.");
  size_t seq_diff = current_seq - seq;
  if (seq_diff >= data.size()) {
    return;
  }
  // Note: PartitionOutput operator will enqueue the serialized pages into the
  // corresponding PartitionOutputBuffer and DestinationBuffer, when the noMoreSplit is
  // set and all drivers are finished, noMoreData() will be called and enqueue a nullptr
  // to all page sequences.
  for (; seq_diff < data.size(); ++seq_diff) {
    auto ptr = data[seq_diff].release();
    data_.emplace_back(ptr);
    ++sequence_;
    if (!ptr) {
      noMore_ = true;
    }
  }
}

}  // namespace io::trino::bridge
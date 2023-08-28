#include "TrinoByteStream.h"

#include "velox/common/base/BitUtil.h"

using namespace facebook::velox;

namespace io::trino::bridge {
void TrinoNullByteStream::appendBool(bool value, int32_t count) {
  if (count == 1 && current_->size > current_->position) {
    bits::setBit(reinterpret_cast<uint64_t*>(current_->buffer), current_->position,
                 value);
    ++current_->position;
    return;
  }
  int32_t offset = 0;
  VELOX_DCHECK(isBits_);
  for (;;) {
    int32_t bitsFit = std::min(count - offset, current_->size - current_->position);
    bits::fillBits(reinterpret_cast<uint64_t*>(current_->buffer), current_->position,
                   current_->position + bitsFit, value);
    current_->position += bitsFit;
    offset += bitsFit;
    if (offset == count) {
      return;
    }
    extend(bits::nbytes(count - offset));
  }
}

void TrinoNullByteStream::flush(facebook::velox::OutputStream* out) {
  updateEnd();
  for (int32_t i = 0; i < ranges_.size(); ++i) {
    int32_t count = i == ranges_.size() - 1 ? lastRangeEnd_ : ranges_[i].size;
    int32_t bytes = isBits_ ? bits::nbytes(count) : count;
    if (isBits_ && isReverseBitOrder_ && !isReversed_) {
      bits::reverseBits(ranges_[i].buffer, bytes);
    }

    out->write(reinterpret_cast<char*>(ranges_[i].buffer), bytes);

  }
  if (isBits_ && isReverseBitOrder_) {
    isReversed_ = true;
  }
}

void TrinoNullByteStream::extend(int32_t bytes) {
  if (current_ && current_->position != current_->size) {
    LOG(FATAL) << "Extend ByteStream before range full: " << current_->position << " vs. "
               << current_->size;
  }

  // Check if rewriting existing content. If so, move to next range and start at
  // 0.
  if (current_ && current_ != &ranges_.back()) {
    ++current_;
    current_->position = 0;
    return;
  }
  ranges_.emplace_back();
  current_ = &ranges_.back();
  lastRangeEnd_ = 0;
  arena_->newRange(bytes, current_);
  std::memset(current_->buffer, 0, current_->size);
  if (isBits_) {
    // size and position are in units of bits for a bits stream.
    current_->size *= 8;
  }
}

}  // namespace io::trino::bridge
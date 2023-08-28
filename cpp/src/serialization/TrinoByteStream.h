#include "velox/common/memory/ByteStream.h"

namespace io::trino::bridge {
// class TrinoNullByteStream : public facebook::velox::memory::ByteStream {
class TrinoNullByteStream {
 public:
  // For output.
  TrinoNullByteStream(facebook::velox::StreamArena* arena, bool isBits = false,
                      bool isReverseBitOrder = false)
      : arena_(arena), isBits_(isBits), isReverseBitOrder_(isReverseBitOrder) {}
  void appendBool(bool value, int32_t count);
  void startWrite(int32_t initialSize) { extend(initialSize); }
  void flush(facebook::velox::OutputStream* stream);

 private:
  void extend(int32_t bytes = facebook::velox::memory::AllocationTraits::kPageSize);

  void updateEnd() {
    if (!ranges_.empty() && current_ == &ranges_.back() &&
        current_->position > lastRangeEnd_) {
      lastRangeEnd_ = current_->position;
    }
  }

  facebook::velox::StreamArena* arena_;
  // Indicates that position in ranges_ is in bits, not bytes.
  const bool isBits_;
  const bool isReverseBitOrder_;

  // True if the bit order in ranges_ has been inverted. Presto requires reverse
  // bit order.
  bool isReversed_ = false;
  std::vector<facebook::velox::ByteRange> ranges_;
  // Pointer to the current element of 'ranges_'.
  facebook::velox::ByteRange* current_ = nullptr;
  int32_t lastRangeEnd_{0};
};
}  // namespace io::trino::bridge
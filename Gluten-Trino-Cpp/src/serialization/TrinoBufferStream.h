#pragma once

#include "velox/common/memory/ByteStream.h"
#include "velox/common/memory/StreamArena.h"
#include "velox/type/Type.h"

// #include <folly/io/IOBuf.h>
#include "TrinoBuffer.h"

namespace io::trino::bridge {

class TrinoBufOutputStream : public facebook::velox::OutputStream {
 public:
  explicit TrinoBufOutputStream(facebook::velox::memory::MemoryPool& pool,
                                facebook::velox::OutputStreamListener* listener = nullptr,
                                int32_t initialSize = facebook::velox::memory::AllocationTraits::kPageSize)
      : OutputStream(listener),
        arena_(std::make_shared<facebook::velox::StreamArena>(&pool)),
        out_(std::make_unique<facebook::velox::ByteStream>(arena_.get())) {
    out_->startWrite(initialSize);
  }

  void write(const char* s, std::streamsize count) override {
    out_->appendStringPiece(folly::StringPiece(s, count));
    if (listener_) {
      listener_->onWrite(s, count);
    }
  }

  std::streampos tellp() const override;

  void seekp(std::streampos pos) override;

  TrinoBuffer getTrinoBuffer(std::shared_ptr<MemoryManager> memoryManager);
  /// 'releaseFn' is executed on iobuf destruction if not null.
  // std::unique_ptr<folly::IOBuf> getIOBuf(
  //     const std::function<void()>& releaseFn = nullptr);

 private:
  std::shared_ptr<facebook::velox::StreamArena> arena_;
  std::unique_ptr<facebook::velox::ByteStream> out_;
};

}  // namespace io::trino::bridge

#include "trino_buffer_stream.h"

using namespace facebook::velox;

namespace io::trino::bridge {

std::streampos TrinoBufOutputStream::tellp() const { return out_->tellp(); }

void TrinoBufOutputStream::seekp(std::streampos pos) { out_->seekp(pos); }

TrinoBuffer TrinoBufOutputStream::getTrinoBuffer(
    std::shared_ptr<MemoryManager> memoryManager) {
  auto& ranges = out_->ranges();
  TrinoBuffer trinoBuffer(memoryManager);
  for (auto& range : ranges) {
    trinoBuffer.init(range.position);
    std::memcpy(trinoBuffer.getAddress(), range.buffer, range.position);
    return trinoBuffer;
  }
  return trinoBuffer;
}

}  // namespace io::trino::bridge
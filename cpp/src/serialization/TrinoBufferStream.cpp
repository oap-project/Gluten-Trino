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
#include "TrinoBufferStream.h"

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
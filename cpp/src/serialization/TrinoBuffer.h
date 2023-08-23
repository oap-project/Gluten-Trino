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
#include <unordered_map>
namespace io::trino::bridge {
class MemoryManager {
 public:
  MemoryManager() {}
  char* allocate(size_t length) { return reinterpret_cast<char*>(std::malloc(length)); }
  void release(char* address) { std::free(address); }
};

class TrinoBuffer {
 public:
  TrinoBuffer(std::shared_ptr<MemoryManager> memoryManager)
      : memoryManager_(memoryManager), address_(nullptr), length_(0) {}

  void init(size_t length) {
    address_ = memoryManager_->allocate(length);
    length_ = length;
  }
  char* getAddress() { return address_; }
  size_t getLength() { return length_; }

  void release() {
    memoryManager_->release(address_);
  }

 private:
  std::shared_ptr<MemoryManager> memoryManager_;
  char* address_;
  size_t length_;
};

using TrinoBufferMap = std::unordered_map<std::string, TrinoBuffer>;
}  // namespace io::trino::bridge
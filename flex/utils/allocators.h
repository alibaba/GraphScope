/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef GRAPHSCOPE_UTILS_ALLOCATORS_H_
#define GRAPHSCOPE_UTILS_ALLOCATORS_H_

#include <stdlib.h>

#include <functional>
#include <string>
#include <vector>

#include "flex/utils/mmap_array.h"

namespace gs {

class ArenaAllocator {
  static constexpr size_t batch_size = 128 * 1024 * 1024;

 public:
  ArenaAllocator(const std::string& prefix)
      : prefix_(prefix),
        cur_loc_(0),
        cur_size_(0)
#ifdef MONITOR_SESSIONS
        ,
        allocated_memory_(0)
#endif
  {
  }
  ~ArenaAllocator() {
    for (auto ptr : mmap_buffers_) {
      delete ptr;
    }
  }

  void reserve(size_t cap) {
    if (cur_size_ - cur_loc_ >= cap) {
      return;
    }
    cap = (cap + batch_size - 1) ^ (batch_size - 1);
    cur_buffer_ = allocate_batch(cap);
    cur_loc_ = 0;
    cur_size_ = cap;
  }

  void* allocate(size_t size) {
#ifdef MONITOR_SESSIONS
    allocated_memory_ += size;
#endif
    if (cur_size_ - cur_loc_ >= size) {
      void* ret = (char*) cur_buffer_ + cur_loc_;
      cur_loc_ += size;
      return ret;
    } else if (size >= batch_size / 2) {
      return allocate_batch(size);
    } else {
      cur_buffer_ = allocate_batch(batch_size);
      void* ret = cur_buffer_;
      cur_loc_ = size;
      cur_size_ = batch_size;
      return ret;
    }
  }

#ifdef MONITOR_SESSIONS
  size_t allocated_memory() const { return allocated_memory_; }
#endif

 private:
  void* allocate_batch(size_t size) {
    if (prefix_.empty()) {
      mmap_array<char>* buf = new mmap_array<char>();
      buf->open_in_memory("");
      buf->resize(size);
      mmap_buffers_.push_back(buf);
      return static_cast<void*>(buf->data());
    } else {
      mmap_array<char>* buf = new mmap_array<char>();
      buf->open(prefix_ + std::to_string(mmap_buffers_.size()), false);
      buf->resize(size);
      mmap_buffers_.push_back(buf);
      return static_cast<void*>(buf->data());
    }
  }

  std::string prefix_;
  std::vector<mmap_array<char>*> mmap_buffers_;

  void* cur_buffer_;
  size_t cur_loc_;
  size_t cur_size_;

#ifdef MONITOR_SESSIONS
  size_t allocated_memory_;
#endif
};

using Allocator = ArenaAllocator;

}  // namespace gs

#endif  // GRAPHSCOPE_UTILS_ALLOCATORS_H_

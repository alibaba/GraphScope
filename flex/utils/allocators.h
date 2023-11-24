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
  static constexpr size_t batch_size = 4096;

 public:
  ArenaAllocator() : cur_buffer_(nullptr), cur_loc_(0), cur_size_(0) {}
  ~ArenaAllocator() {
    if (cur_buffer_ != nullptr) {
      free(cur_buffer_);
    }
    for (auto ptr : buffers_) {
      if (ptr != nullptr) {
        free(ptr);
      }
    }
    for (auto& t : typed_allocations_) {
      void* data = std::get<0>(t);
      size_t span = std::get<1>(t);
      size_t num = std::get<2>(t);
      auto& func = std::get<3>(t);

      char* ptr = static_cast<char*>(data);
      for (size_t i = 0; i < num; ++i) {
        func(ptr);
        ptr += span;
      }

      free(data);
    }
  }

  void reserve(size_t cap) {
    if (cur_size_ - cur_loc_ >= cap) {
      return;
    }
    buffers_.push_back(cur_buffer_);
    cap = (cap + batch_size - 1) ^ (batch_size - 1);
    cur_buffer_ = malloc(cap);
    cur_loc_ = 0;
    cur_size_ = cap;
  }

  void* allocate(size_t size) {
    reserve(size);
    void* ret = (char*) cur_buffer_ + cur_loc_;
    cur_loc_ += size;
    return ret;
  }

  void* allocate_typed(size_t span, size_t num,
                       const std::function<void(void*)>& dtor) {
    void* data = malloc(span * num);
    typed_allocations_.emplace_back(data, span, num, dtor);
    return data;
  }

 private:
  std::vector<void*> buffers_;
  void* cur_buffer_;
  size_t cur_loc_;
  size_t cur_size_;

  std::vector<std::tuple<void*, size_t, size_t, std::function<void(void*)>>>
      typed_allocations_;
};

class MMapAllocator {
  static constexpr size_t batch_size = 128 * 1024 * 1024;

 public:
  MMapAllocator(const std::string& prefix)
      : prefix_(prefix), cur_loc_(0), cur_size_(0) {}
  ~MMapAllocator() {
    for (auto ptr : buffers_) {
      free(ptr);
    }
  }

  void reserve(size_t cap) {
    if (cur_size_ - cur_loc_ >= cap) {
      return;
    }
    cap = (cap + batch_size - 1) ^ (batch_size - 1);
    cur_buffer_ = malloc(cap);
    buffers_.push_back(cur_buffer_);
    cur_loc_ = 0;
    cur_size_ = cap;
  }

  void* allocate_large(size_t size) { return malloc(size); }

  void allocate_new_batch() {
    cur_buffer_ = malloc(batch_size);
    buffers_.push_back(cur_buffer_);
    cur_loc_ = 0;
    cur_size_ = batch_size;
  }

  void* allocate(size_t size) {
    if (cur_size_ - cur_loc_ >= size) {
      void* ret = (char*) cur_buffer_ + cur_loc_;
      cur_loc_ += size;
      return ret;
    } else if (size >= batch_size / 2) {
      return allocate_large(size);
    } else {
      allocate_new_batch();
      void* ret = (char*) cur_buffer_ + cur_loc_;
      cur_loc_ += size;
      return ret;
    }
  }

 private:
  std::string prefix_;
  std::vector<void*> buffers_;

  void* cur_buffer_;
  size_t cur_loc_;
  size_t cur_size_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_UTILS_ALLOCATORS_H_

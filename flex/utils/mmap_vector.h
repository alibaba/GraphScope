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

#ifndef GRAPHSCOPE_UTILS_MMAP_VECTOR_H_
#define GRAPHSCOPE_UTILS_MMAP_VECTOR_H_
#include "flex/utils/mmap_array.h"

namespace gs {
template <typename T>
class mmap_vector {
 public:
  mmap_vector() : size_(0) {}

  void open(const std::string& filename, bool sync_to_file = true) {
    array_.open(filename, sync_to_file);
  }

  void reserve(size_t size) { array_.resize(size); }

  void unlink() { array_.unlink(); }

  ~mmap_vector() {}

  mmap_vector(mmap_vector&& other) {
    array_.swap(other.array_);
    size_ = other.size_;
  }

  void push_back(const T& val) {
    size_t cap = array_.size();
    if (size_ == cap) {
      array_.resize(std::max(cap * 2, 1ul));
    }
    array_.set(size_, val);
    ++size_;
  }

  void emplace_back(T&& val) {
    size_t cap = array_.size();
    if (size_ == cap) {
      array_.resize(std::max(cap * 2, 1ul));
    }
    array_.set(size_, val);
    ++size_;
  }

  void resize(size_t size) {
    size_t cap = std::max(array_.size(), 1ul);
    while (size > cap) {
      cap *= 2;
    }
    array_.resize(cap);
    size_ = size;
  }

  size_t size() const { return size_; }

  const T& operator[](size_t index) const { return array_[index]; }
  T& operator[](size_t index) { return array_[index]; }
  const T* begin() const { return array_.data(); }
  const T* end() const { return array_.data() + size_; }

  void clear() { size_ = 0; }

 private:
  mmap_array<T> array_;
  size_t size_;
};
};      // namespace gs
#endif  // GRAPHSCOPE_UTILS_MMAP_VECTOR_H_

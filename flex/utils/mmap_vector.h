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
#include "flex/utils/property/column.h"

namespace gs {
template <typename EDATA_T>
class mmap_vector {
 public:
  mmap_vector(const std::string& work_dir, const std::string& file_name) {
    array_.open(work_dir + "/" + file_name, true);
    array_.resize(4096);
    size_ = 0;
  }

  ~mmap_vector() { array_.unlink(); }

  mmap_vector(mmap_vector&& other) {
    array_.swap(other.array_);
    size_ = other.size_;
  }

  void push_back(const EDATA_T& val) {
    size_t cap = array_.size();
    if (size_ == cap) {
      array_.resize(cap * 2);
    }
    array_.set(size_, val);
    ++size_;
  }

  void emplace_back(EDATA_T&& val) {
    size_t cap = array_.size();
    if (size_ == cap) {
      array_.resize(cap * 2);
    }
    array_.set(size_, val);
    ++size_;
  }

  void resize(size_t size) {
    size_t cap = array_.size();
    while (size > cap) {
      cap *= 2;
      array_.resize(cap);
    }
    size_ = size;
  }

  size_t size() const { return size_; }

  const EDATA_T& operator[](size_t index) const { return array_[index]; }
  EDATA_T& operator[](size_t index) { return array_[index]; }
  const EDATA_T* begin() const { return array_.data(); }
  const EDATA_T* end() const { return array_.data() + size_; }

  void clear() { size_ = 0; }

 private:
  mmap_array<EDATA_T> array_;
  size_t size_;
};
};  // namespace gs
#endif  // GRAPHSCOPE_UTILS_MMAP_VECTOR_H_

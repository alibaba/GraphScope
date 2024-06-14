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
  mmap_vector(const std::string& work_dir, const std::string& file_name)
      : work_dir_(work_dir), file_name_(file_name) {
    array_.open(work_dir + "/" + file_name_, true);
    array_.resize(4096);
    size_ = 0;
    cap_ = 4096;
  }

  ~mmap_vector() {
    array_.reset();
    unlink((work_dir_ + "/" + file_name_).c_str());
  }

  mmap_vector(mmap_vector&& other) {
    array_.swap(other.array_);
    size_ = other.size_;
    cap_ = other.cap_;
    file_name_.swap(other.file_name_);
    work_dir_.swap(other.work_dir_);
  }

  void push_back(const EDATA_T& val) {
    if (size_ == cap_) {
      array_.resize(cap_ * 2);
      cap_ = cap_ * 2;
    }
    array_.set(size_, val);
    ++size_;
  }

  void emplace_back(EDATA_T&& val) {
    if (size_ == cap_) {
      array_.resize(cap_ * 2);
      cap_ = cap_ * 2;
    }
    array_.set(size_, val);
    ++size_;
  }

  void resize(size_t size) {
    while (size > cap_) {
      cap_ *= 2;
      array_.resize(cap_);
    }
    size_ = size;
  }

  size_t size() const { return size_; }

  const EDATA_T& operator[](size_t index) const { return array_[index]; }
  EDATA_T& operator[](size_t index) { return array_[index]; }
  const EDATA_T* begin() const { return array_.data(); }
  const EDATA_T* end() const { return array_.data() + size_; }

  void clear() {
    size_ = 0;
    cap_ = 0;
  }

 private:
  mmap_array<EDATA_T> array_;
  std::string work_dir_;
  std::string file_name_;
  size_t size_;
  size_t cap_;
};
};  // namespace gs
#endif  // GRAPHSCOPE_UTILS_MMAP_VECTOR_H_

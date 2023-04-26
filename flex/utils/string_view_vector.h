/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef GRAPHSCOPE_UTILS_STRING_VIEW_VECTOR_H_
#define GRAPHSCOPE_UTILS_STRING_VIEW_VECTOR_H_

#include <stdlib.h>
#include <string.h>

#include <string_view>
#include <vector>

namespace gs {

class StringViewVector {
 public:
  StringViewVector() { offsets_.push_back(0); }
  ~StringViewVector() {}

  void push_back(const std::string_view& val) {
    size_t old_size = buffer_.size();
    buffer_.resize(old_size + val.size());
    memcpy(&buffer_[old_size], val.data(), val.size());
    offsets_.push_back(buffer_.size());
  }

  void emplace_back(const std::string_view& val) {
    size_t old_size = buffer_.size();
    buffer_.resize(old_size + val.size());
    memcpy(&buffer_[old_size], val.data(), val.size());
    offsets_.push_back(buffer_.size());
  }

  size_t size() const {
    assert(offsets_.size() > 0);
    return offsets_.size() - 1;
  }

  std::string_view operator[](size_t index) const {
    size_t from = offsets_[index];
    size_t len = offsets_[index + 1] - from;
    return std::string_view(&buffer_[from], len);
  }

  std::vector<char>& content_buffer() { return buffer_; }

  const std::vector<char>& content_buffer() const { return buffer_; }

  std::vector<size_t>& offset_buffer() { return offsets_; }

  const std::vector<size_t>& offset_buffer() const { return offsets_; }

  void clear() {
    buffer_.clear();
    offsets_.clear();
    offsets_.push_back(0);
  }

  void swap(StringViewVector& rhs) {
    buffer_.swap(rhs.buffer_);
    offsets_.swap(rhs.offsets_);
  }

 private:
  std::vector<char> buffer_;
  std::vector<size_t> offsets_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_UTILS_STRING_VIEW_VECTOR_H_

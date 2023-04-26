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

#include "flex/engines/graph_db/app/app_utils.h"

#include <string.h>

namespace gs {

void Encoder::put_long(int64_t v) {
  size_t size = buf_.size();
  buf_.resize(size + sizeof(int64_t));
  memcpy(&buf_[size], &v, sizeof(int64_t));
}

size_t Encoder::skip_long() {
  size_t size = buf_.size();
  buf_.resize(size + sizeof(int64_t));
  return size;
}

void Encoder::put_long_at(size_t pos, int64_t v) {
  memcpy(&buf_[pos], &v, sizeof(int64_t));
}

void Encoder::put_int(int v) {
  size_t size = buf_.size();
  buf_.resize(size + sizeof(int));
  memcpy(&buf_[size], &v, sizeof(int));
}

size_t Encoder::skip_int() {
  size_t size = buf_.size();
  buf_.resize(size + sizeof(int));
  return size;
}

void Encoder::put_int_at(size_t pos, int v) {
  memcpy(&buf_[pos], &v, sizeof(int));
}

void Encoder::put_byte(uint8_t v) { buf_.push_back(static_cast<char>(v)); }

size_t Encoder::skip_byte() {
  size_t size = buf_.size();
  buf_.resize(size + 1);
  return size;
}

void Encoder::put_byte_at(size_t pos, uint8_t v) {
  buf_[pos] = static_cast<char>(v);
}

void Encoder::put_string(const std::string& v) {
  size_t size = buf_.size();
  int len = v.size();
  buf_.resize(size + sizeof(int) + len);
  memcpy(&buf_[size], &len, sizeof(int));
  memcpy(&buf_[size + 4], v.data(), len);
}

void Encoder::put_string_view(const std::string_view& v) {
  size_t size = buf_.size();
  int len = v.size();
  buf_.resize(size + sizeof(int) + len);
  memcpy(&buf_[size], &len, sizeof(int));
  memcpy(&buf_[size + 4], v.data(), len);
}

void Encoder::clear() { buf_.clear(); }

static int64_t char_ptr_to_long(const char* data) {
  const int64_t* ptr = reinterpret_cast<const int64_t*>(data);
  return *ptr;
}

static int char_ptr_to_int(const char* data) {
  const int* ptr = reinterpret_cast<const int*>(data);
  return *ptr;
}

int Decoder::get_int() {
  int ret = char_ptr_to_int(data_);
  data_ += 4;
  return ret;
}

int64_t Decoder::get_long() {
  int64_t ret = char_ptr_to_long(data_);
  data_ += 8;
  return ret;
}

std::string_view Decoder::get_string() {
  int len = get_int();
  std::string_view ret(data_, len);
  data_ += len;
  return ret;
}

uint8_t Decoder::get_byte() { return static_cast<uint8_t>(*(data_++)); }

const char* Decoder::data() const { return data_; }

bool Decoder::empty() const { return data_ == end_; }

}  // namespace gs

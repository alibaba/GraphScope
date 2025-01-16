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

#include "flex/utils/app_utils.h"

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

void Encoder::put_bytes(const char* data, size_t size) {
  buf_.insert(buf_.end(), data, data + size);
}

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

void Encoder::put_var_len_string(const std::string& v) {
  size_t size = buf_.size();
  int len = v.size();
  if (len < (1 << 7)) {
    buf_.resize(size + sizeof(uint8_t) + len);
    buf_[size] = static_cast<char>((len << 1) | 1);
    memcpy(&buf_[size + 1], v.data(), len);
    return;
  } else if (len < (1 << 14)) {
    buf_.resize(size + sizeof(uint16_t) + len);
    uint16_t len16 = len;
    len16 = (len16 << 2) | 2;
    memcpy(&buf_[size], &len16, sizeof(uint16_t));
    memcpy(&buf_[size + 2], v.data(), len);
    return;
  } else if (len < (1 << 21)) {
    buf_.resize(size + sizeof(uint8_t) * 3 + len);
    uint32_t len32 = len;
    len32 = (len32 << 3) | 4;

    buf_[size] = len32 & 0xff;
    buf_[size + 1] = (len32 >> 8) & 0xff;
    buf_[size + 2] = (len32 >> 16) & 0xff;
    memcpy(&buf_[size + 3], v.data(), len);
    return;
  } else {
    buf_.resize(size + sizeof(int) + len);
    len = (len << 4) | 8;
    memcpy(&buf_[size], &len, sizeof(int));
    memcpy(&buf_[size + 4], v.data(), len);
  }
}

void Encoder::put_var_len_string_view(const std::string_view& v) {
  size_t size = buf_.size();
  int len = v.size();
  if (len < (1 << 7)) {
    buf_.resize(size + sizeof(uint8_t) + len);
    buf_[size] = static_cast<char>((len << 1) | 1);
    memcpy(&buf_[size + 1], v.data(), len);
    return;
  } else if (len < (1 << 14)) {
    buf_.resize(size + sizeof(uint16_t) + len);
    uint16_t len16 = len;
    len16 = (len16 << 2) | 2;
    memcpy(&buf_[size], &len16, sizeof(uint16_t));
    memcpy(&buf_[size + 2], v.data(), len);
    return;
  } else if (len < (1 << 21)) {
    buf_.resize(size + sizeof(uint8_t) * 3 + len);
    uint32_t len32 = len;
    len32 = (len32 << 3) | 4;

    buf_[size] = len32 & 0xff;
    buf_[size + 1] = (len32 >> 8) & 0xff;
    buf_[size + 2] = (len32 >> 16) & 0xff;
    memcpy(&buf_[size + 3], v.data(), len);
    return;
  } else {
    buf_.resize(size + sizeof(int) + len);
    len = (len << 4) | 8;
    memcpy(&buf_[size], &len, sizeof(int));
    memcpy(&buf_[size + 4], v.data(), len);
  }
}

void Encoder::put_small_string(const std::string& v) {
  size_t size = buf_.size();
  int len = v.size();
  buf_.resize(size + sizeof(uint8_t) + len);
  buf_[size] = static_cast<char>(len);
  memcpy(&buf_[size + 1], v.data(), len);
}

void Encoder::put_small_string_view(const std::string_view& v) {
  size_t size = buf_.size();
  int len = v.size();
  buf_.resize(size + sizeof(uint8_t) + len);
  buf_[size] = static_cast<char>(len);
  memcpy(&buf_[size + 1], v.data(), len);
}

void Encoder::put_double(double v) {
  size_t size = buf_.size();
  buf_.resize(size + sizeof(double));
  memcpy(&buf_[size], &v, sizeof(double));
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

static double char_ptr_to_double(const char* data) {
  const double* ptr = reinterpret_cast<const double*>(data);
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

double Decoder::get_double() {
  double ret = char_ptr_to_double(data_);
  data_ += 8;
  return ret;
}
std::string_view Decoder::get_bytes() {
  std::string_view ret(data_, end_ - data_);
  data_ = end_;
  return ret;
}
std::string_view Decoder::get_string() {
  int len = get_int();
  std::string_view ret(data_, len);
  data_ += len;
  return ret;
}

std::string_view Decoder::get_small_string() {
  int len = static_cast<int>(get_byte());
  std::string_view ret(data_, len);
  data_ += len;
  return ret;
}

uint8_t Decoder::get_byte() { return static_cast<uint8_t>(*(data_++)); }

const char* Decoder::data() const { return data_; }

size_t Decoder::size() const { return end_ - data_; }

bool Decoder::empty() const { return data_ == end_; }

void Decoder::reset(const char* p, size_t size) {
  data_ = p;
  end_ = p + size;
}

}  // namespace gs

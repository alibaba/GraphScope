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

#ifndef GRAPHSCOPE_APP_UTILS_H_
#define GRAPHSCOPE_APP_UTILS_H_

#include <string>
#include <string_view>
#include <vector>

namespace gs {

class Encoder {
 public:
  Encoder(std::vector<char>& buf) : buf_(buf) {}

  void put_long(int64_t v);

  size_t skip_long();

  void put_long_at(size_t pos, int64_t v);

  void put_int(int v);

  size_t skip_int();

  void put_int_at(size_t pos, int v);

  void put_byte(uint8_t v);

  size_t skip_byte();

  void put_byte_at(size_t pos, uint8_t v);

  void put_string(const std::string& v);

  void put_string_view(const std::string_view& v);

  void clear();

 private:
  std::vector<char>& buf_;
};

class Decoder {
 public:
  Decoder(const char* ptr, size_t size) : data_(ptr), end_(ptr + size) {}
  ~Decoder() {}

  int get_int();

  int64_t get_long();

  std::string_view get_string();

  uint8_t get_byte();

  const char* data() const;

  bool empty() const;

 private:
  const char* data_;
  const char* end_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_APP_UTILS_H_

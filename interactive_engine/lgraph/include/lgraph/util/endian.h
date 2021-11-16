/**
 * Copyright 2021 Alibaba Group Holding Limited.
 *
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

#include <algorithm>
#include <array>
#include "lgraph/common/namespace.h"

namespace LGRAPH_NAMESPACE {

class Endian {
public:
  template <typename T>
  static T SwapEndian(T val, typename std::enable_if<std::is_arithmetic<T>::value, std::nullptr_t>::type = nullptr);

  template <typename T>
  static T ToBigEndian(T val, typename std::enable_if<std::is_arithmetic<T>::value, std::nullptr_t>::type = nullptr);

  template <typename T>
  static T ToLittleEndian(T val, typename std::enable_if<std::is_arithmetic<T>::value, std::nullptr_t>::type = nullptr);
};

template<typename T>
T Endian::SwapEndian(T val, typename std::enable_if<std::is_arithmetic<T>::value, std::nullptr_t>::type) {
  union U {
    T val;
    std::array<std::uint8_t, sizeof(T)> raw;
  } src, dst;

  src.val = val;
  std::reverse_copy(src.raw.begin(), src.raw.end(), dst.raw.begin());
  return dst.val;
}

template<typename T>
T Endian::ToBigEndian(T val, typename std::enable_if<std::is_arithmetic<T>::value, std::nullptr_t>::type) {
  if (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__) {
    return SwapEndian(val);
  }
  return val;
}

template<typename T>
T Endian::ToLittleEndian(T val, typename std::enable_if<std::is_arithmetic<T>::value, std::nullptr_t>::type) {
  if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__) {
    return SwapEndian(val);
  }
  return val;
}

}

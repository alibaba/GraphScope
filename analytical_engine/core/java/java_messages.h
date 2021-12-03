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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_JAVA_MESSAGES_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_JAVA_MESSAGES_H_

#ifdef ENABLE_JAVA_SDK

#include <string>

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace gs {

/**
 * @brief Since Java can not pass Double, Long as reference, we need a wrapper
 * for primitives.
 */
template <typename T>
struct PrimitiveMessage {
  T data;

  PrimitiveMessage() { data = static_cast<T>(-1); }
  explicit PrimitiveMessage(const T in_data) : data(in_data) {}
  inline void setData(const T value) { data = value; }
  inline T getData() { return data; }

  friend inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                              PrimitiveMessage<T>& msg) {
    out_archive >> msg.data;
    return out_archive;
  }
  friend inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                             const PrimitiveMessage<T>& msg) {
    in_archive << msg.data;
    return in_archive;
  }
};

using DoubleMsg = PrimitiveMessage<double>;
using LongMsg = PrimitiveMessage<int64_t>;

// specify overloaded <, > operators
template <typename T>
inline bool operator<(const PrimitiveMessage<T>& lhs,
                      const PrimitiveMessage<T>& rhs) {
  return lhs.data < rhs.data;
}
template <typename T>
inline bool operator>(const PrimitiveMessage<T>& lhs,
                      const PrimitiveMessage<T>& rhs) {
  return lhs.data < rhs.data;
}
template <typename T>
inline bool operator<=(const PrimitiveMessage<T>& lhs,
                       const PrimitiveMessage<T>& rhs) {
  return !(lhs > rhs);
}
template <typename T>
inline bool operator>=(const PrimitiveMessage<T>& lhs,
                       const PrimitiveMessage<T>& rhs) {
  return !(lhs < rhs);
}
template <typename T>
inline bool operator==(const PrimitiveMessage<T>& lhs,
                       const PrimitiveMessage<T>& rhs) {
  return lhs.data == rhs.data;
}
template <typename T>
inline bool operator!=(const PrimitiveMessage<T>& lhs,
                       const PrimitiveMessage<T>& rhs) {
  return !(lhs == rhs);
}
template <typename T>
inline PrimitiveMessage<T>& operator+=(PrimitiveMessage<T>& lhs,
                                       const PrimitiveMessage<T>& rhs) {
  lhs.data += rhs.data;
  return lhs;
}

template <typename T>
inline PrimitiveMessage<T>& operator-=(PrimitiveMessage<T>& lhs,
                                       const PrimitiveMessage<T>& rhs) {
  lhs.data -= rhs.data;
  return lhs;
}
}  // namespace gs

#endif
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_JAVA_MESSAGES_H_

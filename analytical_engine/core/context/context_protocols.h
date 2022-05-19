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

#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_CONTEXT_PROTOCOLS_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_CONTEXT_PROTOCOLS_H_

#include <cstdint>
#include <string>

#include "vineyard/graph/utils/context_protocols.h"  // IWYU pragma: export

namespace gs {

enum class ContextDataType {
  kBool,
  kInt32,
  kInt64,
  kUInt32,
  kUInt64,
  kFloat,
  kDouble,
  kString,
  kUndefined,
};

/* N.B. These values should be the same as vineyard::TypeToInt::value. Because
 * theses values are used to decode in Python side. Refer:
 * python.graphscope.framework.utils._to_numpy_dtype
 */
inline int ContextDataTypeToInt(ContextDataType type) {
  switch (type) {
  case ContextDataType::kBool:
    return vineyard::TypeToInt<bool>::value;
  case ContextDataType::kInt32:
    return vineyard::TypeToInt<int32_t>::value;
  case ContextDataType::kUInt32:
    return vineyard::TypeToInt<uint32_t>::value;
  case ContextDataType::kInt64:
    return vineyard::TypeToInt<int64_t>::value;
  case ContextDataType::kUInt64:
    return vineyard::TypeToInt<uint64_t>::value;
  case ContextDataType::kFloat:
    return vineyard::TypeToInt<float>::value;
  case ContextDataType::kDouble:
    return vineyard::TypeToInt<double>::value;
  case ContextDataType::kString:
    return vineyard::TypeToInt<std::string>::value;
  default:
    return -1;
  }
}

template <typename T>
struct ContextTypeToEnum {
  static constexpr ContextDataType value = ContextDataType::kUndefined;
};

template <>
struct ContextTypeToEnum<int> {
  static constexpr ContextDataType value = ContextDataType::kInt32;
};

template <>
struct ContextTypeToEnum<int64_t> {
  static constexpr ContextDataType value = ContextDataType::kInt64;
};

template <>
struct ContextTypeToEnum<uint32_t> {
  static constexpr ContextDataType value = ContextDataType::kUInt32;
};

template <>
struct ContextTypeToEnum<uint64_t> {
  static constexpr ContextDataType value = ContextDataType::kUInt64;
};

template <>
struct ContextTypeToEnum<float> {
  static constexpr ContextDataType value = ContextDataType::kFloat;
};

template <>
struct ContextTypeToEnum<double> {
  static constexpr ContextDataType value = ContextDataType::kDouble;
};

template <>
struct ContextTypeToEnum<std::string> {
  static constexpr ContextDataType value = ContextDataType::kString;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_CONTEXT_PROTOCOLS_H_

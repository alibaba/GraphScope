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
#ifndef UTILS_ARROW_UTILS_H_
#define UTILS_ARROW_UTILS_H_

#include <arrow/api.h>
#include <memory>
#include "flex/utils/property/types.h"

namespace gs {

// arrow related;

// convert c++ type to arrow type. support other types likes emptyType, Date
template <typename T>
struct CppTypeToArrowType {};

template <>
struct CppTypeToArrowType<int64_t> {
  using Type = arrow::Int64Type;
  using ArrayType = arrow::Int64Array;
  static std::shared_ptr<arrow::DataType> TypeValue() { return arrow::int64(); }
};

template <>
struct CppTypeToArrowType<int32_t> {
  using Type = arrow::Int32Type;
  using ArrayType = arrow::Int32Array;
  static std::shared_ptr<arrow::DataType> TypeValue() { return arrow::int32(); }
};

template <>
struct CppTypeToArrowType<double> {
  using Type = arrow::DoubleType;
  using ArrayType = arrow::DoubleArray;
  static std::shared_ptr<arrow::DataType> TypeValue() {
    return arrow::float64();
  }
};

template <>
struct CppTypeToArrowType<Date> {
  using Type = arrow::Int64Type;
  using ArrayType = arrow::Int64Array;
  static std::shared_ptr<arrow::DataType> TypeValue() { return arrow::int64(); }
};

void assign_to_any_vector(const std::shared_ptr<arrow::Array>& array,
                          std::vector<Any>& vec);

void assign_to_any_vector(const std::shared_ptr<arrow::ChunkedArray>& array,
                          std::vector<Any>& vec);

template <typename T>
struct CppTypeToPropertyType;

template <>
struct CppTypeToPropertyType<int32_t> {
  static constexpr PropertyType value = PropertyType::kInt32;
};

template <>
struct CppTypeToPropertyType<int64_t> {
  static constexpr PropertyType value = PropertyType::kInt64;
};

template <>
struct CppTypeToPropertyType<double> {
  static constexpr PropertyType value = PropertyType::kDouble;
};

template <>
struct CppTypeToPropertyType<std::string> {
  static constexpr PropertyType value = PropertyType::kString;
};

template <>
struct CppTypeToPropertyType<std::string_view> {
  static constexpr PropertyType value = PropertyType::kString;
};

std::shared_ptr<arrow::DataType> PropertyTypeToArrowType(PropertyType type);
}  // namespace gs

#endif  // UTILS_ARROW_UTILS_H_

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
#include "flex/utils/arrow_utils.h"

namespace gs {
std::shared_ptr<arrow::DataType> PropertyTypeToArrowType(PropertyType type) {
  switch (type) {
  case PropertyType::kInt32:
    return arrow::int32();
  case PropertyType::kInt64:
    return arrow::int64();
  case PropertyType::kDouble:
    return arrow::float64();
  case PropertyType::kDate:
    return arrow::timestamp(arrow::TimeUnit::MILLI);
  case PropertyType::kString:
    return arrow::large_utf8();
  case PropertyType::kEmpty:
    return arrow::null();
  default:
    LOG(FATAL) << "Unexpected property type: " << static_cast<int>(type);
    return nullptr;
  }
}

void assign_to_any_vector(const std::shared_ptr<arrow::Array>& array,
                          std::vector<Any>& vec) {
  auto type = array->type();
  CHECK(vec.size() == 0);
  vec.reserve(array->length());
  if (type == arrow::int64()) {
    auto casted = std::static_pointer_cast<arrow::Int64Array>(array);
    for (auto k = 0; k < casted->length(); ++k) {
      vec.emplace_back(AnyConverter<int64_t>::to_any(casted->Value(k)));
    }
  } else if (type == arrow::int32()) {
    auto casted = std::static_pointer_cast<arrow::Int32Array>(array);
    for (auto k = 0; k < casted->length(); ++k) {
      vec.emplace_back(AnyConverter<int32_t>::to_any(casted->Value(k)));
    }
  } else if (type == arrow::float64()) {
    auto casted = std::static_pointer_cast<arrow::DoubleArray>(array);
    for (auto k = 0; k < casted->length(); ++k) {
      vec.emplace_back(AnyConverter<double>::to_any(casted->Value(k)));
    }
  } else if (type == arrow::large_utf8()) {
    auto casted = std::static_pointer_cast<arrow::LargeStringArray>(array);
    for (auto k = 0; k < casted->length(); ++k) {
      auto str = casted->GetView(k);
      std::string_view str_view(str.data(), str.size());
      vec.emplace_back(AnyConverter<std::string_view>::to_any(str_view));
    }
  } else if (type == arrow::utf8()) {
    auto casted = std::static_pointer_cast<arrow::StringArray>(array);
    for (auto k = 0; k < casted->length(); ++k) {
      auto str = casted->GetView(k);
      std::string_view str_view(str.data(), str.size());
      vec.emplace_back(AnyConverter<std::string_view>::to_any(str_view));
    }
  } else {
    LOG(FATAL) << "Not support type: " << type->ToString();
  }
}

template <typename T>
void emplace_into_vector(const std::shared_ptr<arrow::ChunkedArray>& array,
                         std::vector<Any>& vec) {
  using arrow_array_type = typename gs::CppTypeToArrowType<T>::ArrayType;
  for (auto i = 0; i < array->num_chunks(); ++i) {
    auto casted = std::static_pointer_cast<arrow_array_type>(array->chunk(i));
    for (auto k = 0; k < casted->length(); ++k) {
      vec.emplace_back(AnyConverter<T>::to_any(casted->Value(k)));
    }
  }
}

void assign_to_any_vector(const std::shared_ptr<arrow::ChunkedArray>& array,
                          std::vector<Any>& vec) {
  auto type = array->type();
  CHECK(vec.size() == 0);
  vec.reserve(array->length());
  if (type == arrow::int64()) {
    emplace_into_vector<int64_t>(array, vec);
  } else if (type == arrow::int32()) {
    emplace_into_vector<int32_t>(array, vec);
  } else if (type == arrow::float64()) {
    emplace_into_vector<double>(array, vec);
  } else if (type == arrow::large_utf8()) {
    using arrow_array_type = arrow::LargeStringArray;
    for (auto i = 0; i < array->num_chunks(); ++i) {
      auto casted = std::static_pointer_cast<arrow_array_type>(array->chunk(i));
      for (auto k = 0; k < casted->length(); ++k) {
        auto str = casted->GetView(k);
        std::string_view str_view(str.data(), str.size());
        vec.emplace_back(AnyConverter<std::string_view>::to_any(str_view));
      }
    }
  } else if (type == arrow::utf8()) {
    using arrow_array_type = arrow::StringArray;
    for (auto i = 0; i < array->num_chunks(); ++i) {
      auto casted = std::static_pointer_cast<arrow_array_type>(array->chunk(i));
      for (auto k = 0; k < casted->length(); ++k) {
        auto str = casted->GetView(k);
        std::string_view str_view(str.data(), str.size());
        vec.emplace_back(AnyConverter<std::string_view>::to_any(str_view));
      }
    }
  } else {
    LOG(FATAL) << "Not support type: " << type->ToString();
  }
}

}  // namespace gs

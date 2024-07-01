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
  if (type == PropertyType::Bool()) {
    return arrow::boolean();
  } else if (type == PropertyType::Int32()) {
    return arrow::int32();
  } else if (type == PropertyType::Int64()) {
    return arrow::int64();
  } else if (type == PropertyType::UInt32()) {
    return arrow::uint32();
  } else if (type == PropertyType::UInt64()) {
    return arrow::uint64();
  } else if (type == PropertyType::Double()) {
    return arrow::float64();
  } else if (type == PropertyType::Float()) {
    return arrow::float32();
  } else if (type == PropertyType::Date()) {
    return arrow::timestamp(arrow::TimeUnit::MILLI);
  } else if (type == PropertyType::Day()) {
    return arrow::timestamp(arrow::TimeUnit::MILLI);
  } else if (type == PropertyType::StringView()) {
    return arrow::large_utf8();
  } else if (type == PropertyType::StringMap()) {
    return arrow::large_utf8();
  } else if (type == PropertyType::Empty()) {
    return arrow::null();
  } else if (type.type_enum == impl::PropertyTypeImpl::kVarChar) {
    return arrow::large_utf8();
  } else {
    LOG(FATAL) << "Unexpected property type: "
               << static_cast<int>(type.type_enum);
    return nullptr;
  }
}

template <typename T>
void emplace_into_vector(const std::shared_ptr<arrow::ChunkedArray>& array,
                         std::vector<Any>& vec) {
  using arrow_array_type = typename gs::TypeConverter<T>::ArrowArrayType;
  for (int32_t i = 0; i < array->num_chunks(); ++i) {
    auto casted = std::static_pointer_cast<arrow_array_type>(array->chunk(i));
    for (auto k = 0; k < casted->length(); ++k) {
      vec.emplace_back(AnyConverter<T>::to_any(casted->Value(k)));
    }
  }
}

}  // namespace gs

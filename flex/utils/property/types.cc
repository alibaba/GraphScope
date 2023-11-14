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

#include "flex/utils/property/types.h"

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace gs {

grape::InArchive& operator<<(grape::InArchive& in_archive, const Any& value) {
  switch (value.type) {
  case PropertyType::kBool:
    in_archive << value.type << value.value.b;
    break;
  case PropertyType::kInt32:
    in_archive << value.type << value.value.i;
    break;
  case PropertyType::kInt64:
    in_archive << value.type << value.value.l;
    break;
  case PropertyType::kUInt32:
    in_archive << value.type << value.value.ui;
    break;
  case PropertyType::kUInt64:
    in_archive << value.type << value.value.ul;
    break;
  case PropertyType::kDate:
    in_archive << value.type << value.value.d.milli_second;
    break;
  case PropertyType::kString:
    in_archive << value.type << value.value.s;
    break;
  case PropertyType::kDouble:
    in_archive << value.type << value.value.db;
    break;
  case PropertyType::kFloat:
    in_archive << value.type << value.value.f;
    break;
  default:
    in_archive << PropertyType::kEmpty;
    break;
  }

  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive, Any& value) {
  out_archive >> value.type;
  switch (value.type) {
  case PropertyType::kBool:
    out_archive >> value.value.b;
    break;
  case PropertyType::kInt32:
    out_archive >> value.value.i;
    break;
  case PropertyType::kInt64:
    out_archive >> value.value.l;
    break;
  case PropertyType::kUInt32:
    out_archive >> value.value.ui;
    break;
  case PropertyType::kUInt64:
    out_archive >> value.value.ul;
    break;
  case PropertyType::kDate:
    out_archive >> value.value.d.milli_second;
    break;
  case PropertyType::kString:
    out_archive >> value.value.s;
    break;
  case PropertyType::kDouble:
    out_archive >> value.value.db;
    break;
  case PropertyType::kFloat:
    out_archive >> value.value.f;
    break;
  default:
    break;
  }

  return out_archive;
}

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const std::string_view& str) {
  in_archive << str.length();
  in_archive.AddBytes(str.data(), str.length());
  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              std::string_view& str) {
  size_t size;
  out_archive >> size;
  str = std::string_view(reinterpret_cast<char*>(out_archive.GetBytes(size)),
                         size);
  return out_archive;
}

Date::Date(int64_t x) : milli_second(x) {}

std::string Date::to_string() const { return std::to_string(milli_second); }

}  // namespace gs

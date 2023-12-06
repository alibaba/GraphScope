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

const PropertyType PropertyType::kEmpty =
    PropertyType(impl::PropertyTypeImpl::kEmpty);
const PropertyType PropertyType::kBool =
    PropertyType(impl::PropertyTypeImpl::kBool);
const PropertyType PropertyType::kUInt8 =
    PropertyType(impl::PropertyTypeImpl::kUInt8);
const PropertyType PropertyType::kUInt16 =
    PropertyType(impl::PropertyTypeImpl::kUInt16);
const PropertyType PropertyType::kInt32 =
    PropertyType(impl::PropertyTypeImpl::kInt32);
const PropertyType PropertyType::kUInt32 =
    PropertyType(impl::PropertyTypeImpl::kUInt32);
const PropertyType PropertyType::kFloat =
    PropertyType(impl::PropertyTypeImpl::kFloat);
const PropertyType PropertyType::kInt64 =
    PropertyType(impl::PropertyTypeImpl::kInt64);
const PropertyType PropertyType::kUInt64 =
    PropertyType(impl::PropertyTypeImpl::kUInt64);
const PropertyType PropertyType::kDouble =
    PropertyType(impl::PropertyTypeImpl::kDouble);
const PropertyType PropertyType::kDate =
    PropertyType(impl::PropertyTypeImpl::kDate);
const PropertyType PropertyType::kString =
    PropertyType(impl::PropertyTypeImpl::kString);
const PropertyType PropertyType::kStringMap =
    PropertyType(impl::PropertyTypeImpl::kStringMap);

bool PropertyType::operator==(const PropertyType& other) const {
  return type_enum == other.type_enum &&
         additional_type_info.max_length ==
             other.additional_type_info.max_length;
}
bool PropertyType::operator!=(const PropertyType& other) const {
  return !(*this == other);
}

/////////////////////////////// Get Type Instance
//////////////////////////////////
PropertyType PropertyType::empty() {
  return PropertyType(impl::PropertyTypeImpl::kEmpty);
}
PropertyType PropertyType::bool_() {
  return PropertyType(impl::PropertyTypeImpl::kBool);
}
PropertyType PropertyType::uint8() {
  return PropertyType(impl::PropertyTypeImpl::kUInt8);
}
PropertyType PropertyType::uint16() {
  return PropertyType(impl::PropertyTypeImpl::kUInt16);
}
PropertyType PropertyType::int32() {
  return PropertyType(impl::PropertyTypeImpl::kInt32);
}
PropertyType PropertyType::uint32() {
  return PropertyType(impl::PropertyTypeImpl::kUInt32);
}
PropertyType PropertyType::float_() {
  return PropertyType(impl::PropertyTypeImpl::kFloat);
}
PropertyType PropertyType::int64() {
  return PropertyType(impl::PropertyTypeImpl::kInt64);
}
PropertyType PropertyType::uint64() {
  return PropertyType(impl::PropertyTypeImpl::kUInt64);
}
PropertyType PropertyType::double_() {
  return PropertyType(impl::PropertyTypeImpl::kDouble);
}
PropertyType PropertyType::date() {
  return PropertyType(impl::PropertyTypeImpl::kDate);
}
PropertyType PropertyType::string() {
  return PropertyType(impl::PropertyTypeImpl::kString);
}
PropertyType PropertyType::string_map() {
  return PropertyType(impl::PropertyTypeImpl::kStringMap);
}
PropertyType PropertyType::varchar(int32_t max_length) {
  return PropertyType(impl::PropertyTypeImpl::kVarChar, max_length);
}

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const PropertyType& value) {
  in_archive << value.type_enum;
  if (value.type_enum == impl::PropertyTypeImpl::kVarChar) {
    in_archive << value.additional_type_info.max_length;
  }
  return in_archive;
}
grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                              PropertyType& value) {
  out_archive >> value.type_enum;
  if (value.type_enum == impl::PropertyTypeImpl::kVarChar) {
    out_archive >> value.additional_type_info.max_length;
  }
  return out_archive;
}

grape::InArchive& operator<<(grape::InArchive& in_archive, const Any& value) {
  if (value.type == PropertyType::empty()) {
    in_archive << value.type;
  } else if (value.type == PropertyType::bool_()) {
    in_archive << value.type << value.value.b;
  } else if (value.type == PropertyType::uint8()) {
    in_archive << value.type << value.value.u8;
  } else if (value.type == PropertyType::uint16()) {
    in_archive << value.type << value.value.u16;
  } else if (value.type == PropertyType::int32()) {
    in_archive << value.type << value.value.i;
  } else if (value.type == PropertyType::uint32()) {
    in_archive << value.type << value.value.ui;
  } else if (value.type == PropertyType::float_()) {
    in_archive << value.type << value.value.f;
  } else if (value.type == PropertyType::int64()) {
    in_archive << value.type << value.value.l;
  } else if (value.type == PropertyType::uint64()) {
    in_archive << value.type << value.value.ul;
  } else if (value.type == PropertyType::double_()) {
    in_archive << value.type << value.value.db;
  } else if (value.type == PropertyType::date()) {
    in_archive << value.type << value.value.d.milli_second;
  } else if (value.type == PropertyType::string()) {
    in_archive << value.type << value.value.s;
  } else if (value.type.type_enum == impl::PropertyTypeImpl::kVarChar) {
    in_archive << value.type << value.value.vc;
  } else {
    in_archive << PropertyType::kEmpty;
  }

  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive, Any& value) {
  out_archive >> value.type;
  if (value.type == PropertyType::empty()) {
  } else if (value.type == PropertyType::bool_()) {
    out_archive >> value.value.b;
  } else if (value.type == PropertyType::uint8()) {
    out_archive >> value.value.u8;
  } else if (value.type == PropertyType::uint16()) {
    out_archive >> value.value.u16;
  } else if (value.type == PropertyType::int32()) {
    out_archive >> value.value.i;
  } else if (value.type == PropertyType::uint32()) {
    out_archive >> value.value.ui;
  } else if (value.type == PropertyType::float_()) {
    out_archive >> value.value.f;
  } else if (value.type == PropertyType::int64()) {
    out_archive >> value.value.l;
  } else if (value.type == PropertyType::uint64()) {
    out_archive >> value.value.ul;
  } else if (value.type == PropertyType::double_()) {
    out_archive >> value.value.db;
  } else if (value.type == PropertyType::date()) {
    out_archive >> value.value.d.milli_second;
  } else if (value.type == PropertyType::string()) {
    out_archive >> value.value.s;
  } else if (value.type.type_enum == impl::PropertyTypeImpl::kVarChar) {
    out_archive >> value.value.vc;
  } else {
    value.type = PropertyType::kEmpty;
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

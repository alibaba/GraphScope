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
const PropertyType PropertyType::kTimeStamp =
    PropertyType(impl::PropertyTypeImpl::kTimeStamp);
const PropertyType PropertyType::kString =
    PropertyType(impl::PropertyTypeImpl::kString);
const PropertyType PropertyType::kStringMap =
    PropertyType(impl::PropertyTypeImpl::kStringMap);
const PropertyType PropertyType::kVertexGlobalId =
    PropertyType(impl::PropertyTypeImpl::kVertexGlobalId);
const PropertyType PropertyType::kLabel =
    PropertyType(impl::PropertyTypeImpl::kLabel);

bool PropertyType::operator==(const PropertyType& other) const {
  if (type_enum == impl::PropertyTypeImpl::kVarChar &&
      other.type_enum == impl::PropertyTypeImpl::kVarChar) {
    return additional_type_info.max_length ==
           other.additional_type_info.max_length;
  }
  if ((type_enum == impl::PropertyTypeImpl::kString &&
       other.type_enum == impl::PropertyTypeImpl::kVarChar) ||
      (type_enum == impl::PropertyTypeImpl::kVarChar &&
       other.type_enum == impl::PropertyTypeImpl::kString)) {
    return true;
  }
  return type_enum == other.type_enum;
}

bool PropertyType::operator!=(const PropertyType& other) const {
  return !(*this == other);
}

bool PropertyType::IsVarchar() const {
  return type_enum == impl::PropertyTypeImpl::kVarChar;
}

/////////////////////////////// Get Type Instance
//////////////////////////////////
PropertyType PropertyType::Empty() {
  return PropertyType(impl::PropertyTypeImpl::kEmpty);
}
PropertyType PropertyType::Bool() {
  return PropertyType(impl::PropertyTypeImpl::kBool);
}
PropertyType PropertyType::UInt8() {
  return PropertyType(impl::PropertyTypeImpl::kUInt8);
}
PropertyType PropertyType::UInt16() {
  return PropertyType(impl::PropertyTypeImpl::kUInt16);
}
PropertyType PropertyType::Int32() {
  return PropertyType(impl::PropertyTypeImpl::kInt32);
}
PropertyType PropertyType::UInt32() {
  return PropertyType(impl::PropertyTypeImpl::kUInt32);
}
PropertyType PropertyType::Float() {
  return PropertyType(impl::PropertyTypeImpl::kFloat);
}
PropertyType PropertyType::Int64() {
  return PropertyType(impl::PropertyTypeImpl::kInt64);
}
PropertyType PropertyType::UInt64() {
  return PropertyType(impl::PropertyTypeImpl::kUInt64);
}
PropertyType PropertyType::Double() {
  return PropertyType(impl::PropertyTypeImpl::kDouble);
}
PropertyType PropertyType::TimeStamp() {
  return PropertyType(impl::PropertyTypeImpl::kTimeStamp);
}
PropertyType PropertyType::Date() {
  return PropertyType(impl::PropertyTypeImpl::kDate);
}
PropertyType PropertyType::String() {
  return PropertyType(impl::PropertyTypeImpl::kString);
}
PropertyType PropertyType::StringMap() {
  return PropertyType(impl::PropertyTypeImpl::kStringMap);
}
PropertyType PropertyType::Varchar(uint16_t max_length) {
  return PropertyType(impl::PropertyTypeImpl::kVarChar, max_length);
}

PropertyType PropertyType::VertexGlobalId() {
  return PropertyType(impl::PropertyTypeImpl::kVertexGlobalId);
}

PropertyType PropertyType::Label() {
  return PropertyType(impl::PropertyTypeImpl::kLabel);
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
  if (value.type == PropertyType::Empty()) {
    in_archive << value.type;
  } else if (value.type == PropertyType::Bool()) {
    in_archive << value.type << value.value.b;
  } else if (value.type == PropertyType::UInt8()) {
    in_archive << value.type << value.value.u8;
  } else if (value.type == PropertyType::UInt16()) {
    in_archive << value.type << value.value.u16;
  } else if (value.type == PropertyType::Int32()) {
    in_archive << value.type << value.value.i;
  } else if (value.type == PropertyType::UInt32()) {
    in_archive << value.type << value.value.ui;
  } else if (value.type == PropertyType::Float()) {
    in_archive << value.type << value.value.f;
  } else if (value.type == PropertyType::Int64()) {
    in_archive << value.type << value.value.l;
  } else if (value.type == PropertyType::UInt64()) {
    in_archive << value.type << value.value.ul;
  } else if (value.type == PropertyType::Double()) {
    in_archive << value.type << value.value.db;
  } else if (value.type == PropertyType::TimeStamp()) {
    in_archive << value.type << value.value.d.milli_second;
  } else if (value.type == PropertyType::Date()) {
    in_archive << value.type << value.value.day.to_u32();
  } else if (value.type == PropertyType::String()) {
    in_archive << value.type << value.value.s;
  } else if (value.type == PropertyType::VertexGlobalId()) {
    in_archive << value.type << value.value.vertex_gid;
  } else if (value.type == PropertyType::Label()) {
    in_archive << value.type << value.value.label_key;
  } else {
    in_archive << PropertyType::kEmpty;
  }

  return in_archive;
}

grape::OutArchive& operator>>(grape::OutArchive& out_archive, Any& value) {
  out_archive >> value.type;
  if (value.type == PropertyType::Empty()) {
  } else if (value.type == PropertyType::Bool()) {
    out_archive >> value.value.b;
  } else if (value.type == PropertyType::UInt8()) {
    out_archive >> value.value.u8;
  } else if (value.type == PropertyType::UInt16()) {
    out_archive >> value.value.u16;
  } else if (value.type == PropertyType::Int32()) {
    out_archive >> value.value.i;
  } else if (value.type == PropertyType::UInt32()) {
    out_archive >> value.value.ui;
  } else if (value.type == PropertyType::Float()) {
    out_archive >> value.value.f;
  } else if (value.type == PropertyType::Int64()) {
    out_archive >> value.value.l;
  } else if (value.type == PropertyType::UInt64()) {
    out_archive >> value.value.ul;
  } else if (value.type == PropertyType::Double()) {
    out_archive >> value.value.db;
  } else if (value.type == PropertyType::TimeStamp()) {
    int64_t ts;
    out_archive >> ts;
    value.value.d.milli_second = ts;
  } else if (value.type == PropertyType::Date()) {
    uint32_t val;
    out_archive >> val;
    value.value.day.from_u32(val);
  } else if (value.type == PropertyType::String()) {
    out_archive >> value.value.s;
  } else if (value.type == PropertyType::VertexGlobalId()) {
    out_archive >> value.value.vertex_gid;
  } else if (value.type == PropertyType::Label()) {
    out_archive >> value.value.label_key;
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

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const GlobalId& value) {
  in_archive << value.global_id;
  return in_archive;
}
grape::OutArchive& operator>>(grape::OutArchive& out_archive, GlobalId& value) {
  out_archive >> value.global_id;
  return out_archive;
}

grape::InArchive& operator<<(grape::InArchive& in_archive,
                             const LabelKey& value) {
  in_archive << value.label_id;
  return in_archive;
}
grape::OutArchive& operator>>(grape::OutArchive& out_archive, LabelKey& value) {
  out_archive >> value.label_id;
  return out_archive;
}

GlobalId::label_id_t GlobalId::get_label_id(gid_t gid) {
  return static_cast<label_id_t>(gid >> label_id_offset);
}

GlobalId::vid_t GlobalId::get_vid(gid_t gid) {
  return static_cast<vid_t>(gid & vid_mask);
}

GlobalId::GlobalId() : global_id(0) {}

GlobalId::GlobalId(label_id_t label_id, vid_t vid) {
  global_id = (static_cast<uint64_t>(label_id) << label_id_offset) | vid;
}

GlobalId::GlobalId(gid_t gid) : global_id(gid) {}

GlobalId::label_id_t GlobalId::label_id() const {
  return static_cast<label_id_t>(global_id >> label_id_offset);
}

GlobalId::vid_t GlobalId::vid() const {
  return static_cast<vid_t>(global_id & vid_mask);
}

std::string GlobalId::to_string() const { return std::to_string(global_id); }

TimeStamp::TimeStamp(int64_t x) : milli_second(x) {}

std::string TimeStamp::to_string() const { return std::to_string(milli_second); }

Date::Date(int64_t ts) { from_timestamp(ts); }

std::string Date::to_string() const {
  return std::to_string(static_cast<int>(year())) + "-" +
         std::to_string(static_cast<int>(month())) + "-" +
         std::to_string(static_cast<int>(day()));
}

uint32_t Date::to_u32() const { return value.integer; }

void Date::from_u32(uint32_t val) { value.integer = val; }

int Date::year() const { return value.internal.year; }

int Date::month() const { return value.internal.month; }

int Date::day() const { return value.internal.day; }


}  // namespace gs

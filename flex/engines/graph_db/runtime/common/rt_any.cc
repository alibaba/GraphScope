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

#include "flex/engines/graph_db/runtime/common/rt_any.h"

namespace gs {

namespace runtime {
RTAny List::get(size_t idx) const { return impl_->get(idx); }

RTAnyType Set::elem_type() const { return impl_->type(); }

RTAnyType List::elem_type() const { return impl_->type(); }

RTAny Tuple::get(size_t idx) const { return impl_->get(idx); }
PropertyType rt_type_to_property_type(RTAnyType type) {
  switch (type) {
  case RTAnyType::kEmpty:
    return PropertyType::kEmpty;
  case RTAnyType::kI64Value:
    return PropertyType::kInt64;
  case RTAnyType::kI32Value:
    return PropertyType::kInt32;
  case RTAnyType::kF64Value:
    return PropertyType::kDouble;
  case RTAnyType::kBoolValue:
    return PropertyType::kBool;
  case RTAnyType::kStringValue:
    return PropertyType::kString;
  case RTAnyType::kTimestamp:
    return PropertyType::kDate;
  case RTAnyType::kDate32:
    return PropertyType::kDay;
  default:
    LOG(FATAL) << "not support for " << static_cast<int>(type);
  }
}
RTAny::RTAny() : type_(RTAnyType::kUnknown) {}
RTAny::RTAny(RTAnyType type) : type_(type) {}

RTAny::RTAny(const Any& val) {
  if (val.type == PropertyType::Int64()) {
    type_ = RTAnyType::kI64Value;
    value_.i64_val = val.AsInt64();
  } else if (val.type == PropertyType::String()) {
    type_ = RTAnyType::kStringValue;
    value_.str_val = val.AsStringView();
  } else if (val.type == PropertyType::Date()) {
    type_ = RTAnyType::kI64Value;
    value_.date = val.AsDate();
  } else if (val.type == PropertyType::Day()) {
    type_ = RTAnyType::kDate32;
    value_.day = val.AsDay();
  } else if (val.type == PropertyType::Int32()) {
    type_ = RTAnyType::kI32Value;
    value_.i32_val = val.AsInt32();
  } else if (val.type == PropertyType::kDouble) {
    type_ = RTAnyType::kF64Value;
    value_.f64_val = val.AsDouble();
  } else if (val.type == PropertyType::Bool()) {
    type_ = RTAnyType::kBoolValue;
    value_.b_val = val.AsBool();
  } else if (val.type == PropertyType::Empty()) {
    type_ = RTAnyType::kNull;
  } else {
    LOG(FATAL) << "Any value: " << val.to_string()
               << ", type = " << val.type.type_enum;
  }
}

RTAny::RTAny(const EdgeData& val) {
  if (val.type == RTAnyType::kI64Value) {
    type_ = RTAnyType::kI64Value;
    value_.i64_val = val.value.i64_val;
  } else if (val.type == RTAnyType::kStringValue) {
    type_ = RTAnyType::kStringValue;
    value_.str_val =
        std::string_view(val.value.str_val.data(), val.value.str_val.size());
  } else if (val.type == RTAnyType::kI32Value) {
    type_ = RTAnyType::kI32Value;
    value_.i32_val = val.value.i32_val;
  } else if (val.type == RTAnyType::kF64Value) {
    type_ = RTAnyType::kF64Value;
    value_.f64_val = val.value.f64_val;
  } else if (val.type == RTAnyType::kBoolValue) {
    type_ = RTAnyType::kBoolValue;
    value_.b_val = val.value.b_val;
  } else if (val.type == RTAnyType::kTimestamp) {
    type_ = RTAnyType::kTimestamp;
    value_.date = val.value.date_val;
  } else if (val.type == RTAnyType::kDate32) {
    type_ = RTAnyType::kDate32;
    value_.day = val.value.day_val;
  } else {
    LOG(FATAL) << "Any value: " << val.to_string()
               << ", type = " << static_cast<int>(val.type);
  }
}

RTAny::RTAny(const Path& p) {
  type_ = RTAnyType::kPath;
  value_.p = p;
}
RTAny::RTAny(const RTAny& rhs) : type_(rhs.type_) {
  if (type_ == RTAnyType::kBoolValue) {
    value_.b_val = rhs.value_.b_val;
  } else if (type_ == RTAnyType::kI64Value) {
    value_.i64_val = rhs.value_.i64_val;
  } else if (type_ == RTAnyType::kI32Value) {
    value_.i32_val = rhs.value_.i32_val;
  } else if (type_ == RTAnyType::kVertex) {
    value_.vertex = rhs.value_.vertex;
  } else if (type_ == RTAnyType::kStringValue) {
    value_.str_val = rhs.value_.str_val;
  } else if (type_ == RTAnyType::kNull) {
    // do nothing
  } else if (type_ == RTAnyType::kTuple) {
    value_.t = rhs.value_.t;
  } else if (type_ == RTAnyType::kList) {
    value_.list = rhs.value_.list;
  } else if (type_ == RTAnyType::kF64Value) {
    value_.f64_val = rhs.value_.f64_val;
  } else if (type_ == RTAnyType::kMap) {
    value_.map = rhs.value_.map;
  } else if (type_ == RTAnyType::kRelation) {
    value_.relation = rhs.value_.relation;
  } else if (type_ == RTAnyType::kDate32) {
    value_.day = rhs.value_.day;
  } else if (type_ == RTAnyType::kTimestamp) {
    value_.date = rhs.value_.date;
  } else if (type_ == RTAnyType::kEdge) {
    value_.edge = rhs.value_.edge;
  } else {
    LOG(FATAL) << "unexpected type: " << static_cast<int>(type_);
  }
}

RTAny& RTAny::operator=(const RTAny& rhs) {
  type_ = rhs.type_;
  if (type_ == RTAnyType::kBoolValue) {
    value_.b_val = rhs.value_.b_val;
  } else if (type_ == RTAnyType::kI64Value) {
    value_.i64_val = rhs.value_.i64_val;
  } else if (type_ == RTAnyType::kI32Value) {
    value_.i32_val = rhs.value_.i32_val;
  } else if (type_ == RTAnyType::kVertex) {
    value_.vertex = rhs.value_.vertex;
  } else if (type_ == RTAnyType::kStringValue) {
    value_.str_val = rhs.value_.str_val;
  } else if (type_ == RTAnyType::kTuple) {
    value_.t = rhs.value_.t;
  } else if (type_ == RTAnyType::kList) {
    value_.list = rhs.value_.list;
  } else if (type_ == RTAnyType::kF64Value) {
    value_.f64_val = rhs.value_.f64_val;
  } else if (type_ == RTAnyType::kMap) {
    value_.map = rhs.value_.map;
  } else if (type_ == RTAnyType::kEdge) {
    value_.edge = rhs.value_.edge;
  } else if (type_ == RTAnyType::kRelation) {
    value_.relation = rhs.value_.relation;
  } else if (type_ == RTAnyType::kPath) {
    value_.p = rhs.value_.p;
  } else if (type_ == RTAnyType::kDate32) {
    value_.day = rhs.value_.day;
  } else if (type_ == RTAnyType::kTimestamp) {
    value_.date = rhs.value_.date;
  } else {
    LOG(FATAL) << "unexpected type: " << static_cast<int>(type_);
  }
  return *this;
}

RTAnyType RTAny::type() const { return type_; }

Any RTAny::to_any() const {
  switch (type_) {
  case RTAnyType::kBoolValue:
    return Any(value_.b_val);
  case RTAnyType::kI64Value:
    return Any(value_.i64_val);
  case RTAnyType::kI32Value:
    return Any(value_.i32_val);
  case RTAnyType::kF64Value:
    return Any(value_.f64_val);
  case RTAnyType::kStringValue:
    return Any(std::string(value_.str_val));

  case RTAnyType::kDate32:
    return Any(value_.day);
  case RTAnyType::kTimestamp:
    return Any(value_.date);
  default:
    LOG(FATAL) << "not support for " << static_cast<int>(type_);
  }
}

RTAny RTAny::from_vertex(label_t l, vid_t v) {
  RTAny ret;
  ret.type_ = RTAnyType::kVertex;
  ret.value_.vertex.label_ = l;
  ret.value_.vertex.vid_ = v;
  return ret;
}

RTAny RTAny::from_vertex(VertexRecord v) {
  RTAny ret;
  ret.type_ = RTAnyType::kVertex;
  ret.value_.vertex = v;
  return ret;
}

RTAny RTAny::from_edge(const EdgeRecord& v) {
  RTAny ret;
  ret.type_ = RTAnyType::kEdge;
  ret.value_.edge = v;
  return ret;
}

RTAny RTAny::from_bool(bool v) {
  RTAny ret;
  ret.type_ = RTAnyType::kBoolValue;
  ret.value_.b_val = v;
  return ret;
}

RTAny RTAny::from_int64(int64_t v) {
  RTAny ret;
  ret.type_ = RTAnyType::kI64Value;
  ret.value_.i64_val = v;
  return ret;
}

RTAny RTAny::from_uint64(uint64_t v) {
  RTAny ret;
  ret.type_ = RTAnyType::kU64Value;
  ret.value_.u64_val = v;
  return ret;
}

RTAny RTAny::from_int32(int v) {
  RTAny ret;
  ret.type_ = RTAnyType::kI32Value;
  ret.value_.i32_val = v;
  return ret;
}

RTAny RTAny::from_string(const std::string& str) {
  RTAny ret;
  ret.type_ = RTAnyType::kStringValue;
  ret.value_.str_val = std::string_view(str);
  return ret;
}

RTAny RTAny::from_string(const std::string_view& str) {
  RTAny ret;
  ret.type_ = RTAnyType::kStringValue;
  ret.value_.str_val = str;
  return ret;
}

RTAny RTAny::from_date32(Day v) {
  RTAny ret;
  ret.type_ = RTAnyType::kDate32;
  ret.value_.day = v;
  return ret;
}

RTAny RTAny::from_timestamp(Date v) {
  RTAny ret;
  ret.type_ = RTAnyType::kTimestamp;
  ret.value_.date = v;
  return ret;
}
RTAny RTAny::from_tuple(const Tuple& t) {
  RTAny ret;
  ret.type_ = RTAnyType::kTuple;
  ret.value_.t = t;
  return ret;
}

RTAny RTAny::from_list(const List& l) {
  RTAny ret;
  ret.type_ = RTAnyType::kList;
  ret.value_.list = std::move(l);
  return ret;
}

RTAny RTAny::from_double(double v) {
  RTAny ret;
  ret.type_ = RTAnyType::kF64Value;
  ret.value_.f64_val = v;
  return ret;
}

RTAny RTAny::from_map(const Map& m) {
  RTAny ret;
  ret.type_ = RTAnyType::kMap;
  ret.value_.map = std::move(m);
  return ret;
}

RTAny RTAny::from_set(const Set& s) {
  RTAny ret;
  ret.type_ = RTAnyType::kSet;
  ret.value_.set = s;
  return ret;
}

RTAny RTAny::from_relation(const Relation& r) {
  RTAny ret;
  ret.type_ = RTAnyType::kRelation;
  ret.value_.relation = r;
  return ret;
}

bool RTAny::as_bool() const {
  if (type_ == RTAnyType::kNull) {
    return false;
  }
  assert(type_ == RTAnyType::kBoolValue);
  return value_.b_val;
}
int RTAny::as_int32() const {
  assert(type_ == RTAnyType::kI32Value);
  return value_.i32_val;
}
int64_t RTAny::as_int64() const {
  assert(type_ == RTAnyType::kI64Value);
  return value_.i64_val;
}
uint64_t RTAny::as_uint64() const {
  assert(type_ == RTAnyType::kU64Value);
  return value_.u64_val;
}
Day RTAny::as_date32() const {
  assert(type_ == RTAnyType::kDate32);
  return value_.day;
}

Date RTAny::as_timestamp() const {
  assert(type_ == RTAnyType::kTimestamp);
  return value_.date;
}

double RTAny::as_double() const {
  assert(type_ == RTAnyType::kF64Value);
  return value_.f64_val;
}

VertexRecord RTAny::as_vertex() const {
  assert(type_ == RTAnyType::kVertex);
  return value_.vertex;
}

const EdgeRecord& RTAny::as_edge() const {
  assert(type_ == RTAnyType::kEdge);
  return value_.edge;
}

Set RTAny::as_set() const {
  assert(type_ == RTAnyType::kSet);
  return value_.set;
}

std::string_view RTAny::as_string() const {
  if (type_ == RTAnyType::kStringValue) {
    return value_.str_val;
  } else if (type_ == RTAnyType::kUnknown) {
    return std::string_view();
  } else {
    LOG(FATAL) << "unexpected type" << static_cast<int>(type_);
    return std::string_view();
  }
}

List RTAny::as_list() const {
  assert(type_ == RTAnyType::kList);
  return value_.list;
}

Path RTAny::as_path() const {
  assert(type_ == RTAnyType::kPath);
  return value_.p;
}

Tuple RTAny::as_tuple() const {
  assert(type_ == RTAnyType::kTuple);
  return value_.t;
}

Map RTAny::as_map() const {
  assert(type_ == RTAnyType::kMap);
  return value_.map;
}

Relation RTAny::as_relation() const {
  assert(type_ == RTAnyType::kRelation);
  return value_.relation;
}

RTAny TupleImpl<RTAny>::get(size_t idx) const {
  CHECK(idx < values.size());
  return values[idx];
}

int RTAny::numerical_cmp(const RTAny& other) const {
  switch (type_) {
  case RTAnyType::kI64Value:
    switch (other.type_) {
    case RTAnyType::kI32Value: {
      auto ret = value_.i64_val - other.value_.i32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kF64Value: {
      auto ret = value_.i64_val - other.value_.f64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    default:
      LOG(FATAL) << "not support for " << static_cast<int>(other.type_);
    }
    break;
  case RTAnyType::kI32Value:
    switch (other.type_) {
    case RTAnyType::kI64Value: {
      auto ret = value_.i32_val - other.value_.i64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kF64Value: {
      auto ret = value_.i32_val - other.value_.f64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }

    default:
      LOG(FATAL) << "not support for " << static_cast<int>(other.type_);
    }
    break;
  case RTAnyType::kF64Value:
    switch (other.type_) {
    case RTAnyType::kI64Value: {
      auto ret = value_.f64_val - other.value_.i64_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    case RTAnyType::kI32Value: {
      auto ret = value_.f64_val - other.value_.i32_val;
      return ret > 0 ? 1 : (ret < 0 ? -1 : 0);
    }
    default:
      LOG(FATAL) << "not support for " << static_cast<int>(type_);
    }
    break;
  default:
    LOG(FATAL) << "not support for " << static_cast<int>(type_);
  }
}
inline static bool is_numerical_type(const RTAnyType& type) {
  return type == RTAnyType::kI64Value || type == RTAnyType::kI32Value ||
         type == RTAnyType::kF64Value;
}

bool RTAny::operator<(const RTAny& other) const {
  if (type_ != other.type_) {
    if (is_numerical_type(type_) && is_numerical_type(other.type_)) {
      return numerical_cmp(other) < 0;
    } else {
      return false;
    }
  }
  if (type_ == RTAnyType::kI64Value) {
    return value_.i64_val < other.value_.i64_val;

  } else if (type_ == RTAnyType::kI32Value) {
    return value_.i32_val < other.value_.i32_val;

  } else if (type_ == RTAnyType::kStringValue) {
    return value_.str_val < other.value_.str_val;
  } else if (type_ == RTAnyType::kDate32) {
    return value_.day < other.value_.day;
  } else if (type_ == RTAnyType::kTimestamp) {
    return value_.date < other.value_.date;
  } else if (type_ == RTAnyType::kF64Value) {
    return value_.f64_val < other.value_.f64_val;
  } else if (type_ == RTAnyType::kEdge) {
    return value_.edge < other.value_.edge;
  } else if (type_ == RTAnyType::kVertex) {
    return value_.vertex < other.value_.vertex;
  }

  LOG(FATAL) << "not support for " << static_cast<int>(type_);
  return true;
}

bool RTAny::operator==(const RTAny& other) const {
  if (type_ != other.type_) {
    if (is_numerical_type(type_) && is_numerical_type(other.type_)) {
      return numerical_cmp(other) == 0;
    } else {
      return false;
    }
  }

  if (type_ == RTAnyType::kI64Value) {
    return value_.i64_val == other.value_.i64_val;
  } else if (type_ == RTAnyType::kU64Value) {
    return value_.u64_val == other.value_.u64_val;
  } else if (type_ == RTAnyType::kI32Value) {
    return value_.i32_val == other.value_.i32_val;
  } else if (type_ == RTAnyType::kF64Value) {
    return value_.f64_val == other.value_.f64_val;
  } else if (type_ == RTAnyType::kBoolValue) {
    return value_.b_val == other.value_.b_val;
  } else if (type_ == RTAnyType::kStringValue) {
    return value_.str_val == other.value_.str_val;
  } else if (type_ == RTAnyType::kVertex) {
    return value_.vertex == other.value_.vertex;
  } else if (type_ == RTAnyType::kDate32) {
    return value_.day == other.value_.day;
  } else if (type_ == RTAnyType::kTimestamp) {
    return value_.date == other.value_.date;
  }

  LOG(FATAL) << "not support..." << static_cast<int>(type_);
  return true;
}

RTAny RTAny::operator+(const RTAny& other) const {
  int64_t left_i64 = 0;
  double left_f64 = 0;
  bool has_i64 = false;
  bool has_f64 = false;

  if (type_ == RTAnyType::kI32Value) {
    left_i64 = value_.i32_val;
    left_f64 = value_.i32_val;
  } else if (type_ == RTAnyType::kI64Value) {
    left_i64 = value_.i64_val;
    left_f64 = value_.i64_val;
    has_i64 = true;
  } else if (type_ == RTAnyType::kF64Value) {
    left_f64 = value_.f64_val;
    has_f64 = true;
  } else {
    LOG(FATAL) << "not support" << static_cast<int>(type_);
  }

  int64_t right_i64 = 0;
  double right_f64 = 0;

  if (other.type_ == RTAnyType::kI32Value) {
    right_i64 = other.value_.i32_val;
    right_f64 = other.value_.i32_val;
  } else if (other.type_ == RTAnyType::kI64Value) {
    right_i64 = other.value_.i64_val;
    right_f64 = other.value_.i64_val;
    has_i64 = true;
  } else if (other.type_ == RTAnyType::kF64Value) {
    right_f64 = other.value_.f64_val;
    has_f64 = true;
  } else {
    LOG(FATAL) << "not support" << static_cast<int>(type_);
  }
  if (has_f64) {
    return RTAny::from_double(left_f64 + right_f64);
  } else if (has_i64) {
    return RTAny::from_int64(left_i64 + right_i64);
  } else {
    return RTAny::from_int32(value_.i32_val + other.value_.i32_val);
  }
}

RTAny RTAny::operator-(const RTAny& other) const {
  if (type_ == RTAnyType::kI64Value && other.type_ == RTAnyType::kI32Value) {
    return RTAny::from_int64(value_.i64_val - other.value_.i32_val);
  } else if (type_ == RTAnyType::kI32Value &&
             other.type_ == RTAnyType::kI64Value) {
    return RTAny::from_int64(value_.i32_val * 1l - other.value_.i64_val);
  }
  if (type_ == RTAnyType::kF64Value) {
    return RTAny::from_double(value_.f64_val - other.value_.f64_val);
  } else if (type_ == RTAnyType::kI64Value) {
    return RTAny::from_int64(value_.i64_val - other.value_.i64_val);
  } else if (type_ == RTAnyType::kI32Value) {
    return RTAny::from_int32(value_.i32_val - other.value_.i32_val);
  }
  LOG(FATAL) << "not support";
  return RTAny();
}

RTAny RTAny::operator/(const RTAny& other) const {
  bool has_i64 = false;
  bool has_f64 = false;
  double left_f64 = 0;
  int64_t left_i64 = 0;
  if (type_ == RTAnyType::kI64Value) {
    left_i64 = value_.i64_val;
    left_f64 = value_.i64_val;
    has_i64 = true;
  } else if (type_ == RTAnyType::kF64Value) {
    left_f64 = value_.f64_val;
    has_f64 = true;
  } else if (type_ == RTAnyType::kI32Value) {
    left_i64 = value_.i32_val;
    left_f64 = value_.i32_val;
  } else {
    LOG(FATAL) << "not support" << static_cast<int>(type_);
  }

  double right_f64 = 0;
  int right_i64 = 0;
  if (other.type_ == RTAnyType::kI64Value) {
    right_i64 = other.value_.i64_val;
    right_f64 = other.value_.i64_val;
    has_i64 = true;
  } else if (other.type_ == RTAnyType::kF64Value) {
    right_f64 = other.value_.f64_val;
    has_f64 = true;
  } else if (other.type_ == RTAnyType::kI32Value) {
    right_i64 = other.value_.i32_val;
    right_f64 = other.value_.i32_val;
  } else {
    LOG(FATAL) << "not support" << static_cast<int>(other.type_);
  }

  if (has_f64) {
    return RTAny::from_double(left_f64 / right_f64);
  } else if (has_i64) {
    return RTAny::from_int64(left_i64 / right_i64);
  } else {
    return RTAny::from_int32(value_.i32_val / other.value_.i32_val);
  }
}

RTAny RTAny::operator%(const RTAny& other) const {
  bool has_i64 = false;
  int64_t left_i64 = 0;
  if (type_ == RTAnyType::kI64Value) {
    left_i64 = value_.i64_val;
    has_i64 = true;
  } else if (type_ == RTAnyType::kI32Value) {
    left_i64 = value_.i32_val;
  } else {
    LOG(FATAL) << "not support" << static_cast<int>(type_);
  }

  int64_t right_i64 = 0;
  if (other.type_ == RTAnyType::kI64Value) {
    right_i64 = other.value_.i64_val;
    has_i64 = true;
  } else if (other.type_ == RTAnyType::kI32Value) {
    right_i64 = other.value_.i32_val;
  } else {
    LOG(FATAL) << "not support" << static_cast<int>(other.type_);
  }
  if (has_i64) {
    return RTAny::from_int64(left_i64 % right_i64);
  } else {
    return RTAny::from_int32(value_.i32_val % other.value_.i32_val);
  }
}

void RTAny::sink_impl(common::Value* value) const {
  if (type_ == RTAnyType::kI64Value) {
    value->set_i64(value_.i64_val);
  } else if (type_ == RTAnyType::kStringValue) {
    value->set_str(value_.str_val.data(), value_.str_val.size());
  } else if (type_ == RTAnyType::kI32Value) {
    value->set_i32(value_.i32_val);
  } else if (type_ == RTAnyType::kDate32) {
    value->set_i64(value_.day.to_timestamp());
  } else if (type_ == RTAnyType::kTimestamp) {
    value->set_i64(value_.date.milli_second);
  } else if (type_ == RTAnyType::kBoolValue) {
    value->set_boolean(value_.b_val);
  } else if (type_ == RTAnyType::kF64Value) {
    value->set_f64(value_.f64_val);
  } else if (type_ == RTAnyType::kList) {
    LOG(FATAL) << "not support list sink";
  } else if (type_ == RTAnyType::kTuple) {
    auto tup = value_.t;
    for (size_t i = 0; i < tup.size(); ++i) {
      std::string s = tup.get(i).to_string();
      value->mutable_str_array()->add_item(s.data(), s.size());
    }
  } else {
    LOG(FATAL) << "not implemented for " << static_cast<int>(type_);
  }
}

static void sink_any(const Any& any, common::Value* value) {
  if (any.type == PropertyType::Int64()) {
    value->set_i64(any.AsInt64());
  } else if (any.type == PropertyType::StringView()) {
    auto str = any.AsStringView();
    value->set_str(str.data(), str.size());
  } else if (any.type == PropertyType::Date()) {
    value->set_i64(any.AsDate().milli_second);
  } else if (any.type == PropertyType::Int32()) {
    value->set_i32(any.AsInt32());
  } else if (any.type == PropertyType::Double()) {
    value->set_f64(any.AsDouble());
  } else if (any.type == PropertyType::Bool()) {
    value->set_boolean(any.AsBool());
  } else if (any.type == PropertyType::Double()) {
    value->set_f64(any.AsDouble());
  } else if (any.type == PropertyType::Empty()) {
    value->mutable_none();
  } else if (any.type == PropertyType::Day()) {
    value->set_i64(any.AsDay().to_timestamp());
  } else {
    LOG(FATAL) << "Any value: " << any.to_string()
               << ", type = " << any.type.type_enum;
  }
}

static void sink_edge_data(const EdgeData& any, common::Value* value) {
  if (any.type == RTAnyType::kI64Value) {
    value->set_i64(any.value.i64_val);
  } else if (any.type == RTAnyType::kStringValue) {
    value->set_str(any.value.str_val.data(), any.value.str_val.size());
  } else if (any.type == RTAnyType::kI32Value) {
    value->set_i32(any.value.i32_val);
  } else if (any.type == RTAnyType::kF64Value) {
    value->set_f64(any.value.f64_val);
  } else if (any.type == RTAnyType::kBoolValue) {
    value->set_boolean(any.value.b_val);
  } else if (any.type == RTAnyType::kTimestamp) {
    value->set_i64(any.value.date_val.milli_second);
  } else {
    LOG(FATAL) << "Any value: " << any.to_string()
               << ", type = " << static_cast<int>(any.type);
  }
}

void sink_vertex(const GraphReadInterface& graph, const VertexRecord& vertex,
                 results::Vertex* v) {
  v->mutable_label()->set_id(vertex.label_);
  v->set_id(encode_unique_vertex_id(vertex.label_, vertex.vid_));
  const auto& names = graph.schema().get_vertex_property_names(vertex.label_);
  // Add primary keys first, since primary keys are also the properties of a
  // vertex
  auto& primary_key = graph.schema().get_vertex_primary_key(vertex.label_);
  if (primary_key.size() > 1) {
    LOG(ERROR) << "Currently only support single primary key";
  }
  auto pk_name = std::get<1>(primary_key[0]);
  auto pk_prop = v->add_properties();
  pk_prop->mutable_key()->set_name(pk_name);
  sink_any(graph.GetVertexId(vertex.label_, vertex.vid_),
           pk_prop->mutable_value());

  for (size_t i = 0; i < names.size(); ++i) {
    auto prop = v->add_properties();
    prop->mutable_key()->set_name(names[i]);
    sink_any(graph.GetVertexProperty(vertex.label_, vertex.vid_, i),
             prop->mutable_value());
  }
}

template void RTAny::sink(const GraphReadInterface& graph,
                          Encoder& encoder) const;

template void RTAny::sink(const GraphUpdateInterface& graph,
                          Encoder& encoder) const;

void RTAny::sink(const GraphReadInterface& graph, int id,
                 results::Column* col) const {
  col->mutable_name_or_id()->set_id(id);
  if (type_ == RTAnyType::kList) {
    auto collection = col->mutable_entry()->mutable_collection();
    for (size_t i = 0; i < value_.list.size(); ++i) {
      value_.list.get(i).sink_impl(
          collection->add_collection()->mutable_object());
    }
  } else if (type_ == RTAnyType::kSet) {
    auto collection = col->mutable_entry()->mutable_collection();
    for (auto& val : value_.set.values()) {
      val.sink_impl(collection->add_collection()->mutable_object());
    }
  } else if (type_ == RTAnyType::kTuple) {
    auto collection = col->mutable_entry()->mutable_collection();
    for (size_t i = 0; i < value_.t.size(); ++i) {
      value_.t.get(i).sink_impl(collection->add_collection()->mutable_object());
    }
  } else if (type_ == RTAnyType::kVertex) {
    auto v = col->mutable_entry()->mutable_element()->mutable_vertex();
    sink_vertex(graph, value_.vertex, v);

  } else if (type_ == RTAnyType::kMap) {
    auto mp = col->mutable_entry()->mutable_map();
    auto [keys_ptr, vals_ptr] = value_.map.key_vals();
    auto& keys = keys_ptr;
    auto& vals = vals_ptr;
    for (size_t i = 0; i < keys.size(); ++i) {
      if (vals[i].is_null()) {
        continue;
      }
      auto ret = mp->add_key_values();
      keys[i].sink_impl(ret->mutable_key());
      if (vals[i].type_ == RTAnyType::kVertex) {
        auto v = ret->mutable_value()->mutable_element()->mutable_vertex();
        sink_vertex(graph, vals[i].as_vertex(), v);
      } else {
        vals[i].sink_impl(
            ret->mutable_value()->mutable_element()->mutable_object());
      }
    }

  } else if (type_ == RTAnyType::kEdge) {
    auto e = col->mutable_entry()->mutable_element()->mutable_edge();
    auto [label, src, dst, prop, dir] = this->as_edge();
    e->mutable_src_label()->set_id(label.src_label);
    e->mutable_dst_label()->set_id(label.dst_label);
    auto edge_label = generate_edge_label_id(label.src_label, label.dst_label,
                                             label.edge_label);
    e->mutable_label()->set_id(label.edge_label);
    e->set_src_id(encode_unique_vertex_id(label.src_label, src));
    e->set_dst_id(encode_unique_vertex_id(label.dst_label, dst));
    e->set_id(encode_unique_edge_id(edge_label, src, dst));
    auto& prop_names = graph.schema().get_edge_property_names(
        label.src_label, label.dst_label, label.edge_label);
    if (prop_names.size() == 1) {
      auto props = e->add_properties();
      props->mutable_key()->set_name(prop_names[0]);
      sink_edge_data(prop, e->mutable_properties(0)->mutable_value());
    } else if (prop_names.size() > 1) {
      auto rv = prop.as<RecordView>();
      if (rv.size() != prop_names.size()) {
        LOG(ERROR) << "record view size not match with prop names";
      }
      for (size_t i = 0; i < prop_names.size(); ++i) {
        auto props = e->add_properties();
        props->mutable_key()->set_name(prop_names[i]);
        sink_any(rv[i], props->mutable_value());
      }
    }
  } else if (type_ == RTAnyType::kPath) {
    auto mutable_path =
        col->mutable_entry()->mutable_element()->mutable_graph_path();
    auto path_nodes = this->as_path().nodes();

    for (size_t i = 0; i + 1 < path_nodes.size(); ++i) {
      auto node = mutable_path->add_path()->mutable_vertex();
      node->mutable_label()->set_id(path_nodes[i].label());
      node->set_id(
          encode_unique_vertex_id(path_nodes[i].label(), path_nodes[i].vid()));
      // TODO: Add edge
    }
  } else {
    sink_impl(col->mutable_entry()->mutable_element()->mutable_object());
  }
}

// just for ldbc snb interactive queries
void RTAny::encode_sig(RTAnyType type, Encoder& encoder) const {
  if (type == RTAnyType::kI64Value) {
    encoder.put_long(this->as_int64());
  } else if (type == RTAnyType::kStringValue) {
    encoder.put_string_view(this->as_string());
  } else if (type == RTAnyType::kI32Value) {
    encoder.put_int(this->as_int32());
  } else if (type == RTAnyType::kVertex) {
    const auto& v = this->value_.vertex;
    encoder.put_byte(v.label_);
    encoder.put_int(v.vid_);
  } else if (type == RTAnyType::kEdge) {
    const auto& [label, src, dst, prop, dir] = this->as_edge();

    encoder.put_byte(label.src_label);
    encoder.put_byte(label.dst_label);
    encoder.put_byte(label.edge_label);
    encoder.put_int(src);
    encoder.put_int(dst);
    encoder.put_byte(dir == Direction::kOut ? 1 : 0);

  } else if (type == RTAnyType::kBoolValue) {
    encoder.put_byte(this->as_bool() ? 1 : 0);
  } else if (type == RTAnyType::kList) {
    encoder.put_int(this->as_list().size());
    List list = this->as_list();
    for (size_t i = 0; i < list.size(); ++i) {
      list.get(i).encode_sig(list.get(i).type(), encoder);
    }
  } else if (type == RTAnyType::kTuple) {
    Tuple tuple = this->as_tuple();
    encoder.put_int(tuple.size());
    for (size_t i = 0; i < tuple.size(); ++i) {
      tuple.get(i).encode_sig(tuple.get(i).type(), encoder);
    }
  } else if (type == RTAnyType::kNull) {
    encoder.put_int(-1);
  } else if (type == RTAnyType::kF64Value) {
    encoder.put_double(this->as_double());
  } else if (type == RTAnyType::kPath) {
    Path p = this->as_path();
    encoder.put_int(p.len() + 1);
    auto nodes = p.nodes();
    for (size_t i = 0; i < nodes.size(); ++i) {
      encoder.put_byte(nodes[i].label_);
      encoder.put_int(nodes[i].vid_);
    }
  } else if (type == RTAnyType::kRelation) {
    Relation r = this->as_relation();
    encoder.put_byte(r.label);
    encoder.put_int(r.src);
    encoder.put_int(r.dst);
  } else {
    LOG(FATAL) << "not implemented for " << static_cast<int>(type_);
  }
}

std::string RTAny::to_string() const {
  if (type_ == RTAnyType::kI64Value) {
    return std::to_string(value_.i64_val);
  } else if (type_ == RTAnyType::kStringValue) {
    return std::string(value_.str_val);
  } else if (type_ == RTAnyType::kI32Value) {
    return std::to_string(value_.i32_val);
  } else if (type_ == RTAnyType::kTimestamp) {
    return value_.date.to_string();
  } else if (type_ == RTAnyType::kDate32) {
    return value_.day.to_string();
  } else if (type_ == RTAnyType::kVertex) {
#if 0
      return std::string("v") +
             std::to_string(static_cast<int>(value_.vertex.label_)) + "-" +
             std::to_string(value_.vertex.vid_);
#else
    return std::to_string(value_.vertex.vid_);
#endif
  } else if (type_ == RTAnyType::kSet) {
    std::string ret = "{";
    for (auto& val : value_.set.values()) {
      ret += val.to_string();
      ret += ", ";
    }
    ret += "}";
    return ret;
  } else if (type_ == RTAnyType::kEdge) {
    auto [label, src, dst, prop, dir] = value_.edge;
    return std::to_string(src) + " -> " + std::to_string(dst);
  } else if (type_ == RTAnyType::kPath) {
    return value_.p.to_string();
  } else if (type_ == RTAnyType::kBoolValue) {
    return value_.b_val ? "true" : "false";
  } else if (type_ == RTAnyType::kList) {
    std::string ret = "[";
    for (size_t i = 0; i < value_.list.size(); ++i) {
      ret += value_.list.get(i).to_string();
      if (i != value_.list.size() - 1) {
        ret += ", ";
      }
    }
    ret += "]";
    return ret;
  } else if (type_ == RTAnyType::kTuple) {
    std::string ret = "(";
    for (size_t i = 0; i < value_.t.size(); ++i) {
      ret += value_.t.get(i).to_string();
      if (i != value_.t.size() - 1) {
        ret += ", ";
      }
    }
    ret += ")";
    return ret;
  } else if (type_ == RTAnyType::kNull) {
    return "null";
  } else if (type_ == RTAnyType::kF64Value) {
    return std::to_string(value_.f64_val);
  } else {
    LOG(FATAL) << "not implemented for " << static_cast<int>(type_);
    return "";
  }
}

std::shared_ptr<EdgePropVecBase> EdgePropVecBase::make_edge_prop_vec(
    PropertyType type) {
  if (type == PropertyType::Int64()) {
    return std::make_shared<EdgePropVec<int64_t>>();
  } else if (type == PropertyType::StringView()) {
    return std::make_shared<EdgePropVec<std::string_view>>();
  } else if (type == PropertyType::Date()) {
    return std::make_shared<EdgePropVec<Date>>();
  } else if (type == PropertyType::Day()) {
    return std::make_shared<EdgePropVec<Day>>();
  } else if (type == PropertyType::Int32()) {
    return std::make_shared<EdgePropVec<int32_t>>();
  } else if (type == PropertyType::Double()) {
    return std::make_shared<EdgePropVec<double>>();
  } else if (type == PropertyType::Bool()) {
    return std::make_shared<EdgePropVec<bool>>();
  } else if (type == PropertyType::Empty()) {
    return std::make_shared<EdgePropVec<grape::EmptyType>>();
  } else if (type == PropertyType::RecordView()) {
    return std::make_shared<EdgePropVec<RecordView>>();
  } else {
    LOG(FATAL) << "not support for " << type;
    return nullptr;
  }
}
}  // namespace runtime

}  // namespace gs

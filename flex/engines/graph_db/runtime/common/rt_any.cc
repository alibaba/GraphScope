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

const RTAnyType RTAnyType::kVertex =
    RTAnyType(RTAnyType::RTAnyTypeImpl::kVertex);
const RTAnyType RTAnyType::kEdge = RTAnyType(RTAnyType::RTAnyTypeImpl::kEdge);
const RTAnyType RTAnyType::kI64Value =
    RTAnyType(RTAnyType::RTAnyTypeImpl::kI64Value);
const RTAnyType RTAnyType::kU64Value =
    RTAnyType(RTAnyType::RTAnyTypeImpl::kU64Value);
const RTAnyType RTAnyType::kI32Value =
    RTAnyType(RTAnyType::RTAnyTypeImpl::kI32Value);
const RTAnyType RTAnyType::kF64Value =
    RTAnyType(RTAnyType::RTAnyTypeImpl::kF64Value);

const RTAnyType RTAnyType::kBoolValue =
    RTAnyType(RTAnyType::RTAnyTypeImpl::kBoolValue);
const RTAnyType RTAnyType::kStringValue =
    RTAnyType(RTAnyType::RTAnyTypeImpl::kStringValue);
const RTAnyType RTAnyType::kVertexSetValue =
    RTAnyType(RTAnyType::RTAnyTypeImpl::kVertexSetValue);
const RTAnyType RTAnyType::kStringSetValue =
    RTAnyType(RTAnyType::RTAnyTypeImpl::kStringSetValue);
const RTAnyType RTAnyType::kUnknown =
    RTAnyType(RTAnyType::RTAnyTypeImpl::kUnknown);
const RTAnyType RTAnyType::kDate32 =
    RTAnyType(RTAnyType::RTAnyTypeImpl::kDate32);
const RTAnyType RTAnyType::kPath = RTAnyType(RTAnyType::RTAnyTypeImpl::kPath);
const RTAnyType RTAnyType::kNull = RTAnyType(RTAnyType::RTAnyTypeImpl::kNull);
const RTAnyType RTAnyType::kTuple = RTAnyType(RTAnyType::RTAnyTypeImpl::kTuple);
const RTAnyType RTAnyType::kList = RTAnyType(RTAnyType::RTAnyTypeImpl::kList);
const RTAnyType RTAnyType::kMap = RTAnyType(RTAnyType::RTAnyTypeImpl::kMap);
RTAny List::get(size_t idx) const { return impl_->get(idx); }
RTAnyType parse_from_ir_data_type(const ::common::IrDataType& dt) {
  switch (dt.type_case()) {
  case ::common::IrDataType::TypeCase::kDataType: {
    const ::common::DataType ddt = dt.data_type();
    switch (ddt) {
    case ::common::DataType::BOOLEAN:
      return RTAnyType::kBoolValue;
    case ::common::DataType::INT64:
      return RTAnyType::kI64Value;
    case ::common::DataType::STRING:
      return RTAnyType::kStringValue;
    case ::common::DataType::INT32:
      return RTAnyType::kI32Value;
    case ::common::DataType::DATE32:
      return RTAnyType::kDate32;
    case ::common::DataType::STRING_ARRAY:
      return RTAnyType::kList;
    case ::common::DataType::TIMESTAMP:
      return RTAnyType::kDate32;
    case ::common::DataType::DOUBLE:
      return RTAnyType::kF64Value;
    default:
      LOG(FATAL) << "unrecoginized data type - " << ddt;
      break;
    }
  } break;
  case ::common::IrDataType::TypeCase::kGraphType: {
    const ::common::GraphDataType gdt = dt.graph_type();
    switch (gdt.element_opt()) {
    case ::common::GraphDataType_GraphElementOpt::
        GraphDataType_GraphElementOpt_VERTEX:
      return RTAnyType::kVertex;
    case ::common::GraphDataType_GraphElementOpt::
        GraphDataType_GraphElementOpt_EDGE:
      return RTAnyType::kEdge;
    default:
      LOG(FATAL) << "unrecoginized graph data type";
      break;
    }
  } break;
  default:
    break;
  }

  // LOG(FATAL) << "unknown";
  return RTAnyType::kUnknown;
}

RTAny::RTAny() : type_(RTAnyType::kUnknown), value_() {}
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
    value_.i64_val = val.AsDate().milli_second;
  } else if (val.type == PropertyType::Int32()) {
    type_ = RTAnyType::kI32Value;
    value_.i32_val = val.AsInt32();
  } else if (val.type == PropertyType::kDouble) {
    type_ = RTAnyType::kF64Value;
    value_.f64_val = val.AsDouble();
  } else if (val.type == PropertyType::Bool()) {
    type_ = RTAnyType::kBoolValue;
    value_.b_val = val.AsBool();
  } else {
    LOG(FATAL) << "Any value: " << val.to_string()
               << ", type = " << val.type.type_enum;
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
    value_.t = rhs.value_.t.dup();
  } else if (type_ == RTAnyType::kList) {
    value_.list = rhs.value_.list;
  } else if (type_ == RTAnyType::kF64Value) {
    value_.f64_val = rhs.value_.f64_val;
  } else if (type_ == RTAnyType::kMap) {
    value_.map = rhs.value_.map;
  } else {
    LOG(FATAL) << "unexpected type: " << static_cast<int>(type_.type_enum_);
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
    value_.t = rhs.value_.t.dup();
  } else if (type_ == RTAnyType::kList) {
    value_.list = rhs.value_.list;
  } else if (type_ == RTAnyType::kF64Value) {
    value_.f64_val = rhs.value_.f64_val;
  } else if (type_ == RTAnyType::kMap) {
    value_.map = rhs.value_.map;
  } else if (type_ == RTAnyType::kEdge) {
    value_.edge = rhs.value_.edge;
  } else {
    LOG(FATAL) << "unexpected type: " << static_cast<int>(type_.type_enum_);
  }
  return *this;
}

RTAnyType RTAny::type() const { return type_; }

RTAny RTAny::from_vertex(label_t l, vid_t v) {
  RTAny ret;
  ret.type_ = RTAnyType::kVertex;
  ret.value_.vertex.first = l;
  ret.value_.vertex.second = v;
  return ret;
}

RTAny RTAny::from_vertex(const std::pair<label_t, vid_t>& v) {
  RTAny ret;
  ret.type_ = RTAnyType::kVertex;
  ret.value_.vertex = v;
  return ret;
}

RTAny RTAny::from_edge(
    const std::tuple<LabelTriplet, vid_t, vid_t, Any, Direction>& v) {
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

RTAny RTAny::from_string_set(const std::set<std::string>& str_set) {
  RTAny ret;
  ret.type_ = RTAnyType::kStringSetValue;
  ret.value_.str_set = &str_set;
  return ret;
}

RTAny RTAny::from_vertex_list(const std::vector<vid_t>& v_set) {
  RTAny ret;
  ret.type_ = RTAnyType::kVertexSetValue;
  ret.value_.vset = &v_set;
  return ret;
}

RTAny RTAny::from_date32(Date v) {
  RTAny ret;
  ret.type_ = RTAnyType::kDate32;
  ret.value_.i64_val = v.milli_second;
  return ret;
}

RTAny RTAny::from_tuple(std::vector<RTAny>&& v) {
  RTAny ret;
  ret.type_ = RTAnyType::kTuple;
  ret.value_.t.init(std::move(v));
  return ret;
}

RTAny RTAny::from_tuple(const Tuple& t) {
  RTAny ret;
  ret.type_ = RTAnyType::kTuple;
  ret.value_.t = t.dup();
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

bool RTAny::as_bool() const {
  if (type_ == RTAnyType::kNull) {
    return false;
  }
  CHECK(type_ == RTAnyType::kBoolValue)
      << "type_ = " << static_cast<int>(type_.type_enum_);
  return value_.b_val;
}
int RTAny::as_int32() const {
  CHECK(type_ == RTAnyType::kI32Value)
      << "type_ = " << static_cast<int>(type_.type_enum_);
  return value_.i32_val;
}
int64_t RTAny::as_int64() const {
  CHECK(type_ == RTAnyType::kI64Value);
  return value_.i64_val;
}
uint64_t RTAny::as_uint64() const {
  CHECK(type_ == RTAnyType::kU64Value);
  return value_.u64_val;
}
int64_t RTAny::as_date32() const {
  CHECK(type_ == RTAnyType::kDate32);
  return value_.i64_val;
}

double RTAny::as_double() const {
  CHECK(type_ == RTAnyType::kF64Value);
  return value_.f64_val;
}

const std::pair<label_t, vid_t>& RTAny::as_vertex() const {
  CHECK(type_ == RTAnyType::kVertex);
  return value_.vertex;
}
const std::tuple<LabelTriplet, vid_t, vid_t, Any, Direction>& RTAny::as_edge()
    const {
  CHECK(type_ == RTAnyType::kEdge);
  return value_.edge;
}
const std::set<std::string>& RTAny::as_string_set() const {
  CHECK(type_ == RTAnyType::kStringSetValue);
  return *value_.str_set;
}
std::string_view RTAny::as_string() const {
  if (type_ == RTAnyType::kStringValue) {
    return value_.str_val;
  } else if (type_ == RTAnyType::kUnknown) {
    return std::string_view();
  } else {
    LOG(FATAL) << "unexpected type" << static_cast<int>(type_.type_enum_);
    return std::string_view();
  }
}

List RTAny::as_list() const {
  CHECK(type_ == RTAnyType::kList);
  return value_.list;
}
const std::vector<vid_t>& RTAny::as_vertex_list() const {
  CHECK(type_ == RTAnyType::kVertexSetValue);
  return *value_.vset;
}

Path RTAny::as_path() const {
  CHECK(type_ == RTAnyType::kPath);
  return value_.p;
}

Tuple RTAny::as_tuple() const {
  CHECK(type_ == RTAnyType::kTuple);
  return value_.t;
}

Map RTAny::as_map() const {
  CHECK(type_ == RTAnyType::kMap);
  return value_.map;
}

int RTAny::numerical_cmp(const RTAny& other) const {
  switch (type_.type_enum_) {
  case RTAnyType::RTAnyTypeImpl::kI64Value:
    switch (other.type_.type_enum_) {
    case RTAnyType::RTAnyTypeImpl::kI32Value:
      return value_.i64_val - other.value_.i32_val;
    case RTAnyType::RTAnyTypeImpl::kF64Value:
      return value_.i64_val - other.value_.f64_val;
    default:
      LOG(FATAL) << "not support for "
                 << static_cast<int>(other.type_.type_enum_);
    }
    break;
  case RTAnyType::RTAnyTypeImpl::kI32Value:
    switch (other.type_.type_enum_) {
    case RTAnyType::RTAnyTypeImpl::kI64Value:
      return value_.i32_val - other.value_.i64_val;
    case RTAnyType::RTAnyTypeImpl::kF64Value:
      return value_.i32_val - other.value_.f64_val;
    default:
      LOG(FATAL) << "not support for "
                 << static_cast<int>(other.type_.type_enum_);
    }
    break;
  case RTAnyType::RTAnyTypeImpl::kF64Value:
    switch (other.type_.type_enum_) {
    case RTAnyType::RTAnyTypeImpl::kI64Value:
      return value_.f64_val - other.value_.i64_val;
    case RTAnyType::RTAnyTypeImpl::kI32Value:
      return value_.f64_val - other.value_.i32_val;
    default:
      LOG(FATAL) << "not support for " << static_cast<int>(type_.type_enum_);
    }
    break;
  default:
    LOG(FATAL) << "not support for " << static_cast<int>(type_.type_enum_);
  }
}
inline static bool is_numerical_type(const RTAnyType& type) {
  return type == RTAnyType::kI64Value || type == RTAnyType::kI32Value ||
         type == RTAnyType::kF64Value;
}

bool RTAny::operator<(const RTAny& other) const {
  if (type_.type_enum_ != other.type_.type_enum_) {
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
    return value_.i64_val < other.value_.i64_val;
  } else if (type_ == RTAnyType::kF64Value) {
    return value_.f64_val < other.value_.f64_val;
  }

  LOG(FATAL) << "not support for " << static_cast<int>(type_.type_enum_);
  return true;
}

bool RTAny::operator==(const RTAny& other) const {
  // assert(type_ == other.type_);
  if (type_.type_enum_ != other.type_.type_enum_) {
    if (is_numerical_type(type_) && is_numerical_type(other.type_)) {
      return numerical_cmp(other) == 0;
    } else {
      return false;
    }
  }

  if (type_ == RTAnyType::kI64Value) {
    return value_.i64_val == other.value_.i64_val;
  } else if (type_ == RTAnyType::kI32Value) {
    return value_.i32_val == other.value_.i32_val;
  } else if (type_ == RTAnyType::kStringValue) {
    return value_.str_val == other.value_.str_val;
  } else if (type_ == RTAnyType::kVertex) {
    return value_.vertex == other.value_.vertex;
  } else if (type_ == RTAnyType::kDate32) {
    return value_.i64_val == other.value_.i64_val;
  }

  if (type_ == RTAnyType::kI64Value && other.type_ == RTAnyType::kI32Value) {
    return value_.i64_val == other.value_.i32_val;
  } else if (type_ == RTAnyType::kI32Value &&
             other.type_ == RTAnyType::kI64Value) {
    return value_.i32_val == other.value_.i64_val;
  } else if (type_ == RTAnyType::kF64Value) {
    return value_.f64_val == other.value_.f64_val;
  }

  LOG(FATAL) << "not support..." << static_cast<int>(type_.type_enum_);
  return true;
}

RTAny RTAny::operator+(const RTAny& other) const {
  if (type_ == RTAnyType::kI64Value && other.type_ == RTAnyType::kI32Value) {
    return RTAny::from_int64(value_.i64_val + other.value_.i32_val);
  } else if (type_ == RTAnyType::kI32Value &&
             other.type_ == RTAnyType::kI64Value) {
    return RTAny::from_int64(value_.i32_val * 1l + other.value_.i64_val);
  }
  if (type_ == RTAnyType::kF64Value) {
    return RTAny::from_double(value_.f64_val + other.value_.f64_val);
  } else if (type_ == RTAnyType::kI64Value) {
    return RTAny::from_int64(value_.i64_val + other.value_.i64_val);
  } else if (type_ == RTAnyType::kI32Value) {
    return RTAny::from_int32(value_.i32_val + other.value_.i32_val);
  }

  LOG(FATAL) << "not support" << static_cast<int>(type_.type_enum_);
  return RTAny();
}

RTAny RTAny::operator-(const RTAny& other) const {
  // CHECK(type_ == other.type_);

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
  // CHECK(type_ == other.type_);

  if (type_ == RTAnyType::kI64Value && other.type_ == RTAnyType::kI32Value) {
    return RTAny::from_int64(value_.i64_val / other.value_.i32_val);
  } else if (type_ == RTAnyType::kI32Value &&
             other.type_ == RTAnyType::kI64Value) {
    return RTAny::from_int64(value_.i32_val * 1l / other.value_.i64_val);
  }

  if (type_ == RTAnyType::kI64Value) {
    return RTAny::from_int64(value_.i64_val / other.value_.i64_val);
  } else if (type_ == RTAnyType::kF64Value) {
    return RTAny::from_double(value_.f64_val / other.value_.f64_val);
  } else if (type_ == RTAnyType::kI32Value) {
    return RTAny::from_int32(value_.i32_val / other.value_.i32_val);
  }
  LOG(FATAL) << "not support";
  return RTAny();
}

void RTAny::sink_impl(common::Value* value) const {
  if (type_ == RTAnyType::kI64Value) {
    value->set_i64(value_.i64_val);
  } else if (type_ == RTAnyType::kStringValue) {
    value->set_str(value_.str_val.data(), value_.str_val.size());
  } else if (type_ == RTAnyType::kI32Value) {
    value->set_i32(value_.i32_val);
  } else if (type_ == RTAnyType::kStringSetValue) {
    LOG(FATAL) << "not support string set sink";
  } else if (type_ == RTAnyType::kDate32) {
    value->set_i64(value_.i64_val);
  } else if (type_ == RTAnyType::kBoolValue) {
    value->set_boolean(value_.b_val);
  } else if (type_ == RTAnyType::kF64Value) {
    value->set_f64(value_.f64_val);
  } else if (type_ == RTAnyType::kList) {
    LOG(FATAL) << "not support list sink";
  } else if (type_ == RTAnyType::kTuple) {
    auto tup = value_.t;
    for (size_t i = 0; i < value_.t.size(); ++i) {
      std::string s = tup.get(i).to_string();
      value->mutable_str_array()->add_item(s.data(), s.size());
    }
  } else {
    LOG(FATAL) << "not implemented for " << static_cast<int>(type_.type_enum_);
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
  } else {
    LOG(FATAL) << "Any value: " << any.to_string()
               << ", type = " << any.type.type_enum;
  }
}

void sink_vertex(const gs::ReadTransaction& txn,
                 const std::pair<label_t, vid_t>& vertex, results::Vertex* v) {
  v->mutable_label()->set_id(vertex.first);
  v->set_id(encode_unique_vertex_id(vertex.first, vertex.second));
  //  TODO: add properties
  const auto& names =
      txn.graph().schema().get_vertex_property_names(vertex.first);
  for (size_t i = 0; i < names.size(); ++i) {
    auto prop = v->add_properties();
    prop->mutable_key()->set_name(names[i]);
    sink_any(txn.graph().get_vertex_table(vertex.first).at(vertex.second, i),
             prop->mutable_value());
  }
}

void RTAny::sink(const gs::ReadTransaction& txn, int id,
                 results::Column* col) const {
  col->mutable_name_or_id()->set_id(id);
  if (type_ == RTAnyType::kList) {
    auto collection = col->mutable_entry()->mutable_collection();
    for (size_t i = 0; i < value_.list.size(); ++i) {
      value_.list.get(i).sink_impl(
          collection->add_collection()->mutable_object());
    }
  } else if (type_ == RTAnyType::kStringSetValue) {
    auto collection = col->mutable_entry()->mutable_collection();
    for (auto& s : *value_.str_set) {
      collection->add_collection()->mutable_object()->set_str(s);
    }
  } else if (type_ == RTAnyType::kTuple) {
    auto collection = col->mutable_entry()->mutable_collection();
    for (size_t i = 0; i < value_.t.size(); ++i) {
      value_.t.get(i).sink_impl(collection->add_collection()->mutable_object());
    }
  } else if (type_ == RTAnyType::kVertex) {
    auto v = col->mutable_entry()->mutable_element()->mutable_vertex();
    sink_vertex(txn, value_.vertex, v);

  } else if (type_ == RTAnyType::kMap) {
    auto mp = col->mutable_entry()->mutable_map();
    auto [keys_ptr, vals_ptr] = value_.map.key_vals();
    auto& keys = *keys_ptr;
    auto& vals = *vals_ptr;
    for (size_t i = 0; i < keys.size(); ++i) {
      if (vals[i].is_null()) {
        continue;
      }
      auto ret = mp->add_key_values();
      ret->mutable_key()->set_str(keys[i]);
      if (vals[i].type_ == RTAnyType::kVertex) {
        auto v = ret->mutable_value()->mutable_element()->mutable_vertex();
        sink_vertex(txn, vals[i].as_vertex(), v);
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
    auto& prop_names = txn.schema().get_edge_property_names(
        label.src_label, label.dst_label, label.edge_label);
    if (prop_names.size() == 1) {
      auto props = e->add_properties();
      props->mutable_key()->set_name(prop_names[0]);
      sink_any(prop, e->mutable_properties(0)->mutable_value());
    } else if (prop_names.size() > 1) {
      auto rv = prop.AsRecordView();
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
    LOG(FATAL) << "not support path sink";

  } else {
    sink_impl(col->mutable_entry()->mutable_element()->mutable_object());
  }
}

void RTAny::encode_sig(RTAnyType type, Encoder& encoder) const {
  if (type == RTAnyType::kI64Value) {
    encoder.put_long(this->as_int64());
  } else if (type == RTAnyType::kStringValue) {
    encoder.put_string_view(this->as_string());
  } else if (type == RTAnyType::kI32Value) {
    encoder.put_int(this->as_int32());
  } else if (type == RTAnyType::kVertex) {
    const auto& v = this->value_.vertex;
    encoder.put_byte(v.first);
    encoder.put_int(v.second);
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
  } else {
    LOG(FATAL) << "not implemented for " << static_cast<int>(type_.type_enum_);
  }
}

std::string RTAny::to_string() const {
  if (type_ == RTAnyType::kI64Value) {
    return std::to_string(value_.i64_val);
  } else if (type_ == RTAnyType::kStringValue) {
    return std::string(value_.str_val);
  } else if (type_ == RTAnyType::kI32Value) {
    return std::to_string(value_.i32_val);
  } else if (type_ == RTAnyType::kVertex) {
#if 0
      return std::string("v") +
             std::to_string(static_cast<int>(value_.vertex.first)) + "-" +
             std::to_string(value_.vertex.second);
#else
    return std::to_string(value_.vertex.second);
#endif
  } else if (type_ == RTAnyType::kStringSetValue) {
    std::string ret = "{";
    for (auto& str : *value_.str_set) {
      ret += str;
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
  } else if (type_ == RTAnyType::kDate32) {
    return std::to_string(value_.i64_val);
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
    LOG(FATAL) << "not implemented for " << static_cast<int>(type_.type_enum_);
    return "";
  }
}

}  // namespace runtime

}  // namespace gs

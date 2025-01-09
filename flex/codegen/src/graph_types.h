/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#ifndef CODEGEN_SRC_GRAPH_TYPES_H_
#define CODEGEN_SRC_GRAPH_TYPES_H_

#include <cstdint>
#include <type_traits>

#include "flex/codegen/src/string_utils.h"
#include "flex/proto_generated_gie/basic_type.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "glog/logging.h"
#include "google/protobuf/any.h"

namespace gs {

namespace codegen {
using oid_t = int64_t;

enum class DataType {
  kInt32 = 0,
  kInt64 = 1,
  kFloat = 2,
  kDouble = 3,
  kString = 4,
  kInt64Array = 5,
  kInt32Array = 6,
  kBoolean = 7,
  kGlobalVertexId = 8,
  kEdgeId = 9,
  kLength = 10,
  kDate = 11,
  kTime = 12,
  kTimeStamp = 13,
  kLabelId = 14,
  kEmpty = 15,
};

// a parameter const, the real data will be feed at runtime.
struct ParamConst {
  DataType type;
  std::string var_name;
  std::string expr_var_name;
  int32_t id;  // unique id for each param const
};

// implement operator == for ParamConst
inline bool operator==(const ParamConst& lhs, const ParamConst& rhs) {
  return lhs.type == rhs.type && lhs.var_name == rhs.var_name &&
         lhs.expr_var_name == rhs.expr_var_name && lhs.id == rhs.id;
}

}  // namespace codegen

static codegen::DataType primitive_type_to_data_type(
    const common::PrimitiveType& type) {
  switch (type) {
  case common::PrimitiveType::DT_SIGNED_INT32:
    return codegen::DataType::kInt32;
  case common::PrimitiveType::DT_SIGNED_INT64:
    return codegen::DataType::kInt64;
  case common::PrimitiveType::DT_FLOAT:
    return codegen::DataType::kFloat;
  case common::PrimitiveType::DT_DOUBLE:
    return codegen::DataType::kDouble;
  case common::PrimitiveType::DT_BOOL:
    return codegen::DataType::kBoolean;
  default:
    // LOG(FATAL) << "unknown primitive type";
    throw std::runtime_error(
        "unknown primitive type when converting primitive type to data type:" +
        std::to_string(static_cast<int>(type)));
  }
}

static codegen::DataType temporal_type_to_data_type(
    const common::Temporal& type) {
  switch (type.item_case()) {
  case common::Temporal::ItemCase::kDate:
    return codegen::DataType::kDate;
  case common::Temporal::ItemCase::kTime:
    return codegen::DataType::kTime;
  case common::Temporal::kTimestamp:
    return codegen::DataType::kTimeStamp;
  default:
    throw std::runtime_error(
        "unknown temporal type when converting temporal type to data type:" +
        std::to_string(static_cast<int>(type.item_case())));
  }
}

static codegen::DataType common_data_type_pb_2_data_type(
    const common::DataType& data_type) {
  switch (data_type.item_case()) {
  case common::DataType::ItemCase::kPrimitiveType:
    return primitive_type_to_data_type(data_type.primitive_type());
  case common::DataType::ItemCase::kDecimal:
    LOG(FATAL) << "Not support decimal type";
  case common::DataType::ItemCase::kString:
    return codegen::DataType::kString;
  case common::DataType::ItemCase::kTemporal:
    return temporal_type_to_data_type(data_type.temporal());
  case common::DataType::ItemCase::kArray:
  case common::DataType::ItemCase::kMap:
    LOG(FATAL) << "Not support array or map type";
  default:
    // LOG(FATAL) << "unknown data type";
    throw std::runtime_error(
        "unknown data type when converting common_data_type to inner data "
        "type:" +
        data_type.DebugString());
  }
}

static std::string primitive_type_to_str(const common::PrimitiveType& type) {
  switch (type) {
  case common::PrimitiveType::DT_SIGNED_INT32:
    return "int32_t";
  case common::PrimitiveType::DT_UNSIGNED_INT32:
    return "uint32_t";
  case common::PrimitiveType::DT_SIGNED_INT64:
    return "int64_t";
  case common::PrimitiveType::DT_UNSIGNED_INT64:
    return "uint64_t";
  case common::PrimitiveType::DT_FLOAT:
    return "float";
  case common::PrimitiveType::DT_DOUBLE:
    return "double";
  case common::PrimitiveType::DT_BOOL:
    return "bool";
  default:
    // LOG(FATAL) << "unknown primitive type";
    throw std::runtime_error(
        "unknown primitive type when converting primitive type to string:" +
        std::to_string(static_cast<int>(type)));
  }
}

static std::string single_common_data_type_pb_2_str(
    const common::DataType& data_type) {
  switch (data_type.item_case()) {
  case common::DataType::ItemCase::kPrimitiveType:
    return primitive_type_to_str(data_type.primitive_type());
  case common::DataType::ItemCase::kDecimal:
    LOG(FATAL) << "Not support decimal type";
  case common::DataType::ItemCase::kString:
    return "std::string_view";
  case common::DataType::ItemCase::kTemporal:
    LOG(FATAL) << "Not support temporal type";
  case common::DataType::ItemCase::kArray:
  case common::DataType::ItemCase::kMap:
    LOG(FATAL) << "Not support array or map type";
    // TODO: support time32 and timestamp
  default:
    throw std::runtime_error(
        "unknown data type when convert common data type to string:" +
        data_type.DebugString());
  }
}

static std::string common_data_type_pb_2_str(
    const std::vector<common::DataType>& data_types) {
  std::stringstream ss;
  if (data_types.size() == 1) {
    return single_common_data_type_pb_2_str(data_types[0]);
  }
  ss << "std::tuple<";
  for (size_t i = 0; i < data_types.size(); ++i) {
    ss << single_common_data_type_pb_2_str(data_types[i]);
    if (i + 1 < data_types.size()) {
      ss << ", ";
    }
  }
  ss << ">;";
  return ss.str();
}

static std::string arith_to_str(const common::Arithmetic& arith_type) {
  switch (arith_type) {
  case common::Arithmetic::ADD:
    return "+";
  case common::Arithmetic::SUB:
    return "-";
  case common::Arithmetic::MUL:
    return "*";
  case common::Arithmetic::DIV:
    return "/";
  default:
    throw std::runtime_error("unknown arith type");
  }
}

static codegen::ParamConst param_const_pb_to_param_const(
    const common::DynamicParam& param_const_pb) {
  auto data_type_pb = param_const_pb.data_type();
  CHECK(data_type_pb.type_case() == common::IrDataType::kDataType);
  return codegen::ParamConst{
      common_data_type_pb_2_data_type(data_type_pb.data_type()),
      param_const_pb.name(), param_const_pb.name(), param_const_pb.index()};
}

static codegen::ParamConst param_const_pb_to_param_const(
    const common::DynamicParam& param_const_pb,
    const common::IrDataType& ir_data_type) {
  if (ir_data_type.type_case() == common::IrDataType::kDataType) {
    auto primitive_type = ir_data_type.data_type();
    return codegen::ParamConst{common_data_type_pb_2_data_type(primitive_type),
                               param_const_pb.name(), param_const_pb.name(),
                               param_const_pb.index()};
  } else {
    throw std::runtime_error("Expect node type in ir_data_type");
  }
}

// The second params only control the ret value when type is string.In some
// cases, we need to use std::string_view, but in some cases, we need to use
// std::string.
static std::string data_type_2_string(const codegen::DataType& data_type,
                                      bool string_view = true) {
  switch (data_type) {
  case codegen::DataType::kInt32:
    return "int32_t";
  case codegen::DataType::kInt64:
    return "int64_t";
  case codegen::DataType::kDouble:
    return "double";
  case codegen::DataType::kString:
    if (string_view) {
      return "std::string_view";
    } else {
      return "std::string";
    }
  case codegen::DataType::kInt64Array:
    return "std::vector<int64_t>";
  case codegen::DataType::kInt32Array:
    return "std::vector<int32_t>";
  case codegen::DataType::kBoolean:
    return "bool";
  case codegen::DataType::kGlobalVertexId:
    return GLOBAL_VERTEX_ID_T;
  case codegen::DataType::kLength:
    return LENGTH_KEY_T;
  case codegen::DataType::kEdgeId:
    return EDGE_ID_T;
  case codegen::DataType::kDate:
  case codegen::DataType::kTimeStamp:
    return "Date";
  case codegen::DataType::kLabelId:
    return "LabelKey";
  case codegen::DataType::kEmpty:
    return GRAPE_EMPTY_TYPE;
  default:
    // LOG(FATAL) << "unknown data type" << static_cast<int>(data_type);
    throw std::runtime_error(
        "unknown data type when convert inner data_type to string: " +
        std::to_string(static_cast<int>(data_type)));
  }
}

// for different type, generate get_type() call
static std::string decode_type_as_str(const codegen::DataType& data_type) {
  switch (data_type) {
  case codegen::DataType::kInt32:
    return "get_int()";
  case codegen::DataType::kInt64:
    return "get_long()";
  case codegen::DataType::kDouble:
    return "get_double()";
  case codegen::DataType::kString:
    return "get_string()";
  case codegen::DataType::kBoolean:
    return "get_bool()";
  default:
    // LOG(FATAL) << "unknown data type" << static_cast<int>(data_type);
    throw std::runtime_error("unknown data type when decode type as str: " +
                             std::to_string(static_cast<int>(data_type)));
  }
}

static std::string data_type_2_rust_string(const codegen::DataType& data_type) {
  switch (data_type) {
  case codegen::DataType::kInt32:
    return "i32";
  case codegen::DataType::kInt64:
    return "i64";
  case codegen::DataType::kDouble:
    return "double";
  case codegen::DataType::kString:
    return "String";
  case codegen::DataType::kInt64Array:
    return "Vector<i64>";
  case codegen::DataType::kInt32Array:
    return "Vector<i32>";
  case codegen::DataType::kBoolean:
    return "bool";
  case codegen::DataType::kGlobalVertexId:
    return "ID";
  default:
    LOG(FATAL) << "unknown data type" << static_cast<int>(data_type);
    return "";
  }
}

static common::DataType common_value_2_data_type(const common::Value& value) {
  common::DataType ret;
  switch (value.item_case()) {
  case common::Value::kI32:
    ret.set_primitive_type(common::PrimitiveType::DT_SIGNED_INT32);
  case common::Value::kI64:
    ret.set_primitive_type(common::PrimitiveType::DT_SIGNED_INT64);
  case common::Value::kBoolean:
    ret.set_primitive_type(common::PrimitiveType::DT_BOOL);
  case common::Value::kF64:
    ret.set_primitive_type(common::PrimitiveType::DT_DOUBLE);
  case common::Value::kStr:
    ret.mutable_string()->mutable_long_text();
  default:
    LOG(FATAL) << "unknown value" << value.DebugString();
  }
  return ret;
}

static void parse_param_const_from_pb(
    const common::DynamicParam& param_const_pb,
    const common::IrDataType& node_type, codegen::ParamConst& param_cost) {
  auto data_type = param_const_pb.data_type();
  if (data_type.type_case() == common::IrDataType::kDataType) {
    param_cost.type = common_data_type_pb_2_data_type(data_type.data_type());
    param_cost.var_name = param_const_pb.name();
    param_cost.expr_var_name = param_const_pb.name();
    param_cost.id = param_const_pb.index();
    return;
  } else if (node_type.type_case() == common::IrDataType::kDataType) {
    param_cost.type = common_data_type_pb_2_data_type(node_type.data_type());
    param_cost.var_name = param_const_pb.name();
    param_cost.expr_var_name = param_const_pb.name();
    param_cost.id = param_const_pb.index();
    return;
  } else {
    throw std::runtime_error("Fail to get data type from param const");
  }
}

}  // namespace gs

#endif  // CODEGEN_SRC_GRAPH_TYPES_H_
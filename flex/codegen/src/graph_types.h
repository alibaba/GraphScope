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
#include "glog/logging.h"
#include "google/protobuf/any.h"
#include "proto_generated_gie/common.pb.h"

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
  kVertexId = 8,
};

// a parameter const, the real data will be feed at runtime.
struct ParamConst {
  DataType type;
  std::string var_name;
  int32_t id;  // unique id for each param const
};

// implement operator == for ParamConst
inline bool operator==(const ParamConst& lhs, const ParamConst& rhs) {
  return lhs.type == rhs.type && lhs.var_name == rhs.var_name &&
         lhs.id == rhs.id;
}

}  // namespace codegen

static codegen::DataType common_data_type_pb_2_data_type(
    const common::DataType& data_type) {
  switch (data_type) {
  case common::DataType::INT32:
    return codegen::DataType::kInt32;
  case common::DataType::INT64:
    return codegen::DataType::kInt64;
  case common::DataType::DOUBLE:
    return codegen::DataType::kDouble;
  case common::DataType::STRING:
    return codegen::DataType::kString;
  case common::DataType::INT64_ARRAY:
    return codegen::DataType::kInt64Array;
  case common::DataType::INT32_ARRAY:
    return codegen::DataType::kInt32Array;
  case common::DataType::BOOLEAN:
    return codegen::DataType::kBoolean;
  default:
    // LOG(FATAL) << "unknown data type";
    throw std::runtime_error("unknown data type" +
                             std::to_string(static_cast<int>(data_type)));
  }
}

static std::string common_data_type_pb_2_str(
    const common::DataType& data_type) {
  switch (data_type) {
  case common::DataType::BOOLEAN:
    return "bool";
  case common::DataType::INT32:
    return "int32_t";
  case common::DataType::INT64:
    return "int64_t";
  case common::DataType::DOUBLE:
    return "double";
  case common::DataType::STRING:
    return "std::string_view";
  case common::DataType::INT64_ARRAY:
    return "std::vector<int64_t>";
  case common::DataType::INT32_ARRAY:
    return "std::vector<int32_t>";
  default:
    // LOG(FATAL) << "unknown data type";
    // return "";
    throw std::runtime_error("unknown data type" +
                             std::to_string(static_cast<int>(data_type)));
  }
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
      param_const_pb.name(), param_const_pb.index()};
}

static codegen::ParamConst param_const_pb_to_param_const(
    const common::DynamicParam& param_const_pb,
    const common::IrDataType& ir_data_type) {
  if (ir_data_type.type_case() == common::IrDataType::kDataType) {
    auto primitive_type = ir_data_type.data_type();
    return codegen::ParamConst{common_data_type_pb_2_data_type(primitive_type),
                               param_const_pb.name(), param_const_pb.index()};
  } else {
    throw std::runtime_error("Expect node type in ir_data_type");
  }
}

static std::string data_type_2_string(const codegen::DataType& data_type) {
  switch (data_type) {
  case codegen::DataType::kInt32:
    return "int32_t";
  case codegen::DataType::kInt64:
    return "int64_t";
  case codegen::DataType::kDouble:
    return "double";
  case codegen::DataType::kString:
    return "std::string_view";
  case codegen::DataType::kInt64Array:
    return "std::vector<int64_t>";
  case codegen::DataType::kInt32Array:
    return "std::vector<int32_t>";
  case codegen::DataType::kBoolean:
    return "bool";
  case codegen::DataType::kVertexId:
    return VERTEX_ID_T;
  default:
    // LOG(FATAL) << "unknown data type" << static_cast<int>(data_type);
    throw std::runtime_error("unknown data type" +
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
    throw std::runtime_error("unknown data type" +
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
  case codegen::DataType::kVertexId:
    return "ID";
  default:
    LOG(FATAL) << "unknown data type" << static_cast<int>(data_type);
  }
}

static void parse_param_const_from_pb(
    const common::DynamicParam& param_const_pb,
    const common::IrDataType& node_type, codegen::ParamConst& param_cost) {
  auto data_type = param_const_pb.data_type();
  if (data_type.type_case() == common::IrDataType::kDataType) {
    param_cost.type = common_data_type_pb_2_data_type(data_type.data_type());
    param_cost.var_name = param_const_pb.name();
    param_cost.id = param_const_pb.index();
    return;
  } else if (node_type.type_case() == common::IrDataType::kDataType) {
    param_cost.type = common_data_type_pb_2_data_type(node_type.data_type());
    param_cost.var_name = param_const_pb.name();
    param_cost.id = param_const_pb.index();
    return;
  } else {
    throw std::runtime_error("Fail to get data type from param const");
  }
}

}  // namespace gs

#endif  // CODEGEN_SRC_GRAPH_TYPES_H_
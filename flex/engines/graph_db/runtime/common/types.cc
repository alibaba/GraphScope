
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

#include "flex/engines/graph_db/runtime/common/types.h"

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
      return RTAnyType::kStringSetValue;
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

std::string dir_2_str(Direction dir) {
  switch (dir) {
  case Direction::kOut:
    return "Direction::kOut";
  case Direction::kIn:
    return "Direction::kIn";
  case Direction::kBoth:
    return "Direction::kBoth";
  default:
    return "unknown";
  }
}

std::string vopt_2_str(VOpt opt) {
  switch (opt) {
  case VOpt::kStart:
    return "VOpt::kStart";
  case VOpt::kEnd:
    return "VOpt::kEnd";
  case VOpt::kOther:
    return "VOpt::kOther";
  case VOpt::kBoth:
    return "VOpt::kBoth";
  case VOpt::kItself:
    return "VOpt::kItself";
  default:
    return "unknown";
  }
}

std::string join_kind_2_str(JoinKind kind) {
  switch (kind) {
  case JoinKind::kSemiJoin:
    return "JoinKind::kSemiJoin";
  case JoinKind::kInnerJoin:
    return "JoinKind::kInnerJoin";
  case JoinKind::kAntiJoin:
    return "JoinKind::kAntiJoin";
  case JoinKind::kLeftOuterJoin:
    return "JoinKind::kLeftOuterJoin";
  default:
    return "unknown";
  }
}
uint64_t encode_unique_vertex_id(label_t label_id, vid_t vid) {
  // encode label_id and vid to a unique vid
  GlobalId global_id(label_id, vid);
  return global_id.global_id;
}

uint32_t generate_edge_label_id(label_t src_label_id, label_t dst_label_id,
                                label_t edge_label_id) {
  uint32_t unique_edge_label_id = src_label_id;
  static constexpr int num_bits = sizeof(label_t) * 8;
  unique_edge_label_id = unique_edge_label_id << num_bits;
  unique_edge_label_id = unique_edge_label_id | dst_label_id;
  unique_edge_label_id = unique_edge_label_id << num_bits;
  unique_edge_label_id = unique_edge_label_id | edge_label_id;
  return unique_edge_label_id;
}

int64_t encode_unique_edge_id(uint32_t label_id, vid_t src, vid_t dst) {
  // We assume label_id is only used by 24 bits.
  int64_t unique_edge_id = label_id;
  static constexpr int num_bits = sizeof(int64_t) * 8 - sizeof(uint32_t) * 8;
  unique_edge_id = unique_edge_id << num_bits;
  // bitmask for top 44 bits set to 1
  int64_t bitmask = 0xFFFFFFFFFF000000;
  // 24 bit | 20 bit | 20 bit
  if (bitmask & (int64_t) src || bitmask & (int64_t) dst) {
    LOG(ERROR) << "src or dst is too large to be encoded in 20 bits: " << src
               << " " << dst;
  }
  unique_edge_id = unique_edge_id | (src << 20);
  unique_edge_id = unique_edge_id | dst;
  return unique_edge_id;
}
}  // namespace runtime
}  // namespace gs
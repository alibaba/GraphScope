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

#ifndef RUNTIME_COMMON_TYPES_H_
#define RUNTIME_COMMON_TYPES_H_

#include <string>

#include "flex/proto_generated_gie/physical.pb.h"
#include "flex/proto_generated_gie/results.pb.h"
#include "flex/proto_generated_gie/type.pb.h"

#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/property/types.h"
namespace gs {

namespace runtime {

uint64_t encode_unique_vertex_id(label_t label_id, vid_t vid);
uint32_t generate_edge_label_id(label_t src_label_id, label_t dst_label_id,
                                label_t edge_label_id);
int64_t encode_unique_edge_id(uint32_t label_id, vid_t src, vid_t dst);
enum class Direction {
  kOut,
  kIn,
  kBoth,
};

enum class VOpt {
  kStart,
  kEnd,
  kOther,
  kBoth,
  kItself,
};

enum class JoinKind {
  kSemiJoin,
  kInnerJoin,
  kAntiJoin,
  kLeftOuterJoin,
};

// identify the type of accessor
enum class VarType {
  kVertexVar,
  kEdgeVar,
  kPathVar,
};

enum class ContextColumnType {
  kVertex,
  kEdge,
  kValue,
  kPath,
  kOptionalValue,
  kUnknown,
};

class RTAnyType {
 public:
  enum class RTAnyTypeImpl {
    kVertex,
    kEdge,
    kI64Value,
    kU64Value,
    kI32Value,
    kF64Value,
    kBoolValue,
    kStringValue,
    kVertexSetValue,
    kStringSetValue,
    kUnknown,
    kDate32,
    kPath,
    kNull,
    kTuple,
    kList,
    kMap,
  };
  static const RTAnyType kVertex;
  static const RTAnyType kEdge;
  static const RTAnyType kI64Value;
  static const RTAnyType kU64Value;
  static const RTAnyType kI32Value;
  static const RTAnyType kF64Value;
  static const RTAnyType kBoolValue;
  static const RTAnyType kStringValue;
  static const RTAnyType kVertexSetValue;
  static const RTAnyType kStringSetValue;
  static const RTAnyType kUnknown;
  static const RTAnyType kDate32;
  static const RTAnyType kPath;
  static const RTAnyType kNull;
  static const RTAnyType kTuple;
  static const RTAnyType kList;
  static const RTAnyType kMap;

  RTAnyType() : type_enum_(RTAnyTypeImpl::kUnknown) {}
  RTAnyType(const RTAnyType& other)
      : type_enum_(other.type_enum_), null_able_(other.null_able_) {}
  RTAnyType(RTAnyTypeImpl type) : type_enum_(type), null_able_(false) {}
  RTAnyType(RTAnyTypeImpl type, bool null_able)
      : type_enum_(type), null_able_(null_able) {}
  bool operator==(const RTAnyType& other) const {
    return type_enum_ == other.type_enum_;
  }
  RTAnyTypeImpl type_enum_;
  bool null_able_;
};

RTAnyType parse_from_ir_data_type(const common::IrDataType& dt);

std::string dir_2_str(Direction dir);

std::string vopt_2_str(VOpt opt);

std::string join_kind_2_str(JoinKind kind);

VOpt parse_opt(const physical::GetV_VOpt& opt);

struct LabelTriplet {
  LabelTriplet(label_t src, label_t dst, label_t edge)
      : src_label(src), dst_label(dst), edge_label(edge) {}

  std::string to_string() const {
    return "(" + std::to_string(static_cast<int>(src_label)) + "," +
           std::to_string(static_cast<int>(dst_label)) + "," +
           std::to_string(static_cast<int>(edge_label)) + ")";
  }

  bool operator==(const LabelTriplet& rhs) const {
    return src_label == rhs.src_label && dst_label == rhs.dst_label &&
           edge_label == rhs.edge_label;
  }

  bool operator<(const LabelTriplet& rhs) const {
    if (src_label != rhs.src_label) {
      return src_label < rhs.src_label;
    }
    if (dst_label != rhs.dst_label) {
      return dst_label < rhs.dst_label;
    }
    return edge_label < rhs.edge_label;
  }

  label_t src_label;
  label_t dst_label;
  label_t edge_label;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_TYPES_H_
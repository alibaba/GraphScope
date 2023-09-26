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
#ifndef CODEGEN_SRC_HQPS_HQPS_JOIN_UTILS_H_
#define CODEGEN_SRC_HQPS_HQPS_JOIN_UTILS_H_

#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/pb_parser/name_id_parser.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/expr.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"
namespace gs {

namespace internal {
enum class JoinKind { kInnerJoin, kAntiJoin, kLeftOuterJoin };
}

internal::JoinKind join_kind_pb_to_internal(
    const physical::Join::JoinKind& join_kind_pb) {
  switch (join_kind_pb) {
  case physical::Join::JoinKind::Join_JoinKind_INNER:
    return internal::JoinKind::kInnerJoin;
  case physical::Join::JoinKind::Join_JoinKind_ANTI:
    return internal::JoinKind::kAntiJoin;
  case physical::Join::JoinKind::Join_JoinKind_LEFT_OUTER:
    return internal::JoinKind::kLeftOuterJoin;
  default:
    throw std::runtime_error("unknown join_kind_pb");
  }
}

std::string join_kind_to_str(const internal::JoinKind& join_kind) {
  switch (join_kind) {
  case internal::JoinKind::kInnerJoin:
    return "gs::JoinKind::InnerJoin";
  case internal::JoinKind::kAntiJoin:
    return "gs::JoinKind::AntiJoin";
  case internal::JoinKind::kLeftOuterJoin:
    return "gs::JoinKind::LeftOuterJoin";
  default:
    throw std::runtime_error("unknown join_kind");
  }
}

}  // namespace gs

#endif  // CODEGEN_SRC_HQPS_HQPS_JOIN_UTILS_H_
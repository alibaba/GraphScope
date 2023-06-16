#ifndef JOIN_BUILDER_H
#define JOIN_BUILDER_H

#include <string>
#include <vector>

#include "flex/codegen/building_context.h"
#include "flex/codegen/graph_types.h"
#include "flex/codegen/pb_parser/name_id_parser.h"
#include "flex/codegen/codegen_utils.h"
#include "proto_generated_gie/algebra.pb.h"

#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/expr.pb.h"
#include "proto_generated_gie/physical.pb.h"
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

#endif  // JOIN_BUILDER_H
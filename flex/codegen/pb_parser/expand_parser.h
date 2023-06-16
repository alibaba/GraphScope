#ifndef EXPAND_PARSER_H
#define EXPAND_PARSER_H

#include "flex/codegen/pb_parser/internal_struct.h"
#include "proto_generated_gie/physical.pb.h"

namespace gs {

// a static function convert physical::EdgeExpand::Direction to
// internal::Direction
internal::Direction edge_expand_pb_2_internal_direction(
    const physical::EdgeExpand::Direction& direction_pb) {
  switch (direction_pb) {
  case physical::EdgeExpand::Direction::EdgeExpand_Direction_OUT:
    return internal::Direction::kOut;
  case physical::EdgeExpand::Direction::EdgeExpand_Direction_IN:
    return internal::Direction::kIn;
  case physical::EdgeExpand::Direction::EdgeExpand_Direction_BOTH:
    return internal::Direction::kBoth;
  default:
    throw std::runtime_error("unknown direction_pb");
  }
}
}  // namespace gs

#endif  // EXPAND_PARSER_H
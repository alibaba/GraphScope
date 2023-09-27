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
#ifndef CODEGEN_SRC_PB_PARSER_EXPAND_PARSER_H_
#define CODEGEN_SRC_PB_PARSER_EXPAND_PARSER_H_

#include "flex/codegen/src/pb_parser/internal_struct.h"
#include "flex/proto_generated_gie/physical.pb.h"

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

#endif  // CODEGEN_SRC_PB_PARSER_EXPAND_PARSER_H_
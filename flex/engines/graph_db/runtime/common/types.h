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

#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/property/types.h"

namespace gs {

namespace runtime {

uint64_t encode_unique_vertex_id(label_t label_id, vid_t vid);
std::pair<label_t, vid_t> decode_unique_vertex_id(uint64_t unique_id);

uint32_t generate_edge_label_id(label_t src_label_id, label_t dst_label_id,
                                label_t edge_label_id);
int64_t encode_unique_edge_id(uint32_t label_id, vid_t src, vid_t dst);

std::tuple<label_t, label_t, label_t> decode_edge_label_id(
    uint32_t edge_label_id);
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

struct LabelTriplet {
  LabelTriplet(label_t src, label_t dst, label_t edge)
      : src_label(src), dst_label(dst), edge_label(edge) {}

  std::string to_string() const {
    return "(" + std::to_string(static_cast<int>(src_label)) + "-" +
           std::to_string(static_cast<int>(edge_label)) + "-" +
           std::to_string(static_cast<int>(dst_label)) + ")";
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

namespace std {

// operator << for Direction
inline std::ostream& operator<<(std::ostream& os,
                                const gs::runtime::Direction& dir) {
  switch (dir) {
  case gs::runtime::Direction::kOut:
    os << "OUT";
    break;
  case gs::runtime::Direction::kIn:
    os << "IN";
    break;
  case gs::runtime::Direction::kBoth:
    os << "BOTH";
    break;
  }
  return os;
}

// std::to_string
inline std::string to_string(const gs::runtime::Direction& dir) {
  switch (dir) {
  case gs::runtime::Direction::kOut:
    return "OUT";
  case gs::runtime::Direction::kIn:
    return "IN";
  case gs::runtime::Direction::kBoth:
    return "BOTH";
  }
  return "UNKNOWN";
}
}  // namespace std

#endif  // RUNTIME_COMMON_TYPES_H_

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
uint64_t encode_unique_vertex_id(label_t label_id, vid_t vid) {
  // encode label_id and vid to a unique vid
  GlobalId global_id(label_id, vid);
  return global_id.global_id;
}

std::pair<label_t, vid_t> decode_unique_vertex_id(uint64_t unique_id) {
  return std::pair{GlobalId::get_label_id(unique_id),
                   GlobalId::get_vid(unique_id)};
}

uint32_t generate_edge_label_id(label_t src_label_id, label_t dst_label_id,
                                label_t edge_label_id) {
  uint32_t unique_edge_label_id = src_label_id;
  static constexpr int num_bits = sizeof(label_t) * 8;
  static_assert(num_bits * 3 <= sizeof(uint32_t) * 8,
                "label_t is too large to be encoded in 32 bits");
  unique_edge_label_id = unique_edge_label_id << num_bits;
  unique_edge_label_id = unique_edge_label_id | dst_label_id;
  unique_edge_label_id = unique_edge_label_id << num_bits;
  unique_edge_label_id = unique_edge_label_id | edge_label_id;
  return unique_edge_label_id;
}

std::tuple<label_t, label_t, label_t> decode_edge_label_id(
    uint32_t edge_label_id) {
  static constexpr int num_bits = sizeof(label_t) * 8;
  static_assert(num_bits * 3 <= sizeof(uint32_t) * 8,
                "label_t is too large to be encoded in 32 bits");
  auto mask = (1 << num_bits) - 1;
  label_t edge_label = edge_label_id & mask;
  edge_label_id = edge_label_id >> num_bits;
  label_t dst_label = edge_label_id & mask;
  edge_label_id = edge_label_id >> num_bits;
  label_t src_label = edge_label_id & mask;
  return std::make_tuple(src_label, dst_label, edge_label);
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
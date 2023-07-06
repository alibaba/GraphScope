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

#ifndef STORAGES_RT_MUTABLE_GRAPH_TYPES_H_
#define STORAGES_RT_MUTABLE_GRAPH_TYPES_H_

#include <stdint.h>
#include <cstddef>
#include <limits>

namespace gs {

enum class EdgeStrategy {
  kNone,
  kSingle,
  kMultiple,
};

struct Dist {
  int32_t dist = 0;
  Dist(int32_t d) : dist(d) {}
  Dist() : dist(0) {}
  inline Dist& operator=(int32_t d) {
    dist = d;
    return *this;
  }

  void set(int32_t i) { dist = i; }
};

inline bool operator<(const Dist& a, const Dist& b) { return a.dist < b.dist; }
inline bool operator>(const Dist& a, const Dist& b) { return a.dist > b.dist; }

inline bool operator==(const Dist& a, const Dist& b) {
  return a.dist == b.dist;
}

using timestamp_t = uint32_t;
using vid_t = uint32_t;
using oid_t = int64_t;
using label_t = uint8_t;
// distance in path.
using dist_t = Dist;
static constexpr label_t INVALID_LABEL_ID = std::numeric_limits<label_t>::max();
using offset_t = size_t;
using vertex_set_key_t = size_t;
static constexpr vid_t INVALID_VID = std::numeric_limits<vid_t>::max();

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_TYPES_H_

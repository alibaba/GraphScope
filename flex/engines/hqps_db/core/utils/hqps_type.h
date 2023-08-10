#ifndef ENGINES_HQPS_ENGINE_UTILS_TYPE_UTILS_H_
#define ENGINES_HQPS_ENGINE_UTILS_TYPE_UTILS_H_

#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {
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

// distance in path.
using dist_t = Dist;
static constexpr label_t INVALID_LABEL_ID = std::numeric_limits<label_t>::max();
using offset_t = size_t;
using vertex_set_key_t = size_t;
static constexpr vid_t INVALID_VID = std::numeric_limits<vid_t>::max();

}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_UTILS_TYPE_UTILS_H_
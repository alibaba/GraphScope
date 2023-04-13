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

/**
 * @file value_type.h
 *
 * The data types used in the apps implemented in the Flash model.
 */

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_VALUE_TYPE_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_VALUE_TYPE_H_

#include <set>
#include <utility>
#include <vector>

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace gs {

struct EMPTY_TYPE {};

struct BFS_TYPE {
  int dis;
};

struct SSSP_TYPE {
  double dis;
};

struct MULTI_BFS_TYPE {
  int res;
  int64_t seen;
  std::vector<int> d;
};

struct BC_TYPE {
  char d;
  double b, c;
};

struct KATZ_TYPE {
  double val, next;
};

struct CLOSENESS_TYPE {
  int64_t seen, cnt;
  double val;
};

struct HARMONIC_TYPE {
  int64_t seen;
  double val;
};

struct CC_TYPE {
  int tag;
};

struct CC_OPT_TYPE {
  int64_t cid;
};

struct CC_LOG_TYPE {
  int p, s, f;
};

struct SCC_TYPE {
  int fid, scc;
};

struct BCC_TYPE {
  int d, cid, p, dis, bcc;
};

struct BCC_2_TYPE {
  int d, cid, p, dis, nd, pre, oldc, oldd, minp, maxp, tmp;
};

struct PR_TYPE {
  int deg;
  double val, next;
};

struct HITS_TYPE {
  double auth, hub, auth1, hub1;
};

struct MIS_TYPE {
  bool d, b;
  int64_t r;
};

struct MIS_2_TYPE {
  bool d, b;
};

struct MM_TYPE {
  int p, s;
};

struct MM_2_TYPE {
  int p, s, d;
};

struct MIN_COVER_TYPE {
  bool c, s;
  int d, tmp;
};

struct MIN_COVER_TYPE_2 {
  bool c;
  int d, tmp, f;
};

struct MIN_DOMINATING_SET_TYPE {
  bool d, b;
  int max_id, max_cnt;
};

struct MIN_DOMINATING_SET_2_TYPE {
  bool d, b;
  int cnt, cnt1, fid1, fid2, tmp;
};

struct K_CORE_TYPE {
  int d;
};

struct CORE_TYPE {
  int core;
  int cnt;
  std::vector<int> s;
};

struct CORE_2_TYPE {
  int core, old;
};

struct DEGENERACY_TYPE {
  int core, old;
  int d, tmp, rank;
};

struct ONION_TYPE {
  int core, old;
  int rank, tmp, d;
};

struct TRIANGLE_TYPE {
  int deg, count;
  std::set<int> out;
};

struct CYCLIC_TYPE {
  int deg, count;
  std::set<int> in, out;
};

struct RECTANGLE_TYPE {
  int deg, count;
  std::vector<std::pair<int, int>> out;
};

struct K_CLIQUE_2_TYPE {
  int deg, count;
  std::vector<int> out;
};

struct DENSEST_TYPE {
  int core, t;
};

struct COLOR_TYPE {
  int c, cc;
  int deg;
  std::vector<int> colors;
};

struct LPA_TYPE {
  int c, cc;
  std::vector<int> s;
};

struct LPA_BY_COLOR_TYPE {
  int c, cc;
  int deg, label, old, t;
  std::vector<int> colors;
};

struct FLUID_TYPE {
  int lab, old, l1, l2;
};

struct DIAMETER_TYPE {
  int64_t seen;
  int ecc;
};

struct DIAMETER_2_TYPE {
  int64_t seen;
  int dis, ecc;
};

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const MULTI_BFS_TYPE& v) {
  in_archive << v.seen;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     MULTI_BFS_TYPE& v) {
  out_archive >> v.seen;
  return out_archive;
}

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const KATZ_TYPE& v) {
  in_archive << v.val;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     KATZ_TYPE& v) {
  out_archive >> v.val;
  return out_archive;
}

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const CLOSENESS_TYPE& v) {
  in_archive << v.seen;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     CLOSENESS_TYPE& v) {
  out_archive >> v.seen;
  return out_archive;
}

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const HARMONIC_TYPE& v) {
  in_archive << v.seen;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     HARMONIC_TYPE& v) {
  out_archive >> v.seen;
  return out_archive;
}

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const PR_TYPE& v) {
  in_archive << v.deg << v.val;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     PR_TYPE& v) {
  out_archive >> v.deg >> v.val;
  return out_archive;
}

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const HITS_TYPE& v) {
  in_archive << v.auth << v.hub;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     HITS_TYPE& v) {
  out_archive >> v.auth >> v.hub;
  return out_archive;
}

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const MIS_TYPE& v) {
  in_archive << v.d << v.r;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     MIS_TYPE& v) {
  out_archive >> v.d >> v.r;
  return out_archive;
}

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const CORE_TYPE& v) {
  in_archive << v.core;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     CORE_TYPE& v) {
  out_archive >> v.core;
  return out_archive;
}

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const CORE_2_TYPE& v) {
  in_archive << v.core;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     CORE_2_TYPE& v) {
  out_archive >> v.core;
  return out_archive;
}

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const TRIANGLE_TYPE& v) {
  in_archive << v.deg << v.out;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     TRIANGLE_TYPE& v) {
  out_archive >> v.deg >> v.out;
  return out_archive;
}

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const CYCLIC_TYPE& v) {
  in_archive << v.deg << v.in;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     CYCLIC_TYPE& v) {
  out_archive >> v.deg >> v.in;
  return out_archive;
}

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const RECTANGLE_TYPE& v) {
  in_archive << v.deg << v.out;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     RECTANGLE_TYPE& v) {
  out_archive >> v.deg >> v.out;
  return out_archive;
}

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const K_CLIQUE_2_TYPE& v) {
  in_archive << v.deg << v.out;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     K_CLIQUE_2_TYPE& v) {
  out_archive >> v.deg >> v.out;
  return out_archive;
}

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const COLOR_TYPE& v) {
  in_archive << v.c << v.deg;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     COLOR_TYPE& v) {
  out_archive >> v.c >> v.deg;
  return out_archive;
}

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const LPA_TYPE& v) {
  in_archive << v.c;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     LPA_TYPE& v) {
  out_archive >> v.c;
  return out_archive;
}

inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const LPA_BY_COLOR_TYPE& v) {
  in_archive << v.c << v.deg << v.label << v.t;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     LPA_BY_COLOR_TYPE& v) {
  out_archive >> v.c >> v.deg >> v.label >> v.t;
  return out_archive;
}

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_VALUE_TYPE_H_

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

#include "grin/src/predefine.h"

#include "grin/include/index/pk.h"

#if defined(GRIN_ENABLE_VERTEX_PK_INDEX) && \
    defined(GRIN_ENABLE_VERTEX_PRIMARY_KEYS)
/**
 * @brief Get the vertex by primary keys row.
 * The values in the row must be in the same order as the primary keys
 * properties, which can be obtained by
 * ``grin_get_primary_keys_by_vertex_type``.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_VERTEX_TYPE The vertex type.
 * @param GRIN_ROW The values row of primary keys properties.
 * @return The vertex.
 */
GRIN_VERTEX grin_get_vertex_by_primary_keys_row(GRIN_GRAPH g,
                                                GRIN_VERTEX_TYPE label,
                                                GRIN_ROW r) {
  auto _r = static_cast<GRIN_ROW_T*>(r);
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto type = _g->g.lf_indexers_[label].get_type();
  uint32_t vid;

  if (type == gs::PropertyType::kInt64) {
    auto oid = *static_cast<const int64_t*>((*_r)[0]);
    if (!_g->g.get_lid(label, oid, vid)) {
      return GRIN_NULL_VERTEX;
    }
  } else if (type == gs::PropertyType::kInt32) {
    auto oid = *static_cast<const int32_t*>((*_r)[0]);
    if (!_g->g.get_lid(label, oid, vid)) {
      return GRIN_NULL_VERTEX;
    }
  } else if (type == gs::PropertyType::kUInt32) {
    auto oid = *static_cast<const uint32_t*>((*_r)[0]);
    if (!_g->g.get_lid(label, oid, vid)) {
      return GRIN_NULL_VERTEX;
    }
  } else if (type == gs::PropertyType::kUInt64) {
    auto oid = *static_cast<const uint64_t*>((*_r)[0]);
    if (!_g->g.get_lid(label, oid, vid)) {
      return GRIN_NULL_VERTEX;
    }
  } else if (type == gs::PropertyType::kStringView) {
    auto oid = *static_cast<const std::string_view*>((*_r)[0]);
    if (!_g->g.get_lid(label, oid, vid)) {
      return GRIN_NULL_VERTEX;
    }
  } else {
    return GRIN_NULL_VERTEX;
  }
  uint64_t v = ((label * 1ull) << 32) + vid;
  return v;
}
#endif
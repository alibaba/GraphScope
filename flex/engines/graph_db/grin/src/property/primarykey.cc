#include "grin/src/predefine.h"

#include "grin/include/common/error.h"
#include "grin/include/property/primarykey.h"
#ifdef GRIN_ENABLE_VERTEX_PRIMARY_KEYS
/**
 * @brief Get the vertex types that have primary keys
 * In some graph, not every vertex type has primary keys.
 * @param GRIN_GRAPH The graph
 * @return The vertex type list of types that have primary keys
 */
GRIN_VERTEX_TYPE_LIST grin_get_vertex_types_with_primary_keys(GRIN_GRAPH g) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  GRIN_VERTEX_TYPE_LIST_T* vtl = new GRIN_VERTEX_TYPE_LIST_T();
  for (size_t idx = 0; idx < _g->g.vertex_label_num_; ++idx) {
    vtl->push_back(idx);
  }
  return vtl;
}

/**
 * @brief Get the primary keys properties of a vertex type
 * The primary keys properties are the properties that can be used to identify a
 * vertex. They are a subset of the properties of a vertex type.
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX_TYPE The vertex type
 * @return The primary keys properties list
 */
GRIN_VERTEX_PROPERTY_LIST grin_get_primary_keys_by_vertex_type(
    GRIN_GRAPH g, GRIN_VERTEX_TYPE label) {
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto type = _g->g.lf_indexers_[label].get_type();
  GRIN_VERTEX_PROPERTY_LIST_T* vpl = new GRIN_VERTEX_PROPERTY_LIST_T();
  GRIN_VERTEX_PROPERTY vp;
  vp = 0;
  vp += (label * 1u) << 8;
  if (type == gs::PropertyType::kInt64) {
    vp += (GRIN_DATATYPE::Int64 * 1u) << 16;
  } else if (type == gs::PropertyType::kInt32) {
    vp += (GRIN_DATATYPE::Int32 * 1u) << 16;
  } else if (type == gs::PropertyType::kUInt64) {
    vp += (GRIN_DATATYPE::UInt64 * 1u) << 16;
  } else if (type == gs::PropertyType::kUInt32) {
    vp += (GRIN_DATATYPE::UInt32 * 1u) << 16;
  } else if (type == gs::PropertyType::kStringView) {
    vp += (GRIN_DATATYPE::StringView * 1u) << 16;
  } else {
    vp = GRIN_NULL_VERTEX_PROPERTY;
  }
  vpl->emplace_back(vp);
  return vpl;
}

/**
 * @brief Get the primary keys values row of a vertex
 * The values in the row are in the same order as the primary keys properties.
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX The vertex
 * @return The primary keys values row
 */
GRIN_ROW grin_get_vertex_primary_keys_row(GRIN_GRAPH g, GRIN_VERTEX v) {
  GRIN_ROW_T* row = new GRIN_ROW_T();
  auto _g = static_cast<GRIN_GRAPH_T*>(g);
  auto vid = v & (0xffffffff);
  auto label = v >> 32;
  auto type = _g->g.lf_indexers_[label].get_type();
  if (type == gs::PropertyType::kInt64) {
    auto oid = _g->g.get_oid(label, vid).AsInt64();
    auto p = new int64_t(oid);
    row->emplace_back(p);
  } else if (type == gs::PropertyType::kStringView) {
    auto oid = _g->g.get_oid(label, vid).AsStringView();
    auto p = new std::string_view(oid);
    row->emplace_back(p);
  } else {
    return GRIN_NULL_ROW;
  }
  return row;
}
#endif

#include "grin/src/predefine.h"

#include "grin/include/include/common/error.h"
#include "grin/include/include/property/primarykey.h"
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
  for (size_t idx = 0; idx < _g->vertex_label_num_; ++idx) {
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
    GRIN_GRAPH, GRIN_VERTEX_TYPE label) {
  GRIN_VERTEX_PROPERTY_LIST_T* vpl = new GRIN_VERTEX_PROPERTY_LIST_T();
  GRIN_VERTEX_PROPERTY_T vp;
  vp.name = "oid";
  vp.label = label;
  vp.dt = GRIN_DATATYPE::Int64;
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
  auto _v = static_cast<GRIN_VERTEX_T*>(v);
  auto vid = _v->vid;
  auto oid = _g->get_oid(_v->label, vid);
  auto p = new gs::oid_t(oid);
  row->emplace_back(p);
  return row;
}
#endif

#ifdef GRIN_ENABLE_EDGE_PRIMARY_KEYS
GRIN_EDGE_TYPE_LIST grin_get_edge_types_with_primary_keys(GRIN_GRAPH);

GRIN_EDGE_PROPERTY_LIST grin_get_primary_keys_by_edge_type(GRIN_GRAPH,
                                                           GRIN_EDGE_TYPE);

GRIN_ROW grin_get_edge_primary_keys_row(GRIN_GRAPH, GRIN_EDGE);
#endif

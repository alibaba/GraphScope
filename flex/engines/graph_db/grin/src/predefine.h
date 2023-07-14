#include "grin/predefine.h"

#include "../../storages/rt_mutable_graph/mutable_property_fragment.h"

typedef gs::oid_t GRIN_OID_T;
typedef gs::vid_t GRIN_VID_T;

typedef gs::MutablePropertyFragment GRIN_GRAPH_T;

typedef struct GRIN_VERTEX_T {
  uint8_t label;
  uint32_t vid;
} GRIN_VERTEX_T;

typedef struct GRIN_EDGE_T {
  GRIN_VERTEX_T dst;
  GRIN_VERTEX_T src;
  GRIN_DIRECTION dir;
  gs::label_t label;
  gs::Any data;
} GRIN_EDGE_T;

#ifdef GRIN_ENABLE_ADJACENT_LIST
typedef struct GRIN_ADJACENT_LIST_T {
  GRIN_VERTEX_T v;
  GRIN_DIRECTION dir;
  GRIN_EDGE_TYPE edge_label;
} GRIN_ADJACENT_LIST_T;
#endif

#ifdef GRIN_ENABLE_ADJACENT_LIST_ITERATOR
typedef struct GRIN_ADJACENT_LIST_ITERATOR_T {
  std::shared_ptr<gs::MutableCsrConstEdgeIterBase> edge_iter;
  GRIN_ADJACENT_LIST_T* adj_list;
} GRIN_ADJACENT_LIST_ITERATOR_T;
#endif

#ifdef GRIN_WITH_VERTEX_PROPERTY
typedef std::vector<gs::label_t> GRIN_VERTEX_TYPE_LIST_T;

typedef struct GRIN_VERTEX_PROPERTY_T {
  std::string name;
  GRIN_DATATYPE dt;
  GRIN_VERTEX_TYPE label;
} GRIN_VERTEX_PROPERTY_T;

typedef std::vector<GRIN_VERTEX_PROPERTY_T> GRIN_VERTEX_PROPERTY_LIST_T;
#endif

#if defined(GRIN_WITH_VERTEX_PROPERTY) || defined(GRIN_WITH_EDGE_PROPERTY)
typedef std::vector<const void*> GRIN_ROW_T;
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
typedef std::vector<unsigned> GRIN_EDGE_TYPE_LIST_T;
typedef std::vector<unsigned> GRIN_EDGE_PROPERTY_LIST_T;
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST_ITERATOR
typedef struct GRIN_VERTEX_LIST_ITERATOR_T {
  size_t cur_vid;
  GRIN_VERTEX_LIST vertex_list;
} GRIN_VERTEX_LIST_ITERATOR_T;
#endif

GRIN_DATATYPE _get_data_type(const gs::PropertyType& type);
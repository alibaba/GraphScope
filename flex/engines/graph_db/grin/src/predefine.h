#include <vector>

#include "grin/predefine.h"
#include "storages/rt_mutable_graph/loader/loader_factory.h"
#include "storages/rt_mutable_graph/loading_config.h"
#include "storages/rt_mutable_graph/mutable_property_fragment.h"

typedef gs::vid_t GRIN_VID_T;

typedef struct GRIN_GRAPH_T {
  gs::MutablePropertyFragment g;
  std::vector<std::vector<const void*>> vproperties;
  // std::vector<std::vector<const void*>> eproperties;
} GRIN_GRAPH_T;

typedef struct GRIN_EDGE_T {
  GRIN_VERTEX dst;
  GRIN_VERTEX src;
  GRIN_DIRECTION dir;
  gs::label_t label;
  gs::Any data;
} GRIN_EDGE_T;

#ifdef GRIN_WITH_VERTEX_PROPERTY
typedef std::vector<gs::label_t> GRIN_VERTEX_TYPE_LIST_T;
typedef std::vector<GRIN_VERTEX_PROPERTY> GRIN_VERTEX_PROPERTY_LIST_T;
#endif

#if defined(GRIN_WITH_VERTEX_PROPERTY) || defined(GRIN_WITH_EDGE_PROPERTY)
typedef std::vector<const void*> GRIN_ROW_T;
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
typedef std::vector<unsigned> GRIN_EDGE_TYPE_LIST_T;
typedef std::vector<unsigned> GRIN_EDGE_PROPERTY_LIST_T;
#endif

GRIN_DATATYPE _get_data_type(const gs::PropertyType& type);
void init_cache(GRIN_GRAPH_T* g);
#include "grin/predefine.h"

#include "storages/rt_mutable_graph/mutable_property_fragment.h"

typedef gs::oid_t GRIN_OID_T;
typedef gs::vid_t GRIN_VID_T;

typedef gs::MutablePropertyFragment GRIN_GRAPH_T;

typedef struct GRIN_EDGE_T{
    GRIN_VERTEX dst;
    GRIN_VERTEX src;
    GRIN_DIRECTION dir;
    gs::label_t label;
    gs::Any data;
} GRIN_EDGE_T;


#ifdef GRIN_ENABLE_ADJACENT_LIST
typedef struct GRIN_ADJACENT_LIST_T {
  GRIN_VERTEX v;
  GRIN_DIRECTION dir;
  //gs::label_t elabel;
  std::vector<GRIN_EDGE_TYPE> edges_label;
} GRIN_ADJACENT_LIST_T;
#endif

#ifdef GRIN_ENABLE_ADJACENT_LIST_ITERATOR
struct GRIN_ADJACENT_LIST_ITERATOR_T {
  GRIN_ADJACENT_LIST_ITERATOR_T(GRIN_ADJACENT_LIST_T* adj_list):cur_edge_iter(nullptr),cur_label_idx(0),adj_list(*adj_list){}
  void get_cur_edge_iter(GRIN_GRAPH_T* g){
    if(cur_label_idx == adj_list.edges_label.size()){
      return;
    }
    do{
      auto label = adj_list.edges_label[cur_label_idx];
      auto elabel = label >> 16;
      auto vlabel = (label & 0xffff);
      if(adj_list.dir == GRIN_DIRECTION::OUT){
        cur_edge_iter = g->get_outgoing_edges(adj_list.v.label, adj_list.v.vid,
                                vlabel, elabel);
      } else{
        cur_edge_iter = g->get_incoming_edges(adj_list.v.label,adj_list.v.vid,
                                vlabel, elabel);
      }
      ++cur_label_idx;
    }while(((cur_edge_iter == nullptr)||(!cur_edge_iter->is_valid())) && cur_label_idx < adj_list.edges_label.size());
  }
  void next(GRIN_GRAPH_T* g){
    cur_edge_iter->next();
    if(!cur_edge_iter->is_valid()){
      get_cur_edge_iter(g);
    }
  }
  bool is_valid(){
    if(cur_edge_iter == nullptr){
      return false;
    }
    return cur_edge_iter->is_valid();
  }

  GRIN_VERTEX neighbor(){
    GRIN_VERTEX v;
    v.label = (adj_list.edges_label[cur_label_idx - 1]) &(0xffff);
    v.vid = cur_edge_iter->get_neighbor();
    return v;
  }
  GRIN_EDGE_TYPE edge_type(){
    auto elabel = (adj_list.edges_label[cur_label_idx - 1]) >> 16;
    return elabel;
  }
  std::shared_ptr<gs::MutableCsrConstEdgeIterBase> cur_edge_iter;
  gs::label_t cur_label_idx;
  const GRIN_ADJACENT_LIST_T& adj_list;
};
#endif

#ifdef GRIN_WITH_VERTEX_PROPERTY
typedef std::vector<gs::label_t> GRIN_VERTEX_TYPE_LIST_T;

typedef struct GRIN_VERTEX_PROPERTY_T{
  std::string name;
  GRIN_VERTEX_TYPE label;
} GRIN_VERTEX_PROPERTY_T;

typedef std::vector<GRIN_VERTEX_PROPERTY_T> GRIN_VERTEX_PROPERTY_LIST_T;
#endif

#if defined(GRIN_WITH_VERTEX_PROPERTY) || defined(GRIN_WITH_EDGE_PROPERTY)
typedef std::vector<const void*> GRIN_ROW_T;
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
typedef std::vector<gs::label_t> GRIN_EDGE_TYPE_LIST_T;
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST_ITERATOR
typedef struct {
  size_t cur_vid;
  GRIN_VERTEX_LIST vertex_list;
} GRIN_VERTEX_LIST_ITERATOR_T;
#endif

GRIN_DATATYPE _get_data_type(const gs::PropertyType& type);
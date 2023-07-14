#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>     
#include "predefine.h"
#include "common/error.h"
#include "index/internal_id.h"
#include "index/label.h"
#include "index/order.h"
#include "index/pk.h"
#include "partition/partition.h"
#include "partition/reference.h"
#include "partition/topology.h"
#include "property/partition.h"
#include "property/primarykey.h"
#include "property/property.h"
#include "property/propertylist.h"
#include "property/row.h"
#include "property/topology.h"
#include "property/type.h"
#include "topology/adjacentlist.h"
#include "topology/edgelist.h"
#include "topology/structure.h"
#include "topology/vertexlist.h"


#define FOR_VERTEX_BEGIN(g, vl, v) \
  GRIN_VERTEX_LIST_ITERATOR __vli = grin_get_vertex_list_begin(g, vl); \
  unsigned __vcnt = 0; \
  while (!grin_is_vertex_list_end(g, __vli)) { \
    GRIN_VERTEX v = grin_get_vertex_from_iter(g, __vli); \

#ifdef GRIN_WITH_VERTEX_PROPERTY
#define FOR_VERTEX_END(g, vl, v) \
    grin_destroy_vertex(g, v); \
    __vcnt++; \
    grin_get_next_vertex_list_iter(g, __vli); \
  } \
  printf("vertex type %s, checked: %u\n", vt_names[__vtl_i], __vcnt);

#define FOR_VERTEX_LIST_BEGIN(g, vl) \
{ GRIN_VERTEX_TYPE_LIST __vtl = grin_get_vertex_type_list(g); \
  size_t __vtl_sz = grin_get_vertex_type_list_size(g, __vtl); \
  for (size_t __vtl_i = 0; __vtl_i < __vtl_sz; ++__vtl_i) { \
    GRIN_VERTEX_TYPE __vt = grin_get_vertex_type_from_list(g, __vtl, __vtl_i); \
    GRIN_VERTEX_LIST vl = grin_get_vertex_list_by_type(g, __vt); \
    grin_destroy_vertex_type(g, __vt);

#define FOR_VERTEX_LIST_SELECT_MASTER_BEGIN(g, vl) \
{ GRIN_VERTEX_TYPE_LIST __vtl = grin_get_vertex_type_list(g); \
  size_t __vtl_sz = grin_get_vertex_type_list_size(g, __vtl); \
  for (size_t __vtl_i = 0; __vtl_i < __vtl_sz; ++__vtl_i) { \
    GRIN_VERTEX_TYPE __vt = grin_get_vertex_type_from_list(g, __vtl, __vtl_i); \
    GRIN_VERTEX_LIST vl = grin_get_vertex_list_by_type_select_master(g, __vt); \
    grin_destroy_vertex_type(g, __vt);

#define FOR_VERTEX_LIST_SELECT_MIRROR_BEGIN(g, vl) \
{ GRIN_VERTEX_TYPE_LIST __vtl = grin_get_vertex_type_list(g); \
  size_t __vtl_sz = grin_get_vertex_type_list_size(g, __vtl); \
  for (size_t __vtl_i = 0; __vtl_i < __vtl_sz; ++__vtl_i) { \
    GRIN_VERTEX_TYPE __vt = grin_get_vertex_type_from_list(g, __vtl, __vtl_i); \
    GRIN_VERTEX_LIST vl = grin_get_vertex_list_by_type_select_mirror(g, __vt); \
    grin_destroy_vertex_type(g, __vt);

#define FOR_VERTEX_LIST_END(g, vl) \
    grin_destroy_vertex_list(g, vl); \
  } \
  grin_destroy_vertex_type_list(g, __vtl);}
#else
#define FOR_VERTEX_END(g, vl) \
    grin_destroy_vertex(g, v); \
    __vcnt++; \
    grin_get_next_vertex_list_iter(g, __vli); \
  } \
  printf("vertex checked: %u\n", __vcnt);

#define FOR_VERTEX_LIST_BEGIN(g, vl) \
  GRIN_VERTEX_LIST vl = grin_get_vertex_list(g);

#define FOR_VERTEX_LIST_SELECT_MASTER_BEGIN(g, vl) \
  GRIN_VERTEX_LIST vl = grin_get_vertex_list_select_master(g);

#define FOR_VERTEX_LIST_SELECT_MIRROR_BEGIN(g, vl) \
  GRIN_VERTEX_LIST vl = grin_get_vertex_list_select_mirror(g);

#define FOR_VERTEX_LIST_END(g, vl) \
  grin_destroy_vertex_list(g, vl);
#endif



#ifdef GRIN_WITH_EDGE_PROPERTY
#define FOR_ADJ_LIST_BEGIN(g, dir, v, al) \
{ GRIN_EDGE_TYPE_LIST __etl = grin_get_edge_type_list(g); \
  size_t __etl_size = grin_get_edge_type_list_size(g, __etl); \
  for (size_t __etl_i = 0; __etl_i < __etl_size; ++__etl_i) { \
    GRIN_EDGE_TYPE __et = grin_get_edge_type_from_list(g, __etl, __etl_i); \
    GRIN_ADJACENT_LIST al = grin_get_adjacent_list_by_edge_type(g, dir, v, __et); \
    grin_destroy_edge_type(g, __et);
#define FOR_ADJ_LIST_END(g, al) \
    grin_destroy_adjacent_list(g, al); \
  } \
  grin_destroy_edge_type_list(g, __etl);}
#else
#define FOR_ADJ_LIST_BEGIN(g, dir, v, al) \
    GRIN_ADJACENT_LIST al = grin_get_adjacent_list(g, dir, v);
#define FOR_ADJ_LIST_END(g, al) \
    grin_destroy_adjacent_list(g, al);
#endif


const char *vt_names[] = {"person", "software"};
const char *et_names[] = {"created", "knows"};

const char *v_names[][4] = {
  {"josh", "vadas", "peter", "marko"},
  {"lop", "ripple", "wrong", "wrong"}
}; // TODO align with order in local graph

GRIN_GRAPH get_graph(int argc, char** argv, int p) {
#ifdef GRIN_ENABLE_GRAPH_PARTITION
  GRIN_PARTITIONED_GRAPH pg =
      grin_get_partitioned_graph_from_storage(argv[1]);
  GRIN_PARTITION_LIST local_partitions = grin_get_local_partition_list(pg);
  assert(p < grin_get_partition_list_size(pg, local_partitions));
  GRIN_PARTITION partition =
      grin_get_partition_from_list(pg, local_partitions, p);
  GRIN_PARTITION_ID partition_id = grin_get_partition_id(pg, partition);
  GRIN_PARTITION p1 = grin_get_partition_by_id(pg, partition_id);
  if (!grin_equal_partition(pg, partition, p1)) {
    printf("partition not match\n");
  }
  grin_destroy_partition(pg, p1);
  GRIN_GRAPH g = grin_get_local_graph_by_partition(pg, partition);
  grin_destroy_partition(pg, partition);
  grin_destroy_partition_list(pg, local_partitions);
  grin_destroy_partitioned_graph(pg);
#else
  GRIN_GRAPH g = grin_get_graph_from_storage(argv[1]);
#endif
  return g;
}


#ifdef GRIN_ENABLE_GRAPH_PARTITION
GRIN_VERTEX get_one_master_person(GRIN_GRAPH g) {
  GRIN_VERTEX_TYPE vt = grin_get_vertex_type_by_name(g, "person");
  GRIN_VERTEX_LIST vl = grin_get_vertex_list_by_type_select_master(g, vt);
  grin_destroy_vertex_type(g, vt);
  GRIN_VERTEX_LIST_ITERATOR vli = grin_get_vertex_list_begin(g, vl);
  GRIN_VERTEX v = grin_get_vertex_from_iter(g, vli);
  grin_destroy_vertex_list_iter(g, vli);
  grin_destroy_vertex_list(g, vl);
#ifdef GRIN_ENABLE_VERTEX_INTERNAL_ID_INDEX
  printf("Got vertex %s\n", v_names[vt][grin_get_vertex_internal_id_by_type(g, vt, v)]);
#endif
  return v;
}
#endif


GRIN_VERTEX get_one_person(GRIN_GRAPH g) {
  GRIN_VERTEX_TYPE vt = grin_get_vertex_type_by_name(g, "person");
  GRIN_VERTEX_LIST vl = grin_get_vertex_list_by_type(g, vt);
  grin_destroy_vertex_type(g, vt);
  GRIN_VERTEX_LIST_ITERATOR vli = grin_get_vertex_list_begin(g, vl);
  GRIN_VERTEX v = grin_get_vertex_from_iter(g, vli);
  grin_destroy_vertex_list_iter(g, vli);
  grin_destroy_vertex_list(g, vl);
#ifdef GRIN_ENABLE_VERTEX_INTERNAL_ID_INDEX
  printf("Got vertex %s\n", v_names[vt][grin_get_vertex_internal_id_by_type(g, vt, v)]);
#endif
  return v;
}


void test_property_type(int argc, char** argv) {
  printf("+++++++++++++++++++++ Test property/type +++++++++++++++++++++\n");

  GRIN_GRAPH g = get_graph(argc, argv, 0);

  printf("------------ Vertex Type ------------\n");
  GRIN_VERTEX_TYPE_LIST vtl = grin_get_vertex_type_list(g);
  size_t vtl_size = grin_get_vertex_type_list_size(g, vtl);
  printf("vertex type list size: %zu\n", vtl_size);

  for (size_t i = 0; i < vtl_size; ++i) {
    printf("------------ Iterate the %zu-th vertex type ------------\n", i);
    GRIN_VERTEX_TYPE vt = grin_get_vertex_type_from_list(g, vtl, i);
#ifdef GRIN_WITH_VERTEX_TYPE_NAME
    const char* vt_name = grin_get_vertex_type_name(g, vt);
    printf("vertex type name: %s\n", vt_name);
    GRIN_VERTEX_TYPE vt0 = grin_get_vertex_type_by_name(g, vt_name);
    if (!grin_equal_vertex_type(g, vt, vt0)) {
      printf("vertex type name not match\n");
    }
    grin_destroy_vertex_type(g, vt0);
#endif
#ifdef GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_TYPE
    printf("vertex type id: %u\n", grin_get_vertex_type_id(g, vt));
    GRIN_VERTEX_TYPE vt1 =
        grin_get_vertex_type_by_id(g, grin_get_vertex_type_id(g, vt));
    if (!grin_equal_vertex_type(g, vt, vt1)) {
      printf("vertex type id not match\n");
    }
    grin_destroy_vertex_type(g, vt1);
#endif
  }
  grin_destroy_vertex_type_list(g, vtl);

  printf(
      "------------ Create a vertex type list of one type \"person\" "
      "------------\n");
  GRIN_VERTEX_TYPE_LIST vtl2 = grin_create_vertex_type_list(g);
#ifdef GRIN_WITH_VERTEX_TYPE_NAME
  GRIN_VERTEX_TYPE vt2_w = grin_get_vertex_type_by_name(g, "knows");
  if (vt2_w == GRIN_NULL_VERTEX_TYPE) {
    printf("(Correct) vertex type of knows does not exists\n");
  }
  GRIN_VERTEX_TYPE vt2 = grin_get_vertex_type_by_name(g, "person");
  if (vt2 == GRIN_NULL_VERTEX_TYPE) {
    printf("(Wrong) vertex type of person can not be found\n");
  } else {
    const char* vt2_name = grin_get_vertex_type_name(g, vt2);
    printf("vertex type name: %s\n", vt2_name);
  }
#else
  GRIN_VERTEX_TYPE vt2 = get_one_vertex_type(g);
#endif
  grin_insert_vertex_type_to_list(g, vtl2, vt2);
  size_t vtl2_size = grin_get_vertex_type_list_size(g, vtl2);
  printf("created vertex type list size: %zu\n", vtl2_size);
  GRIN_VERTEX_TYPE vt3 = grin_get_vertex_type_from_list(g, vtl2, 0);
  if (!grin_equal_vertex_type(g, vt2, vt3)) {
    printf("vertex type not match\n");
  }
  grin_destroy_vertex_type(g, vt2);
  grin_destroy_vertex_type(g, vt3);
  grin_destroy_vertex_type_list(g, vtl2);

  // edge
  printf("------------ Edge Type ------------\n");
  GRIN_EDGE_TYPE_LIST etl = grin_get_edge_type_list(g);
  size_t etl_size = grin_get_edge_type_list_size(g, etl);
  printf("edge type list size: %zu\n", etl_size);

  for (size_t i = 0; i < etl_size; ++i) {
    printf("------------ Iterate the %zu-th edge type ------------\n", i);
    GRIN_EDGE_TYPE et = grin_get_edge_type_from_list(g, etl, i);
#ifdef GRIN_WITH_EDGE_TYPE_NAME
    const char* et_name = grin_get_edge_type_name(g, et);
    printf("edge type name: %s\n", et_name);
    GRIN_EDGE_TYPE et0 = grin_get_edge_type_by_name(g, et_name);
    if (!grin_equal_edge_type(g, et, et0)) {
      printf("edge type name not match\n");
    }
    grin_destroy_edge_type(g, et0);
#endif
#ifdef GRIN_TRAIT_NATURAL_ID_FOR_EDGE_TYPE
    printf("edge type id: %u\n", grin_get_edge_type_id(g, et));
    GRIN_EDGE_TYPE et1 =
        grin_get_edge_type_by_id(g, grin_get_edge_type_id(g, et));
    if (!grin_equal_edge_type(g, et, et1)) {
      printf("edge type id not match\n");
    }
    grin_destroy_edge_type(g, et1);
#endif
    // relation
    GRIN_VERTEX_TYPE_LIST src_vtl = grin_get_src_types_by_edge_type(g, et);
    size_t src_vtl_size = grin_get_vertex_type_list_size(g, src_vtl);
    printf("source vertex type list size: %zu\n", src_vtl_size);

    GRIN_VERTEX_TYPE_LIST dst_vtl = grin_get_dst_types_by_edge_type(g, et);
    size_t dst_vtl_size = grin_get_vertex_type_list_size(g, dst_vtl);
    printf("destination vertex type list size: %zu\n", dst_vtl_size);

    if (src_vtl_size != dst_vtl_size) {
      printf("source and destination vertex type list size not match\n");
    }
    for (size_t j = 0; j < src_vtl_size; ++j) {
      GRIN_VERTEX_TYPE src_vt = grin_get_vertex_type_from_list(g, src_vtl, j);
      GRIN_VERTEX_TYPE dst_vt = grin_get_vertex_type_from_list(g, dst_vtl, j);
      const char* src_vt_name = grin_get_vertex_type_name(g, src_vt);
      const char* dst_vt_name = grin_get_vertex_type_name(g, dst_vt);
      const char* et_name = grin_get_edge_type_name(g, et);
      printf("edge type name: %s-%s-%s\n", src_vt_name, et_name, dst_vt_name);
      grin_destroy_vertex_type(g, src_vt);
      grin_destroy_vertex_type(g, dst_vt);
    }
    grin_destroy_vertex_type_list(g, src_vtl);
    grin_destroy_vertex_type_list(g, dst_vtl);
  }
  grin_destroy_edge_type_list(g, etl);

  printf(
      "------------ Create an edge type list of one type \"created\" "
      "------------\n");
  GRIN_EDGE_TYPE_LIST etl2 = grin_create_edge_type_list(g);
#ifdef GRIN_WITH_EDGE_TYPE_NAME
  GRIN_EDGE_TYPE et2_w = grin_get_edge_type_by_name(g, "person");
  if (et2_w == GRIN_NULL_EDGE_TYPE) {
    printf("(Correct) edge type of person does not exists\n");
  }
  GRIN_EDGE_TYPE et2 = grin_get_edge_type_by_name(g, "created");
  if (et2 == GRIN_NULL_EDGE_TYPE) {
    printf("(Wrong) edge type of created can not be found\n");
  } else {
    const char* et2_name = grin_get_edge_type_name(g, et2);
    printf("edge type name: %s\n", et2_name);
  }
#else
  GRIN_EDGE_TYPE et2 = get_one_edge_type(g);
#endif
  grin_insert_edge_type_to_list(g, etl2, et2);
  size_t etl2_size = grin_get_edge_type_list_size(g, etl2);
  printf("created edge type list size: %zu\n", etl2_size);
  GRIN_EDGE_TYPE et3 = grin_get_edge_type_from_list(g, etl2, 0);
  if (!grin_equal_edge_type(g, et2, et3)) {
    printf("edge type not match\n");
  }
  grin_destroy_edge_type(g, et2);
  grin_destroy_edge_type(g, et3);
  grin_destroy_edge_type_list(g, etl2);

  grin_destroy_graph(g);
}

void test_property_vertex_property_value(int argc, char** argv) {
  printf("------------ Test Vertex property value ------------\n");
  GRIN_GRAPH g = get_graph(argc, argv, 0);

// value check
  printf("------ check value ------\n");
FOR_VERTEX_LIST_SELECT_MASTER_BEGIN(g, vl)
  GRIN_VERTEX_PROPERTY_LIST vpl = grin_get_vertex_property_list_by_type(g, __vt);
  size_t vpl_size = grin_get_vertex_property_list_size(g, vpl);
  FOR_VERTEX_BEGIN(g, vl, v)
  #ifdef GRIN_ENABLE_VERTEX_INTERNAL_ID_INDEX
    long long int vid = grin_get_vertex_internal_id_by_type(g, __vt, v);
  #else
    long long int vid = __vcnt;
  #endif
  #ifdef GRIN_ENABLE_ROW
    GRIN_ROW row = grin_get_vertex_row(g, v);
  #endif
    for (size_t j = 0; j < vpl_size; ++j) {
      GRIN_VERTEX_PROPERTY vp = grin_get_vertex_property_from_list(g, vpl, j);
      GRIN_DATATYPE dt = grin_get_vertex_property_datatype(g, vp);
      if (dt == Int64) {
        long long int pv =
            grin_get_vertex_property_value_of_int64(g, v, vp);
        assert(grin_get_last_error_code() == NO_ERROR);
      #ifdef GRIN_ENABLE_ROW
        long long int rv = grin_get_int64_from_row(g, row, j);
        assert(pv == rv);
      #endif
      #ifdef GRIN_WITH_VERTEX_PROPERTY_NAME
        printf("%s %s: %lld\n", v_names[__vt][vid], grin_get_vertex_property_name(g, __vt, vp), pv);
      #else
        printf("%s %zu: %lld\n", v_names[__vt][vid], j, pv);
      #endif
      } else if (dt == String) {
        const char* pv =
            grin_get_vertex_property_value_of_string(g, v, vp);
        assert(grin_get_last_error_code() == NO_ERROR);
      #ifdef GRIN_ENABLE_ROW
        const char* rv = grin_get_string_from_row(g, row, j);
        assert(strcmp(pv, rv) == 0);
      #endif
      #ifdef GRIN_WITH_VERTEX_PROPERTY_NAME
        printf("%s %s: %s\n", v_names[__vt][vid], grin_get_vertex_property_name(g, __vt, vp), pv);
      #else
        printf("%s %zu: %s\n", v_names[__vt][vid], j, pv);
      #endif
        grin_destroy_string_value(g, pv);
        grin_destroy_string_value(g, rv);
      }
      grin_destroy_vertex_property(g, vp);
    }
  #ifdef GRIN_ENABLE_ROW
    grin_destroy_row(g, row);
  #endif
  FOR_VERTEX_END(g, vl, v)
  grin_destroy_vertex_property_list(g, vpl);
FOR_VERTEX_LIST_END(g, vl)

// check schema
  printf("------ check schema ------\n");
  GRIN_VERTEX_TYPE_LIST vtl = grin_get_vertex_type_list(g);
  size_t vtl_size = grin_get_vertex_type_list_size(g, vtl);
  for (size_t i = 0; i < vtl_size; ++i) {
    GRIN_VERTEX_TYPE vt = grin_get_vertex_type_from_list(g, vtl, i);
    GRIN_VERTEX_PROPERTY_LIST vpl = grin_get_vertex_property_list_by_type(g, vt);
    size_t vpl_size = grin_get_vertex_property_list_size(g, vpl);
    for (size_t j = 0; j < vpl_size; ++j) {
      GRIN_VERTEX_PROPERTY vp = grin_get_vertex_property_from_list(g, vpl, j);
      GRIN_VERTEX_TYPE vt1 = grin_get_vertex_type_from_property(g, vp);
      assert(grin_equal_vertex_type(g, vt, vt1));
      grin_destroy_vertex_type(g, vt1);

    #ifdef GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_PROPERTY
      unsigned int id = grin_get_vertex_property_id(g, vt, vp);
      GRIN_VERTEX_PROPERTY vp1 = grin_get_vertex_property_by_id(g, vt, id);
      assert(grin_equal_vertex_property(g, vp, vp1));
      grin_destroy_vertex_property(g, vp1);
    #else
      unsigned int id = i;
    #endif

    #ifdef GRIN_WITH_VERTEX_PROPERTY_NAME
      const char* vp_name = grin_get_vertex_property_name(g, vt, vp);
      GRIN_VERTEX_PROPERTY vp2 =
          grin_get_vertex_property_by_name(g, vt, vp_name);
      assert(grin_equal_vertex_property(g, vp, vp2));
    #else
        const char* vp_name = "unknown";
    #endif
      printf("%s %u %s checked\n", vt_names[i], id, vp_name);
    }
    grin_destroy_vertex_property_list(g, vpl);

    // corner case
  #ifdef GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_PROPERTY
    GRIN_VERTEX_PROPERTY vp3 = grin_get_vertex_property_by_id(g, vt, vpl_size);
    assert(vp3 == GRIN_NULL_VERTEX_PROPERTY);
  #endif

  #ifdef GRIN_WITH_VERTEX_PROPERTY_NAME
    GRIN_VERTEX_PROPERTY vp4 =
        grin_get_vertex_property_by_name(g, vt, "unknown");
    assert(vp4 == GRIN_NULL_VERTEX_PROPERTY);
  #endif
    grin_destroy_vertex_type(g, vt);
  }
  grin_destroy_vertex_type_list(g, vtl);

  // corner case
#ifdef GRIN_WITH_VERTEX_PROPERTY_NAME
  GRIN_VERTEX_PROPERTY_LIST vpl1 =
      grin_get_vertex_properties_by_name(g, "unknown");
  assert(vpl1 == GRIN_NULL_VERTEX_PROPERTY_LIST);

  GRIN_VERTEX_PROPERTY_LIST vpl2 =
      grin_get_vertex_properties_by_name(g, "name");
  assert(vpl2 != GRIN_NULL_VERTEX_PROPERTY_LIST);
  
  size_t vpl2_size = grin_get_vertex_property_list_size(g, vpl2);
  for (size_t i = 0; i < vpl2_size; ++i) {
    GRIN_VERTEX_PROPERTY vp5 =
        grin_get_vertex_property_from_list(g, vpl2, i);
    GRIN_VERTEX_TYPE vt5 = grin_get_vertex_type_from_property(g, vp5);
    const char* vp5_name = grin_get_vertex_property_name(g, vt5, vp5);
    assert(strcmp(vp5_name, "name") == 0);
  }
  grin_destroy_vertex_property_list(g, vpl2);
#endif

  grin_destroy_graph(g);
}

void test_property_edge_property_value(int argc, char** argv, GRIN_DIRECTION dir) {
  printf("------------ Test Edge property value ------------\n");
  GRIN_GRAPH g = get_graph(argc, argv, 0);

// value check
  printf("------ check value ------\n");
FOR_VERTEX_LIST_SELECT_MASTER_BEGIN(g, vl)
  FOR_VERTEX_BEGIN(g, vl, v)
    FOR_ADJ_LIST_BEGIN(g, dir, v, al)
      GRIN_EDGE_PROPERTY_LIST epl = grin_get_edge_property_list_by_type(g, __et);
      size_t epl_size = grin_get_edge_property_list_size(g, epl);

      GRIN_ADJACENT_LIST_ITERATOR ali = grin_get_adjacent_list_begin(g, al);
      size_t acnt = 0;
      while (!grin_is_adjacent_list_end(g, ali)) {
        GRIN_EDGE e = grin_get_edge_from_adjacent_list_iter(g, ali);
        GRIN_VERTEX u = grin_get_neighbor_from_adjacent_list_iter(g, ali);
      #ifdef GRIN_ENABLE_VERTEX_INTERNAL_ID_INDEX
        GRIN_VERTEX_TYPE ut = grin_get_vertex_type(g, u);
        long long int vid = grin_get_vertex_internal_id_by_type(g, __vt, v);
        long long int uid = grin_get_vertex_internal_id_by_type(g, ut, u);
        grin_destroy_vertex_type(g, ut);
      #else
        long long int vid = __vcnt;
        long long int uid = acnt;
      #endif
      #ifdef GRIN_ENABLE_ROW
        GRIN_ROW row = grin_get_edge_row(g, e);
      #endif
        for (size_t j = 0; j < epl_size; ++j) {
          GRIN_EDGE_PROPERTY ep = grin_get_edge_property_from_list(g, epl, j);
          GRIN_DATATYPE dt = grin_get_edge_property_datatype(g, ep);
          if (dt == Int64) {
            long long int pv =
                grin_get_edge_property_value_of_int64(g, e, ep);
            assert(grin_get_last_error_code() == NO_ERROR);
          #ifdef GRIN_ENABLE_ROW
            long long int rv = grin_get_int64_from_row(g, row, j);
            assert(pv == rv);
          #endif
          #ifdef GRIN_WITH_EDGE_PROPERTY_NAME
            printf("%s %s %s: %lld\n", v_names[__vt][vid], v_names[ut][uid], 
              grin_get_edge_property_name(g, __et, ep), pv);
          #else
            printf("%s %zu %lld: %lld\n", v_names[__vt][vid], j, uid, pv);
          #endif
          } else if (dt == Double) {
            double pv = grin_get_edge_property_value_of_double(g, e, ep);
            assert(grin_get_last_error_code() == NO_ERROR);
          #ifdef GRIN_ENABLE_ROW
            double rv = grin_get_double_from_row(g, row, j);
            assert(pv == rv);
          #endif
          #ifdef GRIN_WITH_EDGE_PROPERTY_NAME
            printf("%s %s %s: %lf\n", v_names[__vt][vid], v_names[ut][uid], 
              grin_get_edge_property_name(g, __et, ep), pv);
          #else
            printf("%s %zu %lld: %lf\n", v_names[__vt][vid], j, uid, pv);
          #endif
          } else if (dt == String) {
            const char* pv = grin_get_edge_property_value_of_string(g, e, ep);
            assert(grin_get_last_error_code() == NO_ERROR);
          #ifdef GRIN_ENABLE_ROW
            const char* rv = grin_get_string_from_row(g, row, j);
            assert(strcmp(pv, rv) == 0);
          #endif
          #ifdef GRIN_WITH_EDGE_PROPERTY_NAME
            printf("%s %s %s: %s\n", v_names[__vt][vid], v_names[ut][uid], 
              grin_get_edge_property_name(g, __et, ep), pv);
          #else
            printf("%s %zu %lld: %s\n", v_names[__vt][vid], j, uid, pv);
          #endif
          }
        }
      #ifdef GRIN_ENABLE_ROW
        grin_destroy_row(g, row);
      #endif
        grin_destroy_edge(g, e);
        grin_destroy_vertex(g, u);
        acnt++;
        grin_get_next_adjacent_list_iter(g, ali);
      }
      grin_destroy_adjacent_list_iter(g, ali);
      grin_destroy_edge_property_list(g, epl);
    FOR_ADJ_LIST_END(g, al)
  FOR_VERTEX_END(g, vl, v)
FOR_VERTEX_LIST_END(g, vl)

// check schema
  printf("------ check schema ------\n");
  GRIN_EDGE_TYPE_LIST etl = grin_get_edge_type_list(g);
  size_t etl_size = grin_get_edge_type_list_size(g, etl);
  for (size_t i = 0; i < etl_size; ++i) {
    GRIN_EDGE_TYPE et = grin_get_edge_type_from_list(g, etl, i);
    GRIN_EDGE_PROPERTY_LIST epl = grin_get_edge_property_list_by_type(g, et);
    size_t epl_size = grin_get_edge_property_list_size(g, epl);
    for (size_t j = 0; j < epl_size; ++j) {
      GRIN_EDGE_PROPERTY ep = grin_get_edge_property_from_list(g, epl, j);
      GRIN_EDGE_TYPE et1 = grin_get_edge_type_from_property(g, ep);
      assert(grin_equal_edge_type(g, et, et1));
      grin_destroy_edge_type(g, et1);

    #ifdef GRIN_TRAIT_NATURAL_ID_FOR_EDGE_PROPERTY
      unsigned int id = grin_get_edge_property_id(g, et, ep);
      GRIN_EDGE_PROPERTY ep1 = grin_get_edge_property_by_id(g, et, id);
      assert(grin_equal_edge_property(g, ep, ep1));
      grin_destroy_edge_property(g, ep1);
    #else
      unsigned int id = i;
    #endif

    #ifdef GRIN_WITH_EDGE_PROPERTY_NAME
      const char* ep_name = grin_get_edge_property_name(g, et, ep);
      GRIN_EDGE_PROPERTY ep2 =
          grin_get_edge_property_by_name(g, et, ep_name);
      assert(grin_equal_edge_property(g, ep, ep2));
    #else
        const char* ep_name = "unknown";
    #endif
      printf("%s %u %s checked\n", et_names[i], id, ep_name);
    }
    grin_destroy_edge_property_list(g, epl);

    // corner case
  #ifdef GRIN_TRAIT_NATURAL_ID_FOR_EDGE_PROPERTY
    GRIN_EDGE_PROPERTY ep3 = grin_get_edge_property_by_id(g, et, epl_size);
    assert(ep3 == GRIN_NULL_EDGE_PROPERTY);
  #endif

  #ifdef GRIN_WITH_EDGE_PROPERTY_NAME
    GRIN_EDGE_PROPERTY ep4 =
        grin_get_edge_property_by_name(g, et, "unknown");
    assert(ep4 == GRIN_NULL_EDGE_PROPERTY);
  #endif
    grin_destroy_edge_type(g, et);
  }
  grin_destroy_edge_type_list(g, etl);

  // corner case
#ifdef GRIN_WITH_EDGE_PROPERTY_NAME
  GRIN_EDGE_PROPERTY_LIST epl1 =
      grin_get_edge_properties_by_name(g, "unknown");
  assert(epl1 == GRIN_NULL_EDGE_PROPERTY_LIST);

  GRIN_EDGE_PROPERTY_LIST epl2 =
      grin_get_edge_properties_by_name(g, "weight");
  assert(epl2 != GRIN_NULL_EDGE_PROPERTY_LIST);
  
  size_t epl2_size = grin_get_edge_property_list_size(g, epl2);
  for (size_t i = 0; i < epl2_size; ++i) {
    GRIN_EDGE_PROPERTY ep5 =
        grin_get_edge_property_from_list(g, epl2, i);
    GRIN_EDGE_TYPE et5 = grin_get_edge_type_from_property(g, ep5);
    const char* ep5_name = grin_get_edge_property_name(g, et5, ep5);
    assert(strcmp(ep5_name, "weight") == 0);
  }
  grin_destroy_edge_property_list(g, epl2);
#endif

  grin_destroy_graph(g);
}


#ifdef GRIN_ENABLE_VERTEX_PRIMARY_KEYS
void test_property_primary_key(int argc, char** argv) {
  printf(
      "+++++++++++++++++++++ Test property/primary key "
      "+++++++++++++++++++++\n");
  GRIN_GRAPH g = get_graph(argc, argv, 0);
  GRIN_VERTEX_TYPE_LIST vtl = grin_get_vertex_types_with_primary_keys(g);
  size_t vtl_size = grin_get_vertex_type_list_size(g, vtl);
  printf("vertex type num with primary key: %zu\n", vtl_size);

  unsigned id_type[7] = {~0, 0, 0, 1, 0, 1, 0};

  for (size_t i = 0; i < vtl_size; ++i) {
    GRIN_VERTEX_TYPE vt = grin_get_vertex_type_from_list(g, vtl, i);
    const char* vt_name = grin_get_vertex_type_name(g, vt);
    printf("vertex type name: %s\n", vt_name);

    GRIN_VERTEX_PROPERTY_LIST vpl = grin_get_primary_keys_by_vertex_type(g, vt);
    size_t vpl_size = grin_get_vertex_property_list_size(g, vpl);
    assert(vpl_size == 1);

    for (size_t j = 0; j < vpl_size; ++j) {
      GRIN_VERTEX_PROPERTY vp = grin_get_vertex_property_from_list(g, vpl, j);
      const char* vp_name = grin_get_vertex_property_name(g, vt, vp);
      printf("primary key name: %s\n", vp_name);
      grin_destroy_vertex_property(g, vp);
    }

    GRIN_VERTEX_PROPERTY vp = grin_get_vertex_property_from_list(g, vpl, 0);
    GRIN_DATATYPE dt = grin_get_vertex_property_datatype(g, vp);

    for (size_t j = 1; j <= 6; ++j) {
      GRIN_ROW r = grin_create_row(g);
      assert(dt == Int64);
      grin_insert_int64_to_row(g, r, j);
#ifdef GRIN_ENABLE_VERTEX_PK_INDEX
      GRIN_VERTEX v = grin_get_vertex_by_primary_keys_row(g, vt, r);
      if (v != GRIN_NULL_VERTEX && id_type[j] == i) {
        GRIN_ROW nr = grin_get_vertex_primary_keys_row(g, v);
        long long int k = grin_get_int64_from_row(g, nr, 0);
        assert(k == j);
        grin_destroy_row(g, nr);
        grin_destroy_vertex(g, v);
      }
#endif
      grin_destroy_row(g, r);
    }

    grin_destroy_vertex_property(g, vp);
    grin_destroy_vertex_property_list(g, vpl);
    grin_destroy_vertex_type(g, vt);
  }

  grin_destroy_vertex_type_list(g, vtl);
  grin_destroy_graph(g);
}
#endif

void test_error_code(int argc, char** argv) {
  printf("+++++++++++++++++++++ Test error code +++++++++++++++++++++\n");
  GRIN_GRAPH g = get_graph(argc, argv, 0);

  GRIN_VERTEX_TYPE vt1 = grin_get_vertex_type_by_name(g, "person");
  GRIN_VERTEX_TYPE vt2 = grin_get_vertex_type_by_name(g, "software");
  GRIN_VERTEX_PROPERTY vp = grin_get_vertex_property_by_name(g, vt2, "lang");
#ifdef GRIN_ENABLE_GRAPH_PARTITION
  GRIN_VERTEX v = get_one_master_person(g);
#else
  GRIN_VERTEX v = get_one_person(g);
#endif

  const char* value = grin_get_vertex_property_value_of_string(g, v, vp);
  assert(grin_get_last_error_code() == INVALID_VALUE);
}


void test_property(int argc, char** argv) {
  test_property_type(argc, argv);
  test_property_vertex_property_value(argc, argv);
  test_property_edge_property_value(argc, argv, OUT);
  test_property_edge_property_value(argc, argv, IN);
#ifdef GRIN_ENABLE_VERTEX_PRIMARY_KEYS
  test_property_primary_key(argc, argv);
#endif
#ifdef GRIN_WITH_VERTEX_PROPERTY_NAME
  // test_error_code(argc, argv);
#endif
}


void test_partition_reference(int argc, char** argv) {
  printf("+++++++++++++++++++++ Test partition/reference +++++++++++++++++++++\n");
  GRIN_PARTITIONED_GRAPH pg = grin_get_partitioned_graph_from_storage(argv[1]);
  GRIN_PARTITION_LIST local_partitions = grin_get_local_partition_list(pg);
  assert(grin_get_partition_list_size(pg, local_partitions) >= 2);

  GRIN_PARTITION p0 = grin_get_partition_from_list(pg, local_partitions, 0);
  GRIN_PARTITION p1 = grin_get_partition_from_list(pg, local_partitions, 1);
  GRIN_GRAPH g0 = grin_get_local_graph_by_partition(pg, p0);
  GRIN_GRAPH g1 = grin_get_local_graph_by_partition(pg, p1);

FOR_VERTEX_LIST_BEGIN(g0, vl0)
  size_t mcnt = 0;
  FOR_VERTEX_BEGIN(g0, vl0, v0)
    GRIN_VERTEX_REF vref0 = grin_get_vertex_ref_by_vertex(g0, v0);
    if (grin_is_master_vertex(g0, v0)) {
      mcnt++;
#ifdef GRIN_TRAIT_FAST_VERTEX_REF
      long long int sref = grin_serialize_vertex_ref_as_int64(g0, vref0);
      GRIN_VERTEX_REF vref1 = grin_deserialize_int64_to_vertex_ref(g0, sref);
#else
      const char* sref = grin_serialize_vertex_ref(g0, vref0);
      GRIN_VERTEX_REF vref1 = grin_deserialize_vertex_ref(g0, sref);
      grin_destroy_string_value(g0, sref);
#endif
      GRIN_VERTEX v1 = grin_get_vertex_from_vertex_ref(g0, vref1);
      if (!grin_equal_vertex(g0, v0, v1)) {
        printf("vertex not match after deserialize\n");
      }
      GRIN_PARTITION p = grin_get_master_partition_from_vertex_ref(g0, vref0);
      if (!grin_equal_partition(g0, p, p0)) {
        printf("(Wrong) partition not match in vertex ref\n");
      }      
      grin_destroy_partition(pg, p);
      grin_destroy_vertex(g0, v1);
      grin_destroy_vertex_ref(g0, vref1);
    } else if (grin_is_mirror_vertex(g0, v0)) {
#ifdef GRIN_TRAIT_FAST_VERTEX_REF
      long long int sref = grin_serialize_vertex_ref_as_int64(g0, vref0);
      GRIN_VERTEX_REF vref1 = grin_deserialize_int64_to_vertex_ref(g1, sref);
#else
      const char* sref = grin_serialize_vertex_ref(g0, vref0);
      GRIN_VERTEX_REF vref1 = grin_deserialize_vertex_ref(g1, sref);
      grin_destroy_string_value(g0, sref);
#endif
      GRIN_VERTEX v1 = grin_get_vertex_from_vertex_ref(g1, vref1);
      if (!grin_is_master_vertex(g1, v1)) {
        printf("(Wrong) vertex not master after deserialize\n");
      }
      GRIN_PARTITION p = grin_get_master_partition_from_vertex_ref(g0, vref0);
      if (!grin_equal_partition(g0, p, p1)) {
        printf("(Wrong) partition not match in vertex ref\n");
      }
      grin_destroy_partition(pg, p);
      grin_destroy_vertex(g1, v1);
      grin_destroy_vertex_ref(g1, vref1);
    } else {
      printf("(Wrong) vertex other than master or mirror\n");
    }
    grin_destroy_vertex_ref(g0, vref0);
  FOR_VERTEX_END(g0, vl0, v0)
  printf("master checked: %zu\n", mcnt);
FOR_VERTEX_LIST_END(g0, vl0)

  grin_destroy_partition(pg, p0);
  grin_destroy_partition(pg, p1);
  grin_destroy_graph(g0);
  grin_destroy_graph(g1);
  grin_destroy_partition_list(pg, local_partitions);
  grin_destroy_partitioned_graph(pg);
}


void test_partition_topology(int argc, char** argv) {
  printf("+++++++++++++++++++++ Test partition/topology +++++++++++++++++++++\n");
  GRIN_GRAPH g = get_graph(argc, argv, 0);

  printf("----- check master ----- \n");
FOR_VERTEX_LIST_SELECT_MASTER_BEGIN(g, vl)
  FOR_VERTEX_BEGIN(g, vl, v)
  #ifdef GRIN_ENABLE_VERTEX_LIST_ARRAY
    GRIN_VERTEX v1 = grin_get_vertex_from_list(g, vl, __vcnt);
    assert(grin_equal_vertex(g, v, v1));
    grin_destroy_vertex(g, v1);
  #endif
    assert(grin_is_master_vertex(g, v));
  FOR_VERTEX_END(g, vl, v)
FOR_VERTEX_LIST_END(g, vl)

  printf("----- check mirror ----- \n");
FOR_VERTEX_LIST_SELECT_MIRROR_BEGIN(g, vl)
  FOR_VERTEX_BEGIN(g, vl, v)
  #ifdef GRIN_ENABLE_VERTEX_LIST_ARRAY
    GRIN_VERTEX v1 = grin_get_vertex_from_list(g, vl, __vcnt);
    assert(grin_equal_vertex(g, v, v1));
    grin_destroy_vertex(g, v1);
  #endif
    assert(grin_is_mirror_vertex(g, v));
  FOR_VERTEX_END(g, vl, v)
FOR_VERTEX_LIST_END(g, vl)

  grin_destroy_graph(g);
}

void test_partition(int argc, char** argv) {
#ifdef GRIN_ENABLE_GRAPH_PARTITION
  test_partition_reference(argc, argv);
  test_partition_topology(argc, argv);
#endif
}


void test_topology_structure(int argc, char** argv) {
  printf("+++++++++++++++++++++ Test topology/structure +++++++++++++++++++++\n");
  GRIN_GRAPH g = get_graph(argc, argv, 0);
#ifndef GRIN_WITH_VERTEX_PROPERTY
  printf("vertex num: %zu\n", grin_get_vertex_num(g));
#endif

#ifndef GRIN_WITH_EDGE_PROPERTY
  printf("edge num: %zu\n", grin_get_edge_num(g));
#endif
  grin_destroy_graph(g);
}


void test_topology_vertex_list(int argc, char** argv) {
  printf("+++++++++++++++++++++ Test topology/vertex_list +++++++++++++++++++++\n");
  GRIN_GRAPH g = get_graph(argc, argv, 0);

FOR_VERTEX_LIST_BEGIN(g, vl)
  FOR_VERTEX_BEGIN(g, vl, v)
  #ifdef GRIN_ENABLE_VERTEX_LIST_ARRAY
    GRIN_VERTEX v1 = grin_get_vertex_from_list(g, vl, __vcnt);
    assert(grin_equal_vertex(g, v, v1));
    grin_destroy_vertex(g, v1);
  #endif
  FOR_VERTEX_END(g, vl, v)
FOR_VERTEX_LIST_END(g, vl)

  grin_destroy_graph(g);
}


void test_topology_adjacent_list(int argc, char** argv, GRIN_DIRECTION dir) {
  if (dir == IN) {
    printf("+++++++++++++++++++++ Test topology/adjacent_list IN +++++++++++++++++++++\n");
  } else {
    printf("+++++++++++++++++++++ Test topology/adjacent_list OUT +++++++++++++++++++++\n");
  }

  GRIN_GRAPH g = get_graph(argc, argv, 0);

FOR_VERTEX_LIST_BEGIN(g, vl)
  FOR_VERTEX_BEGIN(g, vl, v)
  #ifdef GRIN_ENABLE_VERTEX_INTERNAL_ID_INDEX
    long long int vid = grin_get_vertex_internal_id_by_type(g, __vt, v);
  #else
    long long int vid = __vcnt;
  #endif
  #ifdef GRIN_ENABLE_GRAPH_PARTITION
    if (!grin_is_master_vertex(g, v)) {
      grin_destroy_vertex(g, v);
      grin_get_next_vertex_list_iter(g, __vli);
      continue;
    }
  #endif

    FOR_ADJ_LIST_BEGIN(g, dir, v, al)
      GRIN_ADJACENT_LIST_ITERATOR ali = grin_get_adjacent_list_begin(g, al);
      size_t acnt = 0;
      while (!grin_is_adjacent_list_end(g, ali)) {
        GRIN_EDGE e = grin_get_edge_from_adjacent_list_iter(g, ali);
        GRIN_VERTEX v1 = grin_get_src_vertex_from_edge(g, e);
        GRIN_VERTEX v2 = grin_get_dst_vertex_from_edge(g, e);
        GRIN_VERTEX u = grin_get_neighbor_from_adjacent_list_iter(g, ali);

      #ifdef GRIN_ENABLE_ADJACENT_LIST_ARRAY
        GRIN_EDGE e1 = grin_get_edge_from_adjacent_list(g, al, acnt);
        GRIN_VERTEX e1v1 = grin_get_src_vertex_from_edge(g, e1);
        GRIN_VERTEX e1v2 = grin_get_dst_vertex_from_edge(g, e1);
        assert(grin_equal_vertex(g, v1, e1v1));
        assert(grin_equal_vertex(g, v2, e1v2));
        grin_destroy_edge(g, e1);
        grin_destroy_vertex(g, e1v1);
        grin_destroy_vertex(g, e1v2);
      #endif

        if (dir == OUT) {
          assert(grin_equal_vertex(g, v, v1));
          assert(grin_equal_vertex(g, v2, u));
        } else {
          assert(grin_equal_vertex(g, v, v2));
          assert(grin_equal_vertex(g, v1, u));
        }

        grin_destroy_vertex(g, v1);
        grin_destroy_vertex(g, v2);
        grin_destroy_vertex(g, u);
        grin_destroy_edge(g, e);

        acnt++;
        grin_get_next_adjacent_list_iter(g, ali);
      }
    #ifdef GRIN_ENABLE_ADJAECENT_LIST_ARRAY
      assert(acnt == grin_get_adjacent_list_size(g, al));
    #endif
      grin_destroy_adjacent_list_iter(g, ali);
    #ifdef GRIN_WITH_EDGE_PROPERTY
      printf("vertex %s adjlist, edgetype: %s, checked num: %zu\n", v_names[__vt][vid], et_names[__etl_i], acnt);
    #else
      printf("vertex %s adjlist, checked num: %zu\n", v_names[__vt][vid], acnt);
    #endif
    FOR_ADJ_LIST_END(g, al)
  FOR_VERTEX_END(g, vl, v)
FOR_VERTEX_LIST_END(g, vl)
  grin_destroy_graph(g);
}


void test_topology(int argc, char** argv) {
  test_topology_structure(argc, argv);
  test_topology_vertex_list(argc, argv);
  test_topology_adjacent_list(argc, argv, OUT);
  test_topology_adjacent_list(argc, argv, IN);
}

#if defined(GRIN_ASSUME_ALL_VERTEX_LIST_SORTED) && defined(GRIN_ENABLE_VERTEX_LIST_ARRAY)
void test_index_order(int argc, char** argv) {
  printf("+++++++++++++++++++++ Test index order +++++++++++++++++++++\n");
  GRIN_GRAPH g = get_graph(argc, argv, 0);

FOR_VERTEX_LIST_BEGIN(g, vl)
  FOR_VERTEX_BEGIN(g, vl, v)
    size_t pos = grin_get_position_of_vertex_from_sorted_list(g, vl, v);
    assert(pos == __vcnt);
  FOR_VERTEX_END(g, vl, v)

#ifdef GRIN_ENABLE_GRAPH_PARTITION
{
  GRIN_VERTEX_LIST mvlist = grin_get_vertex_list_by_type_select_master(g, __vt);
  size_t mvlist_sz = grin_get_vertex_list_size(g, mvlist);
  for (size_t i = 0; i < mvlist_sz; ++i) {
    GRIN_VERTEX v = grin_get_vertex_from_list(g, mvlist, i);
    size_t pos = grin_get_position_of_vertex_from_sorted_list(g, mvlist, v);
    assert(pos == i);
    size_t pos1 = grin_get_position_of_vertex_from_sorted_list(g, vl, v);
    GRIN_VERTEX v1 = grin_get_vertex_from_list(g, vl, pos1);
    assert(grin_equal_vertex(g, v, v1));
    grin_destroy_vertex(g, v1);
    grin_destroy_vertex(g, v);
  }
  grin_destroy_vertex_list(g, mvlist);
}
{
  GRIN_VERTEX_LIST mvlist = grin_get_vertex_list_by_type_select_mirror(g, __vt);
  size_t mvlist_sz = grin_get_vertex_list_size(g, mvlist);
  for (size_t i = 0; i < mvlist_sz; ++i) {
    GRIN_VERTEX v = grin_get_vertex_from_list(g, mvlist, i);
    size_t pos = grin_get_position_of_vertex_from_sorted_list(g, mvlist, v);
    assert(pos == i);
    size_t pos1 = grin_get_position_of_vertex_from_sorted_list(g, vl, v);
    GRIN_VERTEX v1 = grin_get_vertex_from_list(g, vl, pos1);
    assert(grin_equal_vertex(g, v, v1));
    grin_destroy_vertex(g, v1);
    grin_destroy_vertex(g, v);
  }
  grin_destroy_vertex_list(g, mvlist);
}
#endif
FOR_VERTEX_LIST_END(g, vl)

  grin_destroy_graph(g);
}
#endif

void test_index_internal_id(int argc, char** argv) {
  printf("+++++++++++++++++++++ Test index internal id +++++++++++++++++++++\n");
  GRIN_GRAPH g = get_graph(argc, argv, 0);

FOR_VERTEX_LIST_BEGIN(g, vl)
  long long int min = grin_get_vertex_internal_id_lower_bound_by_type(g, __vt);
  long long int max = grin_get_vertex_internal_id_upper_bound_by_type(g, __vt);
  FOR_VERTEX_BEGIN(g, vl, v)
#ifdef GRIN_ENABLE_VERTEX_INTERNAL_ID_INDEX
  long long int oid = grin_get_vertex_internal_id_by_type(g, __vt, v);
  assert(oid >= min && oid < max);
  GRIN_VERTEX v1 = grin_get_vertex_by_internal_id_by_type(g, __vt, oid);
  assert(grin_equal_vertex(g, v, v1));
  grin_destroy_vertex(g, v1);
#endif
  FOR_VERTEX_END(g, vl, v)
FOR_VERTEX_LIST_END(g, vl)

  grin_destroy_graph(g);
}


void test_index(int argc, char** argv) {
#if defined(GRIN_ASSUME_ALL_VERTEX_LIST_SORTED) && defined(GRIN_ENABLE_VERTEX_LIST_ARRAY)
  test_index_order(argc, argv);
#endif
#ifdef GRIN_ENABLE_VERTEX_INTERNAL_ID_INDEX
  test_index_internal_id(argc, argv);
#endif
}

void test_vertex_property_value(int argc, char** argv) {
  GRIN_GRAPH g = get_graph(argc, argv, 0);
  GRIN_VERTEX_TYPE vt = grin_get_vertex_type_by_name(g, "person");
  GRIN_VERTEX_PROPERTY vp = grin_get_vertex_property_by_name(g, vt, "age");
  GRIN_VERTEX v = get_one_master_person(g);
  struct timeval t1, t2;
  gettimeofday(&t1, NULL);
  for (int i = 0; i < 1000000; ++i) {
    long long int age = grin_get_vertex_property_value_of_int64(g, v, vp);
  }
  gettimeofday(&t2, NULL);
  double elapsedTime = (t2.tv_sec - t1.tv_sec) * 1000.0;
  elapsedTime += (t2.tv_usec - t1.tv_usec) / 1000.0; 
  printf("%f ms.\n", elapsedTime);
  grin_destroy_vertex(g, v);
  grin_destroy_vertex_property(g, vp);
  grin_destroy_vertex_type(g, vt);
  grin_destroy_graph(g);
}

void test_perf(int argc, char** argv) {
  test_vertex_property_value(argc, argv);
}

int main(int argc, char** argv) {
  test_index(argc, argv);
  test_property(argc, argv);
  test_partition(argc, argv);
  test_topology(argc, argv);
  test_perf(argc, argv);
  return 0;
}

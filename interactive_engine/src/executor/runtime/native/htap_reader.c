/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "global_store_ffi.h"

void output_property(struct Property* prop, FILE* fout) {
  fprintf(fout, "[(%d)]", prop->id);
  if (prop->type == LONG) {
    int64_t ret;
    int flag = get_property_as_long(prop, &ret);
    assert(flag == 0);
    fprintf(fout, "%ld", ret);
  } else if (prop->type == INT) {
    int ret;
    int flag = get_property_as_int(prop, &ret);
    assert(flag == 0);
    fprintf(fout, "%d", ret);
  } else if (prop->type == FLOAT) {
    float ret;
    int flag = get_property_as_float(prop, &ret);
    assert(flag == 0);
    fprintf(fout, "%f", ret);
  } else if (prop->type == DOUBLE) {
    double ret;
    int flag = get_property_as_double(prop, &ret);
    assert(flag == 0);
    // fprintf(fout, "%lf", ret);
    fprintf(fout, "%g", ret);
  } else if (prop->type == STRING) {
    const char* ret = NULL;
    int ret_size = 0;
    int flag = get_property_as_string(prop, &ret, &ret_size);
    assert(flag == 0);
    fprintf(fout, "%.*s", ret_size, ret);
  } else {
    printf("invalid: type is: %d\n", prop->type);
  }
}

void output_properties(PropertiesIterator prop_iter, FILE* fout) {
  struct Property property;
  while (properties_next(prop_iter, &property) == 0) {
    fprintf(fout, ",");
    output_property(&property, fout);
    free_property(&property);
  }
}

void verify_got_all_label_vertices(GraphHandle handle,
                                   PartitionId partition_id) {
  GetAllVerticesIterator iter =
      get_all_vertices(handle, partition_id, NULL, 0, INT64_MAX);

  const int batch = 1024;

  VertexId* id_list = (VertexId*)(malloc(sizeof(VertexId) * batch));
  while (1) {
    int to_terminate = 0;
    int num = 0;
    for (; num != batch; ++num) {
      Vertex v;
      if (get_all_vertices_next(iter, &v) != 0) {
        to_terminate = 1;
        break;
      }
      id_list[num] = get_vertex_id(handle, v);
    }

    if (num != 0) {
      GetVertexIterator gv_iter =
          get_vertices(handle, partition_id, NULL, id_list, num);
      for (int i = 0; i < num; ++i) {
        Vertex v;
        int flag = get_vertices_next(gv_iter, &v);
        assert(flag == 0);
        assert(get_vertex_id(handle, v) == id_list[i]);
      }
      free_get_vertex_iterator(gv_iter);
    }

    if (to_terminate) {
      break;
    }
  }

  free(id_list);
  free_get_all_vertices_iterator(iter);
}

void verify_got_vertices(GraphHandle handle, PartitionId partition_id,
                         LabelId label_id) {
  GetAllVerticesIterator iter =
      get_all_vertices(handle, partition_id, &label_id, 1, INT64_MAX);

  const int batch = 1024;

  VertexId* id_list = (VertexId*)(malloc(sizeof(VertexId) * batch));
  while (1) {
    int to_terminate = 0;
    int num = 0;
    for (; num != batch; ++num) {
      Vertex v;
      if (get_all_vertices_next(iter, &v) != 0) {
        to_terminate = 1;
        break;
      }
      id_list[num] = get_vertex_id(handle, v);
    }

    if (num != 0) {
      GetVertexIterator gv_iter =
          get_vertices(handle, partition_id, &label_id, id_list, num);
      for (int i = 0; i < num; ++i) {
        Vertex v;
        int flag = get_vertices_next(gv_iter, &v);
        assert(flag == 0);
        assert(get_vertex_id(handle, v) == id_list[i]);
      }
      free_get_vertex_iterator(gv_iter);
    }

    if (to_terminate) {
      break;
    }
  }

  free(id_list);
  free_get_all_vertices_iterator(iter);
}

void output_vertex_info(GraphHandle handle, PartitionId partition_id,
                        LabelId label_id) {
  char fname[1024];
  snprintf(fname, sizeof(fname), "./ffi_vd_%d_%d", partition_id, label_id);
  FILE* fout = fopen(fname, "wb");

  GetAllVerticesIterator iter =
      get_all_vertices(handle, partition_id, &label_id, 1, INT64_MAX);
  Vertex v;
  while (get_all_vertices_next(iter, &v) == 0) {
    OuterId id = get_outer_id(handle, v);
    LabelId got_label = get_vertex_label(handle, v);
    assert(got_label == label_id);
    fprintf(fout, "%ld", id);
    PropertiesIterator prop_iter = get_vertex_properties(handle, v);
    output_properties(prop_iter, fout);
    fprintf(fout, "\n");
    free_properties_iterator(prop_iter);
  }

  free_get_all_vertices_iterator(iter);

  fflush(fout);
  fclose(fout);
}

void output_all_vertex_info(GraphHandle handle, PartitionId partition_id) {
  char fname[1024];
  snprintf(fname, sizeof(fname), "./ffi_all_vd_%d", partition_id);
  FILE* fout = fopen(fname, "wb");

  GetAllVerticesIterator iter =
      get_all_vertices(handle, partition_id, NULL, 0, INT64_MAX);
  Vertex v;
  while (get_all_vertices_next(iter, &v) == 0) {
    OuterId id = get_outer_id(handle, v);
    LabelId got_label = get_vertex_label(handle, v);
    fprintf(fout, "%ld", id);
    PropertiesIterator prop_iter = get_vertex_properties(handle, v);
    output_properties(prop_iter, fout);
    fprintf(fout, "\n");
    free_properties_iterator(prop_iter);
  }

  free_get_all_vertices_iterator(iter);

  fflush(fout);
  fclose(fout);
}

void output_out_edge_info(GraphHandle handle, PartitionId partition_id,
                          LabelId vertex_label_num, LabelId edge_label_id) {
  char fname[1024];
  snprintf(fname, sizeof(fname), "./ffi_out_ed_%d_%d", partition_id,
           edge_label_id);
  FILE* fout = fopen(fname, "wb");

  GetAllVerticesIterator iter =
      get_all_vertices(handle, partition_id, NULL, 0, INT64_MAX);
  Vertex src_v;
  while (get_all_vertices_next(iter, &src_v) == 0) {
    OuterId src = get_outer_id(handle, src_v);
    VertexId src_id = get_vertex_id(handle, src_v);
    LabelId src_label = get_vertex_label(handle, src_v);

    OutEdgeIterator oe_iter = get_out_edges(handle, partition_id, src_id,
                                            &edge_label_id, 1, INT64_MAX);
    struct Edge e;
    while (out_edge_next(oe_iter, &e) == 0) {
      // EdgeId edge_id = get_edge_id(handle, &e);

      VertexId dst_id = get_edge_dst_id(handle, &e);
      OuterId dst = get_outer_id_by_vertex_id(handle, dst_id);
      LabelId dst_label = get_edge_dst_label(handle, &e);

      VertexId src_id_got = get_edge_src_id(handle, &e);
      assert(src_id_got == src_id);
      LabelId src_label_got = get_edge_src_label(handle, &e);
      assert(src_label_got == src_label);

      LabelId edge_label_got = get_edge_label(handle, &e);
      assert(edge_label_got == edge_label_id);

      // fprintf(fout, "%lld,%lld,%lld,%d,%d", edge_id, src, dst, src_label,
      fprintf(fout, "%ld,%ld,%d,%d", src, dst, src_label, dst_label);
      PropertiesIterator prop_iter = get_edge_properties(handle, &e);
      output_properties(prop_iter, fout);
      fprintf(fout, "\n");
      free_properties_iterator(prop_iter);
    }

    free_out_edge_iterator(oe_iter);
  }

  free_get_all_vertices_iterator(iter);

  fflush(fout);
  fclose(fout);
}

void output_in_edge_info(GraphHandle handle, PartitionId partition_id,
                         LabelId vertex_label_num, LabelId edge_label_id) {
  char fname[1024];
  snprintf(fname, sizeof(fname), "./ffi_in_ed_%d_%d", partition_id,
           edge_label_id);
  FILE* fout = fopen(fname, "wb");

  GetAllVerticesIterator iter =
      get_all_vertices(handle, partition_id, NULL, 0, INT64_MAX);
  Vertex dst_v;
  while (get_all_vertices_next(iter, &dst_v) == 0) {
    OuterId dst = get_outer_id(handle, dst_v);
    VertexId dst_id = get_vertex_id(handle, dst_v);
    LabelId dst_label = get_vertex_label(handle, dst_v);

    InEdgeIterator ie_iter = get_in_edges(handle, partition_id, dst_id,
                                          &edge_label_id, 1, INT64_MAX);
    struct Edge e;
    while (in_edge_next(ie_iter, &e) == 0) {
      // EdgeId edge_id = get_edge_id(handle, &e);

      VertexId src_id = get_edge_src_id(handle, &e);
      OuterId src = get_outer_id_by_vertex_id(handle, src_id);
      LabelId src_label = get_edge_src_label(handle, &e);

      VertexId dst_id_got = get_edge_dst_id(handle, &e);
      assert(dst_id_got == dst_id);
      LabelId dst_label_got = get_edge_dst_label(handle, &e);
      assert(dst_label_got == dst_label);

      LabelId edge_label_got = get_edge_label(handle, &e);
      assert(edge_label_got == edge_label_id);

      fprintf(
          fout, "%ld,%ld,%d,%d", src, dst, src_label,
          // fprintf(fout, "%lld,%lld,%lld,%d,%d", edge_id, src, dst, src_label,
          dst_label);
      PropertiesIterator prop_iter = get_edge_properties(handle, &e);
      output_properties(prop_iter, fout);
      fprintf(fout, "\n");
      free_properties_iterator(prop_iter);
    }

    free_in_edge_iterator(ie_iter);
  }

  free_get_all_vertices_iterator(iter);

  fflush(fout);
  fclose(fout);
}

void output_all_out_edge_info(GraphHandle handle, PartitionId partition_id) {
  char fname[1024];
  snprintf(fname, sizeof(fname), "./ffi_all_out_ed_%d", partition_id);
  FILE* fout = fopen(fname, "wb");

  GetAllVerticesIterator iter =
      get_all_vertices(handle, partition_id, NULL, 0, INT64_MAX);
  Vertex src_v;
  while (get_all_vertices_next(iter, &src_v) == 0) {
    OuterId src = get_outer_id(handle, src_v);
    VertexId src_id = get_vertex_id(handle, src_v);
    LabelId src_label = get_vertex_label(handle, src_v);

    OutEdgeIterator oe_iter =
        get_out_edges(handle, partition_id, src_id, NULL, 0, INT64_MAX);
    struct Edge e;
    while (out_edge_next(oe_iter, &e) == 0) {
      // EdgeId edge_id = get_edge_id(handle, &e);

      VertexId dst_id = get_edge_dst_id(handle, &e);
      OuterId dst = get_outer_id_by_vertex_id(handle, dst_id);
      LabelId dst_label = get_edge_dst_label(handle, &e);

      VertexId src_id_got = get_edge_src_id(handle, &e);
      assert(src_id_got == src_id);
      LabelId src_label_got = get_edge_src_label(handle, &e);
      assert(src_label_got == src_label);

      // fprintf(fout, "%lld,%lld,%lld,%d,%d", edge_id, src, dst, src_label,
      fprintf(fout, "%ld,%ld,%d,%d", src, dst, src_label, dst_label);
      PropertiesIterator prop_iter = get_edge_properties(handle, &e);
      output_properties(prop_iter, fout);
      fprintf(fout, "\n");
      free_properties_iterator(prop_iter);
    }

    free_out_edge_iterator(oe_iter);
  }

  free_get_all_vertices_iterator(iter);

  fflush(fout);
  fclose(fout);
}

void output_all_in_edge_info(GraphHandle handle, PartitionId partition_id) {
  char fname[1024];
  snprintf(fname, sizeof(fname), "./ffi_all_in_ed_%d", partition_id);
  FILE* fout = fopen(fname, "wb");

  GetAllVerticesIterator iter =
      get_all_vertices(handle, partition_id, NULL, 0, INT64_MAX);
  Vertex dst_v;
  while (get_all_vertices_next(iter, &dst_v) == 0) {
    OuterId dst = get_outer_id(handle, dst_v);
    VertexId dst_id = get_vertex_id(handle, dst_v);
    LabelId dst_label = get_vertex_label(handle, dst_v);

    InEdgeIterator ie_iter =
        get_in_edges(handle, partition_id, dst_id, NULL, 0, INT64_MAX);
    struct Edge e;
    while (in_edge_next(ie_iter, &e) == 0) {
      // EdgeId edge_id = get_edge_id(handle, &e);

      VertexId src_id = get_edge_src_id(handle, &e);
      OuterId src = get_outer_id_by_vertex_id(handle, src_id);
      LabelId src_label = get_edge_src_label(handle, &e);

      VertexId dst_id_got = get_edge_dst_id(handle, &e);
      assert(dst_id_got == dst_id);
      LabelId dst_label_got = get_edge_dst_label(handle, &e);
      assert(dst_label_got == dst_label);

      // fprintf(fout, "%lld,%lld,%lld,%d,%d", edge_id, src, dst, src_label,
      fprintf(fout, "%ld,%ld,%d,%d", src, dst, src_label, dst_label);
      PropertiesIterator prop_iter = get_edge_properties(handle, &e);
      output_properties(prop_iter, fout);
      fprintf(fout, "\n");
      free_properties_iterator(prop_iter);
    }

    free_in_edge_iterator(ie_iter);
  }

  free_get_all_vertices_iterator(iter);

  fflush(fout);
  fclose(fout);
}

void output_edge_info(GraphHandle handle, PartitionId partition_id,
                      LabelId vertex_label_num, LabelId edge_label_id) {
  char fname[1024];
  snprintf(fname, sizeof(fname), "./ffi_ed_%d_%d", partition_id, edge_label_id);
  FILE* fout = fopen(fname, "wb");

  GetAllEdgesIterator iter =
      get_all_edges(handle, partition_id, &edge_label_id, 1, INT64_MAX);
  struct Edge e;
  while (get_all_edges_next(iter, &e) == 0) {
    // EdgeId edge_id = get_edge_id(handle, &e);
    VertexId src_id = get_edge_src_id(handle, &e);
    VertexId dst_id = get_edge_dst_id(handle, &e);
    OuterId src = get_outer_id_by_vertex_id(handle, src_id);
    OuterId dst = get_outer_id_by_vertex_id(handle, dst_id);

    LabelId src_label = get_edge_src_label(handle, &e);
    LabelId dst_label = get_edge_dst_label(handle, &e);

    LabelId edge_label_got = get_edge_label(handle, &e);
    assert(edge_label_got == edge_label_id);

    // fprintf(fout, "%lld,%lld,%lld,%d,%d", edge_id, src, dst, src_label,
    fprintf(fout, "%ld,%ld,%d,%d", src, dst, src_label, dst_label);
    PropertiesIterator prop_iter = get_edge_properties(handle, &e);
    output_properties(prop_iter, fout);
    fprintf(fout, "\n");
    free_properties_iterator(prop_iter);
  }

  free_get_all_edges_iterator(iter);

  fflush(fout);
  fclose(fout);
}

void output_all_edge_info(GraphHandle handle, PartitionId partition_id) {
  char fname[1024];
  snprintf(fname, sizeof(fname), "./ffi_all_ed_%d", partition_id);
  FILE* fout = fopen(fname, "wb");

  GetAllEdgesIterator iter =
      get_all_edges(handle, partition_id, NULL, 0, INT64_MAX);
  struct Edge e;
  while (get_all_edges_next(iter, &e) == 0) {
    // EdgeId edge_id = get_edge_id(handle, &e);
    VertexId src_id = get_edge_src_id(handle, &e);
    VertexId dst_id = get_edge_dst_id(handle, &e);
    OuterId src = get_outer_id_by_vertex_id(handle, src_id);
    OuterId dst = get_outer_id_by_vertex_id(handle, dst_id);

    LabelId src_label = get_edge_src_label(handle, &e);
    LabelId dst_label = get_edge_dst_label(handle, &e);

    // fprintf(fout, "%lld,%lld,%lld,%d,%d", edge_id, src, dst, src_label,
    fprintf(fout, "%ld,%ld,%d,%d", src, dst, src_label, dst_label);
    PropertiesIterator prop_iter = get_edge_properties(handle, &e);
    output_properties(prop_iter, fout);
    fprintf(fout, "\n");
    free_properties_iterator(prop_iter);
  }

  free_get_all_edges_iterator(iter);

  fflush(fout);
  fclose(fout);
}

static int has_partition(int *partition_list, int partition_size, int pid) {
  for (int i = 0; i < partition_size; ++i) {
    if (partition_list[i] == pid) {
      return 1;
    }
  }
  return 0;
}

int main(int argc, char** argv) {
  if (argc < 5) {
    printf(
        "usage: ./htap_reader <object_id> <partition_num> <vertex_label_num> "
        "<edge_label_num>\n");
    return -1;
  }
  ObjectId id = atol(argv[1]);
  PartitionId partition_num = atoi(argv[2]);
  LabelId vertex_label_num = atoi(argv[3]);
  LabelId edge_label_num = atoi(argv[4]);

  GraphHandle handle = get_graph_handle(id, 1);

  int *partition_list = NULL;
  int partition_size = -1;
  get_process_partition_list(handle, &partition_list, &partition_size);
  printf("partition_size = %d\n", partition_size);
  for (int i = 0; i < partition_size; ++i) {
    printf("partition %d = %d\n", i, partition_list[i]);
  }
  Schema schema = get_schema(handle);
  printf("schema = %p\n", schema);

  for (int pid = 0; pid != partition_num; ++pid) {
    if (!has_partition(partition_list, partition_size, pid)) {
      continue;
    }
    for (int lid = 0; lid != vertex_label_num; ++lid) {
      output_vertex_info(handle, pid, lid);
    }
  }

  for (int pid = 0; pid != partition_num; ++pid) {
    if (!has_partition(partition_list, partition_size, pid)) {
      continue;
    }
    output_all_vertex_info(handle, pid);
  }

  for (int pid = 0; pid != partition_num; ++pid) {
    if (!has_partition(partition_list, partition_size, pid)) {
      continue;
    }
    for (int lid = 0; lid != vertex_label_num; ++lid) {
      verify_got_vertices(handle, pid, lid);
    }
  }

  for (int pid = 0; pid != partition_num; ++pid) {
    if (!has_partition(partition_list, partition_size, pid)) {
      continue;
    }
    verify_got_all_label_vertices(handle, pid);
  }

  for (int pid = 0; pid != partition_num; ++pid) {
    if (!has_partition(partition_list, partition_size, pid)) {
      continue;
    }
    for (int lid = 0; lid != edge_label_num; ++lid) {
      output_out_edge_info(handle, pid, vertex_label_num,
                           lid + vertex_label_num);
    }
  }

  for (int pid = 0; pid != partition_num; ++pid) {
    if (!has_partition(partition_list, partition_size, pid)) {
      continue;
    }
    for (int lid = 0; lid != edge_label_num; ++lid) {
      output_in_edge_info(handle, pid, vertex_label_num,
                          lid + vertex_label_num);
    }
  }

  for (int pid = 0; pid != partition_num; ++pid) {
    if (!has_partition(partition_list, partition_size, pid)) {
      continue;
    }
    output_all_out_edge_info(handle, pid);
  }

  for (int pid = 0; pid != partition_num; ++pid) {
    if (!has_partition(partition_list, partition_size, pid)) {
      continue;
    }
    output_all_in_edge_info(handle, pid);
  }

  for (int pid = 0; pid != partition_num; ++pid) {
    if (!has_partition(partition_list, partition_size, pid)) {
      continue;
    }
    for (int lid = 0; lid != edge_label_num; ++lid) {
      output_edge_info(handle, pid, vertex_label_num,
                       lid + vertex_label_num);
    }
  }

  for (int pid = 0; pid != partition_num; ++pid) {
    if (!has_partition(partition_list, partition_size, pid)) {
      continue;
    }
    output_all_edge_info(handle, pid);
  }

  /*
    printf("%lld\n%ldd\n", get_partition_id(handle, 2693328655726429490L),
           get_partition_id(handle, -1589830123050515165L));
  */

  free_graph_handle(handle);

  return 0;
}

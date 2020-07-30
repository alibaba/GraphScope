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
#include <stdio.h>
#include <stdlib.h>

#include "global_store_ffi.h"

void output_property(struct Property* prop, FILE* fout) {
  if (prop->type == LONG) {
    int64_t ret;
    int flag = get_property_as_long(prop, &ret);
    if (flag != 0) {
      printf("get property failed...\n");
    }
    fprintf(fout, "%ld", ret);
  } else if (prop->type == INT) {
    int ret;
    int flag = get_property_as_int(prop, &ret);
    if (flag != 0) {
      printf("get property failed...\n");
    }
    fprintf(fout, "%d", ret);
  } else if (prop->type == FLOAT) {
    float ret;
    int flag = get_property_as_float(prop, &ret);
    if (flag != 0) {
      printf("get property failed...\n");
    }
    fprintf(fout, "%f", ret);
  } else if (prop->type == DOUBLE) {
    double ret;
    int flag = get_property_as_double(prop, &ret);
    if (flag != 0) {
      printf("get property failed...\n");
    }
    fprintf(fout, "%g", ret);
  } else if (prop->type == STRING) {
    const char* ret = NULL;
    int ret_size = 0;
    int flag = get_property_as_string(prop, &ret, &ret_size);
    if (flag != 0) {
      printf("get property failed...\n");
    }
    fprintf(fout, "%.*s", ret_size, ret);
  } else {
    printf("invalid: type is: %d\n", prop->type);
  }
}

void output_properties(PropertiesIterator prop_iter, FILE* fout) {
  struct Property property;
  while (properties_next(prop_iter, &property) == 0) {
    fprintf(fout, "|");
    output_property(&property, fout);
    free_property(&property);
  }
}

void output_edge_topology(GraphHandle handle, PartitionId partition_id,
                          LabelId edge_label_id, LabelId index) {
  char fname[1024];
  snprintf(fname, sizeof(fname), "./ffi_ed_%d_%d", partition_id, index);
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

    LabelId edge_label_got = get_edge_label(handle, &e);
    if (edge_label_got != edge_label_id) {
      printf("got edge label wrong!!!\n");
    }

    fprintf(fout, "%lld|%lld\n", src, dst);
  }

  free_get_all_edges_iterator(iter);
  fflush(fout);
  fclose(fout);
}

int main(int argc, char** argv) {
  if (argc < 5) {
    printf(
        "usage: ./htap_reader <object_id> <channel_num> <vertex_label_num> "
        "<edge_label_num>\n");
    return -1;
  }
  ObjectId id = atol(argv[1]);
  int channel_num = atoi(argv[2]);
  LabelId vertex_label_num = atoi(argv[3]);
  LabelId edge_label_num = atoi(argv[4]);

  GraphHandle handle = get_graph_handle(id, channel_num);

  PartitionId* partition_ids;
  int partition_id_size;
  get_process_partition_list(handle, &partition_ids, &partition_id_size);

  for (int i = 0; i < partition_id_size; ++i) {
    PartitionId partition_id = partition_ids[i];
    for (LabelId j = 0; j < vertex_label_num; ++j) {
      char fname[1024];
      snprintf(fname, sizeof(fname), "./ffi_vd_%d_%d", partition_id, j);
      FILE* fout = fopen(fname, "wb");

      GetAllVerticesIterator iter =
          get_all_vertices(handle, partition_id, &j, 1, INT64_MAX);
      Vertex v;
      while (get_all_vertices_next(iter, &v) == 0) {
        OuterId id = get_outer_id(handle, v);
        LabelId got_label = get_vertex_label(handle, v);
        if (got_label != j) {
          printf("got label error: %d v.s. %d\n", j, got_label);
        }
        PartitionId got_partition_id =
            get_partition_id(handle, get_vertex_id(handle, v));
        if (got_partition_id != partition_id) {
          printf("got partition id error: %d v.s. %d\n", got_partition_id,
                 partition_id);
        }
        fprintf(fout, "%lld\n", id);
      }

      free_get_all_vertices_iterator(iter);

      fflush(fout);
      fclose(fout);
    }
  }

#if 0

  for (int i = 0; i < partition_id_size; ++i) {
    PartitionId partition_id = partition_ids[i];
    LabelId j = 3;
    char fname[1024];
    snprintf(fname, sizeof(fname), "./ffi_vp_%d_%d", partition_id, j);
    FILE* fout = fopen(fname, "wb");

    GetAllVerticesIterator iter =
        get_all_vertices(handle, partition_id, &j, 1, INT64_MAX);
    Vertex v;
    while (get_all_vertices_next(iter, &v) == 0) {
      OuterId id = get_outer_id(handle, v);
      LabelId got_label = get_vertex_label(handle, v);
      if (got_label != j) {
        printf("got label error: %d v.s. %d\n", j, got_label);
      }
      fprintf(fout, "%lld", id);
      PropertiesIterator prop_iter = get_vertex_properties(handle, v);
      output_properties(prop_iter, fout);
      free_properties_iterator(prop_iter);
      fprintf(fout, "\n");
    }

    free_get_all_vertices_iterator(iter);

    fflush(fout);
    fclose(fout);
  }

  for (int i = 0; i < partition_id_size; ++i) {
    PartitionId partition_id = partition_ids[i];
    LabelId j = 4;
    char fname[1024];
    snprintf(fname, sizeof(fname), "./ffi_vp_%d_%d", partition_id, j);
    FILE* fout = fopen(fname, "wb");

    GetAllVerticesIterator iter =
        get_all_vertices(handle, partition_id, &j, 1, INT64_MAX);
    Vertex v;
    while (get_all_vertices_next(iter, &v) == 0) {
      OuterId id = get_outer_id(handle, v);
      LabelId got_label = get_vertex_label(handle, v);
      if (got_label != j) {
        printf("got label error: %d v.s. %d\n", j, got_label);
      }
      fprintf(fout, "%lld", id);
      PropertiesIterator prop_iter = get_vertex_properties(handle, v);
      output_properties(prop_iter, fout);
      free_properties_iterator(prop_iter);
      fprintf(fout, "\n");
    }

    free_get_all_vertices_iterator(iter);

    fflush(fout);
    fclose(fout);
  }

  for (int i = 0; i < partition_id_size; ++i) {
    PartitionId partition_id = partition_ids[i];
    for (LabelId j = 0; j < edge_label_num; ++j) {
      output_edge_topology(handle, i, j + vertex_label_num, j);
    }
  }
#endif

  free_partition_list(partition_ids);
  free_graph_handle(handle);

  return 0;
}

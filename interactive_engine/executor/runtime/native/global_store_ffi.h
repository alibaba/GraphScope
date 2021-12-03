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
#ifndef ANALYTICAL_ENGINE_HTAP_GLOBAL_STORE_FFI_H_
#define ANALYTICAL_ENGINE_HTAP_GLOBAL_STORE_FFI_H_

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int64_t VertexId;
typedef int64_t EdgeId;
typedef int32_t LabelId;
typedef int32_t PartitionId;
typedef int32_t PropertyId;
typedef void* GraphHandle;
typedef void* OutEdgeIterator;
typedef void* InEdgeIterator;
typedef void* GetVertexIterator;
typedef void* GetAllVerticesIterator;
typedef void* GetAllEdgesIterator;
typedef void* PropertiesIterator;
typedef void* Schema;

typedef int64_t Vertex;
struct Edge {
  int64_t src;
  int64_t dst;
  int64_t offset;
};
typedef int64_t ObjectId;

typedef int64_t OuterId;

enum PropertyType {
  INVALID = 0,
  BOOL = 1,
  CHAR = 2,
  SHORT = 3,
  INT = 4,
  LONG = 5,
  FLOAT = 6,
  DOUBLE = 7,
  STRING = 8,
  BYTES = 9,
  INT_LIST = 10,
  LONG_LIST = 11,
  FLOAT_LIST = 12,
  DOUBLE_LIST = 13,
  STRING_LIST = 14,
};

struct Property {
  int id;
  enum PropertyType type;
  void* data;
  int64_t len;
};

// ----------------- graph api -------------------- //

// 获取图存储的句柄
GraphHandle get_graph_handle(ObjectId object_id, PartitionId channel_num);

// 释放图存储的句柄，清理内存空间等
void free_graph_handle(GraphHandle handle);

// ----------------- vertex api -------------------- //

// 查询某个partition内的点数据
// ids是待查询的点id列表
// labels是这些id对应的label
// count表示id和label列表的长度，注意，label可能为null，因为上层并不一定知道某个点的具体label
// limit表示返回的最大结果数
// 返回值是一个迭代器
GetVertexIterator get_vertices(GraphHandle graph, PartitionId partition_id,
                               LabelId* labels, VertexId* ids, int count);

// 释放由get_vertices返回的迭代器（也可以什么都不做，取决于具体实现）
void free_get_vertex_iterator(GetVertexIterator iter);

// 从迭代器取出下一个元素，返回值是一个Vertex
int get_vertices_next(GetVertexIterator iter, Vertex* v_out);

// 查询某个partition内部的所有相关label的点
// labels是待查询label的列表
// labels_count表示这个label列表的长度
// limit表示返回的最大结果数
// 返回值是一个迭代器
// 注意：如果label_count为0或者labels为null，则查询所有label
GetAllVerticesIterator get_all_vertices(GraphHandle graph,
                                        PartitionId partition_id,
                                        LabelId* labels, int labels_count,
                                        int64_t limit);

// 释放由get_all_vertices返回的迭代器（也可以什么都不做，取决于具体实现）
void free_get_all_vertices_iterator(GetAllVerticesIterator iter);

// 从迭代器取出下一个元素，返回值是一个Vertex
int get_all_vertices_next(GetAllVerticesIterator iter, Vertex* v_out);

// 获取点id
VertexId get_vertex_id(GraphHandle graph, Vertex v);

OuterId get_outer_id(GraphHandle graph, Vertex v);

int get_vertex_by_outer_id(GraphHandle graph, LabelId label_id,
                           OuterId outer_id, Vertex* v);

OuterId get_outer_id_by_vertex_id(GraphHandle graph, VertexId v);

// 获取点的label
LabelId get_vertex_label(GraphHandle graph, Vertex v);

// 获取点的某个属性
int get_vertex_property(GraphHandle graph, Vertex v, PropertyId id,
                        struct Property* p_out);

// 获取点的属性列表，返回一个迭代器
PropertiesIterator get_vertex_properties(GraphHandle graph, Vertex v);

// ----------------- edge api -------------------- //

// 查询某个partition内的点的出边
// src_ids是待查询的点id列表
// labels是label列表，表示查询这些点的这些label的出边
// src_ids_count和labels_count分别表示src_id和label列表的长度，limit表示返回的最大结果数，返回值是一个迭代器
// 注意：如果label_count为0或者labels为null，则查询所有label
OutEdgeIterator get_out_edges(GraphHandle graph, PartitionId partition_id,
                              VertexId src_id, LabelId* labels,
                              int labels_count, int64_t limit);

// 释放迭代器
void free_out_edge_iterator(OutEdgeIterator iter);

// 从迭代器取出下一个元素，返回值是一个Edge
int out_edge_next(OutEdgeIterator iter, struct Edge* e_out);

// 查询某个partition内的点的入边
// src_ids是待查询的点id列表
// labels是label列表，表示查询这些点的这些label的出边
// dst_ids_count和labels_count分别表示src_id和label列表的长度
// limit表示返回的最大结果数，返回值是一个迭代器
// 注意：如果label_count为0或者labels为null，则查询所有label
InEdgeIterator get_in_edges(GraphHandle graph, PartitionId partition_id,
                            VertexId dst_id, LabelId* labels, int labels_count,
                            int64_t limit);

// 释放迭代器
void free_in_edge_iterator(InEdgeIterator iter);

// 从迭代器取出下一个元素，返回值是一个Edge
int in_edge_next(InEdgeIterator iter, struct Edge* e_out);

// 查询某个partition内某些label的边数据
// labels是待查询的label列表
// labels_count表示label列表的长度
// limit表示返回的最大结果数
// 返回值是一个迭代器。
// 注意：如果label_count为0或者labels为null，则查询所有label
GetAllEdgesIterator get_all_edges(GraphHandle graph, PartitionId partition_id,
                                  LabelId* labels, int labels_count,
                                  int64_t limit);

// 释放迭代器
void free_get_all_edges_iterator(GetAllEdgesIterator iter);

// 从迭代器取出下一个元素，返回值是一个Edge
int get_all_edges_next(GetAllEdgesIterator iter, struct Edge* e_out);

// 从edge对象获取起点id
VertexId get_edge_src_id(GraphHandle graph, struct Edge* e);

// 从edge对象获取终点id
VertexId get_edge_dst_id(GraphHandle graph, struct Edge* e);

// 从edge对象获取边id
EdgeId get_edge_id(GraphHandle graph, struct Edge* e);

// 从edge对象获取起点label
LabelId get_edge_src_label(GraphHandle graph, struct Edge* e);

// 从edge对象获取终点label
LabelId get_edge_dst_label(GraphHandle graph, struct Edge* e);

// 从edge对象获取边label
LabelId get_edge_label(GraphHandle graph, struct Edge* e);

// 获取边的某个属性
int get_edge_property(GraphHandle graph, struct Edge*, PropertyId id,
                      struct Property* p_out);

// 获取边的属性列表，返回一个迭代器
PropertiesIterator get_edge_properties(GraphHandle graph, struct Edge*);

// 从属性列表迭代器中取出一个属性，如果没有新的元素，则将Property对象内部的data置为nullptr
int properties_next(PropertiesIterator iter, struct Property* p_out);

// 释放迭代器
void free_properties_iterator(PropertiesIterator iter);

// ----------------- property api -------------------- //

// 获取属性值，这里需要在c++里判断类型是否正确，比如：对stirng属性调用get_property_as_int就应该报错，返回-1表示错误，返回0表示正确。
// 如果类型是正确的，则把值填进out指针
int get_property_as_bool(struct Property* property, bool* out);
int get_property_as_char(struct Property* property, char* out);
int get_property_as_short(struct Property* property, int16_t* out);
int get_property_as_int(struct Property* property, int* out);
int get_property_as_long(struct Property* property, int64_t* out);
int get_property_as_float(struct Property* property, float* out);
int get_property_as_double(struct Property* property, double* out);

int get_property_as_string(struct Property* property, const char** out,
                           int* out_len);
int get_property_as_bytes(struct Property* property, const char** out,
                          int* out_len);
int get_property_as_int_list(struct Property* property, const int** out,
                             int* out_len);
int get_property_as_long_list(struct Property* property, const int64_t** out,
                              int* out_len);
int get_property_as_float_list(struct Property* property, const float** out,
                               int* out_len);
int get_property_as_double_list(struct Property* property, const double** out,
                                int* out_len);
int get_property_as_string_list(struct Property* property, const char*** out,
                                const int** out_len, int* out_num);

// *out_num为string的个数
// (*out_len)[i]为第i个string的长度
// (*out)[i]为第i个string的其实地址

// 释放属性对象
void free_property(struct Property* property);

// ------------------ get schema ------------- //

// 获取schema对象
Schema get_schema(GraphHandle graph);

// ------------------ partition list api ------------- //

// 如果 v 不存在，返回 -1
PartitionId get_partition_id(GraphHandle graph, VertexId v);

// primary key所对应的property会提前通过schema里的对应字段返回。
// key是\0结束的字符串，如果 key 不存在，返回 -1
// 否则返回0，结果存在 internal_id 和 partition_id 中
int get_vertex_id_from_primary_key(GraphHandle graph, LabelId label_id,
                                   const char* key, VertexId* internal_id,
                                   PartitionId* partition_id);

// 返回本地的partition列表。
//
// 因为maxgraph已经有GraphHandler，因此不需要传worker_global_index。
//
// partition_ids:
// 接收返回值，调用者（maxgraph）负责使用free_partition_list释放内存
// partition_id_size: partition_ids的长度
//
void get_process_partition_list(GraphHandle graph, PartitionId** partition_ids,
                                int* partition_id_size);

// 释放partition_ids对应的内存
void free_partition_list(PartitionId* partition_ids);

#ifdef __cplusplus
}
#endif

#endif  // ANALYTICAL_ENGINE_HTAP_GLOBAL_STORE_FFI_H_

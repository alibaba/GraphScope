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
#ifndef SRC_CLIENT_DS_STREAM_PROPERTY_GRAPH_HTAP_H_
#define SRC_CLIENT_DS_STREAM_PROPERTY_GRAPH_HTAP_H_

#include <ctype.h>
#include <stddef.h>
#include <stdint.h>

#include "global_store_ffi.h"

#ifdef __cplusplus
extern "C" {
#endif

// c.f.: https://yuque.antfin-inc.com/7br/graphscope/xwxtm3#KFLSY

typedef void* GraphBuilder;
typedef int64_t ObjectID;
typedef uint64_t InstanceId;
typedef int64_t VertexId;
typedef int64_t EdgeId;
typedef int LabelId;
typedef void* Schema;
typedef int PropertyId;
typedef void* GraphHandle;
typedef void* VertexTypeBuilder;
typedef void* EdgeTypeBuilder;

/**
 * step 1: 创建Local的GraphBuilder
 *
 * graph_name: maxgraph给定的graph name
 * schema: Schema handle
 * index: maxgraph对每个builder的给定的index
 *
 * 📌 vineyard client library会从环境变量中得到vineyard_ipc_socket。
 */
GraphBuilder create_graph_builder(const char* graph_name, Schema schema,
                                  const int index);

/**
 * 获取当前builder的id，以及所在的instance id，用于step 2的同步。
 *
 * builder_id: 用于接收返回值
 * instance_id: 用于接收返回值
 */
void get_builder_id(GraphBuilder builder, ObjectId* object_id,
                    InstanceId* instance_id);

/**
 * 创建流程step 2中的global object。
 *
 * size: object_ids/instance_ids的长度
 * object_ids：每个worker上创建的builder的object id
 * instance_ids: 每个object_id对应的instance id
 *
 * object_ids和instance_ids的顺序需要对应，c.f.: get_builder_id
 *
 * 📌 vineyard client library会从环境变量中得到vineyard_ipc_socket
 */
ObjectId build_global_graph_stream(const char* graph_name, size_t size,
                                   ObjectId* object_ids,
                                   InstanceId* instance_ids);

/**
 * 用于step 3中的根据graph name和index获取对应的graph builder
 *
 * graph_name: graph name
 * index: index
 *
 * 对于invalid的输入，例如graph_name错误或者index错误，返回空指针。
 */
GraphBuilder get_graph_builder(const char* graph_name, const int index);

/**
 * 多个property用array的方式给出，property_size指定property array的size。
 */
void add_vertex(GraphBuilder builder, VertexId id, LabelId labelid,
                size_t property_size, Property* properties);

/**
 * (label, src_label, dst_label) 对应于protobuf定义中的repeated EdgeType。
 *
 * 多个property用array的方式给出，property_size指定property array的size。
 */
void add_edge(GraphBuilder builder, EdgeId edgeid, VertexId src_id,
              VertexId dst_id, LabelId label, LabelId src_label,
              LabelId dst_label, size_t property_size, Property* properties);

/**
 * 参数含义与add_vertex一致，都变为array形式，vertex_size给出当前batch的size。
 */
void add_vertices(GraphBuilder builder, size_t vertex_size, VertexId* ids,
                  LabelId* labelids, size_t* property_sizes,
                  Property* properties);

/**
 * 参数含义与add_edge一致，都变为array形式，edge_size给出当前batch的size。
 */
void add_edges(GraphBuilder builder, size_t edge_size, EdgeId* edgeids,
               VertexId* src_id, VertexId* dst_id, LabelId* labels,
               LabelId* src_labels, LabelId* dst_labels, size_t* property_sizes,
               Property* properties);

/**
 * 结束local GraphBuilder的build，点、边写完之后分别调用
 */
void build(GraphBuilder builder);
// as an alias due for backwardscompatibility
void build_vertice(GraphBuilder builder);
void build_vertices(GraphBuilder builder);
void build_edges(GraphBuilder builder);

/**
 * 析构handle
 */
void destroy(GraphBuilder builder);

/////////// schema 接口 /////////////

// 获取schema对象
Schema get_schema(GraphHandle graph);

// 释放schema对象
//TODO: rename to destroy_schema(Schema schema) to be  more consistent
void free_schema(Schema schema);

// 根据property name获取property
// id，如果找到id，则赋值给out，并且返回0，否则返回-1
int get_property_id(Schema schema, const char* name, PropertyId* out);

// 获取属性的类型，如果属性存在，则赋值给out，并且返回0，否则返回-1
int get_property_type(Schema schema, LabelId label, PropertyId id,
                      PropertyType* out);

// 根据属性id获取属性名，如果属性存在，则赋值给out，并且返回0，否则返回-1
int get_property_name(Schema schema, PropertyId id, const char** out);

// 根据label名称获取label id，如果label存在则赋值给out，并且返回0，否则返回-1
int get_label_id(Schema schema, const char* name, LabelId* out);

// 根据label id获取label名称，如果label存在，则赋值给out，并且返回0，否则返回-1
int get_label_name(Schema schema, LabelId label, const char** out);

// 释放从上述接口中获取的字符串
void free_string(char* s);

/********************** 创建Schema相关API **********************/
// 创建Schema builder
Schema create_schema_builder();

// 根据点类型的id、name创建vertex type builder
VertexTypeBuilder build_vertex_type(Schema schema, LabelId label,
                                    const char* name);

// 根据边类型的id、name创建edge type builder
EdgeTypeBuilder build_edge_type(Schema schema, LabelId label, const char* name);

// 根据属性id、name、属性type在点类型中增加属性
void build_vertex_property(VertexTypeBuilder vertex, PropertyId id,
                           const char* name, PropertyType prop_type);

// 根据属性id、name、属性type在边类型中增加属性
void build_edge_property(EdgeTypeBuilder edge, PropertyId id, const char* name,
                         PropertyType prop_type);

// 设置点类型的主键列表
void build_vertex_primary_keys(VertexTypeBuilder vertex, size_t key_count,
                               const char** key_name_list);

// 在边类型中增加一条 <起点类型-->终点类型> 的关系，一个边类型可以增加多条关系
void build_edge_relation(EdgeTypeBuilder edge, const char* src,
                         const char* dst);

// 完成创建指定的点类型并释放空间
void finish_build_vertex(VertexTypeBuilder vertex);

// 完成创建指定的边类型并释放空间
void finish_build_edge(EdgeTypeBuilder edge);

// 完成创建指定的schema并释放空间
Schema finish_build_schema(Schema schema);

#ifdef __cplusplus
}
#endif

#endif  // SRC_CLIENT_DS_STREAM_PROPERTY_GRAPH_HTAP_H_

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
#include "graph_builder_ffi.h"

#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <string>

#include "boost/algorithm/string.hpp"
#include "vineyard/basic/stream/dataframe_stream.h"
#include "vineyard/basic/stream/parallel_stream.h"

#include "property_graph_stream.h"

#ifdef __cplusplus
extern "C" {
#endif

GraphBuilder create_graph_builder(const char *graph_name, Schema schema,
                                  const int index) {
  auto &client = vineyard::Client::Default();
  auto schema_ptr = static_cast<vineyard::MGPropertyGraphSchema *>(schema);
  auto stream = vineyard::PropertyGraphOutStream::Create(client, graph_name,
                                                         schema_ptr, index);
  VINEYARD_CHECK_OK(client.Persist(stream->id()));
  LOG(INFO) << "create graph builder: yields "
            << vineyard::ObjectIDToString(stream->id());
  // create a shared_ptr object on heap.
  return new std::shared_ptr<vineyard::PropertyGraphOutStream>(stream.release());
}

void get_builder_id(GraphBuilder builder, ObjectId *object_id,
                    InstanceId *instance_id) {
  auto stream =
      static_cast<std::shared_ptr<vineyard::PropertyGraphOutStream> *>(builder);
  *object_id = static_cast<vineyard::ObjectID>((*stream)->id());
  *instance_id = (*stream)->instance_id();
}

// TRICK: launch the loader
void launch_property_graph_loader(vineyard::Client &client,
                                  vineyard::ObjectID global_stream_id,
                                  size_t size, InstanceId *instance_ids) {
  std::map<vineyard::InstanceID, vineyard::json> cluster;
  VINEYARD_CHECK_OK(client.ClusterInfo(cluster));
  std::set<std::string> host_list;
  for (size_t idx = 0; idx < size; ++idx) {
    auto iter = cluster.find(instance_ids[idx]);
    VINEYARD_ASSERT(iter != cluster.end());
    host_list.emplace(iter->second["hostname"].get_ref<std::string const &>());
  }
  std::string hosts = boost::algorithm::join(host_list, ",");
  std::string loader_path;
  std::string proc_num = std::to_string(host_list.size());
  std::string frag_num = std::to_string(host_list.size());
  if (const char *env_p = std::getenv("VINEYARD_HOME")) {
    std::string loader_path = std::string(env_p) + "/htap_stream_loader_test";
    std::string command = "mpiexec -env GLOG_v 100 -n " + proc_num +
                          " -hosts " + hosts + " " + loader_path + " " + " " +
                          frag_num + " " + std::to_string(global_stream_id) +
                          " &";
    LOG(INFO) << "launcher command: " << command;
    if (system(command.c_str())) {
      LOG(ERROR) << "failed to launch vineyard loader";
    }
    LOG(INFO) << "launch loader success";
  } else {
    LOG(ERROR) << "failed to find $VINEYARD_HOME to launch vineyard loader";
  }
}

ObjectId build_global_graph_stream(const char *graph_name, size_t size,
                                   ObjectId *object_ids,
                                   InstanceId *instance_ids) {
  LOG(INFO) << "start build_global_graph_stream: size = " << size;
  auto &client = vineyard::Client::Default();

  // build two parallel stream (global dataframe stream) for loading graphs
  std::vector<ObjectID> vertex_streams, edge_streams;

  vineyard::GlobalPGStreamBuilder builder(client);
  for (size_t idx = 0; idx < size; ++idx) {
#ifndef NDEBUG
    LOG(INFO) << "add substream: "
              << "idx = " << idx << " => "
              << vineyard::ObjectIDToString(object_ids[idx]) << " at "
              << instance_ids[idx];
#endif
    // sync remote metadata, to ensure the persisted objects get watched.
    vineyard::ObjectMeta meta;
    VINEYARD_CHECK_OK(client.GetMetaData(
        static_cast<vineyard::ObjectID>(object_ids[idx]), meta, true));

    vertex_streams.emplace_back(meta.GetMemberMeta("vertex_stream").GetId());
    edge_streams.emplace_back(meta.GetMemberMeta("edge_stream").GetId());

    builder.AddStream(idx, static_cast<vineyard::ObjectID>(object_ids[idx]),
                      instance_ids[idx]);
  }
  auto gs = builder.Seal(client);
  VINEYARD_CHECK_OK(client.Persist(gs->id()));
  LOG(INFO) << "start build_global_graph_stream create name: name = "
            << graph_name;
  vineyard::ObjectID global_stream_id = gs->id();
  VINEYARD_CHECK_OK(client.PutName(gs->id(), graph_name));

  // register the stream to an internal name
  VINEYARD_CHECK_OK(client.PutName(gs->id(), "vineyard_internal_htap_stream"));

  // build parallel streams
  {
    vineyard::ParallelStreamBuilder builder(client);
    for (auto const &id : vertex_streams) {
      builder.AddStream(id);
    }
    auto pstream = builder.Seal(client);
    client.PutName(pstream->id(), std::string("__") + graph_name + "_vertex_stream");
    LOG(INFO) << "Generate parallel stream for vertex: " << graph_name << " -> "
              << vineyard::ObjectIDToString(pstream->id());
  }
  {
    vineyard::ParallelStreamBuilder builder(client);
    for (auto const &id : edge_streams) {
      builder.AddStream(id);
    }
    auto pstream = builder.Seal(client);
    client.PutName(pstream->id(), std::string("__") + graph_name + "_edge_stream");
    LOG(INFO) << "Generate parallel stream for edge: " << graph_name << " -> "
              << vineyard::ObjectIDToString(pstream->id());
  }

  LOG(INFO) << "finish build_global_graph_stream, id = " << global_stream_id;
  return global_stream_id;
}

GraphBuilder get_graph_builder(const char *graph_name, const int index) {
  auto &client = vineyard::Client::Default();
  vineyard::ObjectID id;
  VINEYARD_CHECK_OK(client.GetName(graph_name, id));
#ifndef NDEBUG
  LOG(INFO) << "get name " << graph_name << " yields ID "
            << vineyard::ObjectIDToString(id);
#endif
  vineyard::ObjectMeta meta;
  VINEYARD_CHECK_OK(client.GetMetaData(id, meta, true));
#ifndef NDEBUG
  meta.PrintMeta();
#endif
  auto gstream =
      std::dynamic_pointer_cast<vineyard::GlobalPGStream>(client.GetObject(id));
  auto builder = gstream->StreamAt(index);
  return new std::shared_ptr<vineyard::PropertyGraphOutStream>(builder);
}

void add_vertex(GraphBuilder builder, VertexId id, LabelId labelid,
                size_t property_size, Property *properties) {
  auto stream =
      static_cast<std::shared_ptr<vineyard::PropertyGraphOutStream> *>(builder);
  return (*stream)->AddVertex(id, labelid, property_size, properties);
}

void add_edge(GraphBuilder builder, EdgeId edgeid, VertexId src_id,
              VertexId dst_id, LabelId label, LabelId src_label,
              LabelId dst_label, size_t property_size, Property *properties) {
  auto stream =
      static_cast<std::shared_ptr<vineyard::PropertyGraphOutStream> *>(builder);
  return (*stream)->AddEdge(edgeid, src_id, dst_id, label, src_label, dst_label,
                            property_size, properties);
}

void add_vertices(GraphBuilder builder, size_t vertex_size, VertexId *ids,
                  LabelId *labelids, size_t *property_sizes,
                  Property *properties) {
  auto stream =
      static_cast<std::shared_ptr<vineyard::PropertyGraphOutStream> *>(builder);
  return (*stream)->AddVertices(vertex_size, ids, labelids, property_sizes,
                                properties);
}

void add_edges(GraphBuilder builder, size_t edge_size, EdgeId *edgeids,
               VertexId *src_ids, VertexId *dst_ids, LabelId *labels,
               LabelId *src_labels, LabelId *dst_labels, size_t *property_sizes,
               Property *properties) {
  auto stream =
      static_cast<std::shared_ptr<vineyard::PropertyGraphOutStream> *>(builder);
  return (*stream)->AddEdges(edge_size, edgeids, src_ids, dst_ids, labels,
                             src_labels, dst_labels, property_sizes,
                             properties);
}

void build(GraphBuilder builder) {
  auto stream =
      static_cast<std::shared_ptr<vineyard::PropertyGraphOutStream> *>(builder);
  VINEYARD_CHECK_OK((*stream)->Finish());
}

void build_vertice(GraphBuilder builder) { return build_vertices(builder); }

void build_vertices(GraphBuilder builder) {
  LOG(INFO) << "build vertices";
  auto stream =
      static_cast<std::shared_ptr<vineyard::PropertyGraphOutStream> *>(builder);
  (*stream)->FinishAllVertices();
}

void build_edges(GraphBuilder builder) {
  LOG(INFO) << "build edges";
  auto stream =
      static_cast<std::shared_ptr<vineyard::PropertyGraphOutStream> *>(builder);
  (*stream)->FinishAllEdges();
}

void destroy(GraphBuilder builder) {
  auto stream =
      static_cast<std::shared_ptr<vineyard::PropertyGraphOutStream> *>(builder);
  // delete the shared_ptr object on heap, it will then delete the holded
  // object.
  delete stream;
}

void free_schema(Schema schema) {
  // do NOTHING, since the schema is part of Fragment (GraphHandle).
}

int get_property_id(Schema schema, const char *name, PropertyId *out) {
  auto ptr = static_cast<vineyard::MGPropertyGraphSchema *>(schema);
  *out = ptr->GetPropertyId(name);
  if (*out == -1) {
    *out = ptr->GetPropertyId(name);
  }
#ifndef NDEBUG
  LOG(INFO) << "get propery id: " << name << " -> " << *out;
#endif
  return (*out == -1) ? -1 : 0;
}

int get_property_type(Schema schema, LabelId label, PropertyId id,
                      PropertyType *out) {
  auto ptr = static_cast<vineyard::MGPropertyGraphSchema *>(schema);
  *out = vineyard::detail::PropertyTypeFromDataType(
      ptr->GetPropertyType(label, id));
  if (*out == INVALID) {
    *out = vineyard::detail::PropertyTypeFromDataType(
        ptr->GetPropertyType(label, id));
  }
#ifndef NDEBUG
  LOG(INFO) << "get propery type: " << label << " + " << id << " -> " << *out;
#endif
  return (*out == INVALID) ? -1 : 0;
}

int get_property_name(Schema schema, PropertyId id, const char **out) {
  auto ptr = static_cast<vineyard::MGPropertyGraphSchema *>(schema);
  std::string name = ptr->GetPropertyName(id);
#ifndef NDEBUG
  LOG(INFO) << "get propery name: " << id << " -> " << name;
#endif
  if (name.empty()) {
    *out = NULL;
    return -1;
  } else {
    char *p = (char *)calloc(name.size() + 1, sizeof(char));
    std::memcpy(p, name.data(), name.size());
    *out = p;
    return 0;
  }
}

int get_label_id(Schema schema, const char *name, LabelId *out) {
  auto ptr = static_cast<vineyard::MGPropertyGraphSchema *>(schema);
  *out = ptr->GetLabelId(name);
  return (*out == -1) ? -1 : 0;
}

int get_label_name(Schema schema, LabelId label, const char **out) {
  auto ptr = static_cast<vineyard::MGPropertyGraphSchema *>(schema);
  std::string name = ptr->GetLabelName(label);
  if (name.empty()) {
    *out = NULL;
    return -1;
  } else {
    char *p = (char *)calloc(name.size() + 1, sizeof(char));
    std::memcpy(p, name.data(), name.size());
    *out = p;
    return 0;
  }
}

void free_string(char *s) {
#ifndef NDEBUG
  LOG(INFO) << "free label/prop name: " << s;
#endif
  // we do allocate strings in schema APIs.
  free(s);
}

Schema create_schema_builder() { return new vineyard::MGPropertyGraphSchema(); }

VertexTypeBuilder build_vertex_type(Schema schema, LabelId label,
                                    const char *name) {
#ifndef NDEBUG
  LOG(INFO) << "add vertex type: " << label << " -> " << name;
#endif
  auto ptr = static_cast<vineyard::MGPropertyGraphSchema *>(schema);
  return ptr->CreateEntry("VERTEX", label, name);
}

EdgeTypeBuilder build_edge_type(Schema schema, LabelId label,
                                const char *name) {
#ifndef NDEBUG
  LOG(INFO) << "add edge type: " << label << " -> " << name;
#endif
  auto ptr = static_cast<vineyard::MGPropertyGraphSchema *>(schema);
  return ptr->CreateEntry("EDGE", label, name);
}

static bool entry_has_property(vineyard::Entry *entry, std::string const &name) {
  for (auto const &prop: entry->props_) {
    if (prop.name == name) {
      return true;
    }
  }
  return false;
}

void build_vertex_property(VertexTypeBuilder vertex, PropertyId id,
                           const char *name, PropertyType prop_type) {
#ifndef NDEBUG
  LOG(INFO) << "add vertex property: " << id << " -> " << name << ": "
            << prop_type;
#endif
  using entry_t = vineyard::Entry;
  auto entry_ptr = static_cast<entry_t *>(vertex);
  if (entry_has_property(entry_ptr, name)) {
    LOG(WARNING) << "detect duplicate vertex property name, ignored: " << name
                 << ", id = " << id;
    return;
  }
  entry_ptr->AddProperty(/* id, */ name,
                         vineyard::detail::PropertyTypeToDataType(prop_type));
  entry_ptr->props_.rbegin()->id = id;
}

void build_edge_property(EdgeTypeBuilder edge, PropertyId id, const char *name,
                         PropertyType prop_type) {
#ifndef NDEBUG
  LOG(INFO) << "add edge property: " << id << " -> " << name << ": "
            << prop_type;
#endif
  using entry_t = vineyard::Entry;
  auto entry_ptr = static_cast<entry_t *>(edge);
  if (entry_has_property(entry_ptr, name)) {
    LOG(WARNING) << "detect duplicate edge property name, ignored: " << name
                 << ", id = " << id;
    return;
  }
  entry_ptr->AddProperty(/* id, */ name,
                         vineyard::detail::PropertyTypeToDataType(prop_type));
  entry_ptr->props_.rbegin()->id = id;
}

void build_vertex_primary_keys(VertexTypeBuilder vertex, size_t key_count,
                               const char **key_name_list) {
#ifndef NDEBUG
  LOG(INFO) << "add vertex pk: " << key_count;
#endif
  using entry_t = vineyard::Entry;
  auto entry_ptr = static_cast<entry_t *>(vertex);
  std::vector<std::string> names(key_count);
  for (size_t i = 0; i < key_count; ++i) {
    names.emplace_back(key_name_list[i]);
  }
  entry_ptr->AddPrimaryKeys(key_count, names);
}

void build_edge_relation(EdgeTypeBuilder edge, const char *src,
                         const char *dst) {
#ifndef NDEBUG
  LOG(INFO) << "add edge relation: " << src << " -> " << dst;
#endif
  using entry_t = vineyard::Entry;
  auto entry_ptr = static_cast<entry_t *>(edge);
  entry_ptr->AddRelation(src, dst);
}

void finish_build_vertex(VertexTypeBuilder vertex) {
  // do NOTHING since nothing needs to be freed.
}

void finish_build_edge(EdgeTypeBuilder edge) {
  // do NOTHING since nothing needs to be freed.
}

Schema finish_build_schema(Schema schema) {
  // schema is just a metadata of Graph
  return schema;
}

#ifdef __cplusplus
}
#endif

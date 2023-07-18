/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ANALYTICAL_ENGINE_CORE_IO_PROPERTY_PARSER_H_
#define ANALYTICAL_ENGINE_CORE_IO_PROPERTY_PARSER_H_

#include <glog/logging.h>

#include <cstddef>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/table.h"
#include "boost/leaf/error.hpp"
#include "boost/leaf/result.hpp"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/common/util/status.h"

#include "core/server/rpc_utils.h"
#include "proto/attr_value.pb.h"
#include "proto/types.pb.h"

namespace bl = boost::leaf;

namespace gs {
namespace rpc {
class OpDef;
}
}  // namespace gs

template <typename Key, typename Value>
using ProtobufMap = ::google::protobuf::Map<Key, Value>;

using gs::rpc::AttrValue;
using gs::rpc::Chunk;
using gs::rpc::DataType;
using gs::rpc::GSParams;
using gs::rpc::LargeAttrValue;
using gs::rpc::OpDef;

using AttrMap = ProtobufMap<int, AttrValue>;

namespace gs {
namespace detail {
/**
 * @brief This is the model class to represent how to load vertex data from the
 * data source.
 */
struct Vertex {
  std::string label;  // This field is used to set metadata of arrow table
  std::string vid;  // when vid is single digit, it means column index of vertex
  // id. Otherwise, it represents column name
  std::string protocol;  // file/oss/numpy/pandas/vineyard
  std::string values;    // from location, vineyard or pandas
  std::string vformat;   // defines vertex format,

  std::string SerializeToString() const {
    std::stringstream ss;
    ss << "V ";
    ss << label << " " << vid << " ";
    ss << protocol << " " << values << " ";
    ss << vformat << "\n";
    return ss.str();
  }
};

/**
 * @brief This is the model class to represent how to load edge data from the
 * data source.
 */
class Edge {
 public:
  class SubLabel {
   public:
    std::string src_label, dst_label;
    std::string src_vid, dst_vid;
    std::string load_strategy;
    std::string protocol;
    std::string values;
    // This doesn't need to be guarded by eformat
    // eformat is not requires, initialized to empty.
    std::string eformat;

    std::string SerializeToString() const {
      std::stringstream ss;
      ss << src_label << " " << dst_label << " ";
      ss << src_vid << " " << dst_vid << " ";
      ss << protocol << " " << values << " ";
      ss << eformat;
      return ss.str();
    }
  };
  std::string label;
  std::vector<SubLabel> sub_labels;

  std::string SerializeToString() const {
    std::stringstream ss;
    ss << "E ";
    ss << label;
    for (auto& sub_label : sub_labels) {
      ss << sub_label.SerializeToString() << ";";
    }
    ss << "\n";
    return ss.str();
  }
};

/**
 * @brief This is the model class to represent the data source to load a graph
 */
struct Graph {
  std::vector<std::shared_ptr<Vertex>> vertices;
  std::vector<std::shared_ptr<Edge>> edges;
  bool directed = true;
  bool generate_eid = true;
  bool retain_oid = true;
  bool compact_edges = false;
  bool use_perfect_hash = false;

  std::string SerializeToString() const {
    std::stringstream ss;
    ss << "directed: " << directed << "\n";
    ss << "generate_eid: " << generate_eid << "\n";
    ss << "retain_oid: " << retain_oid << "\n";
    ss << "compact_edges: " << compact_edges << "\n";
    ss << "use_perfect_hash: " << use_perfect_hash << "\n";
    for (auto& v : vertices) {
      ss << v->SerializeToString();
    }
    for (auto& e : edges) {
      ss << e->SerializeToString();
    }
    return ss.str();
  }
};
}  // namespace detail

inline void ParseVertex(std::shared_ptr<detail::Graph>& graph,
                        const std::string& data, const AttrMap& attrs) {
  auto vertex = std::make_shared<detail::Vertex>();
  vertex->label = attrs.at(rpc::LABEL).s();
  vertex->vid = attrs.at(rpc::VID).s();
  vertex->protocol = attrs.at(rpc::PROTOCOL).s();
  if (attrs.find(rpc::VFORMAT) != attrs.end()) {
    vertex->vformat = attrs.at(rpc::VFORMAT).s();
  }
  if (vertex->protocol == "pandas") {
    vertex->values = data;
  } else {
    vertex->values = attrs.at(rpc::SOURCE).s();
  }
  graph->vertices.push_back(vertex);
}

inline void ParseEdge(std::shared_ptr<detail::Graph>& graph,
                      const std::string& data, const AttrMap& attrs) {
  std::string label = attrs.at(rpc::LABEL).s();

  bool has_edge_label = false;
  if (!graph->edges.empty() && graph->edges.back()->label == label) {
    has_edge_label = true;
  }

  auto edge =
      has_edge_label ? graph->edges.back() : std::make_shared<detail::Edge>();
  edge->label = label;

  // sub_label: src_label / dst_label
  detail::Edge::SubLabel sub_label;
  sub_label.src_label = attrs.at(rpc::SRC_LABEL).s();
  sub_label.dst_label = attrs.at(rpc::DST_LABEL).s();
  sub_label.src_vid = attrs.at(rpc::SRC_VID).s();
  sub_label.dst_vid = attrs.at(rpc::DST_VID).s();
  sub_label.load_strategy = attrs.at(rpc::LOAD_STRATEGY).s();
  sub_label.protocol = attrs.at(rpc::PROTOCOL).s();
  if (attrs.find(rpc::EFORMAT) != attrs.end()) {
    sub_label.eformat = attrs.at(rpc::EFORMAT).s();
  }
  if (sub_label.protocol == "pandas") {
    sub_label.values = data;
  } else {
    sub_label.values = attrs.at(rpc::SOURCE).s();
  }
  edge->sub_labels.push_back(sub_label);

  if (!has_edge_label) {
    graph->edges.push_back(edge);
  }
}

// The input string is the serialized bytes of an arrow::Table, this function
// split the table to several small tables.
inline void SplitTable(const std::string& data, int num,
                       std::vector<std::string>& sliced_bytes) {
  sliced_bytes.resize(num);
  std::shared_ptr<arrow::Buffer> buffer =
      arrow::Buffer::Wrap(data.data(), data.size());
  std::shared_ptr<arrow::Table> table;
  VINEYARD_CHECK_OK(vineyard::DeserializeTable(buffer, &table));
  std::vector<std::shared_ptr<arrow::Table>> sliced_tables(num);
  int num_rows = table->num_rows();
  int offset = num_rows / num;
  int remainder = num_rows % num;
  int cur = 0;
  sliced_tables[0] = table->Slice(cur, offset + remainder);
  cur = offset + remainder;
  for (int i = 1; i < num; ++i) {
    auto sliced = table->Slice(cur, offset);
    sliced_tables[i] = sliced;
    cur += offset;
  }
  for (int i = 0; i < num; ++i) {
    if (sliced_tables[i]->num_rows() > 0) {
      std::shared_ptr<arrow::Buffer> out_buf;
      VINEYARD_CHECK_OK(vineyard::SerializeTable(sliced_tables[i], &out_buf));
      sliced_bytes[i] = out_buf->ToString();
    }
  }
}

inline void DistributeChunk(const rpc::Chunk& chunk, int num,
                            std::vector<rpc::Chunk>& distributed_chunk) {
  distributed_chunk.resize(num);
  const auto& attrs = chunk.attr();
  std::string protocol = attrs.at(rpc::PROTOCOL).s();
  std::vector<std::string> distributed_values;
  const std::string& data = chunk.buffer();
  if (protocol == "pandas") {
    SplitTable(data, num, distributed_values);
  } else {
    distributed_values.resize(num, attrs.at(rpc::SOURCE).s());
  }
  for (int i = 0; i < num; ++i) {
    distributed_chunk[i].set_buffer(std::move(distributed_values[i]));
    auto* attr = distributed_chunk[i].mutable_attr();
    for (auto& pair : attrs) {
      (*attr)[pair.first].CopyFrom(pair.second);
    }
  }
}

// If contains contents from numpy or pandas, then we should distribute those
// raw bytes evenly across all workers, each worker would only receive a slice,
// in order to reduce the communication overhead.
inline std::vector<rpc::LargeAttrValue> DistributeGraph(
    const rpc::LargeAttrValue& large_attr, int num) {
  std::vector<rpc::LargeAttrValue> distributed_graph(num);
  if (large_attr.has_chunk_list()) {
    size_t chunk_list_size = large_attr.chunk_list().items().size();
    std::vector<std::vector<rpc::Chunk>> distributed_vec(chunk_list_size);
    // split
    for (size_t i = 0; i < chunk_list_size; ++i) {
      DistributeChunk(large_attr.chunk_list().items(i), num,
                      distributed_vec[i]);
    }
    for (int i = 0; i < num; ++i) {
      for (auto& vec : distributed_vec) {
        rpc::Chunk* chunk =
            distributed_graph[i].mutable_chunk_list()->add_items();
        chunk->Swap(&vec[i]);
      }
    }
  }
  return distributed_graph;
}

inline bl::result<std::shared_ptr<detail::Graph>> ParseCreatePropertyGraph(
    const GSParams& params) {
  BOOST_LEAF_AUTO(directed, params.Get<bool>(rpc::DIRECTED));
  BOOST_LEAF_AUTO(generate_eid, params.Get<bool>(rpc::GENERATE_EID));
  BOOST_LEAF_AUTO(retain_oid, params.Get<bool>(rpc::RETAIN_OID));
  BOOST_LEAF_AUTO(compact_edges, params.Get<bool>(rpc::COMPACT_EDGES, false));
  BOOST_LEAF_AUTO(use_perfect_hash,
                  params.Get<bool>(rpc::USE_PERFECT_HASH, false));

  auto graph = std::make_shared<detail::Graph>();
  graph->directed = directed;
  graph->generate_eid = generate_eid;
  graph->retain_oid = retain_oid;
  graph->compact_edges = compact_edges;
  graph->use_perfect_hash = use_perfect_hash;

  const auto& large_attr = params.GetLargeAttr();
  for (const auto& item : large_attr.chunk_list().items()) {
    const auto& chunk_attr = item.attr();
    if (chunk_attr.at(rpc::CHUNK_NAME).s() == "vertex") {
      ParseVertex(graph, item.buffer(), chunk_attr);
    } else if (chunk_attr.at(rpc::CHUNK_NAME).s() == "edge") {
      ParseEdge(graph, item.buffer(), chunk_attr);
    }
  }
  return graph;
}

inline bl::result<std::vector<std::map<int, std::vector<int>>>>
ParseProjectPropertyGraph(const GSParams& params) {
  BOOST_LEAF_AUTO(list, params.Get<rpc::AttrValue_ListValue>(
                            rpc::ARROW_PROPERTY_DEFINITION));
  auto& items = list.func();
  std::map<int, std::vector<int>> vertices, edges;
  CHECK_EQ(items.size(), 2);
  {
    auto item = items[0];
    for (auto& pair : item.attr()) {
      auto props = pair.second.list().i();
      vertices[pair.first] = {props.begin(), props.end()};
    }
  }
  {
    auto item = items[1];
    for (auto& pair : item.attr()) {
      auto props = pair.second.list().i();
      edges[pair.first] = {props.begin(), props.end()};
    }
  }
  std::vector<std::map<int, std::vector<int>>> res;
  res.push_back(vertices);
  res.push_back(edges);
  return res;
}

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_IO_PROPERTY_PARSER_H_

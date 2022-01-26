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

#include <atomic>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "boost/algorithm/string.hpp"
#include "google/protobuf/util/json_util.h"
#include "vineyard/basic/ds/arrow_utils.h"

#include "core/server/rpc_utils.h"
#include "proto/graphscope/proto/attr_value.pb.h"
#include "proto/graphscope/proto/op_def.pb.h"
#include "proto/graphscope/proto/types.pb.h"

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

  std::string SerializeToString() const {
    std::stringstream ss;
    ss << "V ";
    ss << label << " " << vid << " ";
    ss << protocol << " " << values << "\n";
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

    std::string SerializeToString() const {
      std::stringstream ss;
      ss << src_label << " " << dst_label << " ";
      ss << src_vid << " " << dst_vid << " ";
      ss << protocol << " " << values;
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
  bool directed;
  bool generate_eid;

  std::string SerializeToString() const {
    std::stringstream ss;
    ss << "directed: " << directed << "\n";
    ss << "generate_eid: " << generate_eid << "\n";
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
  // TODO (dongze) copy
  vertex->values = data;
  graph->vertices.push_back(vertex);
}

inline void ParseEdge(std::shared_ptr<detail::Graph>& graph,
                      const std::string& data, const AttrMap& attrs) {
  std::string label = attrs.at(rpc::LABEL).s();

  bool label_edge_exists = false;
  if (graph->edges.size() > 0 && graph->edges.back()->label == label) {
    label_edge_exists = true;
  }

  auto edge = label_edge_exists ? graph->edges.back()
                                : std::make_shared<detail::Edge>();
  edge->label = label;

  // sub_label: src_label / dst_label
  detail::Edge::SubLabel sub_label;
  sub_label.src_label = attrs.at(rpc::SRC_LABEL).s();
  sub_label.dst_label = attrs.at(rpc::DST_LABEL).s();
  sub_label.src_vid = attrs.at(rpc::SRC_VID).s();
  sub_label.dst_vid = attrs.at(rpc::DST_VID).s();
  sub_label.load_strategy = attrs.at(rpc::LOAD_STRATEGY).s();
  sub_label.protocol = attrs.at(rpc::PROTOCOL).s();
  // TODO (dongze) copy
  sub_label.values = data;
  edge->sub_labels.push_back(sub_label);

  if (!label_edge_exists) {
    graph->edges.push_back(edge);
  }
}

// The input string is the serialized bytes of an arrow::Table, this function
// split the table to several small tables.
inline std::vector<std::string> SplitTable(const std::string& data, int num) {
  std::shared_ptr<arrow::Buffer> buffer =
      arrow::Buffer::Wrap(data.data(), data.size());
  std::shared_ptr<arrow::Table> table;
  vineyard::DeserializeTable(buffer, &table);
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
  std::vector<std::string> sliced_bytes(num);
  for (int i = 0; i < num; ++i) {
    if (sliced_tables[i]->num_rows() > 0) {
      std::shared_ptr<arrow::Buffer> out_buf;
      vineyard::SerializeTable(sliced_tables[i], &out_buf);
      sliced_bytes[i] = out_buf->ToString();
    }
  }
  return sliced_bytes;
}

inline std::vector<rpc::Chunk> DistributeChunk(const rpc::Chunk& chunk,
                                               int num) {
  std::vector<rpc::Chunk> distributed_chunk(num);
  const auto& attrs = chunk.attr();
  std::string protocol = attrs.at(rpc::PROTOCOL).s();
  std::vector<std::string> distributed_values;
  const std::string& data = chunk.buffer();
  if (protocol == "pandas") {
    distributed_values = SplitTable(data, num);
  } else {
    distributed_values.resize(num, data);
  }
  for (int i = 0; i < num; ++i) {
    distributed_chunk[i].set_buffer(distributed_values[i]);
    auto* attr = distributed_chunk[i].mutable_attr();
    for (auto& pair : attrs) {
      (*attr)[pair.first].CopyFrom(pair.second);
    }
  }
  return distributed_chunk;
}

// If contains contents from numpy or pandas, then we should distribute those
// raw bytes evenly across all workers, each worker would only receive a slice,
// in order to reduce the communication overhead.
inline std::vector<rpc::LargeAttrValue> DistributeGraph(
    const rpc::LargeAttrValue& large_attr, int num) {
  std::vector<rpc::LargeAttrValue> distributed_graph(num);
  if (large_attr.has_chunk_list()) {
    std::vector<std::vector<rpc::Chunk>> distributed_vec;
    // split
    for (const auto& item : large_attr.chunk_list().items()) {
      auto vec = DistributeChunk(item, num);
      distributed_vec.emplace_back(std::move(vec));
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
  // BOOST_LEAF_AUTO(list, params.Get<rpc::AttrValue_ListValue>(
  //                           rpc::ARROW_PROPERTY_DEFINITION));
  BOOST_LEAF_AUTO(directed, params.Get<bool>(rpc::DIRECTED));
  BOOST_LEAF_AUTO(generate_eid, params.Get<bool>(rpc::GENERATE_EID));

  // auto& items = list.func();
  auto graph = std::make_shared<detail::Graph>();
  graph->directed = directed;
  graph->generate_eid = generate_eid;

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

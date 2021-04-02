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

#include "vineyard/io/io/local_io_adaptor.h"

#include "core/server/rpc_utils.h"
#include "proto/attr_value.pb.h"
#include "proto/op_def.pb.h"
#include "proto/types.pb.h"

template <typename Key, typename Value>
using ProtobufMap = ::google::protobuf::Map<Key, Value>;

using AttrMap = ProtobufMap<int, gs::rpc::AttrValue>;

using gs::rpc::DataType;
using gs::rpc::OpDef;

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
  std::string values;    // from location

  // The following fields are only needed when protocol is numpy/pandas
  std::vector<std::pair<std::string, DataType>> properties;
  std::vector<std::string> data;
  size_t row_num;
  size_t column_num;

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
    std::vector<std::string> data;  // from numpy, properties
    size_t row_num;
    size_t column_num;
    std::vector<std::pair<std::string, DataType>> properties;

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

inline void ParseLoader(std::string& protocol, std::string& values,
                        std::vector<std::string>& data, size_t& row_num,
                        size_t& column_num, const AttrMap& attrs) {
  protocol = attrs.at(rpc::PROTOCOL).s();

  if (protocol == "numpy" || protocol == "pandas") {
    row_num = attrs.at(rpc::ROW_NUM).i();
    column_num = attrs.at(rpc::COLUMN_NUM).i();
    // Use key start from 10000 + col_index to store raw bytes.
    // see ref python/graphscope/framework/loader.py
    for (size_t i = 0; i < column_num; ++i) {
      data.push_back(attrs.at(10000 + i).s());
    }
  } else {
    values = attrs.at(rpc::VALUES).s();
  }
}

inline void ParseProperties(std::vector<std::pair<std::string, DataType>>& m,
                            const rpc::AttrValue& attr) {
  const auto& props = attr.list().func();
  for (const auto& prop : props) {
    CHECK_EQ(prop.attr().size(), 1);
    m.emplace_back(prop.name(), prop.attr().begin()->second.type());
  }
}

inline detail::Edge::SubLabel ParseSubLabel(const AttrMap& attrs) {
  detail::Edge::SubLabel sub_label;
  sub_label.src_label = attrs.at(rpc::SRC_LABEL).s();
  sub_label.dst_label = attrs.at(rpc::DST_LABEL).s();
  sub_label.src_vid = attrs.at(rpc::SRC_VID).s();
  sub_label.dst_vid = attrs.at(rpc::DST_VID).s();
  sub_label.load_strategy = attrs.at(rpc::LOAD_STRATEGY).s();

  ParseLoader(sub_label.protocol, sub_label.values, sub_label.data,
              sub_label.row_num, sub_label.column_num,
              attrs.at(rpc::LOADER).func().attr());
  // The param PROPERTIES is only required when protocol is numpy or pandas.
  if (attrs.find(rpc::PROPERTIES) != attrs.end()) {
    ParseProperties(sub_label.properties, attrs.at(rpc::PROPERTIES));
  }

  return sub_label;
}

inline std::shared_ptr<detail::Vertex> ParseVertex(const AttrMap& attrs) {
  auto vertex = std::make_shared<detail::Vertex>();
  vertex->label = attrs.at(rpc::LABEL).s();
  vertex->vid = attrs.at(rpc::VID).s();

  if (attrs.find(rpc::PROPERTIES) != attrs.end()) {
    ParseProperties(vertex->properties, attrs.at(rpc::PROPERTIES));
  }
  ParseLoader(vertex->protocol, vertex->values, vertex->data, vertex->row_num,
              vertex->column_num, attrs.at(rpc::LOADER).func().attr());
  return vertex;
}

inline std::shared_ptr<detail::Edge> ParseEdge(const AttrMap& attrs) {
  auto edge = std::make_shared<detail::Edge>();
  edge->label = attrs.at(rpc::LABEL).s();

  auto sub_label_defs = attrs.at(rpc::SUB_LABEL).list().func();
  for (const auto& sub_label_def : sub_label_defs) {
    edge->sub_labels.push_back(ParseSubLabel(sub_label_def.attr()));
  }

  return edge;
}

inline bl::result<std::shared_ptr<detail::Graph>> ParseCreatePropertyGraph(
    const gs::rpc::GSParams& params) {
  BOOST_LEAF_AUTO(list, params.Get<rpc::AttrValue_ListValue>(
                            rpc::ARROW_PROPERTY_DEFINITION));
  BOOST_LEAF_AUTO(directed, params.Get<bool>(rpc::DIRECTED));
  BOOST_LEAF_AUTO(generate_eid, params.Get<bool>(rpc::GENERATE_EID));

  auto& items = list.func();
  auto graph = std::make_shared<detail::Graph>();

  graph->directed = directed;
  graph->generate_eid = generate_eid;

  for (const auto& item : items) {
    if (item.name() == "vertex") {
      graph->vertices.push_back(ParseVertex(item.attr()));
    } else if (item.name() == "edge") {
      graph->edges.push_back(ParseEdge(item.attr()));
    }
  }
  return graph;
}

inline bl::result<std::vector<std::map<int, std::vector<int>>>>
ParseProjectPropertyGraph(const gs::rpc::GSParams& params) {
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

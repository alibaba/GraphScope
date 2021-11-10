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
#include "proto/attr_value.pb.h"
#include "proto/op_def.pb.h"
#include "proto/types.pb.h"

template <typename Key, typename Value>
using ProtobufMap = ::google::protobuf::Map<Key, Value>;

using gs::rpc::AttrValue;
using gs::rpc::DataType;
using gs::rpc::GSParams;
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

  // The following fields are only needed when protocol is numpy/pandas
  std::vector<std::pair<std::string, DataType>> properties;

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
                        const AttrMap& attrs) {
  protocol = attrs.at(rpc::PROTOCOL).s();
  values = attrs.at(rpc::VALUES).s();
}

inline detail::Edge::SubLabel ParseSubLabel(const AttrMap& attrs) {
  detail::Edge::SubLabel sub_label;
  sub_label.src_label = attrs.at(rpc::SRC_LABEL).s();
  sub_label.dst_label = attrs.at(rpc::DST_LABEL).s();
  sub_label.src_vid = attrs.at(rpc::SRC_VID).s();
  sub_label.dst_vid = attrs.at(rpc::DST_VID).s();
  sub_label.load_strategy = attrs.at(rpc::LOAD_STRATEGY).s();

  ParseLoader(sub_label.protocol, sub_label.values,
              attrs.at(rpc::LOADER).func().attr());

  return sub_label;
}

inline std::shared_ptr<detail::Vertex> ParseVertex(const AttrMap& attrs) {
  auto vertex = std::make_shared<detail::Vertex>();
  vertex->label = attrs.at(rpc::LABEL).s();
  vertex->vid = attrs.at(rpc::VID).s();

  ParseLoader(vertex->protocol, vertex->values,
              attrs.at(rpc::LOADER).func().attr());
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

inline std::vector<AttrMap> DistributeLoader(const AttrMap& attrs, int num) {
  std::vector<AttrMap> distributed_attrs(num);

  std::string protocol = attrs.at(rpc::PROTOCOL).s();
  std::vector<std::string> distributed_values;
  const std::string& data = attrs.at(rpc::VALUES).s();
  if (protocol == "pandas") {
    distributed_values = SplitTable(data, num);
  } else {
    distributed_values.resize(num, data);
  }
  for (int i = 0; i < num; ++i) {
    distributed_attrs[i][rpc::PROTOCOL].set_s(protocol);
    distributed_attrs[i][rpc::VALUES].set_s(std::move(distributed_values[i]));
  }
  return distributed_attrs;
}

inline std::vector<AttrMap> DistributeVertex(const AttrMap& attrs, int num) {
  auto loader_name = attrs.at(rpc::LOADER).func().name();
  auto loader_attr = attrs.at(rpc::LOADER).func().attr();
  auto sliced_attrs = DistributeLoader(loader_attr, num);

  std::vector<AttrMap> distributed_attrs(num);
  for (int i = 0; i < num; ++i) {
    rpc::NameAttrList* list = new rpc::NameAttrList();
    list->set_name(loader_name);
    list->mutable_attr()->swap(sliced_attrs[i]);
    distributed_attrs[i][rpc::LOADER].set_allocated_func(list);
  }
  for (auto& pair : attrs) {
    if (pair.first != rpc::LOADER) {
      for (int i = 0; i < num; ++i) {
        distributed_attrs[i][pair.first].CopyFrom(pair.second);
      }
    }
  }

  return distributed_attrs;
}

inline std::vector<AttrMap> DistributeSubLabel(const AttrMap& attrs, int num) {
  auto loader_name = attrs.at(rpc::LOADER).func().name();
  auto loader_attr = attrs.at(rpc::LOADER).func().attr();
  auto sliced_attrs = DistributeLoader(loader_attr, num);

  std::vector<AttrMap> distributed_attrs(num);
  for (int i = 0; i < num; ++i) {
    rpc::NameAttrList* list = new rpc::NameAttrList();
    list->set_name(loader_name);
    list->mutable_attr()->swap(sliced_attrs[i]);
    distributed_attrs[i][rpc::LOADER].set_allocated_func(list);
  }
  for (auto& pair : attrs) {
    if (pair.first != rpc::LOADER) {
      for (int i = 0; i < num; ++i) {
        distributed_attrs[i][pair.first].CopyFrom(pair.second);
      }
    }
  }

  return distributed_attrs;
}

inline std::vector<AttrMap> DistributeEdge(const AttrMap& attrs, int num) {
  std::vector<AttrMap> distributed_attrs(num);
  std::vector<std::pair<std::string, std::vector<AttrMap>>>
      named_distributed_sub_labels;
  auto sub_label_defs = attrs.at(rpc::SUB_LABEL).list().func();
  for (const auto& sub_label_def : sub_label_defs) {
    auto sub_label_attrs = DistributeSubLabel(sub_label_def.attr(), num);
    named_distributed_sub_labels.emplace_back(sub_label_def.name(),
                                              std::move(sub_label_attrs));
  }

  for (int i = 0; i < num; ++i) {
    for (auto& pair : named_distributed_sub_labels) {
      rpc::NameAttrList* func =
          distributed_attrs[i][rpc::SUB_LABEL].mutable_list()->add_func();
      func->set_name(pair.first);
      func->mutable_attr()->swap(pair.second[i]);
    }
  }
  for (auto& pair : attrs) {
    if (pair.first != rpc::SUB_LABEL) {
      for (int i = 0; i < num; ++i) {
        distributed_attrs[i][pair.first].CopyFrom(pair.second);
      }
    }
  }
  return distributed_attrs;
}

// If contains contents from numpy or pandas, then we should distribute those
// raw bytes evenly across all workers, each worker would only receive a slice,
// in order to reduce the communication overhead.
inline std::vector<std::map<int, rpc::AttrValue>> DistributeGraph(
    const std::map<int, rpc::AttrValue>& params, int num) {
  std::vector<std::map<int, rpc::AttrValue>> distributed_graph(num);
  // Initialize empty property definition, maybe filled afterwards.
  for (int i = 0; i < num; ++i) {
    distributed_graph[i][rpc::ARROW_PROPERTY_DEFINITION] = rpc::AttrValue();
  }

  if (params.find(rpc::ARROW_PROPERTY_DEFINITION) != params.end()) {
    auto items = params.at(rpc::ARROW_PROPERTY_DEFINITION).list().func();
    std::vector<std::pair<std::string, std::vector<AttrMap>>> named_items;

    for (const auto& item : items) {
      std::vector<AttrMap> vec;
      if (item.name() == "vertex") {
        vec = std::move(DistributeVertex(item.attr(), num));
      } else if (item.name() == "edge") {
        vec = std::move(DistributeEdge(item.attr(), num));
      }
      named_items.emplace_back(item.name(), std::move(vec));
    }
    for (int i = 0; i < num; ++i) {
      for (auto& pair : named_items) {
        rpc::NameAttrList* func =
            distributed_graph[i][rpc::ARROW_PROPERTY_DEFINITION]
                .mutable_list()
                ->add_func();
        func->set_name(pair.first);
        func->mutable_attr()->swap(pair.second[i]);
      }
    }
  }
  for (auto& pair : params) {
    if (pair.first != rpc::ARROW_PROPERTY_DEFINITION) {
      for (int i = 0; i < num; ++i) {
        distributed_graph[i][pair.first].CopyFrom(pair.second);
      }
    }
  }
  return distributed_graph;
}

inline bl::result<std::shared_ptr<detail::Graph>> ParseCreatePropertyGraph(
    const GSParams& params) {
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

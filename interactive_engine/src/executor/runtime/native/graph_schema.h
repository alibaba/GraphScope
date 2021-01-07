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
#ifndef MODULES_GRAPH_FRAGMENT_HTAP_GRAPH_SCHEMA_H_
#define MODULES_GRAPH_FRAGMENT_HTAP_GRAPH_SCHEMA_H_

#include <algorithm>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "boost/leaf/all.hpp"

#include "vineyard/common/util/json.h"
#include "vineyard/graph/fragment/property_graph_types.h"
#include "vineyard/graph/fragment/graph_schema.h"

namespace vineyard {

using PropertyType = std::shared_ptr<arrow::DataType>;

class MGPropertyGraphSchema {
  enum class SchemaType {
    kMaxGraph,
    kAnalytical,
  };

 public:
  using LabelId = int;
  using PropertyId = int;
  using Entry = vineyard::Entry;

  PropertyId GetPropertyId(const std::string& name);
  PropertyType GetPropertyType(LabelId label_id, PropertyId prop_id);
  std::string GetPropertyName(PropertyId prop_id);

  LabelId GetLabelId(const std::string& name);
  std::string GetLabelName(LabelId label_id);

  void set_fnum(size_t fnum) { fnum_ = fnum; }

  Entry* CreateEntry(const std::string& type, LabelId label_id,
                     const std::string& name);

  void AddEntry(const Entry& entry) {
    if (entry.type == "VERTEX") {
      vertex_entries_.push_back(entry);
    } else {
      edge_entries_.push_back(entry);
    }
  }

  void ToJSON(vineyard::json& root) const;
  void FromJSON(vineyard::json const& root);

  std::string ToJSONString() const;
  void FromJSONString(std::string const& schema);

  size_t fnum() const { return fnum_; }

  void DumpToFile(std::string const& path);

  MGPropertyGraphSchema TransformToMaxGraph();

  const std::vector<Entry>& VertexEntries() const { return vertex_entries_; }

  const std::vector<Entry>& EdgeEntries() const { return edge_entries_; }

  void set_unique_property_names(
      const std::vector<std::string>& property_names) {
    unique_property_names_ = property_names;
  }

  void set_schema_type(SchemaType type) { schema_type_ = type; }

 private:
  SchemaType schema_type_ = SchemaType::kAnalytical;
  size_t fnum_;
  std::vector<Entry> vertex_entries_;
  std::vector<Entry> edge_entries_;

  // In Analytical engine, assume label ids of vertex entries are continuous
  // from zero, and property ids of each label is also continuous from zero.
  // When transform schema to Maxgraph style, we gather all property names and
  // unique them, assign each name a id (index of the vector), then preserve a
  // vector<int> for each label, stores mappings from original id to transformed
  // id.
  std::vector<std::string> unique_property_names_;
};

}  // namespace vineyard

#endif  // MODULES_GRAPH_FRAGMENT_HTAP_GRAPH_SCHEMA_H_

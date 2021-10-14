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
/** Copyright 2020 Alibaba Group Holding Limited.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include "graph_schema.h"

#include <cctype>
#include <fstream>
#include <set>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "glog/logging.h"

#include "common/util/json.h"

namespace vineyard {

namespace detail {

std::string PropertyTypeToString(PropertyType type) {
  if (arrow::boolean()->Equals(type)) {
    return "BOOL";
  } else if (arrow::int16()->Equals(type)) {
    return "SHORT";
  } else if (arrow::int32()->Equals(type)) {
    return "INT";
  } else if (arrow::int64()->Equals(type)) {
    return "LONG";
  } else if (arrow::float32()->Equals(type)) {
    return "FLOAT";
  } else if (arrow::float64()->Equals(type)) {
    return "DOUBLE";
  } else if (arrow::utf8()->Equals(type)) {
    return "STRING";
  } else if (arrow::large_utf8()->Equals(type)) {
    return "STRING";
  }
  LOG(ERROR) << "Unsupported arrow type " << type->ToString();
  return "NULL";
}

std::string toupper(const std::string& s) {
  std::string upper_s = s;
  std::transform(s.begin(), s.end(), upper_s.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  return upper_s;
}

PropertyType PropertyTypeFromString(const std::string& type) {
  auto type_upper = toupper(type);
  if (type_upper == "BOOL") {
    return arrow::boolean();
  } else if (type_upper == "SHORT") {
    return arrow::int16();
  } else if (type_upper == "INT") {
    return arrow::int32();
  } else if (type_upper == "LONG") {
    return arrow::int64();
  } else if (type_upper == "FLOAT") {
    return arrow::float32();
  } else if (type_upper == "DOUBLE") {
    return arrow::float64();
  } else if (type_upper == "STRING") {
    return arrow::large_utf8();
  } else {
    LOG(ERROR) << "Unsupported property type " << type;
  }
  return arrow::null();
}

}  // namespace detail

MGPropertyGraphSchema::PropertyId MGPropertyGraphSchema::GetPropertyId(
    const std::string& name) {
  PropertyId id;
  for (auto const& entry : vertex_entries_) {
    id = entry.GetPropertyId(name);
    if (id != -1) {
      return id;
    }
  }
  for (auto const& entry : edge_entries_) {
    id = entry.GetPropertyId(name);
    if (id != -1) {
      return id;
    }
  }
  return -1;
}

PropertyType MGPropertyGraphSchema::GetPropertyType(LabelId label_id,
                                                  PropertyId prop_id) {
  PropertyType type;
  for (auto const& entry : vertex_entries_) {
    if (entry.id == label_id) {
      type = entry.GetPropertyType(prop_id);
      if (!type->Equals(arrow::null())) {
        return type;
      }
    }
  }
  for (auto const& entry : edge_entries_) {
    if (entry.id == label_id) {
      type = entry.GetPropertyType(prop_id);
      if (!type->Equals(arrow::null())) {
        return type;
      }
    }
  }
  return arrow::null();
}

std::string MGPropertyGraphSchema::GetPropertyName(PropertyId prop_id) {
  std::string name;
  for (auto const& entry : vertex_entries_) {
    name = entry.GetPropertyName(prop_id);
    if (!name.empty()) {
      return name;
    }
  }
  for (auto const& entry : edge_entries_) {
    name = entry.GetPropertyName(prop_id);
    if (!name.empty()) {
      return name;
    }
  }
  return "";
}

MGPropertyGraphSchema::LabelId MGPropertyGraphSchema::GetLabelId(
    const std::string& name) {
  for (auto const& entry : vertex_entries_) {
    if (entry.label == name) {
      return entry.id;
    }
  }
  for (auto const& entry : edge_entries_) {
    if (entry.label == name) {
      return entry.id;
    }
  }
  return -1;
}

std::string MGPropertyGraphSchema::GetLabelName(LabelId label_id) {
  for (auto const& entry : vertex_entries_) {
    if (entry.id == label_id) {
      return entry.label;
    }
  }
  for (auto const& entry : edge_entries_) {
    if (entry.id == label_id) {
      return entry.label;
    }
  }
  return "";
}

MGPropertyGraphSchema::Entry* MGPropertyGraphSchema::CreateEntry(
    const std::string& type, LabelId label_id, const std::string& name) {
  if (type == "VERTEX") {
    vertex_entries_.emplace_back(
        Entry{.id = label_id, .label = name, .type = type});
    return &*vertex_entries_.rbegin();
  } else {
    edge_entries_.emplace_back(
        Entry{.id = label_id, .label = name, .type = type});
    return &*edge_entries_.rbegin();
  }
}

void MGPropertyGraphSchema::ToJSON(vineyard::json& root) const {
  root["partitionNum"] = fnum_;
  vineyard::json types = vineyard::json::array();
  for (auto const& entry : vertex_entries_) {
    types.emplace_back(entry.ToJSON());
  }
  for (auto const& entry : edge_entries_) {
    types.emplace_back(entry.ToJSON());
  }
  root["types"] = types;
  if (!unique_property_names_.empty()) {
    vineyard::put_container(root, "uniquePropertyNames",
                            unique_property_names_);
  }
}

void MGPropertyGraphSchema::FromJSON(vineyard::json const& root) {
  fnum_ = root["partitionNum"].get<size_t>();
  for (auto const& item : root["types"]) {
    Entry entry;
    entry.FromJSON(item);
    if (entry.type == "VERTEX") {
      vertex_entries_.push_back(std::move(entry));
    } else {
      edge_entries_.push_back(std::move(entry));
    }
  }
  if (root.contains("uniquePropertyNames")) {
    vineyard::get_container(root, "uniquePropertyNames",
                            unique_property_names_);
  }
}

std::string MGPropertyGraphSchema::ToJSONString() const {
  vineyard::json root;
  ToJSON(root);
  return vineyard::json_to_string(root);
}

void MGPropertyGraphSchema::FromJSONString(std::string const& schema) {
  vineyard::json root = vineyard::json::parse(schema);
  FromJSON(root);
}

void MGPropertyGraphSchema::DumpToFile(std::string const& path) {
  std::ofstream json_file;
  json_file.open(path);
  json_file << this->ToJSONString();
  json_file.close();
}

MGPropertyGraphSchema MGPropertyGraphSchema::TransformToMaxGraph() {
  if (schema_type_ == SchemaType::kMaxGraph) {
    return *this;
  }
  unique_property_names_.clear();
  MGPropertyGraphSchema new_schema;
  std::set<std::string> prop_names;
  for (const auto& entry : vertex_entries_) {
    for (const auto& prop : entry.props_) {
      prop_names.insert(prop.name);
    }
  }
  for (const auto& entry : edge_entries_) {
    for (const auto& prop : entry.props_) {
      prop_names.insert(prop.name);
    }
  }
  unique_property_names_.assign(prop_names.begin(), prop_names.end());
  std::map<std::string, int> name_to_idx;
  // mg's prop id: starts from 1
  int maximum_possible_mg_prop_id = 1 + unique_property_names_.size();
  for (size_t i = 0; i < unique_property_names_.size(); ++i) {
    name_to_idx[unique_property_names_[i]] = 1 + i;  // starts from 1
  }
  for (const auto& entry : vertex_entries_) {
    Entry new_entry = entry;
    new_entry.mapping.resize(new_entry.props_.size(), -1);
    new_entry.reverse_mapping.resize(maximum_possible_mg_prop_id, -1);
    new_entry.valid_properties.resize(maximum_possible_mg_prop_id, 1);
    for (auto& prop : new_entry.props_) {
      int new_id = name_to_idx[prop.name];
      new_entry.mapping[prop.id] = new_id;
      new_entry.reverse_mapping[new_id] = prop.id;
      prop.id = new_id;
    }
    new_schema.AddEntry(new_entry);
  }
  int vertex_label_num = vertex_entries_.size();
  for (const auto& entry : edge_entries_) {
    Entry new_entry = entry;
    new_entry.mapping.resize(new_entry.props_.size(), -1);
    new_entry.reverse_mapping.resize(maximum_possible_mg_prop_id, -1);
    new_entry.valid_properties.resize(maximum_possible_mg_prop_id, 1);
    new_entry.id += vertex_label_num;
    for (auto& prop : new_entry.props_) {
      int new_id = name_to_idx[prop.name];
      new_entry.mapping[prop.id] = new_id;
      new_entry.reverse_mapping[new_id] = prop.id;
      prop.id = new_id;
    }
    new_schema.AddEntry(new_entry);
  }
  new_schema.set_unique_property_names(unique_property_names_);
  new_schema.set_fnum(fnum_);
  new_schema.set_schema_type(SchemaType::kMaxGraph);
  return new_schema;
}

}  // namespace vineyard

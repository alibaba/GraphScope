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

#include "flex/storages/rt_mutable_graph/schema.h"

#include <yaml-cpp/yaml.h>

namespace gs {

Schema::Schema() = default;
Schema::~Schema() = default;

void Schema::add_vertex_label(
    const std::string& label, const std::vector<PropertyType>& property_types,
    const std::vector<std::string>& property_names,
    const std::vector<std::pair<PropertyType, std::string>>& primary_key,
    const std::vector<StorageStrategy>& strategies, size_t max_vnum) {
  label_t v_label_id = vertex_label_to_index(label);
  vproperties_[v_label_id] = property_types;
  vprop_names_[v_label_id] = property_names;
  vprop_storage_[v_label_id] = strategies;
  vprop_storage_[v_label_id].resize(vproperties_[v_label_id].size(),
                                    StorageStrategy::kMem);
  v_primary_keys_[v_label_id] = primary_key;
  max_vnum_[v_label_id] = max_vnum;
}

void Schema::add_edge_label(const std::string& src_label,
                            const std::string& dst_label,
                            const std::string& edge_label,
                            const std::vector<PropertyType>& properties,
                            const std::vector<std::string>& prop_names,
                            EdgeStrategy oe, EdgeStrategy ie) {
  label_t src_label_id = vertex_label_to_index(src_label);
  label_t dst_label_id = vertex_label_to_index(dst_label);
  label_t edge_label_id = edge_label_to_index(edge_label);

  uint32_t label_id =
      generate_edge_label(src_label_id, dst_label_id, edge_label_id);
  eproperties_[label_id] = properties;
  oe_strategy_[label_id] = oe;
  ie_strategy_[label_id] = ie;
  eprop_names_[label_id] = prop_names;
}

label_t Schema::vertex_label_num() const {
  return static_cast<label_t>(vlabel_indexer_.size());
}

label_t Schema::edge_label_num() const {
  return static_cast<label_t>(elabel_indexer_.size());
}

bool Schema::contains_vertex_label(const std::string& label) const {
  label_t ret;
  return vlabel_indexer_.get_index(label, ret);
}

label_t Schema::get_vertex_label_id(const std::string& label) const {
  label_t ret;
  CHECK(vlabel_indexer_.get_index(label, ret))
      << "Fail to get vertex label: " << label;
  return ret;
}

void Schema::set_vertex_properties(
    label_t label_id, const std::vector<PropertyType>& types,
    const std::vector<StorageStrategy>& strategies) {
  vproperties_[label_id] = types;
  vprop_storage_[label_id] = strategies;
  vprop_storage_[label_id].resize(types.size(), StorageStrategy::kMem);
}

const std::vector<PropertyType>& Schema::get_vertex_properties(
    const std::string& label) const {
  label_t index;
  CHECK(vlabel_indexer_.get_index(label, index));
  return vproperties_[index];
}

const std::vector<PropertyType>& Schema::get_vertex_properties(
    label_t label) const {
  return vproperties_[label];
}

const std::vector<std::string>& Schema::get_vertex_property_names(
    const std::string& label) const {
  label_t index;
  CHECK(vlabel_indexer_.get_index(label, index));
  CHECK(index < vprop_names_.size());
  return vprop_names_[index];
}

const std::vector<StorageStrategy>& Schema::get_vertex_storage_strategies(
    const std::string& label) const {
  label_t index;
  CHECK(vlabel_indexer_.get_index(label, index));
  return vprop_storage_[index];
}

size_t Schema::get_max_vnum(const std::string& label) const {
  label_t index;
  CHECK(vlabel_indexer_.get_index(label, index));
  return max_vnum_[index];
}

bool Schema::exist(const std::string& src_label, const std::string& dst_label,
                   const std::string& edge_label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(edge_label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return eproperties_.find(index) != eproperties_.end();
}

const std::vector<PropertyType>& Schema::get_edge_properties(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return eproperties_.at(index);
}

PropertyType Schema::get_edge_property(label_t src, label_t dst,
                                       label_t edge) const {
  uint32_t index = generate_edge_label(src, dst, edge);
  auto& vec = eproperties_.at(index);
  return vec.empty() ? PropertyType::kEmpty : vec[0];
}
const std::vector<std::string>& Schema::get_edge_property_names(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return eprop_names_.at(index);
}

bool Schema::valid_edge_property(const std::string& src_label,
                                 const std::string& dst_label,
                                 const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return eproperties_.find(index) != eproperties_.end();
}

EdgeStrategy Schema::get_outgoing_edge_strategy(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return oe_strategy_.at(index);
}

EdgeStrategy Schema::get_incoming_edge_strategy(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return ie_strategy_.at(index);
}

label_t Schema::get_edge_label_id(const std::string& label) const {
  label_t ret;
  CHECK(elabel_indexer_.get_index(label, ret));
  return ret;
}

bool Schema::contains_edge_label(const std::string& label) const {
  label_t ret;
  return elabel_indexer_.get_index(label, ret);
}

std::string Schema::get_vertex_label_name(label_t index) const {
  std::string ret;
  vlabel_indexer_.get_key(index, ret);
  return ret;
}

std::string Schema::get_edge_label_name(label_t index) const {
  std::string ret;
  elabel_indexer_.get_key(index, ret);
  return ret;
}

const std::vector<std::pair<PropertyType, std::string>>&
Schema::get_vertex_primary_key(label_t index) const {
  CHECK(v_primary_keys_.size() > index);
  return v_primary_keys_.at(index);
}

void Schema::Serialize(std::unique_ptr<grape::LocalIOAdaptor>& writer) {
  vlabel_indexer_.Serialize(writer);
  elabel_indexer_.Serialize(writer);
  grape::InArchive arc;
  arc << vproperties_ << vprop_storage_ << eproperties_ << ie_strategy_
      << oe_strategy_ << max_vnum_;
  CHECK(writer->WriteArchive(arc));
}

void Schema::Deserialize(std::unique_ptr<grape::LocalIOAdaptor>& reader) {
  vlabel_indexer_.Deserialize(reader);
  elabel_indexer_.Deserialize(reader);
  grape::OutArchive arc;
  CHECK(reader->ReadArchive(arc));
  arc >> vproperties_ >> vprop_storage_ >> eproperties_ >> ie_strategy_ >>
      oe_strategy_ >> max_vnum_;
}

label_t Schema::vertex_label_to_index(const std::string& label) {
  label_t ret;
  vlabel_indexer_.add(label, ret);
  if (vproperties_.size() <= ret) {
    vproperties_.resize(ret + 1);
    vprop_storage_.resize(ret + 1);
    max_vnum_.resize(ret + 1);
    vprop_names_.resize(ret + 1);
    v_primary_keys_.resize(ret + 1);
  }
  return ret;
}

label_t Schema::edge_label_to_index(const std::string& label) {
  label_t ret;
  elabel_indexer_.add(label, ret);
  return ret;
}

uint32_t Schema::generate_edge_label(label_t src, label_t dst,
                                     label_t edge) const {
  uint32_t ret = 0;
  ret |= src;
  ret <<= 8;
  ret |= dst;
  ret <<= 8;
  ret |= edge;
  return ret;
}

bool Schema::Equals(const Schema& other) const {
  if (vertex_label_num() != other.vertex_label_num() ||
      edge_label_num() != other.edge_label_num()) {
    return false;
  }
  for (label_t i = 0; i < vertex_label_num(); ++i) {
    std::string label_name = get_vertex_label_name(i);
    {
      const auto& lhs = get_vertex_properties(label_name);
      const auto& rhs = other.get_vertex_properties(label_name);
      if (lhs != rhs) {
        return false;
      }
    }
    {
      const auto& lhs = get_vertex_storage_strategies(label_name);
      const auto& rhs = other.get_vertex_storage_strategies(label_name);
      if (lhs != rhs) {
        return false;
      }
    }
    if (get_max_vnum(label_name) != other.get_max_vnum(label_name)) {
      return false;
    }
  }
  for (label_t src_label = 0; src_label < vertex_label_num(); ++src_label) {
    for (label_t dst_label = 0; dst_label < vertex_label_num(); ++dst_label) {
      for (label_t edge_label = 0; edge_label < edge_label_num();
           ++edge_label) {
        std::string src_label_name = get_vertex_label_name(src_label);
        std::string dst_label_name = get_vertex_label_name(dst_label);
        std::string edge_label_name = get_edge_label_name(edge_label);
        auto lhs_exists =
            exist(src_label_name, dst_label_name, edge_label_name);
        auto rhs_exists =
            other.exist(src_label_name, dst_label_name, edge_label_name);
        if (lhs_exists != rhs_exists) {
          return false;
        }
        if (lhs_exists) {
          {
            const auto& lhs = get_edge_properties(
                src_label_name, dst_label_name, edge_label_name);
            const auto& rhs = other.get_edge_properties(
                src_label_name, dst_label_name, edge_label_name);
            if (lhs != rhs) {
              return false;
            }
          }
          {
            auto lhs = get_incoming_edge_strategy(
                src_label_name, dst_label_name, edge_label_name);
            auto rhs = other.get_incoming_edge_strategy(
                src_label_name, dst_label_name, edge_label_name);
            if (lhs != rhs) {
              return false;
            }
          }
          {
            auto lhs = get_outgoing_edge_strategy(
                src_label_name, dst_label_name, edge_label_name);
            auto rhs = other.get_outgoing_edge_strategy(
                src_label_name, dst_label_name, edge_label_name);
            if (lhs != rhs) {
              return false;
            }
          }
        }
      }
    }
  }
  return true;
}

namespace config_parsing {

static PropertyType StringToPropertyType(const std::string& str) {
  if (str == "int32" || str == DT_SIGNED_INT32) {
    return PropertyType::kInt32;
  } else if (str == "Date") {
    return PropertyType::kDate;
  } else if (str == "String" || str == DT_STRING) {
    return PropertyType::kString;
  } else if (str == "Empty") {
    return PropertyType::kEmpty;
  } else if (str == "int64" || str == DT_SIGNED_INT64) {
    return PropertyType::kInt64;
  } else if (str == "double" || str == DT_DOUBLE) {
    return PropertyType::kDouble;
  } else {
    return PropertyType::kEmpty;
  }
}

EdgeStrategy StringToEdgeStrategy(const std::string& str) {
  if (str == "None") {
    return EdgeStrategy::kNone;
  } else if (str == "Single") {
    return EdgeStrategy::kSingle;
  } else if (str == "Multiple") {
    return EdgeStrategy::kMultiple;
  } else {
    return EdgeStrategy::kMultiple;
  }
}

StorageStrategy StringToStorageStrategy(const std::string& str) {
  if (str == "None") {
    return StorageStrategy::kNone;
  } else if (str == "Mem") {
    return StorageStrategy::kMem;
  } else {
    return StorageStrategy::kMem;
  }
}

static bool parse_vertex_properties(YAML::Node node,
                                    const std::string& label_name,
                                    std::vector<PropertyType>& types,
                                    std::vector<std::string>& names,
                                    std::vector<StorageStrategy>& strategies) {
  if (!node || !node.IsSequence()) {
    LOG(ERROR) << "Expect properties for " << label_name << " to be a sequence";
    return false;
  }

  int prop_num = node.size();
  if (prop_num == 0) {
    LOG(ERROR) << "At least one property is needed for " << label_name;
    return false;
  }

  for (int i = 0; i < prop_num; ++i) {
    std::string prop_type_str, strategy_str, prop_name_str;
    if (!get_scalar(node[i], "property_name", prop_name_str)) {
      LOG(ERROR) << "name of vertex-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return false;
    }
    if (!node[i]["property_type"]) {
      LOG(ERROR) << "type of vertex-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return false;
    }
    auto prop_type_node = node[i]["property_type"];
    if (!prop_type_node["primitive_type"]) {
      LOG(ERROR) << "type of vertex-" << label_name << " prop-" << i - 1
                 << " is not primitive...";
      return false;
    }
    if (!get_scalar(prop_type_node, "primitive_type", prop_type_str)) {
      LOG(ERROR) << "type of vertex-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return false;
    }
    {
      if (node[i]["x_csr_params"]) {
        get_scalar(node[i]["x_csr_params"], "storage_strategy", strategy_str);
      }
    }
    types.push_back(StringToPropertyType(prop_type_str));
    strategies.push_back(StringToStorageStrategy(strategy_str));
    VLOG(10) << "prop-" << i - 1 << " name: " << prop_name_str
             << " type: " << prop_type_str << " strategy: " << strategy_str;
    names.push_back(prop_name_str);
  }

  return true;
}

static bool parse_edge_properties(YAML::Node node,
                                  const std::string& label_name,
                                  std::vector<PropertyType>& types,
                                  std::vector<std::string>& names) {
  if (!node) {
    VLOG(10) << "Found no edge properties specified for edge: " << label_name;
    return true;
  }
  if (!node.IsSequence()) {
    LOG(ERROR) << "properties of edge -" << label_name
               << " not set properly, should be a sequence...";
    return false;
  }

  int prop_num = node.size();

  for (int i = 0; i < prop_num; ++i) {
    std::string prop_type_str, strategy_str, prop_name_str;
    if (node[i]["property_type"]) {
      if (!get_scalar(node[i]["property_type"], "primitive_type",
                      prop_type_str)) {
        LOG(ERROR) << "Only support primitive type for edge property";
        return false;
      }
    } else {
      LOG(ERROR) << "type of edge-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return false;
    }
    if (!get_scalar(node[i], "property_name", prop_name_str)) {
      LOG(ERROR) << "name of edge-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return false;
    }

    types.push_back(StringToPropertyType(prop_type_str));
    names.push_back(prop_name_str);
  }

  return true;
}

static bool parse_vertex_schema(YAML::Node node, Schema& schema) {
  std::string label_name;
  if (!get_scalar(node, "type_name", label_name)) {
    return false;
  }
  // Can not add two vertex label with same name
  if (schema.has_vertex_label(label_name)) {
    LOG(ERROR) << "Vertex label " << label_name << " already exists";
    return false;
  }

  size_t max_num = ((size_t) 1) << 32;
  if (node["x_csr_params"]) {
    auto csr_node = node["x_csr_params"];
    get_scalar(csr_node, "max_vertex_num", max_num);
  }
  std::vector<PropertyType> property_types;
  std::vector<std::string> property_names;
  std::vector<StorageStrategy> strategies;
  if (!parse_vertex_properties(node["properties"], label_name, property_types,
                               property_names, strategies)) {
    return false;
  }
  if (!node["primary_keys"]) {
    LOG(ERROR) << "Expect field primary_keys for " << label_name;
    return false;
  }
  auto primary_key_node = node["primary_keys"];
  if (!primary_key_node.IsSequence()) {
    LOG(ERROR) << "[Primary_keys] should be sequence";
    return false;
  }
  // remove primary key from properties.

  std::vector<int> primary_key_inds(primary_key_node.size(), -1);
  std::vector<std::pair<PropertyType, std::string>> primary_keys;
  for (auto i = 0; i < primary_key_node.size(); ++i) {
    auto cur_primary_key = primary_key_node[i];
    std::string primary_key_name = primary_key_node[0].as<std::string>();
    for (size_t j = 0; j < property_names.size(); ++j) {
      if (property_names[j] == primary_key_name) {
        primary_key_inds[i] = j;
        break;
      }
    }
    if (primary_key_inds[i] == -1) {
      LOG(ERROR) << "Primary key " << primary_key_name
                 << " is not found in properties";
      return false;
    }
    if (property_types[primary_key_inds[i]] != PropertyType::kInt64) {
      LOG(ERROR) << "Primary key " << primary_key_name << " should be int64";
      return false;
    }
    primary_keys.emplace_back(property_types[primary_key_inds[i]],
                              property_names[primary_key_inds[i]]);
    // remove primary key from properties.
    property_names.erase(property_names.begin() + primary_key_inds[i]);
    property_types.erase(property_types.begin() + primary_key_inds[i]);
  }

  schema.add_vertex_label(label_name, property_types, property_names,
                          primary_keys, strategies, max_num);
  return true;
}

static bool parse_vertices_schema(YAML::Node node, Schema& schema) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "vertex is not set properly";
    return false;
  }
  int num = node.size();
  for (int i = 0; i < num; ++i) {
    if (!parse_vertex_schema(node[i], schema)) {
      return false;
    }
  }
  return true;
}

static bool parse_edge_schema(YAML::Node node, Schema& schema) {
  std::string src_label_name, dst_label_name, edge_label_name;
  if (!node["type_name"]) {
    LOG(ERROR) << "edge type_name is not set properly";
    return false;
  }
  edge_label_name = node["type_name"].as<std::string>();
  // get vertex type pair relation
  auto vertex_type_pair_node = node["vertex_type_pair_relations"];
  if (!vertex_type_pair_node || !vertex_type_pair_node.IsMap()) {
    LOG(ERROR) << "edge [vertex_type_pair_relations] is not set properly";
    return false;
  }

  if (!get_scalar(vertex_type_pair_node, "source_vertex", src_label_name)) {
    LOG(ERROR) << "Expect field source_vertex for edge [" << edge_label_name
               << "] in vertex_type_pair_relations";
    return false;
  }
  if (!get_scalar(vertex_type_pair_node, "destination_vertex",
                  dst_label_name)) {
    LOG(ERROR) << "Expect field destination_vertex for edge ["
               << edge_label_name << "] in vertex_type_pair_relations";
    return false;
  }

  // check whether edge triplet exists in current schema
  if (schema.has_edge_label(src_label_name, dst_label_name, edge_label_name)) {
    LOG(ERROR) << "Edge [" << edge_label_name << "] from [" << src_label_name
               << "] to [" << dst_label_name << "] already exists";
    return false;
  }

  std::vector<PropertyType> property_types;
  std::vector<std::string> prop_names;
  if (!parse_edge_properties(node["properties"], edge_label_name,
                             property_types, prop_names)) {
    return false;
  }
  EdgeStrategy ie = EdgeStrategy::kMultiple;
  EdgeStrategy oe = EdgeStrategy::kMultiple;
  {
    auto csr_node = node["x_csr_params"];
    std::string ie_str, oe_str;
    if (get_scalar(node, "outgoing_edge_strategy", oe_str)) {
      oe = StringToEdgeStrategy(oe_str);
    }
    if (get_scalar(node, "incoming_edge_strategy", ie_str)) {
      ie = StringToEdgeStrategy(ie_str);
    }
  }
  VLOG(10) << "edge " << edge_label_name << " from " << src_label_name << " to "
           << dst_label_name << " with " << property_types.size()
           << " properties";
  schema.add_edge_label(src_label_name, dst_label_name, edge_label_name,
                        property_types, prop_names, oe, ie);
  return true;
}

static bool parse_edges_schema(YAML::Node node, Schema& schema) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "edge is not set properly";
    return false;
  }
  int num = node.size();
  for (int i = 0; i < num; ++i) {
    if (!parse_edge_schema(node[i], schema)) {
      return false;
    }
  }

  return true;
}

static bool parse_schema_config_file(const std::string& path, Schema& schema) {
  YAML::Node graph_node = YAML::LoadFile(path);
  if (!graph_node || !graph_node.IsMap()) {
    LOG(ERROR) << "graph is not set properly";
    return false;
  }
  if (!expect_config(graph_node, "store_type", std::string("mutable_csr"))) {
    return false;
  }
  auto schema_node = graph_node["schema"];

  if (!graph_node["schema"]) {
    LOG(ERROR) << "schema is not set in scheme yaml file";
    return false;
  }

  if (!parse_vertices_schema(schema_node["vertex_types"], schema)) {
    return false;
  }

  if (schema_node["edge_types"]) {
    if (!parse_edges_schema(schema_node["edge_types"], schema)) {
      return false;
    }
  }

  if (graph_node["stored_procedures"]) {
    auto stored_procedure_node = graph_node["stored_procedures"];
    auto directory = stored_procedure_node["directory"].as<std::string>();
    // check is directory
    if (!std::filesystem::exists(directory)) {
      LOG(WARNING) << "plugin directory - " << directory << " not found...";
    }
    std::vector<std::string> files_got;
    if (!get_sequence(stored_procedure_node, "enable_lists", files_got)) {
      LOG(ERROR) << "stored_procedures is not set properly";
    }
    for (auto& f : files_got) {
      if (!std::filesystem::exists(f)) {
        LOG(ERROR) << "plugin - " << f << " file not found...";
      } else {
        schema.EmplacePlugin(std::filesystem::canonical(f));
      }
    }
  }

  return true;
}

}  // namespace config_parsing

const std::vector<std::string>& Schema::GetPluginsList() const {
  return plugin_list_;
}

void Schema::EmplacePlugin(const std::string& plugin) {
  plugin_list_.emplace_back(plugin);
}

// check whether prop in vprop_names, or is the primary key
bool Schema::vertex_has_property(const std::string& label,
                                 const std::string& prop) const {
  auto v_label_id = get_vertex_label_id(label);
  CHECK(v_label_id < vprop_names_.size());
  auto& v_prop_names = vprop_names_[v_label_id];
  return std::find(v_prop_names.begin(), v_prop_names.end(), prop) !=
             v_prop_names.end() ||
         vertex_has_primary_key(label, prop);
}

bool Schema::vertex_has_primary_key(const std::string& label,
                                    const std::string& prop) const {
  auto v_label_id = get_vertex_label_id(label);
  CHECK(v_label_id < vprop_names_.size());
  auto& keys = v_primary_keys_[v_label_id];
  for (auto i = 0; i < keys.size(); ++i) {
    if (keys[i].second == prop) {
      return true;
    }
  }
  return false;
}

bool Schema::edge_has_property(const std::string& src_label,
                               const std::string& dst_label,
                               const std::string& edge_label,
                               const std::string& prop) const {
  auto e_label_id = get_edge_label_id(edge_label);
  auto src_label_id = get_vertex_label_id(src_label);
  auto dst_label_id = get_vertex_label_id(dst_label);
  auto label_id = generate_edge_label(src_label_id, dst_label_id, e_label_id);
  if (eprop_names_.find(label_id) == eprop_names_.end()) {
    LOG(FATAL) << "edge label " << edge_label << ": (" << src_label << ", "
               << dst_label << ") not found,  e_label_id "
               << std::to_string(label_id)
               << ", total size: " << eprop_names_.size();
  }
  auto& e_prop_names = eprop_names_.at(label_id);
  return std::find(e_prop_names.begin(), e_prop_names.end(), prop) !=
         e_prop_names.end();
}

bool Schema::has_vertex_label(const std::string& label) const {
  label_t ret;
  return vlabel_indexer_.get_index(label, ret);
}

bool Schema::has_edge_label(const std::string& src_label,
                            const std::string& dst_label,
                            const std::string& label) const {
  label_t edge_label_id;
  auto src_label_id = get_vertex_label_id(src_label);
  auto dst_label_id = get_vertex_label_id(dst_label);
  if (!elabel_indexer_.get_index(label, edge_label_id)) {
    return false;
  }
  auto e_label_id =
      generate_edge_label(src_label_id, dst_label_id, edge_label_id);
  return eprop_names_.find(e_label_id) != eprop_names_.end();
}

Schema Schema::LoadFromYaml(const std::string& schema_config) {
  Schema schema;
  if (!schema_config.empty() && std::filesystem::exists(schema_config)) {
    if (!config_parsing::parse_schema_config_file(schema_config, schema)) {
      LOG(FATAL) << "Failed to parse schema config file: " << schema_config;
    }
  }
  return schema;
}

}  // namespace gs

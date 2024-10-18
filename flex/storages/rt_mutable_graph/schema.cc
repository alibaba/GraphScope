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
#include "flex/utils/exception.h"

#include <yaml-cpp/yaml.h>

namespace gs {

bool Schema::IsBuiltinPlugin(const std::string& plugin_name) {
  for (uint8_t i = 0; i < BUILTIN_PLUGIN_NUM; i++) {
    if (plugin_name == BUILTIN_PLUGIN_NAMES[i]) {
      return true;
    }
  }
  return false;
}

Schema::Schema() : has_multi_props_edge_(false){};
Schema::~Schema() = default;

void Schema::Clear() {
  vlabel_indexer_.Clear();
  elabel_indexer_.Clear();
  vproperties_.clear();
  vprop_names_.clear();
  v_primary_keys_.clear();
  vprop_storage_.clear();
  eproperties_.clear();
  eprop_names_.clear();
  ie_strategy_.clear();
  oe_strategy_.clear();
  ie_mutability_.clear();
  oe_mutability_.clear();
  sort_on_compactions_.clear();
  max_vnum_.clear();
  plugin_name_to_path_and_id_.clear();
  plugin_dir_.clear();
  has_multi_props_edge_ = false;
}

void Schema::add_vertex_label(
    const std::string& label, const std::vector<PropertyType>& property_types,
    const std::vector<std::string>& property_names,
    const std::vector<std::tuple<PropertyType, std::string, size_t>>&
        primary_key,
    const std::vector<StorageStrategy>& strategies, size_t max_vnum,
    const std::string& description) {
  label_t v_label_id = vertex_label_to_index(label);
  vproperties_[v_label_id] = property_types;
  vprop_names_[v_label_id] = property_names;
  vprop_storage_[v_label_id] = strategies;
  vprop_storage_[v_label_id].resize(vproperties_[v_label_id].size(),
                                    StorageStrategy::kMem);
  v_primary_keys_[v_label_id] = primary_key;
  max_vnum_[v_label_id] = max_vnum;
  v_descriptions_[v_label_id] = description;
}

void Schema::add_edge_label(const std::string& src_label,
                            const std::string& dst_label,
                            const std::string& edge_label,
                            const std::vector<PropertyType>& properties,
                            const std::vector<std::string>& prop_names,
                            EdgeStrategy oe, EdgeStrategy ie, bool oe_mutable,
                            bool ie_mutable, bool sort_on_compaction,
                            const std::string& description) {
  label_t src_label_id = vertex_label_to_index(src_label);
  label_t dst_label_id = vertex_label_to_index(dst_label);
  label_t edge_label_id = edge_label_to_index(edge_label);

  uint32_t label_id =
      generate_edge_label(src_label_id, dst_label_id, edge_label_id);
  eproperties_[label_id] = properties;
  if (properties.size() > 1) {
    has_multi_props_edge_ = true;
  }
  oe_strategy_[label_id] = oe;
  ie_strategy_[label_id] = ie;
  oe_mutability_[label_id] = oe_mutable;
  ie_mutability_[label_id] = ie_mutable;
  eprop_names_[label_id] = prop_names;
  sort_on_compactions_[label_id] = sort_on_compaction;
  e_descriptions_[label_id] = description;
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
  THROW_EXCEPTION_IF(!vlabel_indexer_.get_index(label, ret),
                     "Fail to get vertex label: " + label);
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
  label_t index = get_vertex_label_id(label);
  return vproperties_[index];
}

const std::vector<PropertyType>& Schema::get_vertex_properties(
    label_t label) const {
  return vproperties_[label];
}

const std::vector<std::string>& Schema::get_vertex_property_names(
    const std::string& label) const {
  label_t index = get_vertex_label_id(label);
  return get_vertex_property_names(index);
}

const std::vector<std::string>& Schema::get_vertex_property_names(
    label_t label) const {
  THROW_EXCEPTION_IF(
      label >= vprop_names_.size(),
      "Fail to get vertex property names: " + std::to_string(label) +
          ", out of range of vprop_names_ " +
          std::to_string(vprop_names_.size()));
  return vprop_names_[label];
}

const std::string& Schema::get_vertex_description(
    const std::string& label) const {
  label_t index = get_vertex_label_id(label);
  return get_vertex_description(index);
}

const std::string& Schema::get_vertex_description(label_t label) const {
  THROW_EXCEPTION_IF(
      label >= v_descriptions_.size(),
      "Fail to get vertex description: " + std::to_string(label) +
          ", out of range of v_descriptions_ " +
          std::to_string(v_descriptions_.size()));
  return v_descriptions_[label];
}

const std::vector<StorageStrategy>& Schema::get_vertex_storage_strategies(
    const std::string& label) const {
  label_t index = get_vertex_label_id(label);
  THROW_EXCEPTION_IF(
      index >= vprop_storage_.size(),
      "Fail to get vertex storage strategies: " + std::to_string(index) +
          ", out of range of vprop_storage_ " +
          std::to_string(vprop_storage_.size()));
  return vprop_storage_[index];
}

size_t Schema::get_max_vnum(const std::string& label) const {
  label_t index = get_vertex_label_id(label);
  return max_vnum_[index];
}

bool Schema::exist(const std::string& src_label, const std::string& dst_label,
                   const std::string& edge_label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(edge_label);
  uint32_t index = generate_edge_label(src, dst, edge);
  return eproperties_.find(index) != eproperties_.end();
}

bool Schema::exist(label_t src_label, label_t dst_label,
                   label_t edge_label) const {
  uint32_t index = generate_edge_label(src_label, dst_label, edge_label);
  return eproperties_.find(index) != eproperties_.end();
}

const std::vector<PropertyType>& Schema::get_edge_properties(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  uint32_t index = generate_edge_label(src, dst, edge);
  return eproperties_.at(index);
}

const std::vector<PropertyType>& Schema::get_edge_properties(
    label_t src_label, label_t dst_label, label_t label) const {
  THROW_EXCEPTION_IF(
      src_label >= vlabel_indexer_.size(),
      "vertex label " + std::to_string(src_label) + " not found");
  THROW_EXCEPTION_IF(
      dst_label >= vlabel_indexer_.size(),
      "vertex label " + std::to_string(dst_label) + " not found");
  THROW_EXCEPTION_IF(label >= elabel_indexer_.size(),
                     "edge label " + std::to_string(label) + " not found");
  uint32_t index = generate_edge_label(src_label, dst_label, label);
  return eproperties_.at(index);
}

std::string Schema::get_edge_description(const std::string& src_label,
                                         const std::string& dst_label,
                                         const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  return get_edge_description(src, dst, edge);
}

std::string Schema::get_edge_description(label_t src_label, label_t dst_label,
                                         label_t label) const {
  THROW_EXCEPTION_IF(
      src_label >= vlabel_indexer_.size(),
      "vertex label " + std::to_string(src_label) + " not found");
  THROW_EXCEPTION_IF(
      dst_label >= vlabel_indexer_.size(),
      "vertex label " + std::to_string(dst_label) + " not found");
  THROW_EXCEPTION_IF(label >= elabel_indexer_.size(),
                     "edge label " + std::to_string(label) + " not found");
  uint32_t index = generate_edge_label(src_label, dst_label, label);
  THROW_EXCEPTION_IF(index >= e_descriptions_.size(),
                     "Fail to get edge description: " + std::to_string(index) +
                         ", out of range of e_descriptions_ " +
                         std::to_string(e_descriptions_.size()));
  return e_descriptions_.at(index);
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
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  return get_edge_property_names(src, dst, edge);
}

const std::vector<std::string>& Schema::get_edge_property_names(
    const label_t& src_label, const label_t& dst_label,
    const label_t& label) const {
  THROW_EXCEPTION_IF(
      src_label >= vlabel_indexer_.size(),
      "vertex label " + std::to_string(src_label) + " not found");
  THROW_EXCEPTION_IF(
      dst_label >= vlabel_indexer_.size(),
      "vertex label " + std::to_string(dst_label) + " not found");
  THROW_EXCEPTION_IF(label >= elabel_indexer_.size(),
                     "edge label " + std::to_string(label) + " not found");
  uint32_t index = generate_edge_label(src_label, dst_label, label);
  return eprop_names_.at(index);
}

bool Schema::valid_edge_property(const std::string& src_label,
                                 const std::string& dst_label,
                                 const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  uint32_t index = generate_edge_label(src, dst, edge);
  return eproperties_.find(index) != eproperties_.end();
}

EdgeStrategy Schema::get_outgoing_edge_strategy(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  uint32_t index = generate_edge_label(src, dst, edge);
  return oe_strategy_.at(index);
}

EdgeStrategy Schema::get_incoming_edge_strategy(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  uint32_t index = generate_edge_label(src, dst, edge);
  return ie_strategy_.at(index);
}

bool Schema::outgoing_edge_mutable(const std::string& src_label,
                                   const std::string& dst_label,
                                   const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  uint32_t index = generate_edge_label(src, dst, edge);
  return oe_mutability_.at(index);
}

bool Schema::incoming_edge_mutable(const std::string& src_label,
                                   const std::string& dst_label,
                                   const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  uint32_t index = generate_edge_label(src, dst, edge);
  return ie_mutability_.at(index);
}

bool Schema::get_sort_on_compaction(const std::string& src_label,
                                    const std::string& dst_label,
                                    const std::string& label) const {
  label_t src = get_vertex_label_id(src_label);
  label_t dst = get_vertex_label_id(dst_label);
  label_t edge = get_edge_label_id(label);
  uint32_t index = generate_edge_label(src, dst, edge);
  THROW_EXCEPTION_IF(
      sort_on_compactions_.find(index) == sort_on_compactions_.end(),
      "Fail to get sort on compaction: " + std::to_string(index) +
          ", out of range of sort_on_compactions_ " +
          std::to_string(sort_on_compactions_.size()));
  return sort_on_compactions_.at(index);
}

label_t Schema::get_edge_label_id(const std::string& label) const {
  label_t ret;
  THROW_EXCEPTION_IF(!elabel_indexer_.get_index(label, ret),
                     "Edge label " + label + " not found");
  return ret;
}

bool Schema::contains_edge_label(const std::string& label) const {
  label_t ret;
  return elabel_indexer_.get_index(label, ret);
}

std::string Schema::get_vertex_label_name(label_t index) const {
  std::string ret;
  THROW_EXCEPTION_IF(
      !vlabel_indexer_.get_key(index, ret),
      "No vertex label found for label id: " + std::to_string(index));
  return ret;
}

std::string Schema::get_edge_label_name(label_t index) const {
  std::string ret;
  THROW_EXCEPTION_IF(
      !elabel_indexer_.get_key(index, ret),
      "No edge label found for label id: " + std::to_string(index));
  return ret;
}

const std::vector<std::tuple<PropertyType, std::string, size_t>>&
Schema::get_vertex_primary_key(label_t index) const {
  THROW_EXCEPTION_IF(index >= v_primary_keys_.size(),
                     "Fail to get vertex primary key: " +
                         std::to_string(index) + ", out of range");
  return v_primary_keys_[index];
}

// Note that plugin_dir_ and plugin_name_to_path_and_id_ are not serialized.
void Schema::Serialize(std::unique_ptr<grape::LocalIOAdaptor>& writer) const {
  vlabel_indexer_.Serialize(writer);
  elabel_indexer_.Serialize(writer);
  grape::InArchive arc;
  arc << v_primary_keys_ << vproperties_ << vprop_names_ << vprop_storage_
      << eproperties_ << eprop_names_ << ie_strategy_ << oe_strategy_
      << ie_mutability_ << oe_mutability_ << sort_on_compactions_ << max_vnum_
      << v_descriptions_ << e_descriptions_ << description_ << version_;
  CHECK(writer->WriteArchive(arc));
}

// Note that plugin_dir_ and plugin_name_to_path_and_id_ are not deserialized.
void Schema::Deserialize(std::unique_ptr<grape::LocalIOAdaptor>& reader) {
  vlabel_indexer_.Deserialize(reader);
  elabel_indexer_.Deserialize(reader);
  grape::OutArchive arc;
  CHECK(reader->ReadArchive(arc));
  arc >> v_primary_keys_ >> vproperties_ >> vprop_names_ >> vprop_storage_ >>
      eproperties_ >> eprop_names_ >> ie_strategy_ >> oe_strategy_ >>
      ie_mutability_ >> oe_mutability_ >> sort_on_compactions_ >> max_vnum_ >>
      v_descriptions_ >> e_descriptions_ >> description_ >> version_;
  has_multi_props_edge_ = false;
  for (auto& eprops : eproperties_) {
    if (eprops.second.size() > 1) {
      has_multi_props_edge_ = true;
      break;
    }
  }
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
    v_descriptions_.resize(ret + 1);
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
  // When compare two schemas, we only compare the properties and strategies
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

void RelationToEdgeStrategy(const std::string& rel_str,
                            EdgeStrategy& ie_strategy,
                            EdgeStrategy& oe_strategy) {
  if (rel_str == "ONE_TO_MANY") {
    ie_strategy = EdgeStrategy::kSingle;
    oe_strategy = EdgeStrategy::kMultiple;
  } else if (rel_str == "ONE_TO_ONE") {
    ie_strategy = EdgeStrategy::kSingle;
    oe_strategy = EdgeStrategy::kSingle;
  } else if (rel_str == "MANY_TO_ONE") {
    ie_strategy = EdgeStrategy::kMultiple;
    oe_strategy = EdgeStrategy::kSingle;
  } else if (rel_str == "MANY_TO_MANY") {
    ie_strategy = EdgeStrategy::kMultiple;
    oe_strategy = EdgeStrategy::kMultiple;
  } else {
    LOG(WARNING) << "relation " << rel_str
                 << " is not valid, using default value: kMultiple";
    ie_strategy = EdgeStrategy::kMultiple;
    oe_strategy = EdgeStrategy::kMultiple;
  }
}

StorageStrategy StringToStorageStrategy(const std::string& str) {
  if (str == "None") {
    return StorageStrategy::kNone;
  } else if (str == "Mem") {
    return StorageStrategy::kMem;
  } else if (str == "Disk") {
    return StorageStrategy::kDisk;
  } else {
    return StorageStrategy::kMem;
  }
}

static bool parse_property_type(YAML::Node node, PropertyType& type) {
  try {
    type = node.as<gs::PropertyType>();
    return true;
  } catch (const YAML::BadConversion& e) {
    LOG(ERROR) << "Failed to parse property type: " << e.what();
    return false;
  }
}

static Status parse_vertex_properties(YAML::Node node,
                                      const std::string& label_name,
                                      std::vector<PropertyType>& types,
                                      std::vector<std::string>& names,
                                      std::vector<StorageStrategy>& strategies,
                                      const std::string& version) {
  if (!node || node.IsNull()) {
    VLOG(10) << "Found no vertex properties specified for vertex: "
             << label_name;
    return Status::OK();
  }
  if (!node.IsSequence()) {
    LOG(ERROR) << "Expect properties for " << label_name << " to be a sequence";
    return Status(StatusCode::INVALID_SCHEMA,
                  "Expect properties for " + label_name + " to be a sequence");
  }

  int prop_num = node.size();
  if (prop_num == 0) {
    LOG(ERROR) << "At least one property is needed for " << label_name;
    return Status(StatusCode::INVALID_SCHEMA,
                  "At least one property is needed for " + label_name);
  }

  for (int i = 0; i < prop_num; ++i) {
    std::string strategy_str, prop_name_str;
    if (!get_scalar(node[i], "property_name", prop_name_str)) {
      LOG(ERROR) << "Name of vertex-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return Status(StatusCode::INVALID_SCHEMA,
                    "Name of vertex-" + label_name + " prop-" +
                        std::to_string(i - 1) + " is not specified...");
    }
    if (!node[i]["property_type"]) {
      LOG(ERROR) << "type of vertex-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return Status(StatusCode::INVALID_SCHEMA,
                    "type of vertex-" + label_name + " prop-" +
                        std::to_string(i - 1) + " is not specified...");
    }
    auto prop_type_node = node[i]["property_type"];
    PropertyType prop_type;
    if (!parse_property_type(prop_type_node, prop_type)) {
      LOG(ERROR) << "Fail to parse property type of vertex-" << label_name
                 << " prop-" << i - 1;
      return Status(StatusCode::INVALID_SCHEMA,
                    "Fail to parse property type of vertex-" + label_name +
                        " prop-" + std::to_string(i - 1));
    }
    {
      if (node[i]["x_csr_params"]) {
        get_scalar(node[i]["x_csr_params"], "storage_strategy", strategy_str);
      }
    }
    types.push_back(prop_type);
    strategies.push_back(StringToStorageStrategy(strategy_str));
    VLOG(10) << "prop-" << i - 1 << " name: " << prop_name_str
             << " type: " << prop_type << " strategy: " << strategy_str;
    names.push_back(prop_name_str);
  }

  return Status::OK();
}

static Status parse_edge_properties(YAML::Node node,
                                    const std::string& label_name,
                                    std::vector<PropertyType>& types,
                                    std::vector<std::string>& names,
                                    const std::string& version) {
  if (!node || node.IsNull()) {
    VLOG(10) << "Found no edge properties specified for edge: " << label_name;
    return Status::OK();
  }
  if (!node.IsSequence()) {
    LOG(ERROR) << "properties of edge -" << label_name
               << " not set properly, should be a sequence...";
    return Status(StatusCode::INVALID_SCHEMA,
                  "properties of edge -" + label_name +
                      " not set properly, should be a sequence...");
  }

  int prop_num = node.size();

  for (int i = 0; i < prop_num; ++i) {
    std::string strategy_str, prop_name_str;
    if (!node[i]["property_type"]) {
      LOG(ERROR) << "type of edge-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return Status(StatusCode::INVALID_SCHEMA,
                    "type of edge-" + label_name + " prop-" +
                        std::to_string(i - 1) + " is not specified...");
    }
    auto prop_type_node = node[i]["property_type"];
    PropertyType prop_type;
    if (!parse_property_type(prop_type_node, prop_type)) {
      LOG(ERROR) << "type of edge-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return Status(StatusCode::INVALID_SCHEMA,
                    "type of edge-" + label_name + " prop-" +
                        std::to_string(i - 1) + " is not specified...");
    }
    // For edge properties, we have some constrains on the property type. We
    // currently only support var_char. long_text string are not supported.
    if (prop_type == PropertyType::StringMap()) {
      LOG(ERROR) << "Please use varchar as the type of edge-" << label_name
                 << " prop-" << i - 1
                 << ", if you want to use string property: " << prop_type
                 << ", prop_type.enum" << prop_type.type_enum;
      return Status(StatusCode::INVALID_SCHEMA,
                    "Please use varchar as the type of edge-" + label_name +
                        " prop-" + std::to_string(i - 1) +
                        ", if you want to "
                        "use string property.");
    }

    if (!get_scalar(node[i], "property_name", prop_name_str)) {
      LOG(ERROR) << "name of edge-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return Status(StatusCode::INVALID_SCHEMA,
                    "name of edge-" + label_name + " prop-" +
                        std::to_string(i - 1) + " is not specified...");
    }

    types.push_back(prop_type);
    names.push_back(prop_name_str);
  }

  return Status::OK();
}

static Status parse_vertex_schema(YAML::Node node, Schema& schema) {
  std::string label_name;
  if (!get_scalar(node, "type_name", label_name)) {
    return Status(StatusCode::INVALID_SCHEMA, "vertex type_name is not set");
  }
  // Cannot add two vertex label with same name
  if (schema.has_vertex_label(label_name)) {
    LOG(ERROR) << "Vertex label " << label_name << " already exists";
    return Status(StatusCode::INVALID_SCHEMA,
                  "Vertex label " + label_name + " already exists");
  }

  size_t max_num = ((size_t) 1) << 32;
  if (node["x_csr_params"]) {
    auto csr_node = node["x_csr_params"];
    get_scalar(csr_node, "max_vertex_num", max_num);
  }
  std::vector<PropertyType> property_types;
  std::vector<std::string> property_names;
  std::vector<StorageStrategy> strategies;
  std::string description;  // default is empty string

  if (node["description"]) {
    description = node["description"].as<std::string>();
  }

  if (node["nullable"]) {
    LOG(ERROR) << "nullable is not supported yet";
    return Status(StatusCode::UNIMPLEMENTED, "nullable is not supported yet");
  }

  if (node["default_value"]) {
    LOG(ERROR) << "default_value is not supported yet";
    return Status(StatusCode::UNIMPLEMENTED,
                  "default_value is not supported yet");
  }

  RETURN_IF_NOT_OK(parse_vertex_properties(node["properties"], label_name,
                                           property_types, property_names,
                                           strategies, schema.GetVersion()));
  if (!node["primary_keys"]) {
    LOG(ERROR) << "Expect field primary_keys for " << label_name;
    return Status(StatusCode::INVALID_SCHEMA,
                  "Expect field primary_keys for " + label_name);
  }
  auto primary_key_node = node["primary_keys"];
  if (!primary_key_node.IsSequence()) {
    LOG(ERROR) << "[Primary_keys] should be sequence";
    return Status(StatusCode::INVALID_SCHEMA,
                  "[Primary_keys] should be sequence");
  }
  // remove primary key from properties.

  std::vector<int> primary_key_inds(primary_key_node.size(), -1);
  std::vector<std::tuple<PropertyType, std::string, size_t>> primary_keys;
  for (size_t i = 0; i < primary_key_node.size(); ++i) {
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
      return Status(
          StatusCode::INVALID_SCHEMA,
          "Primary key " + primary_key_name + " is not found in properties");
    }
    if (property_types[primary_key_inds[i]] != PropertyType::kInt64 &&
        property_types[primary_key_inds[i]] != PropertyType::kStringView &&
        property_types[primary_key_inds[i]] != PropertyType::kUInt64 &&
        property_types[primary_key_inds[i]] != PropertyType::kInt32 &&
        property_types[primary_key_inds[i]] != PropertyType::kUInt32 &&
        !property_types[primary_key_inds[i]].IsVarchar()) {
      LOG(ERROR) << "Primary key " << primary_key_name
                 << " should be int64/int32/uint64/uint32 or string/varchar";
      return Status(StatusCode::INVALID_SCHEMA,
                    "Primary key " + primary_key_name +
                        " should be int64/int32/uint64/"
                        "uint32 or string/varchar");
    }
    primary_keys.emplace_back(property_types[primary_key_inds[i]],
                              property_names[primary_key_inds[i]],
                              primary_key_inds[i]);
    // remove primary key from properties.
    property_names.erase(property_names.begin() + primary_key_inds[i]);
    property_types.erase(property_types.begin() + primary_key_inds[i]);
    strategies.erase(strategies.begin() + primary_key_inds[i]);
  }

  schema.add_vertex_label(label_name, property_types, property_names,
                          primary_keys, strategies, max_num, description);
  // check the type_id equals to storage's label_id
  int32_t type_id;
  if (!get_scalar(node, "type_id", type_id)) {
    LOG(WARNING) << "type_id is not set properly for type: " << label_name
                 << ", try to use incremental id";
    type_id = schema.vertex_label_num() - 1;
  }
  auto label_id = schema.get_vertex_label_id(label_name);
  if (label_id != type_id) {
    LOG(ERROR) << "type_id is not equal to label_id for type: " << label_name;
    return Status(StatusCode::INVALID_SCHEMA,
                  "type_id is not equal to label_id for type: " + label_name);
  }
  return Status::OK();
}

static Status parse_vertices_schema(YAML::Node node, Schema& schema) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "vertex is not set properly";
    return Status(StatusCode::INVALID_SCHEMA, "vertex is not set properly");
  }
  int num = node.size();
  for (int i = 0; i < num; ++i) {
    RETURN_IF_NOT_OK(parse_vertex_schema(node[i], schema));
  }
  return Status::OK();
}

static Status parse_edge_schema(YAML::Node node, Schema& schema) {
  std::string edge_label_name;
  if (!node["type_name"]) {
    LOG(ERROR) << "edge type_name is not set properly";
    return Status(StatusCode::INVALID_SCHEMA,
                  "edge type_name is not set properly");
  }
  edge_label_name = node["type_name"].as<std::string>();

  std::vector<PropertyType> property_types;
  std::vector<std::string> prop_names;
  std::string description;  // default is empty string
  RETURN_IF_NOT_OK(parse_edge_properties(node["properties"], edge_label_name,
                                         property_types, prop_names,
                                         schema.GetVersion()));

  if (node["description"]) {
    description = node["description"].as<std::string>();
  }
  if (node["nullable"]) {
    LOG(ERROR) << "nullable is not supported yet";
    return Status(StatusCode::UNIMPLEMENTED, "nullable is not supported yet");
  }

  if (node["default_value"]) {
    LOG(ERROR) << "default_value is not supported yet";
    return Status(StatusCode::UNIMPLEMENTED,
                  "default_value is not supported yet");
  }

  EdgeStrategy default_ie = EdgeStrategy::kMultiple;
  EdgeStrategy default_oe = EdgeStrategy::kMultiple;
  bool default_sort_on_compaction = false;

  // get vertex type pair relation
  auto vertex_type_pair_node = node["vertex_type_pair_relations"];
  // vertex_type_pair_node can be a list or a map
  if (!vertex_type_pair_node) {
    LOG(ERROR) << "edge [vertex_type_pair_relations] is not set";
    return Status(StatusCode::INVALID_SCHEMA,
                  "edge [vertex_type_pair_relations] is not set");
  }
  if (!vertex_type_pair_node.IsSequence()) {
    LOG(ERROR) << "edge [vertex_type_pair_relations] should be a sequence";
    return Status(StatusCode::INVALID_SCHEMA,
                  "edge [vertex_type_pair_relations] should be a sequence");
  }
  for (size_t i = 0; i < vertex_type_pair_node.size(); ++i) {
    std::string src_label_name, dst_label_name;
    auto cur_node = vertex_type_pair_node[i];
    EdgeStrategy cur_ie = default_ie;
    EdgeStrategy cur_oe = default_oe;
    bool cur_sort_on_compaction = default_sort_on_compaction;
    if (!get_scalar(cur_node, "source_vertex", src_label_name)) {
      LOG(ERROR) << "Expect field source_vertex for edge [" << edge_label_name
                 << "] in vertex_type_pair_relations";
      return Status(StatusCode::INVALID_SCHEMA,
                    "Expect field source_vertex for edge [" + edge_label_name +
                        "] in vertex_type_pair_relations");
    }
    if (!get_scalar(cur_node, "destination_vertex", dst_label_name)) {
      LOG(ERROR) << "Expect field destination_vertex for edge ["
                 << edge_label_name << "] in vertex_type_pair_relations";
      return Status(StatusCode::INVALID_SCHEMA,
                    "Expect field destination_vertex for edge [" +
                        edge_label_name + "] in vertex_type_pair_relations");
    }
    // check whether edge triplet exists in current schema
    if (schema.has_edge_label(src_label_name, dst_label_name,
                              edge_label_name)) {
      LOG(ERROR) << "Edge [" << edge_label_name << "] from [" << src_label_name
                 << "] to [" << dst_label_name << "] already exists";
      return Status(StatusCode::INVALID_SCHEMA,
                    "Edge [" + edge_label_name + "] from [" + src_label_name +
                        "] to [" + dst_label_name + "] already exists");
    }

    std::string relation_str;
    if (get_scalar(cur_node, "relation", relation_str)) {
      RelationToEdgeStrategy(relation_str, cur_ie, cur_oe);
    } else {
      LOG(WARNING) << "relation not defined, using default ie strategy: "
                   << cur_ie << ", oe strategy: " << cur_oe;
    }
    // check if x_csr_params presents
    bool oe_mutable = true, ie_mutable = true;
    if (cur_node["x_csr_params"]) {
      auto csr_node = cur_node["x_csr_params"];
      if (csr_node["edge_storage_strategy"]) {
        std::string edge_storage_strategy_str;
        if (get_scalar(csr_node, "edge_storage_strategy",
                       edge_storage_strategy_str)) {
          if (edge_storage_strategy_str == "ONLY_IN") {
            cur_oe = EdgeStrategy::kNone;
            VLOG(10) << "Store only in edges for edge: " << src_label_name
                     << "-[" << edge_label_name << "]->" << dst_label_name;
          } else if (edge_storage_strategy_str == "ONLY_OUT") {
            cur_ie = EdgeStrategy::kNone;
            VLOG(10) << "Store only out edges for edge: " << src_label_name
                     << "-[" << edge_label_name << "]->" << dst_label_name;
          } else if (edge_storage_strategy_str == "BOTH_OUT_IN" ||
                     edge_storage_strategy_str == "BOTH_IN_OUT") {
            VLOG(10) << "Store both in and out edges for edge: "
                     << src_label_name << "-[" << edge_label_name << "]->"
                     << dst_label_name;
          } else {
            LOG(ERROR) << "edge_storage_strategy is not set properly for edge: "
                       << src_label_name << "-[" << edge_label_name << "]->"
                       << dst_label_name;
            return Status(
                StatusCode::INVALID_SCHEMA,
                "edge_storage_strategy is not set properly for edge: " +
                    src_label_name + "-[" + edge_label_name + "]->" +
                    dst_label_name);
          }
        }
      }
      // try to parse sort on compaction
      if (csr_node["sort_on_compaction"]) {
        std::string sort_on_compaction_str;
        if (get_scalar(csr_node, "sort_on_compaction",
                       sort_on_compaction_str)) {
          if (sort_on_compaction_str == "true" ||
              sort_on_compaction_str == "TRUE") {
            VLOG(10) << "Sort on compaction for edge: " << src_label_name
                     << "-[" << edge_label_name << "]->" << dst_label_name;
            cur_sort_on_compaction = true;
          } else if (sort_on_compaction_str == "false" ||
                     sort_on_compaction_str == "FALSE") {
            VLOG(10) << "Do not sort on compaction for edge: " << src_label_name
                     << "-[" << edge_label_name << "]->" << dst_label_name;
            cur_sort_on_compaction = false;
          } else {
            LOG(ERROR) << "sort_on_compaction is not set properly for edge: "
                       << src_label_name << "-[" << edge_label_name << "]->"
                       << dst_label_name << "expect TRUE/FALSE";
            return Status(StatusCode::INVALID_SCHEMA,
                          "sort_on_compaction is not set properly for edge: " +
                              src_label_name + "-[" + edge_label_name + "]->" +
                              dst_label_name + "expect TRUE/FALSE");
          }
        }
      } else {
        VLOG(10) << "Do not sort on compaction for edge: " << src_label_name
                 << "-[" << edge_label_name << "]->" << dst_label_name;
      }

      if (csr_node["oe_mutability"]) {
        std::string mutability_str;
        if (get_scalar(csr_node, "oe_mutability", mutability_str)) {
          // mutability_str to upper_case
          std::transform(mutability_str.begin(), mutability_str.end(),
                         mutability_str.begin(), ::toupper);
          if (mutability_str == "IMMUTABLE") {
            oe_mutable = false;
          } else if (mutability_str == "MUTABLE") {
            oe_mutable = true;
          } else {
            LOG(ERROR) << "oe_mutability is not set properly for edge: "
                       << src_label_name << "-[" << edge_label_name << "]->"
                       << dst_label_name
                       << ", expect IMMUTABLE/MUTABLE, got:" << mutability_str;
            return Status(StatusCode::INVALID_SCHEMA,
                          "oe_mutability is not set properly for edge: " +
                              src_label_name + "-[" + edge_label_name + "]->" +
                              dst_label_name + ", expect IMMUTABLE/MUTABLE");
          }
        }
      }
      if (csr_node["ie_mutability"]) {
        std::string mutability_str;
        if (get_scalar(csr_node, "ie_mutability", mutability_str)) {
          // mutability_str to upper_case
          std::transform(mutability_str.begin(), mutability_str.end(),
                         mutability_str.begin(), ::toupper);
          if (mutability_str == "IMMUTABLE") {
            ie_mutable = false;
          } else if (mutability_str == "MUTABLE") {
            ie_mutable = true;
          } else {
            LOG(ERROR) << "ie_mutability is not set properly for edge: "
                       << src_label_name << "-[" << edge_label_name << "]->"
                       << dst_label_name
                       << ", expect IMMUTABLE/MUTABLE, got:" << mutability_str;
            return Status(StatusCode::INVALID_SCHEMA,
                          "ie_mutability is not set properly for edge: " +
                              src_label_name + "-[" + edge_label_name + "]->" +
                              dst_label_name + ", expect IMMUTABLE/MUTABLE");
          }
        }
      }
    }

    VLOG(10) << "edge " << edge_label_name << " from " << src_label_name
             << " to " << dst_label_name << " with " << property_types.size()
             << " properties";
    schema.add_edge_label(src_label_name, dst_label_name, edge_label_name,
                          property_types, prop_names, cur_oe, cur_ie,
                          oe_mutable, ie_mutable, cur_sort_on_compaction,
                          description);
  }

  // check the type_id equals to storage's label_id
  int32_t type_id;
  if (!get_scalar(node, "type_id", type_id)) {
    LOG(WARNING) << "type_id is not set properly for type: " << edge_label_name
                 << ", try to use incremental id";
    type_id = schema.edge_label_num() - 1;
  }
  auto label_id = schema.get_edge_label_id(edge_label_name);
  if (label_id != type_id) {
    LOG(ERROR) << "type_id is not equal to label_id for type: "
               << edge_label_name;
    return Status(
        StatusCode::INVALID_SCHEMA,
        "type_id is not equal to label_id for type: " + edge_label_name);
  }
  return Status::OK();
}

static Status parse_edges_schema(YAML::Node node, Schema& schema) {
  if (node.IsNull()) {
    LOG(INFO) << "No edge is set";
    return Status::OK();
  }
  if (!node.IsSequence()) {
    LOG(ERROR) << "edge is not set properly";
    return Status(StatusCode::INVALID_SCHEMA, "edge is not set properly");
  }
  int num = node.size();
  VLOG(10) << "Try to parse " << num << "edge configuration";
  for (int i = 0; i < num; ++i) {
    RETURN_IF_NOT_OK(parse_edge_schema(node[i], schema));
  }
  return Status::OK();
}

static Status parse_stored_procedures_v00(
    const YAML::Node& stored_procedure_node, const std::string& parent_dir,
    Schema& schema) {
  std::string directory = "plugins";  // default plugin directory
  if (stored_procedure_node["directory"]) {
    directory = stored_procedure_node["directory"].as<std::string>();
  }
  // check is directory
  LOG(INFO) << "Parse directory: " << directory;
  if (!std::filesystem::exists(directory)) {
    LOG(ERROR) << "plugin directory - " << directory
               << " not found, try with parent dir:" << parent_dir;
    directory = parent_dir + "/" + directory;
    if (!std::filesystem::exists(directory)) {
      LOG(ERROR) << "plugin directory - " << directory << " not found...";
    }
  }
  schema.SetPluginDir(directory);
  std::vector<std::pair<std::string, std::string>> plugin_name_or_paths;
  {
    std::vector<std::string> plugin_names;
    if (!get_sequence(stored_procedure_node, "enable_lists", plugin_names)) {
      LOG(ERROR) << "stored_procedures is not set properly";
    }
    size_t plugin_cnt = 0;
    for (auto& plugin_name_or_path : plugin_names) {
      // The plugins names specified in enable_lists can be either the name of
      // the plugin or the path to the plugin.
      auto real_path = directory + "/" + plugin_name_or_path;
      if (std::filesystem::exists(real_path)) {
        plugin_name_or_paths.emplace_back(
            std::string("plugin_") + std::to_string(plugin_cnt++), real_path);
      } else if (std::filesystem::exists(plugin_name_or_path)) {
        plugin_name_or_paths.emplace_back(
            std::string("plugin_") + std::to_string(plugin_cnt++),
            plugin_name_or_path);
      } else {
        LOG(WARNING) << "plugin " << plugin_name_or_path << " not found";
      }
    }
  }

  // plugin_name_or_path contains the plugin name or path.
  // for path, we just use it as the plugin name, and emplace into the map,
  // for name, we try to find the plugin in the directory
  if (!schema.EmplacePlugins(plugin_name_or_paths)) {
    LOG(ERROR) << "Fail to emplace all plugins";
    return Status(StatusCode::INVALID_SCHEMA, "Fail to emplace all plugins");
  }
  return Status::OK();
}

static Status parse_stored_procedures_v01(
    const YAML::Node& stored_procedure_node, Schema& schema) {
  if (!stored_procedure_node.IsSequence()) {
    LOG(ERROR) << "stored_procedures is not set properly";
    return Status(StatusCode::INVALID_SCHEMA,
                  "stored_procedures is not set properly");
  }
  std::vector<std::pair<std::string, std::string>> plugin_name_and_path;
  for (auto& cur_node : stored_procedure_node) {
    if (cur_node["name"] && cur_node["library"]) {
      VLOG(10) << "Parse stored procedure: "
               << cur_node["name"].as<std::string>()
               << " with library: " << cur_node["library"].as<std::string>();
      plugin_name_and_path.push_back(
          std::make_pair(cur_node["name"].as<std::string>(),
                         cur_node["library"].as<std::string>()));
    } else {
      LOG(WARNING) << "Library or name set properly for stored procedure";
      return Status(StatusCode::INVALID_SCHEMA,
                    "Library or name set properly for stored procedure");
    }
  }
  // emplace all the plugins
  if (!schema.EmplacePlugins(plugin_name_and_path)) {
    LOG(ERROR) << "Fail to emplace all plugins";
    return Status(StatusCode::INVALID_SCHEMA, "Fail to emplace all plugins");
  }
  return Status::OK();
}

static Status parse_stored_procedures(const YAML::Node& stored_procedure_node,
                                      const std::string& parent_dir,
                                      Schema& schema) {
  auto version = schema.GetVersion();
  if (version == "v0.0") {
    return parse_stored_procedures_v00(stored_procedure_node, parent_dir,
                                       schema);
  } else if (version == "v0.1") {
    return parse_stored_procedures_v01(stored_procedure_node, schema);
  } else {
    LOG(ERROR) << "Unrecognized version: " << version;
    return Status(
        StatusCode::INVALID_SCHEMA,
        "Unsupported version when parsing stored procedures: " + version);
  }
}

static Status parse_schema_from_yaml_node(const YAML::Node& graph_node,
                                          Schema& schema,
                                          const std::string& parent_dir = "") {
  if (!graph_node || !graph_node.IsMap()) {
    LOG(ERROR) << "graph schema is not set properly";
    return Status(StatusCode::INVALID_SCHEMA,
                  "graph schema is not set properly");
  }
  if (!expect_config(graph_node, "store_type", std::string("mutable_csr"))) {
    LOG(WARNING) << "store_type is not set properly, use default value: "
                 << "mutable_csr";
  }

  if (graph_node["description"]) {
    schema.SetDescription(graph_node["description"].as<std::string>());
  }

  // check whether a version field is specified for the schema, if
  // specified, we will use it to check the compatibility of the schema.
  // If not specified, we will use the default version.
  if (graph_node["version"]) {
    auto version = graph_node["version"].as<std::string>();
    const auto& supported_versions = Schema::GetCompatibleVersions();
    if (std::find(supported_versions.begin(), supported_versions.end(),
                  version) == supported_versions.end()) {
      LOG(ERROR) << "Unsupported schema version: " << version;
      return Status(StatusCode::INVALID_SCHEMA,
                    "Unsupported schema version: " + version);
    }
    schema.SetVersion(version);
  } else {
    schema.SetVersion(Schema::DEFAULT_SCHEMA_VERSION);
  }
  VLOG(10) << "Parse schema version: " << schema.GetVersion();

  auto schema_node = graph_node["schema"];

  if (!graph_node["schema"]) {
    LOG(ERROR) << "expect schema field, but not found";
    return Status(StatusCode::INVALID_SCHEMA,
                  "expect schema field, but not found");
  }

  RETURN_IF_NOT_OK(parse_vertices_schema(schema_node["vertex_types"], schema));

  if (schema_node["edge_types"]) {
    RETURN_IF_NOT_OK(parse_edges_schema(schema_node["edge_types"], schema));
  }
  LOG(INFO) << "Parse stored_procedures";

  if (graph_node["stored_procedures"]) {
    RETURN_IF_NOT_OK(parse_stored_procedures(graph_node["stored_procedures"],
                                             parent_dir, schema));
  }
  return Status::OK();
}

static Status parse_schema_config_file(const std::string& path,
                                       Schema& schema) {
  YAML::Node graph_node = YAML::LoadFile(path);
  // get the directory of path
  auto parent_dir = std::filesystem::path(path).parent_path().string();
  return parse_schema_from_yaml_node(graph_node, schema, parent_dir);
}

}  // namespace config_parsing

const std::unordered_map<std::string, std::pair<std::string, uint8_t>>&
Schema::GetPlugins() const {
  return plugin_name_to_path_and_id_;
}

// For the input procedures, try to load each of them.
// Only keep the procedures that are successfully loaded.
bool Schema::EmplacePlugins(
    const std::vector<std::pair<std::string, std::string>>&
        plugin_name_and_paths) {
  std::vector<std::string> all_procedure_yamls;
  if (!plugin_dir_.empty()) {
    all_procedure_yamls = get_yaml_files(plugin_dir_);
  }

  std::vector<std::string> all_procedure_names;

  uint8_t cur_plugin_id = RESERVED_PLUGIN_NUM;
  std::unordered_set<std::string> plugin_names;
  for (auto& name_path : plugin_name_and_paths) {
    if (cur_plugin_id > MAX_PLUGIN_ID) {
      LOG(ERROR) << "Too many plugins, max plugin id is " << MAX_PLUGIN_ID;
      return false;
    }
    if (Schema::IsBuiltinPlugin(name_path.first)) {
      LOG(WARNING) << "Plugin name " << name_path.first
                   << " is a built-in plugin, skipped";
      continue;
    }
    if (name_path.second.empty()) {
      // if the path is empty, try to find from plugin_dir.
      plugin_names.insert(name_path.first);
      continue;
    }
    if (std::filesystem::exists(name_path.second)) {
      plugin_name_to_path_and_id_.emplace(
          name_path.first, std::make_pair(name_path.second, cur_plugin_id++));
    } else {
      auto real_file = plugin_dir_ + "/" + name_path.second;
      if (!std::filesystem::exists(real_file)) {
        LOG(ERROR) << "plugin - " << real_file
                   << " file not found, try to "
                      "find the plugin in the directory...";
        // it seems that f is not the filename, but the plugin name, try to
        // find the plugin in the directory
        LOG(ERROR) << "plugin - " << name_path.first << " not found...";
      } else {
        plugin_name_to_path_and_id_.emplace(
            name_path.first, std::make_pair(real_file, cur_plugin_id++));
      }
    }
  }
  // if there exists any plugins specified by name, add them
  // Iterator over the map, and add the plugin path and name to the vector
  for (auto cur_yaml : all_procedure_yamls) {
    if (cur_plugin_id > MAX_PLUGIN_ID) {
      LOG(ERROR) << "Too many plugins, max plugin id is " << MAX_PLUGIN_ID;
      return false;
    }
    YAML::Node root;
    try {
      root = YAML::LoadFile(cur_yaml);
    } catch (std::exception& e) {
      LOG(ERROR) << "Exception when loading from yaml: " << cur_yaml << ":"
                 << e.what();
      continue;
    }
    if (root["name"] && root["library"]) {
      std::string name = root["name"].as<std::string>();
      if (Schema::IsBuiltinPlugin(name)) {
        LOG(WARNING) << "Plugin name " << name
                     << " is a built-in plugin, skipped";
        continue;
      }
      std::string path = root["library"].as<std::string>();
      if (plugin_names.find(name) != plugin_names.end()) {
        if (plugin_name_to_path_and_id_.find(name) !=
            plugin_name_to_path_and_id_.end()) {
          LOG(ERROR) << "Plugin " << name << " already exists, skip";
        } else if (!std::filesystem::exists(path)) {
          path = plugin_dir_ + "/" + path;
          if (!std::filesystem::exists(path)) {
            LOG(ERROR) << "plugin - " << name << "not found from " << path;
          } else {
            plugin_name_to_path_and_id_.emplace(
                name, std::make_pair(path, cur_plugin_id++));
          }
        } else {
          plugin_name_to_path_and_id_.emplace(
              name, std::make_pair(path, cur_plugin_id++));
        }
      } else {
        VLOG(10)
            << "Skip load plugin " << name << ", found in " << cur_yaml
            << ", but not specified in the enable_lists in graph schema yaml";
      }
    } else {
      LOG(ERROR) << "Invalid yaml file: " << cur_yaml
                 << ", name or library not found.";
    }
  }
  // Emplace the built-in plugins
  plugin_name_to_path_and_id_.emplace(
      Schema::BUILTIN_COUNT_VERTICES_PLUGIN_NAME,
      std::make_pair("", Schema::BUILTIN_COUNT_VERTICES_PLUGIN_ID));
  plugin_name_to_path_and_id_.emplace(
      Schema::BUILTIN_PAGERANK_PLUGIN_NAME,
      std::make_pair("", Schema::BUILTIN_PAGERANK_PLUGIN_ID));
  plugin_name_to_path_and_id_.emplace(
      Schema::BUILTIN_K_DEGREE_NEIGHBORS_PLUGIN_NAME,
      std::make_pair("", Schema::BUILTIN_K_DEGREE_NEIGHBORS_PLUGIN_ID));
  plugin_name_to_path_and_id_.emplace(
      Schema::BUILTIN_TVSP_PLUGIN_NAME,
      std::make_pair("", Schema::BUILTIN_TVSP_PLUGIN_ID));

  LOG(INFO) << "Load " << plugin_name_to_path_and_id_.size() << " plugins";
  return true;
}

void Schema::SetPluginDir(const std::string& dir) { plugin_dir_ = dir; }

void Schema::RemovePlugin(const std::string& name) {
  plugin_name_to_path_and_id_.erase(name);
}

std::string Schema::GetPluginDir() const { return plugin_dir_; }

std::string Schema::GetDescription() const { return description_; }

void Schema::SetDescription(const std::string& description) {
  description_ = description;
}

void Schema::SetVersion(const std::string& version) { version_ = version; }
std::string Schema::GetVersion() const { return version_; }

bool Schema::has_multi_props_edge() const { return has_multi_props_edge_; }

// check whether prop in vprop_names, or is the primary key
bool Schema::vertex_has_property(const std::string& label,
                                 const std::string& prop) const {
  auto v_label_id = get_vertex_label_id(label);
  THROW_EXCEPTION_IF(v_label_id >= vprop_names_.size(),
                     "vertex label id out of range of vprop_names_");
  auto& v_prop_names = vprop_names_[v_label_id];
  return std::find(v_prop_names.begin(), v_prop_names.end(), prop) !=
             v_prop_names.end() ||
         vertex_has_primary_key(label, prop);
}

bool Schema::vertex_has_primary_key(const std::string& label,
                                    const std::string& prop) const {
  auto v_label_id = get_vertex_label_id(label);
  THROW_EXCEPTION_IF(v_label_id >= v_primary_keys_.size(),
                     "vertex label id out of range of v_primary_keys_");
  auto& keys = v_primary_keys_[v_label_id];
  for (size_t i = 0; i < keys.size(); ++i) {
    if (std::get<1>(keys[i]) == prop) {
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
  if (!has_vertex_label(src_label) || !has_vertex_label(dst_label)) {
    LOG(ERROR) << "src_label or dst_label not found:" << src_label << ", "
               << dst_label;
    return false;
  }
  auto src_label_id = get_vertex_label_id(src_label);
  auto dst_label_id = get_vertex_label_id(dst_label);
  if (!elabel_indexer_.get_index(label, edge_label_id)) {
    return false;
  }
  return has_edge_label(src_label_id, dst_label_id, edge_label_id);
}

bool Schema::has_edge_label(label_t src_label, label_t dst_label,
                            label_t edge_label) const {
  uint32_t e_label_id = generate_edge_label(src_label, dst_label, edge_label);
  return eprop_names_.find(e_label_id) != eprop_names_.end();
}

Result<Schema> Schema::LoadFromYaml(const std::string& schema_config) {
  Schema schema;
  if (!schema_config.empty() && std::filesystem::exists(schema_config)) {
    auto status =
        config_parsing::parse_schema_config_file(schema_config, schema);
    if (status.ok()) {
      return schema;
    } else {
      return Result<Schema>(status);
    }
  }
  return Result<Schema>(
      Status(StatusCode::INVALID_SCHEMA, "Schema config file not found"));
}

Result<Schema> Schema::LoadFromYamlNode(const YAML::Node& schema_yaml_node) {
  Schema schema;
  auto status =
      config_parsing::parse_schema_from_yaml_node(schema_yaml_node, schema);
  if (status.ok()) {
    return schema;
  } else {
    return Result<Schema>(status);
  }
}

const std::vector<std::string>& Schema::GetCompatibleVersions() {
  return COMPATIBLE_VERSIONS;
}

const std::vector<std::string> Schema::COMPATIBLE_VERSIONS = {
    "v0.0",  // v0.0 is the version before schema unified, and is the
             // default version, if no version is specified
    "v0.1"   // v0.1 is the version after schema unified
};

}  // namespace gs

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
}

void Schema::add_vertex_label(
    const std::string& label, const std::vector<PropertyType>& property_types,
    const std::vector<std::string>& property_names,
    const std::vector<std::tuple<PropertyType, std::string, size_t>>&
        primary_key,
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
                            EdgeStrategy oe, EdgeStrategy ie, bool oe_mutable,
                            bool ie_mutable, bool sort_on_compaction) {
  label_t src_label_id = vertex_label_to_index(src_label);
  label_t dst_label_id = vertex_label_to_index(dst_label);
  label_t edge_label_id = edge_label_to_index(edge_label);

  uint32_t label_id =
      generate_edge_label(src_label_id, dst_label_id, edge_label_id);
  eproperties_[label_id] = properties;
  oe_strategy_[label_id] = oe;
  ie_strategy_[label_id] = ie;
  oe_mutability_[label_id] = oe_mutable;
  ie_mutability_[label_id] = ie_mutable;
  eprop_names_[label_id] = prop_names;
  sort_on_compactions_[label_id] = sort_on_compaction;
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

const std::vector<std::string>& Schema::get_vertex_property_names(
    label_t label) const {
  CHECK(label < vprop_names_.size());
  return vprop_names_[label];
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

bool Schema::exist(label_t src_label, label_t dst_label,
                   label_t edge_label) const {
  uint32_t index = generate_edge_label(src_label, dst_label, edge_label);
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

const std::vector<PropertyType>& Schema::get_edge_properties(
    label_t src_label, label_t dst_label, label_t label) const {
  CHECK(src_label < vlabel_indexer_.size());
  CHECK(dst_label < vlabel_indexer_.size());
  CHECK(label < elabel_indexer_.size());
  uint32_t index = generate_edge_label(src_label, dst_label, label);
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

const std::vector<std::string>& Schema::get_edge_property_names(
    const label_t& src_label, const label_t& dst_label,
    const label_t& label) const {
  CHECK(src_label < vlabel_indexer_.size());
  CHECK(dst_label < vlabel_indexer_.size());
  CHECK(label < elabel_indexer_.size());
  uint32_t index = generate_edge_label(src_label, dst_label, label);
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

bool Schema::outgoing_edge_mutable(const std::string& src_label,
                                   const std::string& dst_label,
                                   const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return oe_mutability_.at(index);
}

bool Schema::incoming_edge_mutable(const std::string& src_label,
                                   const std::string& dst_label,
                                   const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return ie_mutability_.at(index);
}

bool Schema::get_sort_on_compaction(const std::string& src_label,
                                    const std::string& dst_label,
                                    const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  CHECK(sort_on_compactions_.find(index) != sort_on_compactions_.end());
  return sort_on_compactions_.at(index);
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

const std::vector<std::tuple<PropertyType, std::string, size_t>>&
Schema::get_vertex_primary_key(label_t index) const {
  CHECK(v_primary_keys_.size() > index);
  return v_primary_keys_.at(index);
}

// Note that plugin_dir_ and plugin_name_to_path_and_id_ are not serialized.
void Schema::Serialize(std::unique_ptr<grape::LocalIOAdaptor>& writer) const {
  vlabel_indexer_.Serialize(writer);
  elabel_indexer_.Serialize(writer);
  grape::InArchive arc;
  arc << v_primary_keys_ << vproperties_ << vprop_names_ << vprop_storage_
      << eproperties_ << eprop_names_ << ie_strategy_ << oe_strategy_
      << ie_mutability_ << oe_mutability_ << sort_on_compactions_ << max_vnum_;
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
      ie_mutability_ >> oe_mutability_ >> sort_on_compactions_ >> max_vnum_;
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
  } else if (str == "uint32" || str == DT_UNSIGNED_INT32) {
    return PropertyType::kUInt32;
  } else if (str == "bool" || str == DT_BOOL) {
    return PropertyType::kBool;
  } else if (str == "Date" || str == DT_DATE) {
    return PropertyType::kDate;
  } else if (str == "Day" || str == DT_DAY) {
    return PropertyType::kDay;
  } else if (str == "String" || str == DT_STRING) {
    // DT_STRING is a alias for VARCHAR(STRING_DEFAULT_MAX_LENGTH);
    return PropertyType::Varchar(PropertyType::STRING_DEFAULT_MAX_LENGTH);
  } else if (str == DT_STRINGMAP) {
    return PropertyType::kStringMap;
  } else if (str == "Empty") {
    return PropertyType::kEmpty;
  } else if (str == "int64" || str == DT_SIGNED_INT64) {
    return PropertyType::kInt64;
  } else if (str == "uint64" || str == DT_UNSIGNED_INT64) {
    return PropertyType::kUInt64;
  } else if (str == "float" || str == DT_FLOAT) {
    return PropertyType::kFloat;
  } else if (str == "double" || str == DT_DOUBLE) {
    return PropertyType::kDouble;
  } else {
    return PropertyType::kEmpty;
  }
}
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
  std::string prop_type_str{};
  if (node["primitive_type"]) {
    if (!get_scalar(node, "primitive_type", prop_type_str)) {
      return false;
    }
  } else if (node["varchar"]) {
    auto varchar_node = node["varchar"];
    int length{};
    if (!varchar_node["max_length"] ||
        !get_scalar(varchar_node, "max_length", length)) {
      return false;
    }
    type = PropertyType::Varchar(length);
    return true;
  } else if (node["date"]) {
    auto format = node["date"].as<std::string>();
    prop_type_str = DT_DATE;
  } else if (node["day"]) {
    auto format = node["day"].as<std::string>();
    prop_type_str = DT_DAY;
  } else {
    return false;
  }
  type = StringToPropertyType(prop_type_str);
  return true;
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
    std::string strategy_str, prop_name_str;
    if (!get_scalar(node[i], "property_name", prop_name_str)) {
      LOG(ERROR) << "Name of vertex-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return false;
    }
    if (!node[i]["property_type"]) {
      LOG(ERROR) << "type of vertex-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return false;
    }
    auto prop_type_node = node[i]["property_type"];
    PropertyType prop_type;
    if (!parse_property_type(prop_type_node, prop_type)) {
      LOG(ERROR) << "type of vertex-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return false;
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
    std::string strategy_str, prop_name_str;
    if (!node[i]["property_type"]) {
      LOG(ERROR) << "type of edge-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return false;
    }
    auto prop_type_node = node[i]["property_type"];
    PropertyType prop_type;
    if (!parse_property_type(prop_type_node, prop_type)) {
      LOG(ERROR) << "type of edge-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return false;
    }
    if (!get_scalar(node[i], "property_name", prop_name_str)) {
      LOG(ERROR) << "name of edge-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return false;
    }

    types.push_back(prop_type);
    names.push_back(prop_name_str);
  }

  return true;
}

static bool parse_vertex_schema(YAML::Node node, Schema& schema) {
  std::string label_name;
  if (!get_scalar(node, "type_name", label_name)) {
    return false;
  }
  // Cannot add two vertex label with same name
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
      return false;
    }
    if (property_types[primary_key_inds[i]] != PropertyType::kInt64 &&
        property_types[primary_key_inds[i]] != PropertyType::kString &&
        property_types[primary_key_inds[i]] != PropertyType::kUInt64 &&
        property_types[primary_key_inds[i]] != PropertyType::kInt32 &&
        property_types[primary_key_inds[i]] != PropertyType::kUInt32) {
      LOG(ERROR) << "Primary key " << primary_key_name
                 << " should be int64 or string";
      return false;
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
                          primary_keys, strategies, max_num);
  // check the type_id equals to storage's label_id
  int32_t type_id;
  if (!get_scalar(node, "type_id", type_id)) {
    LOG(ERROR) << "type_id is not set properly for type: " << label_name;
    return false;
  }
  auto label_id = schema.get_vertex_label_id(label_name);
  if (label_id != type_id) {
    LOG(ERROR) << "type_id is not equal to label_id for type: " << label_name;
    return false;
  }
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
  std::string edge_label_name;
  if (!node["type_name"]) {
    LOG(ERROR) << "edge type_name is not set properly";
    return false;
  }
  edge_label_name = node["type_name"].as<std::string>();

  std::vector<PropertyType> property_types;
  std::vector<std::string> prop_names;
  if (!parse_edge_properties(node["properties"], edge_label_name,
                             property_types, prop_names)) {
    return false;
  }
  EdgeStrategy default_ie = EdgeStrategy::kMultiple;
  EdgeStrategy default_oe = EdgeStrategy::kMultiple;
  bool default_sort_on_compaction = false;

  // get vertex type pair relation
  auto vertex_type_pair_node = node["vertex_type_pair_relations"];
  // vertex_type_pair_node can be a list or a map
  if (!vertex_type_pair_node) {
    LOG(ERROR) << "edge [vertex_type_pair_relations] is not set";
    return false;
  }
  if (!vertex_type_pair_node.IsSequence()) {
    LOG(ERROR) << "edge [vertex_type_pair_relations] should be a sequence";
    return false;
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
      return false;
    }
    if (!get_scalar(cur_node, "destination_vertex", dst_label_name)) {
      LOG(ERROR) << "Expect field destination_vertex for edge ["
                 << edge_label_name << "] in vertex_type_pair_relations";
      return false;
    }
    // check whether edge triplet exists in current schema
    if (schema.has_edge_label(src_label_name, dst_label_name,
                              edge_label_name)) {
      LOG(ERROR) << "Edge [" << edge_label_name << "] from [" << src_label_name
                 << "] to [" << dst_label_name << "] already exists";
      return false;
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
            return false;
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
            return false;
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
            return false;
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
            return false;
          }
        }
      }
    }

    VLOG(10) << "edge " << edge_label_name << " from " << src_label_name
             << " to " << dst_label_name << " with " << property_types.size()
             << " properties";
    schema.add_edge_label(src_label_name, dst_label_name, edge_label_name,
                          property_types, prop_names, cur_oe, cur_ie,
                          oe_mutable, ie_mutable, cur_sort_on_compaction);
  }

  // check the type_id equals to storage's label_id
  int32_t type_id;
  if (!get_scalar(node, "type_id", type_id)) {
    LOG(ERROR) << "type_id is not set properly for type: " << edge_label_name;
    return false;
  }
  auto label_id = schema.get_edge_label_id(edge_label_name);
  if (label_id != type_id) {
    LOG(ERROR) << "type_id is not equal to label_id for type: "
               << edge_label_name;
    return false;
  }
  return true;
}

static bool parse_edges_schema(YAML::Node node, Schema& schema) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "edge is not set properly";
    return false;
  }
  int num = node.size();
  VLOG(10) << "Try to parse " << num << "edge configuration";
  for (int i = 0; i < num; ++i) {
    if (!parse_edge_schema(node[i], schema)) {
      return false;
    }
  }
  return true;
}

static bool parse_schema_from_yaml_node(const YAML::Node& graph_node,
                                        Schema& schema,
                                        const std::string& parent_dir = "") {
  if (!graph_node || !graph_node.IsMap()) {
    LOG(ERROR) << "graph is not set properly";
    return false;
  }
  if (!expect_config(graph_node, "store_type", std::string("mutable_csr"))) {
    LOG(WARNING) << "store_type is not set properly, use default value: "
                 << "mutable_csr";
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
  LOG(INFO) << "Parse stored_procedures";

  if (graph_node["stored_procedures"]) {
    auto stored_procedure_node = graph_node["stored_procedures"];
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
        return true;
      }
    }
    schema.SetPluginDir(directory);
    std::vector<std::string> plugin_name_or_path;
    if (!get_sequence(stored_procedure_node, "enable_lists",
                      plugin_name_or_path)) {
      LOG(ERROR) << "stored_procedures is not set properly";
      return true;
    }

    // plugin_name_or_path contains the plugin name or path.
    // for path, we just use it as the plugin name, and emplace into the map,
    // for name, we try to find the plugin in the directory
    if (!schema.EmplacePlugins(plugin_name_or_path)) {
      LOG(ERROR) << "Fail to emplace all plugins";
    }
  }

  return true;
}

static bool parse_schema_config_file(const std::string& path, Schema& schema) {
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

bool Schema::EmplacePlugins(
    const std::vector<std::string>& plugin_paths_or_names) {
  std::vector<std::string> all_procedure_yamls;
  if (!plugin_dir_.empty()) {
    all_procedure_yamls = get_yaml_files(plugin_dir_);
  }

  std::vector<std::string> all_procedure_names;

  uint8_t cur_plugin_id = RESERVED_PLUGIN_NUM;
  std::unordered_set<std::string> plugin_names;
  for (auto& f : plugin_paths_or_names) {
    if (cur_plugin_id > MAX_PLUGIN_ID) {
      LOG(ERROR) << "Too many plugins, max plugin id is " << MAX_PLUGIN_ID;
      return false;
    }
    if (std::filesystem::exists(f)) {
      plugin_name_to_path_and_id_.emplace(f,
                                          std::make_pair(f, cur_plugin_id++));
    } else {
      auto real_file = plugin_dir_ + "/" + f;
      if (!std::filesystem::exists(real_file)) {
        LOG(ERROR) << "plugin - " << real_file
                   << " file not found, try to "
                      "find the plugin in the directory...";
        // it seems that f is not the filename, but the plugin name, try to
        // find the plugin in the directory
        VLOG(1) << "plugin - " << f << " found...";
        plugin_names.insert(f);
      } else {
        plugin_name_to_path_and_id_.emplace(
            real_file, std::make_pair(real_file, cur_plugin_id++));
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
  LOG(INFO) << "Load " << plugin_name_to_path_and_id_.size() << " plugins";
  return true;
}

void Schema::SetPluginDir(const std::string& dir) { plugin_dir_ = dir; }

std::string Schema::GetPluginDir() const { return plugin_dir_; }

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

Result<Schema> Schema::LoadFromYamlNode(const YAML::Node& schema_yaml_node) {
  Schema schema;
  if (!config_parsing::parse_schema_from_yaml_node(schema_yaml_node, schema)) {
    return Result<Schema>(
        Status(StatusCode::InvalidSchema, "Failed to parse schema"), schema);
  }
  return schema;
}

}  // namespace gs

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
    const std::tuple<PropertyType, std::string, int32_t>& primary_key,
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

int32_t Schema::get_vertex_primary_key_ind(label_t index) const {
  CHECK(v_primary_keys_.size() > index);
  return std::get<2>(v_primary_keys_.at(index));
}

PropertyType Schema::get_vertex_primary_key_type(label_t index) const {
  CHECK(v_primary_keys_.size() > index);
  return std::get<0>(v_primary_keys_.at(index));
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

template <typename T>
bool get_scalar(YAML::Node node, const std::string& key, T& value) {
  YAML::Node cur = node[key];
  if (cur && cur.IsScalar()) {
    value = cur.as<T>();
    return true;
  }
  return false;
}

template <typename T>
bool get_sequence(YAML::Node node, const std::string& key,
                  std::vector<T>& seq) {
  YAML::Node cur = node[key];
  if (cur && cur.IsSequence()) {
    int num = cur.size();
    seq.clear();
    for (int i = 0; i < num; ++i) {
      seq.push_back(cur[i].as<T>());
    }
    return true;
  }
  return false;
}

static bool expect_config(YAML::Node root, const std::string& key,
                          const std::string& value) {
  std::string got;
  if (!get_scalar(root, key, got)) {
    LOG(ERROR) << key << " not set properly...";
    return false;
  }
  if (got != value) {
    LOG(ERROR) << key << " - " << got << " is not supported...";
    return false;
  }
  return true;
}

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

static bool fetch_src_dst_column_mapping(YAML::Node node,
                                         const std::string& key,
                                         int32_t& column_id) {
  if (node[key]) {
    auto column_mappings = node[key];
    if (!column_mappings.IsSequence()) {
      LOG(ERROR) << "column_mappings is not set properly";
      return false;
    }
    if (column_mappings.size() > 1) {
      LOG(ERROR) << "Only only source vertex mapping is needed";
      return false;
    }
    auto column_mapping = column_mappings[0]["column"];
    if (!get_scalar(column_mapping, "index", column_id)) {
      LOG(ERROR) << "No index is set for source vertex mapping";
      return false;
    }
  } else {
    LOG(WARNING)
        << "source_vertex_mappings is not set, use default src_column = 0";
  }
  return true;
}

static bool parse_vertex_properties(YAML::Node node,
                                    const std::string& label_name,
                                    std::vector<PropertyType>& types,
                                    std::vector<std::string>& names,
                                    std::vector<StorageStrategy>& strategies) {
  if (!node || !node.IsSequence()) {
    LOG(ERROR) << "properties of vertex-" << label_name
               << " not set properly... ";
    return false;
  }

  int prop_num = node.size();
  if (prop_num == 0) {
    LOG(ERROR) << "properties of vertex-" << label_name
               << " not set properly... ";
    return false;
  }

  // if (!expect_config(node[0], "name", "_ID") ||
  //     !expect_config(node[0], "type", "int64")) {
  //   LOG(ERROR) << "the first property of vertex-" << label_name
  //              << " should be _ID with type int64";
  //   return false;
  // }

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
    LOG(INFO) << "no edge properties specified for edge-" << label_name;
    return true;
  }
  if (!node.IsSequence()) {
    LOG(ERROR) << "properties of vertex-" << label_name
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
    LOG(ERROR) << "primary key of vertex-" << label_name << "is not set ";
    return false;
  }
  auto primary_key_node = node["primary_keys"];
  if (!primary_key_node.IsSequence() || primary_key_node.size() != 1) {
    LOG(ERROR) << "Primary key should be sequence, and only one primary key is "
                  "supported";
    return false;
  }
  // remove primary key from properties.
  std::string primary_key_name = primary_key_node[0].as<std::string>();
  int primary_key_ind = -1;
  for (int i = 0; i < property_names.size(); ++i) {
    if (property_names[i] == primary_key_name) {
      primary_key_ind = i;
      break;
    }
  }
  if (primary_key_ind == -1) {
    LOG(ERROR) << "Primary key " << primary_key_name
               << " is not found in properties";
    return false;
  }

  if (property_types[primary_key_ind] != PropertyType::kInt64) {
    LOG(ERROR) << "Primary key " << primary_key_name << " should be int64";
    return false;
  }
  auto tuple =
      std::make_tuple(property_types[primary_key_ind],
                      property_names[primary_key_ind], primary_key_ind);
  // remove primary key from properties.
  property_names.erase(property_names.begin() + primary_key_ind);
  property_types.erase(property_types.begin() + primary_key_ind);
  std::string debug_str;
  {
    std::stringstream ss;
    for (auto i = 0; i < property_names.size(); ++i) {
      ss << property_names[i] << "(";
      ss << property_types[i] << "),";
    }
    debug_str = ss.str();
  }
  LOG(INFO) << "After erase, got properties " << debug_str;

  schema.add_vertex_label(label_name, property_types, property_names, tuple,
                          strategies, max_num);
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
    LOG(ERROR) << "source_vertex is not set properly";
    return false;
  }
  if (!get_scalar(vertex_type_pair_node, "destination_vertex",
                  dst_label_name)) {
    LOG(ERROR) << "destination_vertex is not set properly";
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

static bool access_file(std::string& file_path) {
  if (file_path.size() == 0) {
    return false;
  }
  if (file_path[0] == '/') {
    std::filesystem::path path(file_path);
    return std::filesystem::exists(path);
  }
  char* flex_data_dir = std::getenv("FLEX_DATA_DIR");
  if (flex_data_dir != NULL) {
    auto temp = std::string(flex_data_dir) + "/" + file_path;
    std::filesystem::path path(temp);
    if (std::filesystem::exists(path)) {
      file_path = temp;
      return true;
    }
  }
  file_path =
      std::filesystem::current_path().generic_string() + "/" + file_path;
  std::filesystem::path path(file_path);
  return std::filesystem::exists(path);
}

static bool parse_vertex_files(YAML::Node node,
                               const std::string& data_location,
                               std::vector<VertexLoadingMeta>& files) {
  std::string label_name;
  if (!get_scalar(node, "type_name", label_name)) {
    return false;
  }
  YAML::Node files_node = node["inputs"];

  if (node["column_mappings"]) {
    LOG(ERROR) << "column_mappings is not supported currently";
    return false;
  }
  if (files_node) {
    if (!files_node.IsSequence()) {
      LOG(ERROR) << "files is not set properly";
      return false;
    }
    int num = files_node.size();
    for (int i = 0; i < num; ++i) {
      std::string file_format;
      std::string file_path;
      if (!get_scalar(files_node[i], "format", file_format)) {
        return false;
      }
      if (file_format != "standard_csv") {
        LOG(ERROR) << "file_format is not set properly, currenly only support "
                      "standard_csv";
        return false;
      }
      if (!get_scalar(files_node[i], "path", file_path)) {
        LOG(ERROR) << "file_path is not set properly, should be path: xxx";
        return false;
      }
      if (!data_location.empty()) {
        file_path = data_location + "/" + file_path;
      }
      if (!access_file(file_path)) {
        LOG(ERROR) << "vertex file - " << file_path << " file not found...";
      }
      std::filesystem::path path(file_path);
      files.emplace_back(label_name, std::filesystem::canonical(path));
    }
    return true;
  } else {
    return true;
  }
}

static bool parse_vertices_files_schema(YAML::Node node,
                                        const std::string& data_location,
                                        std::vector<VertexLoadingMeta>& files) {
  if (!node.IsSequence()) {
    LOG(FATAL) << "vertex is not set properly";
    return false;
  }
  int num = node.size();
  for (int i = 0; i < num; ++i) {
    if (!parse_vertex_files(node[i], data_location, files)) {
      return false;
    }
  }
  return true;
}

static bool parse_edge_files(YAML::Node node, const std::string& data_location,
                             std::vector<EdgeLoadingMeta>& files) {
  if (!node["type_triplet"]) {
    LOG(FATAL) << "edge [type_triplet] is not set properly";
    return false;
  }
  auto triplet_node = node["type_triplet"];
  std::string src_label, dst_label, edge_label;
  if (!get_scalar(triplet_node, "source_vertex", src_label)) {
    LOG(FATAL) << "source_vertex is not set properly";
    return false;
  }
  if (!get_scalar(triplet_node, "destination_vertex", dst_label)) {
    LOG(FATAL) << "destination_vertex is not set properly";
    return false;
  }
  if (!get_scalar(triplet_node, "edge", edge_label)) {
    LOG(FATAL) << "edge is not set properly";
    return false;
  }

  // parse the vertex mapping. currently we only need one column to identify the
  // vertex.
  int32_t src_column = 0;
  int32_t dst_column = 1;

  if (!fetch_src_dst_column_mapping(node, "source_vertex_mappings",
                                    src_column)) {
    LOG(ERROR) << "source_vertex_mappings is not set properly";
    return false;
  }
  if (!fetch_src_dst_column_mapping(node, "destination_vertex_mappings",
                                    dst_column)) {
    LOG(ERROR) << "destination_vertex_mappings is not set properly";
    return false;
  }

  YAML::Node files_node = node["inputs"];
  if (files_node) {
    if (!files_node.IsSequence()) {
      LOG(ERROR) << "files is not set properly";
      return false;
    }
    int num = files_node.size();
    for (int i = 0; i < num; ++i) {
      std::string file_format;
      std::string file_path;
      if (!get_scalar(files_node[i], "format", file_format)) {
        LOG(ERROR) << "format is not set properly";
        return false;
      }
      if (file_format != "standard_csv") {
        LOG(ERROR) << "file_format is not set properly";
        return false;
      }
      if (!get_scalar(files_node[i], "path", file_path)) {
        LOG(ERROR) << "file_path is not set properly";
        return false;
      }
      if (!data_location.empty()) {
        file_path = data_location + "/" + file_path;
      }
      if (!access_file(file_path)) {
        LOG(ERROR) << "edge file - " << file_path << " file not found...";
      }
      std::filesystem::path path(file_path);
      VLOG(10) << "src " << src_label << " dst " << dst_label << " edge "
               << edge_label << " src_column " << src_column << " dst_column "
               << dst_column << " path " << std::filesystem::canonical(path);
      files.emplace_back(src_label, dst_label, edge_label, src_column,
                         dst_column, std::filesystem::canonical(path));
    }
  } else {
    LOG(WARNING) << "No edge files found for edge " << edge_label << "...";
  }
  return true;
}

static bool parse_edges_files_schema(YAML::Node node,
                                     const std::string& data_location,
                                     std::vector<EdgeLoadingMeta>& files) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "edge is not set properly";
    return false;
  }
  int num = node.size();
  LOG(INFO) << " Try to parse " << num << "edge configuration";
  for (int i = 0; i < num; ++i) {
    if (!parse_edge_files(node[i], data_location, files)) {
      return false;
    }
  }
  return true;
}

static bool parse_bulk_load_config_file(
    const std::string& load_config, std::string& data_source,
    std::string delimiter, std::string& method,
    std::vector<VertexLoadingMeta>& vertex_load_meta,
    std::vector<EdgeLoadingMeta>& edge_load_meta) {
  YAML::Node root = YAML::LoadFile(load_config);
  data_source = "file";
  std::string data_location;
  delimiter = "|";
  if (root["loading_config"]) {
    get_scalar(root["loading_config"], "data_source", data_source);
    get_scalar(root["loading_config"], "data_location", data_location);
    get_scalar(root["loading_config"], "method", method);
    if (root["loading_config"]["meta_data"]) {
      get_scalar(root["loading_config"]["meta_data"], "delimiter", delimiter);
    }
  }
  if (data_location.empty()) {
    LOG(WARNING) << "data_location is not set";
  }
  if (data_source != "file") {
    LOG(ERROR) << "Only support file data source now";
    return false;
  }
  LOG(INFO) << "data_source: " << data_source
            << ", data_location: " << data_location << ", method: " << method
            << ", delimiter: " << delimiter;

  if (root["vertex_mappings"]) {
    VLOG(10) << "vertex_mappings is set";
    if (!parse_vertices_files_schema(root["vertex_mappings"], data_location,
                                     vertex_load_meta)) {
      return false;
    }
  }
  if (root["edge_mappings"]) {
    VLOG(10) << "edge_mappings is set";
    if (!parse_edges_files_schema(root["edge_mappings"], data_location,
                                  edge_load_meta)) {
      return false;
    }
  }
  return true;
}

static bool parse_schema_config_file(
    const std::string& path, Schema& schema,
    std::vector<std::string>& stored_procedures) {
  YAML::Node graph_node = YAML::LoadFile(path);
  if (!graph_node || !graph_node.IsMap()) {
    LOG(ERROR) << "graph is not set properly";
    return false;
  }
  if (!expect_config(graph_node, "store_type", "mutable_csr")) {
    LOG(ERROR) << "graph_store is not set properly";
    return false;
  }
  auto schema_node = graph_node["schema"];

  if (!graph_node["schema"]) {
    LOG(ERROR) << "schema is not set";
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
      LOG(ERROR) << "plugin directory - " << directory << " not found...";
      return false;
    }
    std::vector<std::string> files_got;
    if (!get_sequence(stored_procedure_node, "enable_lists", files_got)) {
      LOG(ERROR) << "stored_procedures is not set properly";
    }
    for (auto& f : files_got) {
      if (!std::filesystem::exists(f)) {
        LOG(ERROR) << "plugin - " << f << " file not found...";
        return false;
      }
      stored_procedures.push_back(std::filesystem::canonical(f));
    }
  }

  return true;
}

}  // namespace config_parsing

std::tuple<Schema, std::vector<VertexLoadingMeta>, std::vector<EdgeLoadingMeta>,
           std::vector<std::string>, LoadingConfig>
Schema::LoadFromYaml(const std::string& schema_config,
                     const std::string& bulk_load_config) {
  Schema schema;
  std::vector<std::string> plugins;
  if (!schema_config.empty() && std::filesystem::exists(schema_config)) {
    config_parsing::parse_schema_config_file(schema_config, schema, plugins);
  }

  std::vector<VertexLoadingMeta> vertex_load_meta;
  std::vector<EdgeLoadingMeta> edge_load_meta;
  LoadingConfig load_config;
  if (!bulk_load_config.empty() && std::filesystem::exists(bulk_load_config)) {
    config_parsing::parse_bulk_load_config_file(
        bulk_load_config, load_config.data_source_, load_config.delimiter_,
        load_config.method_, vertex_load_meta, edge_load_meta);
  }

  return std::make_tuple(schema, vertex_load_meta, edge_load_meta, plugins,
                         load_config);
}

}  // namespace gs

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

#include "flex/storages/rt_mutable_graph/loading_config.h"

#include <yaml-cpp/yaml.h>
#include <filesystem>
#include <string>
#include <tuple>

namespace gs {
namespace config_parsing {

// fetch the primary key of the src and dst vertex label in the edge file,
// also check whether the primary key exists in the schema, and number equals.
static bool fetch_src_dst_column_mapping(const Schema& schema, YAML::Node node,
                                         label_t label_id,
                                         const std::string& key,
                                         std::vector<size_t>& column_inds) {
  if (node[key]) {
    auto column_mappings = node[key];
    if (!column_mappings.IsSequence()) {
      LOG(ERROR) << "value for column_mappings should be a sequence";
      return false;
    }
    auto& schema_primary_key = schema.get_vertex_primary_key(label_id);

    if (column_mappings.size() != schema_primary_key.size()) {
      LOG(INFO) << "Specification in " << key
                << " doesn't match schema primary key for label "
                << schema.get_vertex_label_name(label_id);
      return false;
    }
    column_inds.resize(column_mappings.size());
    for (auto i = 0; i < column_mappings.size(); ++i) {
      auto column_mapping = column_mappings[i]["column"];
      auto name = column_mapping["name"].as<std::string>();
      if (name != schema_primary_key[i].second) {
        LOG(ERROR) << "Expect column name [" << schema_primary_key[i].second
                   << "] for source vertex mapping, at index: " << i
                   << ", got: " << name;
        return false;
      }
      if (!get_scalar(column_mapping, "index", column_inds[i])) {
        LOG(ERROR) << "Expect column index for source vertex mapping";
        return false;
      }
    }

  } else {
    LOG(ERROR) << "No primary key column mapping for [" << key << "]";
  }
  return true;
}

// Parse the mappings and check whether property exists in the schema.
template <typename FUNC>
static bool parse_column_mappings(
    YAML::Node node, const Schema& schema, const std::string& label_name,
    std::vector<std::pair<size_t, std::string>>& column_mappings,
    FUNC condition) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "column_mappings should be a sequence";
    return false;
  }
  int num = node.size();
  for (int i = 0; i < num; ++i) {
    auto column_mapping = node[i]["column"];
    if (!column_mapping) {
      LOG(ERROR) << "column_mappings should have field [column]";
      return false;
    }
    int32_t column_id;
    if (!get_scalar(column_mapping, "index", column_id)) {
      LOG(ERROR) << "Expect column index for column mapping";
      return false;
    }
    std::string property_name;  // property name is optional.
    if (!get_scalar(node[i], "property", property_name)) {
      LOG(ERROR) << "Expect property name for column mapping";
      return false;
    }
    if (!condition(label_name, property_name)) {
      LOG(ERROR) << "Property [" << property_name << "] does not exist in "
                 << "the schema for label : " << label_name;
      return false;
    }
    column_mappings.emplace_back(column_id, property_name);
  }
  // If no column mapping is set, use default mapping.
  return true;
}

// These files share the same column mapping.
static bool parse_vertex_files(
    YAML::Node node, const Schema& schema, const std::string& data_location,
    std::unordered_map<label_t, std::vector<std::string>>& files,
    std::unordered_map<label_t, std::vector<std::pair<size_t, std::string>>>&
        vertex_mapping) {
  std::string label_name;
  if (!get_scalar(node, "type_name", label_name)) {
    return false;
  }
  // Check label exists in schema
  if (!schema.has_vertex_label(label_name)) {
    LOG(ERROR) << "Vertex label [" << label_name << "] does not exist in "
               << "the schema";
    return false;
  }
  auto label_id = schema.get_vertex_label_id(label_name);

  // Check whether the label has been set.
  if (files.find(label_id) != files.end()) {
    LOG(ERROR) << "Loading configuration for Vertex label [" << label_name
               << "] has been set";
    return false;
  }

  YAML::Node files_node = node["inputs"];

  if (node["column_mappings"]) {
    // parse the vertex mapping, can check whether the mapping to schema is
    // correct.
    auto column_mappings = node["column_mappings"];
    if (!parse_column_mappings(column_mappings, schema, label_name,
                               vertex_mapping[label_id],
                               [&](const std::string& vertex_label_name,
                                   const std::string& property_name) {
                                 return schema.vertex_has_property(
                                     vertex_label_name, property_name);
                               })) {
      LOG(ERROR) << "Failed to parse vertex mapping";
      return false;
    }
    LOG(INFO) << "Successfully parsed vertex mapping size: "
              << vertex_mapping.size();
  } else {
    // if no column_mappings is given, use default mapping.
    VLOG(10) << "No vertex mapping is given, use default mapping";
    vertex_mapping.emplace(label_id,
                           std::vector<std::pair<size_t, std::string>>());
  }
  if (files_node) {
    if (!files_node.IsSequence()) {
      LOG(ERROR) << "Expect field [inputs] for vertex [" << label_name
                 << "] to be a list";
      return false;
    }
    int num = files_node.size();
    for (int i = 0; i < num; ++i) {
      std::string file_path = files_node[i].as<std::string>();
      if (!access_file(data_location, file_path)) {
        LOG(ERROR) << "vertex file - " << file_path << " file not found...";
      }
      std::filesystem::path path(file_path);
      files[label_id].emplace_back(std::filesystem::canonical(path));
    }
    return true;
  } else {
    LOG(ERROR) << "vertex [" << label_name << "] does not have input files";
    return false;
  }
}

static bool parse_vertices_files_schema(
    YAML::Node node, const Schema& schema, const std::string& data_location,
    std::unordered_map<label_t, std::vector<std::string>>& files,
    std::unordered_map<label_t, std::vector<std::pair<size_t, std::string>>>&
        column_mappings) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "vertex is not set properly";
    return false;
  }
  int num = node.size();
  for (int i = 0; i < num; ++i) {
    if (!parse_vertex_files(node[i], schema, data_location, files,
                            column_mappings)) {
      return false;
    }
  }
  return true;
}

static bool parse_edge_files(
    YAML::Node node, const Schema& schema, const std::string& data_location,
    std::unordered_map<
        std::tuple<label_t, label_t, label_t>, std::vector<std::string>,
        boost::hash<std::tuple<label_t, label_t, label_t>>>& files,
    std::unordered_map<typename LoadingConfig::edge_triplet_type,
                       std::vector<std::pair<size_t, std::string>>,
                       boost::hash<typename LoadingConfig::edge_triplet_type>>&
        edge_mapping,
    std::unordered_map<typename LoadingConfig::edge_triplet_type,
                       std::pair<std::vector<size_t>, std::vector<size_t>>,
                       boost::hash<typename LoadingConfig::edge_triplet_type>>&
        edge_src_dst_col) {
  if (!node["type_triplet"]) {
    LOG(ERROR) << "edge [type_triplet] is not set properly";
    return false;
  }
  auto triplet_node = node["type_triplet"];
  std::string src_label, dst_label, edge_label;
  if (!get_scalar(triplet_node, "edge", edge_label)) {
    LOG(ERROR) << "Field [edge] is not set for edge [" << triplet_node << "]";
    return false;
  }
  if (!get_scalar(triplet_node, "source_vertex", src_label)) {
    LOG(ERROR) << "Field [source_vertex] is not set for edge [" << edge_label
               << "]";
    return false;
  }
  if (!get_scalar(triplet_node, "destination_vertex", dst_label)) {
    LOG(ERROR) << "Field [destination_vertex] is not set for edge ["
               << edge_label << "]";
    return false;
  }

  {
    // check whether src_label, dst_label and edge_label exist in schema
    if (!schema.has_vertex_label(src_label)) {
      LOG(ERROR) << "Vertex label [" << src_label << "] does not exist in "
                 << "the schema";
      return false;
    }
    if (!schema.has_vertex_label(dst_label)) {
      LOG(ERROR) << "Vertex label [" << dst_label << "] does not exist in "
                 << "the schema";
      return false;
    }
    if (!schema.has_edge_label(src_label, dst_label, edge_label)) {
      LOG(ERROR) << "Edge label [" << edge_label << "] does not exist in "
                 << "the schema";
      return false;
    }
  }
  auto src_label_id = schema.get_vertex_label_id(src_label);
  auto dst_label_id = schema.get_vertex_label_id(dst_label);
  auto edge_label_id = schema.get_edge_label_id(edge_label);

  if (files.find(std::make_tuple(src_label_id, dst_label_id, edge_label_id)) !=
      files.end()) {
    LOG(ERROR) << "Edge [" << edge_label << "] between [" << src_label
               << "] and "
               << "[" << dst_label << "] loading config already exists";
    return false;
  }

  // parse the vertex mapping. currently we only need one column to identify the
  // vertex.
  {
    std::vector<size_t> src_columns, dst_columns;

    if (!fetch_src_dst_column_mapping(schema, node, src_label_id,
                                      "source_vertex_mappings", src_columns)) {
      LOG(ERROR) << "Field [source_vertex_mappings] is not set for edge ["
                 << src_label << "->[" << edge_label << "]->" << dst_label
                 << "]";
      return false;
    }
    if (!fetch_src_dst_column_mapping(schema, node, dst_label_id,
                                      "destination_vertex_mappings",
                                      dst_columns)) {
      LOG(ERROR) << "Field [destination_vertex_mappings] is not set for edge["
                 << src_label << "->[" << edge_label << "]->" << dst_label;
      return false;
    }

    VLOG(10) << "src: " << src_label << ", dst: " << dst_label
             << " src_column size:  " << src_columns.size() << " dst_column "
             << dst_columns.size();
    edge_src_dst_col[std::tuple{src_label_id, dst_label_id, edge_label_id}] =
        std::pair{src_columns, dst_columns};
  }

  if (node["column_mappings"]) {
    auto column_mappings = node["column_mappings"];
    if (!parse_column_mappings(
            column_mappings, schema, edge_label,
            edge_mapping[std::tuple{src_label_id, dst_label_id, edge_label_id}],
            [&](const std::string& edge_label_name,
                const std::string& property_name) {
              return schema.edge_has_property(src_label, dst_label,
                                              edge_label_name, property_name);
            })) {
      LOG(ERROR) << "Failed to parse edge mapping";
      return false;
    }
    VLOG(10) << "Successfully parsed edge mapping size: "
             << edge_mapping.size();
  } else {
    VLOG(10) << "No edge column mapping is given, use default mapping";
    // use default mapping
    edge_mapping.emplace(std::tuple{src_label_id, dst_label_id, edge_label_id},
                         std::vector<std::pair<size_t, std::string>>{});
  }

  YAML::Node files_node = node["inputs"];
  if (files_node) {
    if (!files_node.IsSequence()) {
      LOG(ERROR) << "files is not set properly";
      return false;
    }
    int num = files_node.size();
    for (int i = 0; i < num; ++i) {
      std::string file_path = files_node[i].as<std::string>();
      if (!access_file(data_location, file_path)) {
        LOG(ERROR) << "edge file - " << file_path << " file not found...";
      }
      std::filesystem::path path(file_path);
      VLOG(10) << "src " << src_label << " dst " << dst_label << " edge "
               << edge_label << " path " << std::filesystem::canonical(path);
      files[std::tuple{src_label_id, dst_label_id, edge_label_id}].emplace_back(
          std::filesystem::canonical(path));
    }
  } else {
    LOG(ERROR) << "No edge files found for edge " << edge_label << "...";
  }
  return true;
}

static bool parse_edges_files_schema(
    YAML::Node node, const Schema& schema, const std::string& data_location,
    std::unordered_map<
        std::tuple<label_t, label_t, label_t>, std::vector<std::string>,
        boost::hash<std::tuple<label_t, label_t, label_t>>>& files,
    std::unordered_map<typename LoadingConfig::edge_triplet_type,
                       std::vector<std::pair<size_t, std::string>>,
                       boost::hash<typename LoadingConfig::edge_triplet_type>>&
        edge_mapping,
    std::unordered_map<std::tuple<label_t, label_t, label_t>,
                       std::pair<std::vector<size_t>, std::vector<size_t>>,
                       boost::hash<typename LoadingConfig::edge_triplet_type>>&
        edge_src_dst_col) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "Field [edge_mappings] should be a list";
    return false;
  }
  int num = node.size();
  LOG(INFO) << " Try to parse " << num << "edge configuration";
  for (int i = 0; i < num; ++i) {
    if (!parse_edge_files(node[i], schema, data_location, files, edge_mapping,
                          edge_src_dst_col)) {
      return false;
    }
  }
  return true;
}

static bool parse_bulk_load_config_file(const std::string& config_file,
                                        const Schema& schema,
                                        LoadingConfig& load_config) {
  YAML::Node root = YAML::LoadFile(config_file);
  std::string data_location;
  load_config.scheme_ = "file";  // default data source is file
  load_config.method_ = "init";
  load_config.delimiter_ = "|";
  load_config.format_ = "csv";
  if (root["loading_config"]) {
    auto loading_config_node = root["loading_config"];
    if (loading_config_node["data_source"]) {
      auto data_source_node = loading_config_node["data_source"];
      get_scalar(data_source_node, "scheme", load_config.scheme_);
      get_scalar(data_source_node, "location", data_location);
    }
    get_scalar(loading_config_node, "import_option", load_config.method_);
    auto format_node = loading_config_node["format"];
    if (format_node) {
      get_scalar(format_node, "type", load_config.format_);
      if (load_config.format_ == "csv") {
        if (format_node["meta_data"]) {
          get_scalar(format_node["meta_data"], "delimiter",
                     load_config.delimiter_);
        }
      } else {
        LOG(ERROR) << "Only support csv format now";
        return false;
      }
    }
  }
  // only delimeter with | is supported now
  if (load_config.GetValidDelimiters().find(load_config.delimiter_) ==
      load_config.GetValidDelimiters().end()) {
    LOG(ERROR) << "Not valid delimiter: " << load_config.delimiter_
               << ", supported delimeters: '|'";
    return false;
  }
  if (load_config.method_ != "init") {
    LOG(ERROR) << "Only support init method now";
    return false;
  }
  if (data_location.empty()) {
    LOG(WARNING) << "No data location is configured, If it is intended, "
                    "please ignore this warning. Proceeding assuming all files "
                    "are give in absolute path";
  }

  if (load_config.scheme_ != "file") {
    LOG(ERROR) << "Only support [file] data source now";
    return false;
  }
  LOG(INFO) << "scheme: " << load_config.scheme_
            << ", data_location: " << data_location
            << ", method: " << load_config.method_
            << ", delimiter: " << load_config.delimiter_;

  if (root["vertex_mappings"]) {
    VLOG(10) << "vertex_mappings is set";
    if (!parse_vertices_files_schema(root["vertex_mappings"], schema,
                                     data_location,
                                     load_config.vertex_loading_meta_,
                                     load_config.vertex_column_mappings_)) {
      return false;
    }
  }
  if (root["edge_mappings"]) {
    VLOG(10) << "edge_mappings is set";
    if (!parse_edges_files_schema(root["edge_mappings"], schema, data_location,
                                  load_config.edge_loading_meta_,
                                  load_config.edge_column_mappings_,
                                  load_config.edge_src_dst_col_)) {
      return false;
    }
  }

  VLOG(10) << "Finish parsing bulk load config file";

  return true;
}
}  // namespace config_parsing

LoadingConfig LoadingConfig::ParseFromYaml(const Schema& schema,
                                           const std::string& yaml_file) {
  LoadingConfig load_config(schema);
  if (!yaml_file.empty() && std::filesystem::exists(yaml_file)) {
    if (!config_parsing::parse_bulk_load_config_file(yaml_file, schema,
                                                     load_config)) {
      LOG(FATAL) << "Failed to parse bulk load config file: " << yaml_file;
    }
  }
  return load_config;
}

LoadingConfig::LoadingConfig(const Schema& schema)
    : schema_(schema),
      scheme_("file"),
      delimiter_("|"),
      method_("init"),
      format_("csv") {}

LoadingConfig::LoadingConfig(const Schema& schema,
                             const std::string& data_source,
                             const std::string& delimiter,
                             const std::string& method,
                             const std::string& format)
    : schema_(schema),
      scheme_(data_source),
      delimiter_(delimiter),
      method_(method),
      format_(format) {}

bool LoadingConfig::AddVertexSources(const std::string& label,
                                     const std::string& file_path) {
  auto label_id = schema_.get_vertex_label_id(label);
  vertex_loading_meta_[label_id].emplace_back(file_path);
  return true;
}

bool LoadingConfig::AddEdgeSources(const std::string& src_label,
                                   const std::string& dst_label,
                                   const std::string& edge_label,
                                   size_t src_pri_key_ind,
                                   size_t dst_pri_key_ind,
                                   const std::string& file_path) {
  LOG(INFO) << "Add edge source: " << src_label << ", " << dst_label
            << ", edge label" << edge_label << ", src_col: " << src_pri_key_ind
            << ", dst_col: " << dst_pri_key_ind << ",path: " << file_path;
  auto edge_label_id = schema_.get_edge_label_id(edge_label);
  auto src_label_id = schema_.get_vertex_label_id(src_label);
  auto dst_label_id = schema_.get_vertex_label_id(dst_label);
  auto key = std::make_tuple(src_label_id, dst_label_id, edge_label_id);
  edge_loading_meta_[key].emplace_back(file_path);
  return true;
}

void LoadingConfig::SetScheme(const std::string& scheme) { scheme_ = scheme; }
void LoadingConfig::SetDelimiter(const std::string& delimiter) {
  delimiter_ = delimiter;
}
void LoadingConfig::SetMethod(const std::string& method) { method_ = method; }

// getters
const std::string& LoadingConfig::GetScheme() const { return scheme_; }

const std::string& LoadingConfig::GetDelimiter() const { return delimiter_; }

const std::string& LoadingConfig::GetMethod() const { return method_; }

const std::unordered_map<LoadingConfig::schema_label_type,
                         std::vector<std::string>>&
LoadingConfig::GetVertexLoadingMeta() const {
  return vertex_loading_meta_;
}

const std::unordered_map<LoadingConfig::edge_triplet_type,
                         std::vector<std::string>,
                         boost::hash<LoadingConfig::edge_triplet_type>>&
LoadingConfig::GetEdgeLoadingMeta() const {
  return edge_loading_meta_;
}

const std::vector<std::pair<size_t, std::string>>&
LoadingConfig::GetVertexColumnMappings(label_t label_id) const {
  CHECK(vertex_column_mappings_.find(label_id) !=
        vertex_column_mappings_.end());
  return vertex_column_mappings_.at(label_id);
}

const std::vector<std::pair<size_t, std::string>>&
LoadingConfig::GetEdgeColumnMappings(label_t src_label_id, label_t dst_label_id,
                                     label_t edge_label_id) const {
  auto key = std::make_tuple(src_label_id, dst_label_id, edge_label_id);
  CHECK(edge_column_mappings_.find(key) != edge_column_mappings_.end());
  return edge_column_mappings_.at(key);
}

const std::pair<std::vector<size_t>, std::vector<size_t>>&
LoadingConfig::GetEdgeSrcDstCol(label_t src_label_id, label_t dst_label_id,
                                label_t edge_label_id) const {
  auto key = std::make_tuple(src_label_id, dst_label_id, edge_label_id);
  CHECK(edge_src_dst_col_.find(key) != edge_src_dst_col_.end());
  return edge_src_dst_col_.at(key);
}

const std::unordered_set<std::string>& LoadingConfig::GetValidDelimiters() {
  return LoadingConfig::valid_delimiter_;
}
// define delimeter here
const std::unordered_set<std::string> LoadingConfig::valid_delimiter_ = {"|"};

}  // namespace gs

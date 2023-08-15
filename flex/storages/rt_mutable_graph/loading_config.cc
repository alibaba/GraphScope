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

static bool fetch_src_dst_column_mapping(YAML::Node node,
                                         const std::string& key,
                                         int32_t& column_id) {
  if (node[key]) {
    auto column_mappings = node[key];
    if (!column_mappings.IsSequence()) {
      LOG(ERROR) << "value for column_mappings should be a sequence";
      return false;
    }
    if (column_mappings.size() > 1) {
      LOG(ERROR) << "Only only source vertex mapping is needed";
      return false;
    }
    auto column_mapping = column_mappings[0]["column"];
    if (!get_scalar(column_mapping, "index", column_id)) {
      LOG(ERROR) << "Expect column index for source vertex mapping";
      return false;
    }
  } else {
    LOG(WARNING)
        << "source_vertex_mappings is not set, use default src_column = 0";
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
        LOG(ERROR) << "Field [path] is not set properly for vertex ["
                   << label_name << "]";
        return false;
      }
      if (!data_location.empty()) {
        file_path = data_location + "/" + file_path;
      }
      if (!access_file(file_path)) {
        LOG(ERROR) << "vertex file - " << file_path << " file not found...";
      }
      std::filesystem::path path(file_path);
      files[label_id].emplace_back(std::filesystem::canonical(path));
    }
    return true;
  } else {
    LOG(FATAL) << "vertex [" << label_name << "] does not have input files";
  }
}

static bool parse_vertices_files_schema(
    YAML::Node node, const Schema& schema, const std::string& data_location,
    std::unordered_map<label_t, std::vector<std::string>>& files,
    std::unordered_map<label_t, std::vector<std::pair<size_t, std::string>>>&
        column_mappings) {
  if (!node.IsSequence()) {
    LOG(FATAL) << "vertex is not set properly";
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
                       std::pair<size_t, size_t>,
                       boost::hash<typename LoadingConfig::edge_triplet_type>>&
        edge_src_dst_col) {
  if (!node["type_triplet"]) {
    LOG(FATAL) << "edge [type_triplet] is not set properly";
    return false;
  }
  auto triplet_node = node["type_triplet"];
  std::string src_label, dst_label, edge_label;
  if (!get_scalar(triplet_node, "edge", edge_label)) {
    LOG(FATAL) << "Field [edge] is not set for edge [" << triplet_node << "]";
    return false;
  }
  if (!get_scalar(triplet_node, "source_vertex", src_label)) {
    LOG(FATAL) << "Field [source_vertex] is not set for edge [" << edge_label
               << "]";
    return false;
  }
  if (!get_scalar(triplet_node, "destination_vertex", dst_label)) {
    LOG(FATAL) << "Field [destination_vertex] is not set for edge ["
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
    if (!schema.has_edge_label(edge_label)) {
      LOG(ERROR) << "Edge label [" << edge_label << "] does not exist in "
                 << "the schema";
      return false;
    }
  }
  auto src_label_id = schema.get_vertex_label_id(src_label);
  auto dst_label_id = schema.get_vertex_label_id(dst_label);
  auto edge_label_id = schema.get_edge_label_id(edge_label);

  // parse the vertex mapping. currently we only need one column to identify the
  // vertex.
  {
    int32_t src_column = 0;
    int32_t dst_column = 1;

    if (!fetch_src_dst_column_mapping(node, "source_vertex_mappings",
                                      src_column)) {
      LOG(ERROR) << "Field [source_vertex_mappings] is not set for edge ["
                 << src_label << "->[" << edge_label << "]->" << dst_label
                 << "]";
      return false;
    }
    if (!fetch_src_dst_column_mapping(node, "destination_vertex_mappings",
                                      dst_column)) {
      LOG(ERROR) << "Field [destination_vertex_mappings] is not set for edge["
                 << src_label << "->[" << edge_label << "]->" << dst_label;
      return false;
    }

    VLOG(10) << "src: " << src_label << ", dst: " << dst_label << " src_column "
             << src_column << " dst_column " << dst_column;
    edge_src_dst_col[std::tuple{src_label_id, dst_label_id, edge_label_id}] =
        std::pair{src_column, dst_column};
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
      std::string file_format;
      std::string file_path;
      if (!get_scalar(files_node[i], "format", file_format)) {
        LOG(ERROR) << "Field [format] is not set for edge [" << edge_label
                   << "]'s inputs";
        return false;
      }
      if (file_format != "standard_csv") {
        LOG(ERROR) << "Currently only support standard_csv, but got "
                   << file_format << " for edge [" << edge_label
                   << "]'s inputs";
        return false;
      }
      if (!get_scalar(files_node[i], "path", file_path)) {
        LOG(ERROR) << "Field [path] is not set for edge [" << edge_label
                   << "]'s inputs";
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
               << edge_label << " path " << std::filesystem::canonical(path);
      files[std::tuple{src_label_id, dst_label_id, edge_label_id}].emplace_back(
          std::filesystem::canonical(path));
    }
  } else {
    LOG(FATAL) << "No edge files found for edge " << edge_label << "...";
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
                       std::pair<size_t, size_t>,
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
  std::string& data_source = load_config.data_source_;
  std::string& delimiter = load_config.delimiter_;
  std::string& method = load_config.method_;

  YAML::Node root = YAML::LoadFile(config_file);
  data_source = "file";
  std::string data_location;
  if (root["loading_config"]) {
    get_scalar(root["loading_config"], "data_source", data_source);
    get_scalar(root["loading_config"], "data_location", data_location);
    get_scalar(root["loading_config"], "method", method);
    if (root["loading_config"]["meta_data"]) {
      get_scalar(root["loading_config"]["meta_data"], "delimiter", delimiter);
    }
  }
  // only delimeter with | is supported now
  if (delimiter != "|") {
    LOG(FATAL) << "Only support | as delimiter now";
    return false;
  }
  if (method != "init") {
    LOG(FATAL) << "Only support init method now";
    return false;
  }
  if (data_location.empty()) {
    LOG(WARNING) << "data_location is not set";
  }
  if (data_source != "file") {
    LOG(ERROR) << "Only support [file] data source now";
    return false;
  }
  LOG(INFO) << "data_source: " << data_source
            << ", data_location: " << data_location << ", method: " << method
            << ", delimiter: " << delimiter;

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
    : schema_(schema), data_source_("file"), delimiter_("|"), method_("init") {}

LoadingConfig::LoadingConfig(const Schema& schema,
                             const std::string& data_source,
                             const std::string& delimiter,
                             const std::string& method)
    : schema_(schema),
      data_source_(data_source),
      delimiter_(delimiter),
      method_(method) {}

bool LoadingConfig::AddVertexSources(const std::string& label,
                                     const std::string& file_path) {
  auto label_id = schema_.get_vertex_label_id(label);
  vertex_loading_meta_[label_id].emplace_back(file_path);
  return true;
}

bool LoadingConfig::AddEdgeSources(const std::string& src_label,
                                   const std::string& dst_label,
                                   const std::string& edge_label,
                                   int32_t src_pri_key_ind,
                                   int32_t dst_pri_key_ind,
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

void LoadingConfig::SetDataSource(const std::string& data_source) {
  data_source_ = data_source;
}
void LoadingConfig::SetDelimiter(const std::string& delimiter) {
  delimiter_ = delimiter;
}
void LoadingConfig::SetMethod(const std::string& method) { method_ = method; }

// getters
const std::string& LoadingConfig::GetDataSource() const { return data_source_; }

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

const std::pair<size_t, size_t>& LoadingConfig::GetEdgeSrcDstCol(
    label_t src_label_id, label_t dst_label_id, label_t edge_label_id) const {
  auto key = std::make_tuple(src_label_id, dst_label_id, edge_label_id);
  CHECK(edge_src_dst_col_.find(key) != edge_src_dst_col_.end());
  return edge_src_dst_col_.at(key);
}

}  // namespace gs

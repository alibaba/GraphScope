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

static bool parse_vertex_files(
    YAML::Node node, const std::string& data_location,
    std::vector<std::pair<std::string, std::string>>& files) {
  std::string label_name;
  if (!get_scalar(node, "type_name", label_name)) {
    return false;
  }
  YAML::Node files_node = node["inputs"];

  if (node["column_mappings"]) {
    LOG(ERROR)
        << "configuration for column_mappings is not supported currently";
    return false;
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
      files.emplace_back(label_name, std::filesystem::canonical(path));
    }
    return true;
  } else {
    return true;
  }
}

static bool parse_vertices_files_schema(
    YAML::Node node, const std::string& data_location,
    std::vector<std::pair<std::string, std::string>>& files) {
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

static bool parse_edge_files(
    YAML::Node node, const std::string& data_location,
    std::vector<std::tuple<std::string, std::string, std::string, int32_t,
                           int32_t, std::string>>& files) {
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

  // parse the vertex mapping. currently we only need one column to identify the
  // vertex.
  int32_t src_column = 0;
  int32_t dst_column = 1;

  if (!fetch_src_dst_column_mapping(node, "source_vertex_mappings",
                                    src_column)) {
    LOG(ERROR) << "Field [source_vertex_mappings] is not set for edge ["
               << src_label << "->[" << edge_label << "]->" << dst_label << "]";
    return false;
  }
  if (!fetch_src_dst_column_mapping(node, "destination_vertex_mappings",
                                    dst_column)) {
    LOG(ERROR) << "Field [destination_vertex_mappings] is not set for edge["
               << src_label << "->[" << edge_label << "]->" << dst_label;
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
               << edge_label << " src_column " << src_column << " dst_column "
               << dst_column << " path " << std::filesystem::canonical(path);
      files.emplace_back(src_label, dst_label, edge_label, src_column,
                         dst_column, std::filesystem::canonical(path));
    }
  } else {
    LOG(FATAL) << "No edge files found for edge " << edge_label << "...";
  }
  return true;
}

static bool parse_edges_files_schema(
    YAML::Node node, const std::string& data_location,
    std::vector<std::tuple<std::string, std::string, std::string, int32_t,
                           int32_t, std::string>>& files) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "Field [edge_mappings] should be a list";
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
    std::string& delimiter, std::string& method,
    std::vector<std::pair<std::string, std::string>>& vertex_load_meta,
    std::vector<std::tuple<std::string, std::string, std::string, int32_t,
                           int32_t, std::string>>& edge_load_meta) {
  YAML::Node root = YAML::LoadFile(load_config);
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
}  // namespace config_parsing

LoadingConfig LoadingConfig::ParseFromYaml(const std::string& yaml_file) {
  LoadingConfig load_config;
  if (!yaml_file.empty() && std::filesystem::exists(yaml_file)) {
    if (!config_parsing::parse_bulk_load_config_file(
            yaml_file, load_config.data_source_, load_config.delimiter_,
            load_config.method_, load_config.vertex_loading_config_,
            load_config.edge_loading_config_)) {
      LOG(FATAL) << "Failed to parse bulk load config file: " << yaml_file;
    }
  }
  return load_config;
}

}  // namespace gs

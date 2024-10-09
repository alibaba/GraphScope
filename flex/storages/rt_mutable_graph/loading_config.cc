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
#include <iostream>
#include <sstream>
#include <string>
#include <tuple>
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/utils/exception.h"

namespace gs {

namespace config_parsing {

// When file_path is absolute path, try to find the file in the absolute path.
// When data_location is give, try to find the file in data_location first.
// When data_location is not given, try to find the file Under FLEX_DATA_DIR
// When FLEX_DATA_DIR is not set, try to find the file under current path.

static bool access_file(const std::string data_location,
                        std::string& file_path) {
  if (file_path.size() == 0) {
    return false;
  }
  if (file_path[0] == '/') {
    std::filesystem::path path(file_path);
    return std::filesystem::exists(path);
  }

  std::string real_location;
  if (!data_location.empty()) {
    real_location = data_location;
  } else if (std::getenv("FLEX_DATA_DIR") != NULL) {
    real_location = std::string(std::getenv("FLEX_DATA_DIR"));
  } else {
    real_location = std::filesystem::current_path().generic_string();
  }

  file_path = real_location + "/" + file_path;
  std::filesystem::path path(file_path);
  return std::filesystem::exists(path);
}

// fetch the primary key of the src and dst vertex label in the edge file,
// also check whether the primary key exists in the schema, and number equals.
static bool fetch_src_dst_column_mapping(
    const Schema& schema, YAML::Node node, label_t label_id,
    const std::string& key,
    std::vector<std::pair<std::string, size_t>>& columns) {
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
    columns.resize(column_mappings.size());
    for (size_t i = 0; i < column_mappings.size(); ++i) {
      auto column_mapping = column_mappings[i]["column"];

      if (!get_scalar(column_mapping, "index", columns[i].second)) {
        LOG(ERROR) << "Expect column index for source vertex mapping";
        return false;
      }
      if (get_scalar(column_mapping, "name", columns[i].first)) {
        VLOG(10) << "Column name for col_id: " << columns[i].second
                 << " is set to: " << columns[i].first;
      }

      std::string property_name;
      if (get_scalar(column_mappings[i], "property", property_name)) {
        if (property_name != std::get<1>(schema_primary_key[i])) {
          LOG(ERROR) << "Expect mapped property name ["
                     << std::get<1>(schema_primary_key[i])
                     << "] for source vertex mapping, at index: " << i
                     << ", got: " << property_name;
          return false;
        }
      }
    }
    return true;
  } else {
    LOG(WARNING) << "No primary key column mapping for [" << key << "]";
    return false;
  }
}

// Function to parse memory size represented as a string
uint64_t parse_block_size(const std::string& memorySizeStr) {
  // Create a stringstream to parse the memory size string
  std::istringstream ss(memorySizeStr);

  // Extract the numeric part of the string
  uint64_t memorySize;
  ss >> memorySize;

  // Handle unit prefixes (e.g., KB, MB, GB, etc.)
  std::string unit;
  ss >> unit;

  // Convert the value to bytes based on the unit
  if (unit == "KB") {
    memorySize *= 1024;
  } else if (unit == "MB") {
    memorySize *= 1024 * 1024;
  } else if (unit == "GB") {
    memorySize *= 1024 * 1024 * 1024;
  }

  return memorySize;
}

// Parse the mappings and check whether property exists in the schema.
template <typename FUNC>
static bool parse_column_mappings(
    YAML::Node node, const Schema& schema, const std::string& label_name,
    std::vector<std::tuple<size_t, std::string, std::string>>& column_mappings,
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
    int32_t column_id = -1;
    if (!get_scalar(column_mapping, "index", column_id)) {
      VLOG(10) << "Column index for column mapping is not set, skip";
    } else {
      if (column_id < 0) {
        LOG(ERROR) << "Column index for column mapping should be non-negative";
        return false;
      }
    }
    std::string column_name = "";
    if (!get_scalar(column_mapping, "name", column_name)) {
      VLOG(10) << "Column name for col_id: " << column_id
               << " is not set, make it empty";
    }
    // At least one need to be specified.
    if (column_id == -1 && column_name.empty()) {
      LOG(ERROR) << "Expect column index or name for column mapping";
      return false;
    }

    std::string property_name;  // property name is optional.
    if (!get_scalar(node[i], "property", property_name)) {
      LOG(ERROR) << "Expect property name for column mapping, when parsing "
                    "column mapping for label: "
                 << label_name << ", column_id: " << column_id
                 << ", column_name: " << column_name;
      return false;
    }
    if (!condition(label_name, property_name)) {
      LOG(ERROR) << "Property [" << property_name << "] does not exist in "
                 << "the schema for label : " << label_name;
      return false;
    }
    column_mappings.emplace_back(column_id, column_name, property_name);
  }
  // If no column mapping is set, use default mapping.
  return true;
}

static void set_default_csv_loading_config(
    std::unordered_map<std::string, std::string>& metadata) {
  metadata[reader_options::DELIMITER] = "|";
  metadata[reader_options::HEADER_ROW] = "true";
  metadata[reader_options::QUOTING] = "false";
  metadata[reader_options::QUOTE_CHAR] = "\"";
  metadata[reader_options::DOUBLE_QUOTE] = "false";
  metadata[reader_options::ESCAPE_CHAR] = "\\";
  metadata[reader_options::ESCAPING] = "false";
  metadata[reader_options::BATCH_READER] = "true";
  metadata[reader_options::BATCH_SIZE_KEY] =
      std::to_string(reader_options::DEFAULT_BLOCK_SIZE);
}

// These files share the same column mapping.
static Status parse_vertex_files(
    YAML::Node node, const Schema& schema, const std::string& scheme,
    const std::string& data_location,
    std::unordered_map<label_t, std::vector<std::string>>& files,
    std::unordered_map<
        label_t, std::vector<std::tuple<size_t, std::string, std::string>>>&
        vertex_mapping) {
  std::string label_name;
  if (!get_scalar(node, "type_name", label_name)) {
    return Status(StatusCode::INVALID_IMPORT_FILE,
                  "Vertex label name is not set");
  }
  // Check label exists in schema
  if (!schema.has_vertex_label(label_name)) {
    LOG(ERROR) << "Vertex label [" << label_name << "] does not exist in "
               << "the schema";
    return Status(StatusCode::INVALID_IMPORT_FILE, "Vertex label [" +
                                                       label_name +
                                                       "] does not exist in "
                                                       "the schema");
  }
  auto label_id = schema.get_vertex_label_id(label_name);

  // Check whether the label has been set.
  if (files.find(label_id) != files.end()) {
    LOG(ERROR) << "Loading configuration for Vertex label [" << label_name
               << "] has been set";
    return Status(StatusCode::INVALID_IMPORT_FILE,
                  "Loading configuration for Vertex label [" + label_name +
                      "] has been set");
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
      return Status(StatusCode::INVALID_IMPORT_FILE,
                    "Failed to parse vertex mapping");
    }
    LOG(INFO) << "Successfully parsed vertex mapping size: "
              << vertex_mapping.size();
  } else {
    // if no column_mappings is given, use default mapping.
    VLOG(10) << "No vertex mapping is given, use default mapping";
    vertex_mapping.emplace(
        label_id, std::vector<std::tuple<size_t, std::string, std::string>>());
  }
  if (files_node) {
    if (!files_node.IsSequence()) {
      LOG(ERROR) << "Expect field [inputs] for vertex [" << label_name
                 << "] to be a list";
      return Status(
          StatusCode::INVALID_IMPORT_FILE,
          "Expect field [inputs] for vertex [" + label_name + "] to be a list");
    }
    int num = files_node.size();
    for (int i = 0; i < num; ++i) {
      std::string file_path = files_node[i].as<std::string>();
      if (file_path.empty()) {
        LOG(ERROR) << "file path is empty";
        return Status(StatusCode::INVALID_IMPORT_FILE,
                      "The input for vertex [" + label_name + "] is empty");
      }
      if (scheme == "file") {
        if (!access_file(data_location, file_path)) {
          LOG(ERROR) << "vertex file - [" << file_path << "] file not found...";
          return Status(StatusCode::INVALID_IMPORT_FILE,
                        "vertex file - [" + file_path + "] file not found...");
        }
        std::filesystem::path path(file_path);
        files[label_id].emplace_back(std::filesystem::canonical(path));
      } else {
        // append file_path to data_location
        if (!data_location.empty()) {
          file_path = data_location + "/" + file_path;
        }
        files[label_id].emplace_back(file_path);
      }
    }
    return Status::OK();
  } else {
    LOG(ERROR) << "vertex [" << label_name << "] does not have input files";
    return Status(StatusCode::INVALID_IMPORT_FILE,
                  "vertex [" + label_name + "] does not have input files");
  }
}

static Status parse_vertices_files_schema(
    YAML::Node node, const Schema& schema, const std::string& scheme,
    const std::string& data_location,
    std::unordered_map<label_t, std::vector<std::string>>& files,
    std::unordered_map<
        label_t, std::vector<std::tuple<size_t, std::string, std::string>>>&
        column_mappings) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "vertex is not set properly";
    return Status(StatusCode::INVALID_IMPORT_FILE,
                  "vertex is not set properly");
  }
  int num = node.size();
  for (int i = 0; i < num; ++i) {
    RETURN_IF_NOT_OK(parse_vertex_files(node[i], schema, scheme, data_location,
                                        files, column_mappings));
  }
  return Status::OK();
}

static Status parse_edge_files(
    YAML::Node node, const Schema& schema, const std::string& scheme,
    const std::string& data_location,
    std::unordered_map<
        std::tuple<label_t, label_t, label_t>, std::vector<std::string>,
        boost::hash<std::tuple<label_t, label_t, label_t>>>& files,
    std::unordered_map<
        typename LoadingConfig::edge_triplet_type,
        std::vector<std::tuple<size_t, std::string, std::string>>,
        boost::hash<typename LoadingConfig::edge_triplet_type>>& edge_mapping,
    std::unordered_map<typename LoadingConfig::edge_triplet_type,
                       std::pair<std::vector<std::pair<std::string, size_t>>,
                                 std::vector<std::pair<std::string, size_t>>>,
                       boost::hash<typename LoadingConfig::edge_triplet_type>>&
        edge_src_dst_col) {
  if (!node["type_triplet"]) {
    LOG(ERROR) << "edge [type_triplet] is not set properly";
    return Status(StatusCode::INVALID_IMPORT_FILE,
                  "edge [type_triplet] is not set properly");
  }
  auto triplet_node = node["type_triplet"];
  std::string src_label, dst_label, edge_label;
  if (!get_scalar(triplet_node, "edge", edge_label)) {
    std::stringstream ss;
    ss << "Field [edge] is not set for edge [" << triplet_node << "]";
    auto err_str = ss.str();
    LOG(ERROR) << err_str;
    return Status(StatusCode::INVALID_IMPORT_FILE, err_str);
  }
  if (!get_scalar(triplet_node, "source_vertex", src_label)) {
    LOG(ERROR) << "Field [source_vertex] is not set for edge [" << edge_label
               << "]";
    return Status(
        StatusCode::INVALID_IMPORT_FILE,
        "Field [source_vertex] is not set for edge [" + edge_label + "]");
  }
  if (!get_scalar(triplet_node, "destination_vertex", dst_label)) {
    LOG(ERROR) << "Field [destination_vertex] is not set for edge ["
               << edge_label << "]";
    return Status(
        StatusCode::INVALID_IMPORT_FILE,
        "Field [destination_vertex] is not set for edge [" + edge_label + "]");
  }

  {
    // check whether src_label, dst_label and edge_label exist in schema
    if (!schema.has_vertex_label(src_label)) {
      LOG(ERROR) << "Vertex label [" << src_label << "] does not exist in "
                 << "the schema";
      return Status(StatusCode::INVALID_IMPORT_FILE, "Vertex label [" +
                                                         src_label +
                                                         "] does not exist in "
                                                         "the schema");
    }
    if (!schema.has_vertex_label(dst_label)) {
      LOG(ERROR) << "Vertex label [" << dst_label << "] does not exist in "
                 << "the schema";
      return Status(StatusCode::INVALID_IMPORT_FILE, "Vertex label [" +
                                                         dst_label +
                                                         "] does not exist in "
                                                         "the schema");
    }
    if (!schema.has_edge_label(src_label, dst_label, edge_label)) {
      LOG(ERROR) << "Edge label [" << edge_label << "] does not exist in "
                 << "the schema";
      return Status(StatusCode::INVALID_IMPORT_FILE, "Edge label [" +
                                                         edge_label +
                                                         "] does not exist in "
                                                         "the schema");
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
    return Status(StatusCode::INVALID_IMPORT_FILE,
                  "Edge [" + edge_label + "] between [" + src_label + "] and " +
                      "[" + dst_label + "] loading config already exists");
  }

  // parse the vertex mapping. currently we only need one column to identify the
  // vertex.
  {
    std::vector<std::pair<std::string, size_t>> src_columns, dst_columns;

    if (!fetch_src_dst_column_mapping(schema, node, src_label_id,
                                      "source_vertex_mappings", src_columns)) {
      LOG(WARNING) << "Field [source_vertex_mappings] is not set for edge ["
                   << src_label << "->[" << edge_label << "]->" << dst_label
                   << "], using default choice: column_id 0";
      src_columns.emplace_back("", 0);
    }
    if (!fetch_src_dst_column_mapping(schema, node, dst_label_id,
                                      "destination_vertex_mappings",
                                      dst_columns)) {
      LOG(WARNING) << "Field [destination_vertex_mappings] is not set for edge["
                   << src_label << "->[" << edge_label << "]->" << dst_label
                   << "], using default choice: column_id 1";
      dst_columns.emplace_back("", 1);
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
      return Status(StatusCode::INVALID_IMPORT_FILE,
                    "Failed to parse edge mapping");
    }
    VLOG(10) << "Successfully parsed edge mapping size: "
             << edge_mapping.size();
  } else {
    VLOG(10) << "No edge column mapping is given, use default mapping";
    // use default mapping
    edge_mapping.emplace(
        std::tuple{src_label_id, dst_label_id, edge_label_id},
        std::vector<std::tuple<size_t, std::string, std::string>>{});
  }

  YAML::Node files_node = node["inputs"];
  if (files_node) {
    if (!files_node.IsSequence()) {
      LOG(ERROR) << "files is not sequence";
      return Status(StatusCode::INVALID_IMPORT_FILE, "files is not sequence");
    }
    int num = files_node.size();
    for (int i = 0; i < num; ++i) {
      std::string file_path = files_node[i].as<std::string>();
      if (file_path.empty()) {
        LOG(ERROR) << "file path is empty";
        return Status(StatusCode::INVALID_IMPORT_FILE,
                      "The input for edge [" + edge_label + "] is empty");
      }
      if (scheme == "file") {
        if (!access_file(data_location, file_path)) {
          LOG(ERROR) << "edge file - [" << file_path << "] file not found...";
          return Status(StatusCode::INVALID_IMPORT_FILE,
                        "edge file - [" + file_path + "] file not found...");
        }
        std::filesystem::path path(file_path);
        VLOG(10) << "src " << src_label << " dst " << dst_label << " edge "
                 << edge_label << " path " << std::filesystem::canonical(path);
        files[std::tuple{src_label_id, dst_label_id, edge_label_id}]
            .emplace_back(std::filesystem::canonical(path));
      } else {
        // append file_path to data_location
        if (!data_location.empty()) {
          file_path = data_location + "/" + file_path;
        }
        files[std::tuple{src_label_id, dst_label_id, edge_label_id}]
            .emplace_back(file_path);
      }
    }
  } else {
    LOG(ERROR) << "No edge files found for edge " << edge_label << "...";
    return Status(StatusCode::INVALID_IMPORT_FILE,
                  "No edge files found for edge " + edge_label + "...");
  }
  return Status::OK();
}

static Status parse_edges_files_schema(
    YAML::Node node, const Schema& schema, const std::string& scheme,
    const std::string& data_location,
    std::unordered_map<
        std::tuple<label_t, label_t, label_t>, std::vector<std::string>,
        boost::hash<std::tuple<label_t, label_t, label_t>>>& files,
    std::unordered_map<
        typename LoadingConfig::edge_triplet_type,
        std::vector<std::tuple<size_t, std::string, std::string>>,
        boost::hash<typename LoadingConfig::edge_triplet_type>>& edge_mapping,
    std::unordered_map<std::tuple<label_t, label_t, label_t>,
                       std::pair<std::vector<std::pair<std::string, size_t>>,
                                 std::vector<std::pair<std::string, size_t>>>,
                       boost::hash<typename LoadingConfig::edge_triplet_type>>&
        edge_src_dst_col) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "Field [edge_mappings] should be a list";
    return Status(StatusCode::INVALID_IMPORT_FILE,
                  "Field [edge_mappings] should be a list");
  }
  int num = node.size();
  LOG(INFO) << " Try to parse " << num << " edge configuration";
  for (int i = 0; i < num; ++i) {
    RETURN_IF_NOT_OK(parse_edge_files(node[i], schema, scheme, data_location,
                                      files, edge_mapping, edge_src_dst_col));
  }
  return Status::OK();
}

Status parse_bulk_load_config_file(const std::string& config_file,
                                   const Schema& schema,
                                   LoadingConfig& load_config) {
  YAML::Node root = YAML::LoadFile(config_file);
  return parse_bulk_load_config_yaml(root, schema, load_config);
}

Status parse_bulk_load_method(const YAML::Node& node, BulkLoadMethod& method) {
  std::string method_str;
  if (get_scalar(node, "import_option", method_str)) {
    if (method_str == "init") {
      method = BulkLoadMethod::kInit;
    } else if (method_str == "overwrite") {
      method = BulkLoadMethod::kOverwrite;
    } else {
      LOG(ERROR) << "Unknown import_option: " << method_str;
      return Status(StatusCode::INVALID_ARGUMENT,
                    "Unknown import_option" + method_str);
    }
  } else {
    LOG(WARNING) << "import_option is not set, using default init method";
  }
  return Status::OK();
}

Status parse_bulk_load_config_yaml(const YAML::Node& root, const Schema& schema,
                                   LoadingConfig& load_config) {
  std::string data_location;
  load_config.scheme_ = "file";  // default data source is file
  load_config.method_ = BulkLoadMethod::kInit;
  load_config.format_ = "csv";

  if (root["loading_config"]) {
    auto loading_config_node = root["loading_config"];
    if (loading_config_node["data_source"]) {
      auto data_source_node = loading_config_node["data_source"];
      get_scalar(data_source_node, "scheme", load_config.scheme_);
      get_scalar(data_source_node, "location", data_location);
    }

    if (loading_config_node["x_csr_params"]) {
      if (get_scalar(loading_config_node["x_csr_params"],
                     loader_options::PARALLELISM, load_config.parallelism_)) {
        VLOG(10) << "Parallelism is set to: " << load_config.parallelism_;
      }
      if (get_scalar(loading_config_node["x_csr_params"],
                     loader_options::BUILD_CSR_IN_MEM,
                     load_config.build_csr_in_mem_)) {
        VLOG(10) << "Build csr in memory is set to: "
                 << load_config.build_csr_in_mem_;
      }
      if (get_scalar(loading_config_node["x_csr_params"],
                     loader_options::USE_MMAP_VECTOR,
                     load_config.use_mmap_vector_)) {
        VLOG(10) << "Use mmap vector is set to: "
                 << load_config.use_mmap_vector_;
      }
    }

    RETURN_IF_NOT_OK(
        parse_bulk_load_method(loading_config_node, load_config.method_));
    auto format_node = loading_config_node["format"];
    // default format is csv
    if (format_node) {
      // TODO: support other format, and make clear which args are csv specific
      //  What if format node is not specified?
      get_scalar(format_node, "type", load_config.format_);
      if (load_config.format_ == "csv") {
        // set default delimiter before we parsing meta_data
        set_default_csv_loading_config(load_config.metadata_);

        // put all key values in meta_data into metadata_
        if (format_node["metadata"]) {
          auto meta_data_node = format_node["metadata"];
          if (!meta_data_node.IsMap()) {
            LOG(ERROR) << "metadata should be a map";
            return Status(StatusCode::INVALID_ARGUMENT,
                          "metadata should be a map");
          }
          for (auto it = meta_data_node.begin(); it != meta_data_node.end();
               ++it) {
            // override previous settings.
            auto key = it->first.as<std::string>();
            if (key != reader_options::NULL_VALUES) {
              VLOG(1) << "Got metadata key: " << key
                      << " value: " << it->second.as<std::string>();
            }
            if (reader_options::CSV_META_KEY_WORDS.find(key) !=
                reader_options::CSV_META_KEY_WORDS.end()) {
              if (key == reader_options::BATCH_SIZE_KEY) {
                // special case for block size
                // parse block size (MB, b, KB, B) to bytes
                auto block_size_str = it->second.as<std::string>();
                auto block_size = parse_block_size(block_size_str);
                load_config.metadata_[reader_options::BATCH_SIZE_KEY] =
                    std::to_string(block_size);
              } else if (key == reader_options::NULL_VALUES) {
                // special case for null values
                auto null_values = it->second.as<std::vector<std::string>>();
                load_config.null_values_ = null_values;
              } else {
                load_config.metadata_[key] = it->second.as<std::string>();
              }
            }
          }
        }
      } else {
        // for other format, put customized metadata into metadata_
        if (format_node["metadata"]) {
          LOG(INFO) << "Setting metadata for format: " << load_config.format_;
          auto meta_data_node = format_node["metadata"];
          if (!meta_data_node.IsMap()) {
            LOG(ERROR) << "metadata should be a map";
            return Status(StatusCode::INVALID_ARGUMENT,
                          "metadata should be a map");
          }
          for (auto it = meta_data_node.begin(); it != meta_data_node.end();
               ++it) {
            // override previous settings.
            auto key = it->first.as<std::string>();
            VLOG(1) << "Got metadata key: " << key
                    << " value: " << it->second.as<std::string>();
            load_config.metadata_[key] = it->second.as<std::string>();
          }
        }
      }
    } else {
      LOG(INFO) << "No format is specified, using default csv format";
      set_default_csv_loading_config(load_config.metadata_);
    }
  } else {
    LOG(ERROR) << "loading_config is not set";
    return Status(StatusCode::INVALID_ARGUMENT, "loading_config is not set");
  }
  if (load_config.method_ != BulkLoadMethod::kInit &&
      load_config.method_ != BulkLoadMethod::kOverwrite) {
    LOG(ERROR) << "Only support init/overwrite method now";
    return Status(StatusCode::INVALID_ARGUMENT,
                  "Only support init/overwrite method now");
  }
  if (data_location.empty()) {
    LOG(WARNING) << "No data location is configured, If it is intended, "
                    "please ignore this warning. Proceeding assuming all files "
                    "are give in absolute path";
  }

  LOG(INFO) << "scheme: " << load_config.scheme_
            << ", data_location: " << data_location
            << ", method: " << load_config.method_ << ", delimiter: "
            << load_config.metadata_[reader_options::DELIMITER]
            << ", include header row: "
            << load_config.metadata_[reader_options::HEADER_ROW];

  if (root["vertex_mappings"]) {
    VLOG(10) << "vertex_mappings is set";
    // if (!parse_vertices_files_schema(root["vertex_mappings"], schema,
    //                                  load_config.scheme_, data_location,
    //                                  load_config.vertex_loading_meta_,
    //                                  load_config.vertex_column_mappings_)) {
    //   return false;
    RETURN_IF_NOT_OK(parse_vertices_files_schema(
        root["vertex_mappings"], schema, load_config.scheme_, data_location,
        load_config.vertex_loading_meta_, load_config.vertex_column_mappings_));
  }
  if (root["edge_mappings"]) {
    VLOG(10) << "edge_mappings is set";
    RETURN_IF_NOT_OK(parse_edges_files_schema(
        root["edge_mappings"], schema, load_config.scheme_, data_location,
        load_config.edge_loading_meta_, load_config.edge_column_mappings_,
        load_config.edge_src_dst_col_));
    // if (!parse_edges_files_schema(
    //         root["edge_mappings"], schema, load_config.scheme_,
    //         data_location, load_config.edge_loading_meta_,
    //         load_config.edge_column_mappings_,
    //         load_config.edge_src_dst_col_)) {
    //   return false;
    //}
  }

  VLOG(10) << "Finish parsing bulk load config file";

  return Status::OK();
}
}  // namespace config_parsing

Result<LoadingConfig> LoadingConfig::ParseFromYamlFile(
    const Schema& schema, const std::string& yaml_file) {
  LoadingConfig load_config(schema);
  if (!yaml_file.empty() && std::filesystem::exists(yaml_file)) {
    if (!config_parsing::parse_bulk_load_config_file(yaml_file, schema,
                                                     load_config)
             .ok()) {
      LOG(ERROR) << "Failed to parse bulk load config file: " << yaml_file;
      return gs::Result<LoadingConfig>(
          gs::Status(gs::StatusCode::INVALID_IMPORT_FILE,
                     "Failed to parse bulk load config file: " + yaml_file),
          load_config);
    }
  }
  return load_config;
}

Result<LoadingConfig> LoadingConfig::ParseFromYamlNode(
    const Schema& schema, const YAML::Node& yaml_node) {
  LoadingConfig load_config(schema);
  try {
    if (!yaml_node.IsNull()) {
      auto status = config_parsing::parse_bulk_load_config_yaml(
          yaml_node, schema, load_config);
      if (!status.ok()) {
        LOG(ERROR) << "Failed to parse bulk load config: ";
        return gs::Result<LoadingConfig>(status, load_config);
      }
    }
  } catch (const YAML::Exception& e) {
    return gs::Result<LoadingConfig>(
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
                   "Failed to parse yaml node: " + std::string(e.what())),
        load_config);
  }
  return load_config;
}

LoadingConfig::LoadingConfig(const Schema& schema)
    : schema_(schema),
      scheme_("file"),
      method_(BulkLoadMethod::kInit),
      format_("csv"),
      parallelism_(loader_options::DEFAULT_PARALLELISM),
      build_csr_in_mem_(loader_options::DEFAULT_BUILD_CSR_IN_MEM),
      use_mmap_vector_(loader_options::DEFAULT_USE_MMAP_VECTOR) {}

LoadingConfig::LoadingConfig(const Schema& schema,
                             const std::string& data_source,
                             const std::string& delimiter,
                             const BulkLoadMethod& method,
                             const std::string& format)
    : schema_(schema),
      scheme_(data_source),
      method_(method),
      format_(format),
      parallelism_(loader_options::DEFAULT_PARALLELISM),
      build_csr_in_mem_(loader_options::DEFAULT_BUILD_CSR_IN_MEM),
      use_mmap_vector_(loader_options::DEFAULT_USE_MMAP_VECTOR) {
  metadata_[reader_options::DELIMITER] = delimiter;
}

Status LoadingConfig::AddVertexSources(const std::string& label,
                                       const std::string& file_path) {
  auto label_id = schema_.get_vertex_label_id(label);
  vertex_loading_meta_[label_id].emplace_back(file_path);
  return Status::OK();
}

Status LoadingConfig::AddEdgeSources(const std::string& src_label,
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
  return Status::OK();
}

void LoadingConfig::SetScheme(const std::string& scheme) { scheme_ = scheme; }
void LoadingConfig::SetDelimiter(const char& delimiter) {
  metadata_[reader_options::DELIMITER] = std::string(1, delimiter);
}
void LoadingConfig::SetMethod(const BulkLoadMethod& method) {
  method_ = method;
}

// getters
const std::string& LoadingConfig::GetScheme() const { return scheme_; }

const std::string& LoadingConfig::GetDelimiter() const {
  return metadata_.at(reader_options::DELIMITER);
}

bool LoadingConfig::GetHasHeaderRow() const {
  auto str = metadata_.at(reader_options::HEADER_ROW);
  return str == "true" || str == "True" || str == "TRUE";
}

const std::string& LoadingConfig::GetFormat() const { return format_; }

const BulkLoadMethod& LoadingConfig::GetMethod() const { return method_; }

char LoadingConfig::GetEscapeChar() const {
  const auto& escape_char = metadata_.at(reader_options::ESCAPE_CHAR);
  if (escape_char.size() != 1) {
    throw std::runtime_error("Escape char should be a single character");
  }
  return escape_char[0];
}

bool LoadingConfig::GetIsEscaping() const {
  auto str = metadata_.at(reader_options::ESCAPING);
  return str == "true" || str == "True" || str == "TRUE";
}

char LoadingConfig::GetQuotingChar() const {
  const auto& quote_char = metadata_.at(reader_options::QUOTE_CHAR);
  if (quote_char.size() != 1) {
    throw std::runtime_error("Quoting char should be a single character");
  }
  return quote_char[0];
}

bool LoadingConfig::GetIsQuoting() const {
  auto str = metadata_.at(reader_options::QUOTING);
  return str == "true" || str == "True" || str == "TRUE";
}

bool LoadingConfig::GetIsDoubleQuoting() const {
  auto str = metadata_.at(reader_options::DOUBLE_QUOTE);
  return str == "true" || str == "True" || str == "TRUE";
}

const std::vector<std::string>& LoadingConfig::GetNullValues() const {
  return null_values_;
}

int32_t LoadingConfig::GetBatchSize() const {
  if (metadata_.find(reader_options::BATCH_SIZE_KEY) == metadata_.end()) {
    return reader_options::DEFAULT_BLOCK_SIZE;
  }
  auto str = metadata_.at(reader_options::BATCH_SIZE_KEY);
  return std::stoi(str);
}

bool LoadingConfig::GetIsBatchReader() const {
  if (metadata_.find(reader_options::BATCH_READER) == metadata_.end()) {
    return reader_options::DEFAULT_BATCH_READER;
  }
  auto str = metadata_.at(reader_options::BATCH_READER);
  return str == "true" || str == "True" || str == "TRUE";
}

std::string LoadingConfig::GetMetaData(const std::string& key) const {
  if (metadata_.find(key) == metadata_.end()) {
    return "";
  }
  return metadata_.at(key);
}

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

const std::vector<std::tuple<size_t, std::string, std::string>>&
LoadingConfig::GetVertexColumnMappings(label_t label_id) const {
  THROW_EXCEPTION_IF(
      vertex_column_mappings_.find(label_id) == vertex_column_mappings_.end(),
      "Vertex label id not found in vertex column mappings");
  return vertex_column_mappings_.at(label_id);
}

const std::vector<std::tuple<size_t, std::string, std::string>>&
LoadingConfig::GetEdgeColumnMappings(label_t src_label_id, label_t dst_label_id,
                                     label_t edge_label_id) const {
  auto key = std::make_tuple(src_label_id, dst_label_id, edge_label_id);
  THROW_EXCEPTION_IF(
      edge_column_mappings_.find(key) == edge_column_mappings_.end(),
      "Edge label id not found in edge column mappings");
  return edge_column_mappings_.at(key);
}

const std::pair<std::vector<std::pair<std::string, size_t>>,
                std::vector<std::pair<std::string, size_t>>>&
LoadingConfig::GetEdgeSrcDstCol(label_t src_label_id, label_t dst_label_id,
                                label_t edge_label_id) const {
  auto key = std::make_tuple(src_label_id, dst_label_id, edge_label_id);
  THROW_EXCEPTION_IF(edge_src_dst_col_.find(key) == edge_src_dst_col_.end(),
                     "Edge label id not found in edge column mappings");
  return edge_src_dst_col_.at(key);
}

}  // namespace gs

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

#ifndef STORAGE_RT_MUTABLE_GRAPH_LOADING_CONFIG_H_
#define STORAGE_RT_MUTABLE_GRAPH_LOADING_CONFIG_H_

#include <boost/functional/hash.hpp>

#include <filesystem>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "flex/storages/rt_mutable_graph/schema.h"
#include "flex/utils/yaml_utils.h"

namespace gs {

class LoadingConfig;

namespace config_parsing {
static bool parse_bulk_load_config_file(const std::string& config_file,
                                        const Schema& schema,
                                        LoadingConfig& load_config);
}

// Provide meta info about bulk loading.
class LoadingConfig {
 public:
  using schema_label_type = Schema::label_type;
  using edge_triplet_type =
      std::tuple<schema_label_type, schema_label_type,
                 schema_label_type>;  // src_label_t, dst_label_t, edge_label_t
  static const std::unordered_set<std::string> valid_delimiter_;

  // Check whether loading config file is consistent with schema
  static LoadingConfig ParseFromYaml(const Schema& schema,
                                     const std::string& yaml_file);

  LoadingConfig(const Schema& schema);

  LoadingConfig(const Schema& schema, const std::string& data_source,
                const std::string& delimiter, const std::string& method,
                const std::string& format);

  // Add source files for vertex label. Each label can have multiple files.
  bool AddVertexSources(const std::string& label, const std::string& file_path);

  // Add source files for edge triplet. Each label can have multiple files.
  // When adding edge source files, src_id and dst_id column also need to be
  // specified.
  bool AddEdgeSources(const std::string& src_label,
                      const std::string& dst_label,
                      const std::string& edge_label, size_t src_pri_key_ind,
                      size_t dst_pri_key_ind, const std::string& file_path);

  void SetScheme(const std::string& data_source);
  void SetDelimiter(const std::string& delimiter);
  void SetMethod(const std::string& method);

  // getters
  const std::string& GetScheme() const;
  const std::string& GetDelimiter() const;
  const std::string& GetMethod() const;
  const std::unordered_map<schema_label_type, std::vector<std::string>>&
  GetVertexLoadingMeta() const;
  const std::unordered_map<edge_triplet_type, std::vector<std::string>,
                           boost::hash<edge_triplet_type>>&
  GetEdgeLoadingMeta() const;

  // Get vertex column mappings. Each element in the vector is a pair of
  // <column_index, property_name>.
  const std::vector<std::pair<size_t, std::string>>& GetVertexColumnMappings(
      label_t label_id) const;

  // Get edge column mappings. Each element in the vector is a pair of
  // <column_index, schema_property_name>.
  const std::vector<std::pair<size_t, std::string>>& GetEdgeColumnMappings(
      label_t src_label_id, label_t dst_label_id, label_t edge_label_id) const;

  // Get src_id and dst_id column index for edge label.
  const std::pair<std::vector<size_t>, std::vector<size_t>>& GetEdgeSrcDstCol(
      label_t src_label_id, label_t dst_label_id, label_t edge_label_id) const;

  static const std::unordered_set<std::string>& GetValidDelimiters();

 private:
  const Schema& schema_;
  std::string scheme_;     // "file", "hdfs", "oss", "s3"
  std::string delimiter_;  // "\t", ",", " ", "|"
  std::string method_;     // init, append, overwrite
  std::string format_;     // csv, tsv, json, parquet

  std::unordered_map<schema_label_type, std::vector<std::string>>
      vertex_loading_meta_;  // <vertex_label_id, std::vector<file_path_>>
  std::unordered_map<schema_label_type,
                     std::vector<std::pair<size_t, std::string>>>
      vertex_column_mappings_;  // match which column in file to which property
                                // in schema

  std::unordered_map<edge_triplet_type, std::vector<std::string>,
                     boost::hash<edge_triplet_type>>
      edge_loading_meta_;  // key: <src_label, dst_label, edge_label>
                           // value:
                           // <file_path>
  // All Edge Files share the same File schema.
  std::unordered_map<edge_triplet_type,
                     std::vector<std::pair<size_t, std::string>>,
                     boost::hash<edge_triplet_type>>
      edge_column_mappings_;  // match which column in file to which property in
                              // schema

  std::unordered_map<edge_triplet_type,
                     std::pair<std::vector<size_t>, std::vector<size_t>>,
                     boost::hash<edge_triplet_type>>
      edge_src_dst_col_;  // Which two columns are src_id and dst_id

  friend bool config_parsing::parse_bulk_load_config_file(
      const std::string& config_file, const Schema& schema,
      LoadingConfig& load_config);
};

}  // namespace gs

#endif  // STORAGE_RT_MUTABLE_GRAPH_LOADING_CONFIG_H_
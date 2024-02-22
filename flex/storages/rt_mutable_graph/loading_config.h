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
#include "arrow/api.h"
#include "arrow/csv/options.h"
#include "flex/storages/rt_mutable_graph/schema.h"
#include "flex/utils/arrow_utils.h"
#include "flex/utils/yaml_utils.h"

#include "boost/algorithm/string.hpp"

namespace gs {

namespace reader_options {
static const int32_t DEFAULT_BLOCK_SIZE = (1 << 20);  // 1MB

// KEY_WORDS for configurations
static const char* DELIMITER = "delimiter";
static const char* HEADER_ROW = "header_row";
static const char* INCLUDE_COLUMNS = "include_columns";
static const char* COLUMN_TYPES = "column_types";
static const char* ESCAPING = "escaping";
static const char* ESCAPE_CHAR = "escape_char";
static const char* QUOTING = "quoting";
static const char* QUOTE_CHAR = "quote_char";
static const char* DOUBLE_QUOTE = "double_quote";
static const char* BATCH_SIZE_KEY = "batch_size";
// whether or not to use record batch reader. If true, the reader will read
// data in batches, otherwise, the reader will read data row by row.
static const char* BATCH_READER = "batch_reader";

static const std::unordered_set<std::string> CSV_META_KEY_WORDS = {
    DELIMITER,    HEADER_ROW,     INCLUDE_COLUMNS, COLUMN_TYPES,
    ESCAPING,     ESCAPE_CHAR,    QUOTING,         QUOTE_CHAR,
    DOUBLE_QUOTE, BATCH_SIZE_KEY, BATCH_READER};

}  // namespace reader_options

class LoadingConfig;

namespace config_parsing {
bool parse_bulk_load_config_file(const std::string& config_file,
                                 const Schema& schema,
                                 LoadingConfig& load_config);

bool parse_bulk_load_config_yaml(const YAML::Node& yaml_node,
                                 const Schema& schema,
                                 LoadingConfig& load_config);
}  // namespace config_parsing

// Provide meta info about bulk loading.
class LoadingConfig {
 public:
  using schema_label_type = Schema::label_type;
  using edge_triplet_type =
      std::tuple<schema_label_type, schema_label_type,
                 schema_label_type>;  // src_label_t, dst_label_t, edge_label_t

  // Check whether loading config file is consistent with schema
  static LoadingConfig ParseFromYamlFile(const Schema& schema,
                                         const std::string& yaml_file);
  static gs::Result<LoadingConfig> ParseFromYamlNode(
      const Schema& schema, const YAML::Node& yaml_node);

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
  void SetDelimiter(const char& delimiter);
  void SetMethod(const std::string& method);

  // getters
  const std::string& GetScheme() const;
  const std::string& GetDelimiter() const;
  const std::string& GetMethod() const;
  const std::string& GetFormat() const;
  bool GetHasHeaderRow() const;
  const std::string& GetEscapeChar() const;
  bool GetIsEscaping() const;
  const std::string& GetQuotingChar() const;
  bool GetIsQuoting() const;
  bool GetIsDoubleQuoting() const;
  int32_t GetBatchSize() const;
  bool GetIsBatchReader() const;
  std::string GetMetaData(const std::string& key) const;
  const std::unordered_map<schema_label_type, std::vector<std::string>>&
  GetVertexLoadingMeta() const;
  const std::unordered_map<edge_triplet_type, std::vector<std::string>,
                           boost::hash<edge_triplet_type>>&
  GetEdgeLoadingMeta() const;

  // Get vertex column mappings. Each element in the vector is a pair of
  // <column_index, property_name>.
  const std::vector<std::tuple<size_t, std::string, std::string>>&
  GetVertexColumnMappings(label_t label_id) const;

  // Get edge column mappings. Each element in the vector is a pair of
  // <column_index,column_name, schema_property_name>.
  const std::vector<std::tuple<size_t, std::string, std::string>>&
  GetEdgeColumnMappings(label_t src_label_id, label_t dst_label_id,
                        label_t edge_label_id) const;

  // Get src_id and dst_id column index for edge label.
  const std::pair<std::vector<std::pair<std::string, size_t>>,
                  std::vector<std::pair<std::string, size_t>>>&
  GetEdgeSrcDstCol(label_t src_label_id, label_t dst_label_id,
                   label_t edge_label_id) const;

 private:
  const Schema& schema_;
  std::string scheme_;  // "file", "hdfs", "oss", "s3"
  std::string method_;  // init, append, overwrite
  std::string format_;  // csv, tsv, json, parquet

  // meta_data, stores all the meta info about loading
  std::unordered_map<std::string, std::string> metadata_;

  std::unordered_map<schema_label_type, std::vector<std::string>>
      vertex_loading_meta_;  // <vertex_label_id, std::vector<file_path_>>
  std::unordered_map<schema_label_type,
                     std::vector<std::tuple<size_t, std::string, std::string>>>
      vertex_column_mappings_;  // match which column in file to which property
                                // in schema. {col_ind, col_name,
                                // schema_prop_name}
                                // col_name can be empty

  std::unordered_map<edge_triplet_type, std::vector<std::string>,
                     boost::hash<edge_triplet_type>>
      edge_loading_meta_;  // key: <src_label, dst_label, edge_label>
                           // value:
                           // <file_path>
  // All Edge Files share the same File schema.
  std::unordered_map<edge_triplet_type,
                     std::vector<std::tuple<size_t, std::string, std::string>>,
                     boost::hash<edge_triplet_type>>
      edge_column_mappings_;  // match which column in file to which property in
                              // schema, {col_ind, col_name, schema_prop_name}
                              // col_name can be empty

  // key: <src_label, dst_label, edge_label>,
  //  value: <{<src_col_name, src_col_id>,...}, {<dst_col_name,
  //  dst_col_id>,...}>
  // for csv loader, we just need the column_id, but for odps loader, we also
  // need the column_name
  std::unordered_map<edge_triplet_type,
                     std::pair<std::vector<std::pair<std::string, size_t>>,
                               std::vector<std::pair<std::string, size_t>>>,
                     boost::hash<edge_triplet_type>>
      edge_src_dst_col_;

  friend bool config_parsing::parse_bulk_load_config_file(
      const std::string& config_file, const Schema& schema,
      LoadingConfig& load_config);

  friend bool config_parsing::parse_bulk_load_config_yaml(
      const YAML::Node& root, const Schema& schema, LoadingConfig& load_config);
};

}  // namespace gs

#endif  // STORAGE_RT_MUTABLE_GRAPH_LOADING_CONFIG_H_

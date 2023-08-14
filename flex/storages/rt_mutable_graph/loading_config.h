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

#include <filesystem>
#include <string>
#include "flex/storages/rt_mutable_graph/schema.h"

namespace gs {
// Provide meta info about bulk loading.
struct LoadingConfig {
  std::string data_source_;  // "file", "hdfs", "oss", "s3"
  std::string delimiter_;    // "\t", ",", " ", "|"
  std::string method_;       // init, append, overwrite

  std::vector<std::pair<std::string, std::string>>
      vertex_loading_config_;  // <vertex_label_name_, file_path_>
  std::vector<std::tuple<std::string, std::string, std::string, int32_t,
                         int32_t, std::string>>
      edge_loading_config_;  // <src_label, dst_label,
                             // edge_label, src_pri_key_ind,
                             // dst_pri_key_ind, file_path>

  static LoadingConfig ParseFromConfigYaml(const std::string& yaml_file) {
    LoadingConfig load_config;
    if (!bulk_load_config.empty() &&
        std::filesystem::exists(bulk_load_config)) {
      if (!config_parsing::parse_bulk_load_config_file(
              bulk_load_config, load_config.data_source_,
              load_config.delimiter_, load_config.method_,
              load_config.vertex_loading_config_,
              load_config.edge_loading_config_)) {
        LOG(FATAL) << "Failed to parse bulk load config file: "
                   << bulk_load_config;
      }
    }
    return load_config;
  }
};

}  // namespace gs

#endif  // STORAGE_RT_MUTABLE_GRAPH_LOADING_CONFIG_H_
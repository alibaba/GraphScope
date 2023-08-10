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

#ifndef STORAGE_RT_MUTABLE_GRAPH_LOAD_CONFIG_H_
#define STORAGE_RT_MUTABLE_GRAPH_LOAD_CONFIG_H_

#include <string>

namespace gs {
// Provide meta info about bulk loading.
struct LoadingConfig {
  std::string data_source_;  // "file", "hdfs", "oss", "s3"
  std::string delimiter_;    // "\t", ",", " ", "|"
  std::string method_;       // init, append, overwrite
};

// Meta info about vertex/edge loading.
struct VertexLoadingMeta {
  VertexLoadingMeta() = default;
  VertexLoadingMeta(const std::string& vertex_label_name,
                    const std::string& file_path)
      : vertex_label_name_(vertex_label_name), file_path_(file_path) {}
  std::string vertex_label_name_;
  std::string file_path_;
};

struct EdgeLoadingMeta {
  EdgeLoadingMeta(const std::string& src_label, const std::string& dst_label,
                  const std::string& edge_label, int32_t src_primary_key_ind,
                  int32_t dst_primary_key_ind, const std::string& file_path)
      : src_label_(src_label),
        dst_label_(dst_label),
        edge_label_(edge_label),
        src_primary_key_ind_(src_primary_key_ind),
        dst_primary_key_ind_(dst_primary_key_ind),
        file_path_(file_path) {}

  std::string src_label_;
  std::string dst_label_;
  std::string edge_label_;
  int32_t src_primary_key_ind_;
  int32_t dst_primary_key_ind_;
  std::string file_path_;
};

}  // namespace gs

#endif  // STORAGE_RT_MUTABLE_GRAPH_LOAD_CONFIG_H_
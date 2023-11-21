
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

#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_FRAGMENT_LOADER_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_FRAGMENT_LOADER_H_

#include "flex/storages/rt_mutable_graph/loader/basic_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loader/i_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include "arrow/util/value_parsing.h"

#include "grape/util.h"

namespace gs {

// LoadFragment for csv files.
class CSVFragmentLoader : public IFragmentLoader {
 public:
  CSVFragmentLoader(const Schema& schema, const LoadingConfig& loading_config,
                    int32_t thread_num)
      : loading_config_(loading_config),
        schema_(schema),
        thread_num_(thread_num),
        basic_fragment_loader_(schema_),
        read_vertex_table_time_(0),
        read_edge_table_time_(0),
        convert_to_internal_vertex_time_(0),
        convert_to_internal_edge_time_(0),
        basic_frag_loader_vertex_time_(0),
        basic_frag_loader_edge_time_(0) {
    vertex_label_num_ = schema_.vertex_label_num();
    edge_label_num_ = schema_.edge_label_num();
  }

  ~CSVFragmentLoader() {}

  FragmentLoaderType GetFragmentLoaderType() const override {
    return FragmentLoaderType::kCSVFragmentLoader;
  }

  void LoadFragment(MutablePropertyFragment& fragment) override;

 private:
  void loadVertices();

  void loadEdges();

  void addVertices(label_t v_label_id, const std::vector<std::string>& v_files);

  template <typename KEY_T>
  void addVerticesImpl(label_t v_label_id, const std::string& v_label_name,
                       const std::vector<std::string> v_file,
                       IdIndexer<KEY_T, vid_t>& indexer);

  template <typename KEY_T>
  void addVertexBatch(
      label_t v_label_id, IdIndexer<KEY_T, vid_t>& indexer,
      std::shared_ptr<arrow::Array>& primary_key_col,
      const std::vector<std::shared_ptr<arrow::Array>>& property_cols);

  template <typename KEY_T>
  void addVertexBatch(
      label_t v_label_id, IdIndexer<KEY_T, vid_t>& indexer,
      std::shared_ptr<arrow::ChunkedArray>& primary_key_col,
      const std::vector<std::shared_ptr<arrow::ChunkedArray>>& property_cols);

  void addEdges(label_t src_label_id, label_t dst_label_id, label_t e_label_id,
                const std::vector<std::string>& e_files);

  template <typename EDATA_T>
  void addEdgesImpl(label_t src_label_id, label_t dst_label_id,
                    label_t e_label_id,
                    const std::vector<std::string>& e_files);

  void fillEdgeReaderMeta(arrow::csv::ReadOptions& read_options,
                          arrow::csv::ParseOptions& parse_options,
                          arrow::csv::ConvertOptions& convert_options,
                          const std::string& e_file, label_t src_label_id,
                          label_t dst_label_id, label_t label_id) const;

  void fillVertexReaderMeta(arrow::csv::ReadOptions& read_options,
                            arrow::csv::ParseOptions& parse_options,
                            arrow::csv::ConvertOptions& convert_options,
                            const std::string& v_file, label_t v_label) const;

  const LoadingConfig& loading_config_;
  const Schema& schema_;
  size_t vertex_label_num_, edge_label_num_;
  int32_t thread_num_;

  mutable BasicFragmentLoader basic_fragment_loader_;

  std::atomic<double> read_vertex_table_time_, read_edge_table_time_;
  std::atomic<double> convert_to_internal_vertex_time_,
      convert_to_internal_edge_time_;
  std::atomic<double> basic_frag_loader_vertex_time_,
      basic_frag_loader_edge_time_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_CSV_FRAGMENT_LOADER_H_
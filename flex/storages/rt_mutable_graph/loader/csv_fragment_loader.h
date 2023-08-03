
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

  ~CSVFragmentLoader() {
    LOG(INFO) << "Performance of CSVFragmentLoader:";
    LOG(INFO) << "read_vertex_table_time: " << read_vertex_table_time_;
    LOG(INFO) << "read_edge_table_time: " << read_edge_table_time_;
    LOG(INFO) << "convert_to_internal_vertex_time: "
              << convert_to_internal_vertex_time_;
    LOG(INFO) << "convert_to_internal_edge_time: "
              << convert_to_internal_edge_time_;
    LOG(INFO) << "basic_frag_loader_vertex_time: "
              << basic_frag_loader_vertex_time_;
    LOG(INFO) << "basic_frag_loader_edge_time: "
              << basic_frag_loader_edge_time_;
  }

  FragmentLoaderType GetFragmentLoaderType() const override {
    return FragmentLoaderType::kCSVFragmentLoader;
  }

  void LoadFragment(MutablePropertyFragment& fragment) override;

 private:
  void loadVertices();

  void loadEdges();

  void addVertices(label_t v_label_id, const std::vector<std::string>& v_files);

  void addVerticesImpl(label_t v_label_id, const std::string& v_label_name,
                       const std::vector<std::string> v_file,
                       IdIndexer<oid_t, vid_t>& indexer);

  void addVerticesImplWithStreamReader(const std::string& filename,
                                       label_t v_label_id,
                                       IdIndexer<oid_t, vid_t>& indexer);

  void addVerticesImplWithTableReader(const std::string& filename,
                                      label_t v_label_id,
                                      IdIndexer<oid_t, vid_t>& indexer);

  void addVertexBatch(
      label_t v_label_id, IdIndexer<oid_t, vid_t>& indexer,
      std::shared_ptr<arrow::Array>& primary_key_col,
      const std::vector<std::shared_ptr<arrow::Array>>& property_cols);

  void addVertexBatch(
      label_t v_label_id, IdIndexer<oid_t, vid_t>& indexer,
      std::shared_ptr<arrow::ChunkedArray>& primary_key_col,
      const std::vector<std::shared_ptr<arrow::ChunkedArray>>& property_cols);

  void addEdges(label_t src_label_id, label_t dst_label_id, label_t e_label_id,
                const std::vector<std::string>& e_files);

  template <typename EDATA_T>
  void addEdgesImpl(label_t src_label_id, label_t dst_label_id,
                    label_t e_label_id,
                    const std::vector<std::string>& e_files);

  template <typename EDATA_T>
  void addEdgesImplWithStreamReader(
      const std::string& file_name, label_t src_label_id, label_t dst_label_id,
      label_t e_label_id, std::vector<int32_t>& ie_degree,
      std::vector<int32_t>& oe_degree,
      std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& edges);

  template <typename EDATA_T>
  void addEdgesImplWithTableReader(
      const std::string& filename, label_t src_label_id, label_t dst_label_id,
      label_t e_label_id, std::vector<int32_t>& ie_degree,
      std::vector<int32_t>& oe_degree,
      std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& edges);

  // Create VertexStreamReader
  std::shared_ptr<arrow::csv::StreamingReader> createVertexStreamReader(
      label_t v_label, const std::string& v_file);

  // Create VertexTableReader
  std::shared_ptr<arrow::csv::TableReader> createVertexTableReader(
      label_t v_label, const std::string& v_file);

  // Create EdgeStreamReader
  std::shared_ptr<arrow::csv::StreamingReader> createEdgeStreamReader(
      label_t src_label_id, label_t dst_label_id, label_t e_label,
      const std::string& e_file);

  // Create EdgeTableReader
  std::shared_ptr<arrow::csv::TableReader> createEdgeTableReader(
      label_t src_label_id, label_t dst_label_id, label_t e_label,
      const std::string& e_file);

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
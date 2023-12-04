
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

#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_ODPS_FRAGMENT_LOADER_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_ODPS_FRAGMENT_LOADER_H_

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>

#include <boost/convert.hpp>
#include <boost/convert/strtol.hpp>
#include <charconv>

#include "arrow/util/value_parsing.h"
#include "common/configuration.h"
#include "flex/storages/rt_mutable_graph/loader/basic_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loader/i_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loader/loader_factory.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/third_party/httplib.h"
#include "grape/util.h"
#include "nlohmann/json.hpp"
#include "storage_api.hpp"
#include "storage_api_arrow.hpp"

using apsara::odps::sdk::storage_api::TableBatchScanResp;
using apsara::odps::sdk::storage_api::TableBatchWriteResp;
using apsara::odps::sdk::storage_api::TableIdentifier;
using apsara::odps::sdk::storage_api::arrow_adapter::ArrowClient;

namespace gs {

template <typename T>
struct ConvertTo {};

template <>
struct ConvertTo<double> {
  static double convert(const std::string_view& str) {
    auto value = boost::convert<double>(str, boost::cnv::strtol());
    if (value.has_value()) {
      return value.get();
    }
    LOG(FATAL) << "Fail to convert " << str << ", to double";
  }
};

// LoadFragment for csv files.
class ODPSFragmentLoader : public IFragmentLoader {
 public:
  ODPSFragmentLoader(const std::string& work_dir, const Schema& schema,
                     const LoadingConfig& loading_config, int32_t thread_num)
      : loading_config_(loading_config),
        schema_(schema),
        thread_num_(thread_num),
        basic_fragment_loader_(schema_, work_dir),
        read_vertex_table_time_(0),
        read_edge_table_time_(0),
        convert_to_internal_vertex_time_(0),
        convert_to_internal_edge_time_(0),
        basic_frag_loader_vertex_time_(0),
        basic_frag_loader_edge_time_(0) {
    vertex_label_num_ = schema_.vertex_label_num();
    edge_label_num_ = schema_.edge_label_num();
    if (thread_num_ > MAX_PRODUCER_NUM) {
      thread_num_ = MAX_PRODUCER_NUM;
      LOG(WARNING) << "thread_num_ is too large, set to " << MAX_PRODUCER_NUM;
    }
    // get dump_to_csv from env DUMP_TO_CSV
    char* dump_to_csv = std::getenv("DUMP_TO_CSV");
    if (dump_to_csv == nullptr) {
      LOG(WARNING) << "DUMP_TO_CSV is not set";
    }
    std::string tmp = dump_to_csv;
    LOG(INFO) << "dump_to_csv: " << tmp;
    if (tmp == "true" || tmp == "TRUE") {
      dump_to_csv_ = true;
    } else {
      dump_to_csv_ = false;
    }
    LOG(INFO) << "dump to csv " << dump_to_csv_;
    if (dump_to_csv_) {
      // get output_directory from env OUTPUT_DIRECTORY
      char* output_directory = std::getenv("OUTPUT_DIRECTORY");
      if (output_directory == nullptr) {
        LOG(FATAL) << "OUTPUT_DIRECTORY is not set";
      }
      output_directory_ = output_directory;
      LOG(INFO) << "output_directory: " << output_directory_;
    }
  }

  static std::shared_ptr<IFragmentLoader> Make(
      const std::string& work_dir, const Schema& schema,
      const LoadingConfig& loading_config, int32_t thread_num);

  ~ODPSFragmentLoader() {}

  void LoadFragment() override;

 private:
  std::shared_ptr<ArrowClient> getArrowClient(int connect_timeout = 5,
                                              int rw_timeout = 10);

  void dump_table_to_csv(std::shared_ptr<arrow::Table> table,
                         const std::string& table_name);

  TableBatchScanResp createReadSession(
      const TableIdentifier& table_identifier,
      const std::vector<std::string>& selected_cols,
      const std::vector<std::string>& partition_cols,
      const std::vector<std::string>& selected_partitions);

  TableBatchScanResp getReadSession(std::string session_id,
                                    const TableIdentifier& table_identifier);

  void getReadSessionStatus(const std::string& session_id, int* split_count,
                            const TableIdentifier& table_identifier);

  void preprocessRead(std::string* session_id, int* split_count,
                      const TableIdentifier& table_identifier,
                      const std::vector<std::string>& selected_cols,
                      const std::vector<std::string>& partition_cols,
                      const std::vector<std::string>& selected_partitions);

  void init();

  void loadVertices();

  void loadEdges();

  void addVertices(label_t v_label_id, const std::vector<std::string>& v_files);

  template <typename KEY_T>
  void addVerticesImpl(label_t v_label_id, const std::string& v_label_name,
                       const std::vector<std::string> v_file,
                       IdIndexer<KEY_T, vid_t>& indexer);

  template <typename KEY_T>
  void addVerticesImplWithStreamReader(const std::string& filename,
                                       label_t v_label_id,
                                       IdIndexer<KEY_T, vid_t>& indexer);

  template <typename KEY_T>
  void addVerticesImplWithTableReader(const std::string& filename,
                                      label_t v_label_id,
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
      std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& parsed_edges);

  void parseLocation(const std::string& odps_table_path,
                     TableIdentifier& table_identifier,
                     std::vector<std::string>& partition_names,
                     std::vector<std::string>& selected_partitions);

  std::vector<std::string> columnMappingsToSelectedCols(
      const std::vector<std::tuple<size_t, std::string, std::string>>&
          column_mappings);

  std::shared_ptr<arrow::Table> readTable(const std::string& session_id,
                                          int split_count,
                                          const TableIdentifier& table_id);
  void producerRoutine(
      const std::string& session_id, const TableIdentifier& table_identifier,
      std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>&
          all_batches_,
      std::vector<int> indices);

  bool readRows(std::string session_id, const TableIdentifier& table_identifier,
                std::vector<std::shared_ptr<arrow::RecordBatch>>& res_batches,
                int split_index);

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

  // odps table related
  std::string access_id_;
  std::string access_key_;
  std::string odps_endpoint_;
  std::string tunnel_endpoint_;
  std::string output_directory_;
  std::shared_ptr<ArrowClient> arrow_client_ptr_;

  static const bool registered_;
  size_t MAX_PRODUCER_NUM = 8;

  bool dump_to_csv_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_ODPS_FRAGMENT_LOADER_H_

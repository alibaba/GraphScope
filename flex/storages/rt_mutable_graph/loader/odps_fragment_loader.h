
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

#include <charconv>

#include "arrow/util/value_parsing.h"
#include "common/configuration.h"
#include "flex/storages/rt_mutable_graph/loader/abstract_arrow_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loader/basic_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loader/i_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loader/loader_factory.h"
#include "flex/storages/rt_mutable_graph/loader/odps_client.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/third_party/httplib.h"
#include "grape/util.h"

namespace gs {

class ODPSStreamRecordBatchSupplier : public IRecordBatchSupplier {
 public:
  ODPSStreamRecordBatchSupplier(label_t label_id, const std::string& file_path,
                                const ODPSReadClient& odps_table_reader,
                                const std::string& session_id, int split_count,
                                TableIdentifier table_identifier, int worker_id,
                                int worker_num);

  std::shared_ptr<arrow::RecordBatch> GetNextBatch() override;

 private:
  label_t label_id_;
  std::string file_path_;
  const ODPSReadClient& odps_read_client_;
  std::string session_id_;
  int split_count_;
  TableIdentifier table_identifier_;

  int32_t cur_split_index_;
  int32_t worker_num_;
  ReadRowsReq read_rows_req_;
  std::shared_ptr<Reader> cur_batch_reader_;

  // temporally store the string data from each record batch
  std::vector<std::shared_ptr<arrow::Array>> string_arrays_;
};

class ODPSTableRecordBatchSupplier : public IRecordBatchSupplier {
 public:
  ODPSTableRecordBatchSupplier(label_t label_id, const std::string& file_path,
                               const ODPSReadClient& odps_table_reader,
                               const std::string& session_id, int split_count,
                               TableIdentifier table_identifier,
                               int thread_num);

  std::shared_ptr<arrow::RecordBatch> GetNextBatch() override;

 private:
  label_t label_id_;
  std::string file_path_;
  const ODPSReadClient& odps_read_client_;
  std::string session_id_;
  int split_count_;
  TableIdentifier table_identifier_;

  std::shared_ptr<arrow::Table> table_;
  std::shared_ptr<arrow::TableBatchReader> reader_;
};

/*
 * ODPSFragmentLoader is used to load graph data from ODPS Table.
 * It fetch the data via ODPS tunnel/halo API.
 * You need to set the following environment variables:
 * 1. ODPS_ACCESS_ID
 * 2. ODPS_ACCESS_KEY
 * 3. ODPS_ENDPOINT
 * 4. ODPS_TUNNEL_ENDPOINT(optional)
 */
class ODPSFragmentLoader : public AbstractArrowFragmentLoader {
 public:
  ODPSFragmentLoader(const std::string& work_dir, const Schema& schema,
                     const LoadingConfig& loading_config, int32_t thread_num)
      : AbstractArrowFragmentLoader(work_dir, schema, loading_config,
                                    thread_num) {}

  static std::shared_ptr<IFragmentLoader> Make(
      const std::string& work_dir, const Schema& schema,
      const LoadingConfig& loading_config, int32_t thread_num);

  ~ODPSFragmentLoader() {}

  void LoadFragment() override;

 private:
  void init();

  void parseLocation(const std::string& odps_table_path,
                     TableIdentifier& table_identifier,
                     std::vector<std::string>& partition_names,
                     std::vector<std::string>& selected_partitions);

  void loadVertices();

  void loadEdges();

  void addVertices(label_t v_label_id, const std::vector<std::string>& v_files);

  void addEdges(label_t src_label_id, label_t dst_label_id, label_t e_label_id,
                const std::vector<std::string>& e_files);

  std::vector<std::string> columnMappingsToSelectedCols(
      const std::vector<std::tuple<size_t, std::string, std::string>>&
          column_mappings);

  ODPSReadClient odps_read_client_;

  static const bool registered_;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_ODPS_FRAGMENT_LOADER_H_

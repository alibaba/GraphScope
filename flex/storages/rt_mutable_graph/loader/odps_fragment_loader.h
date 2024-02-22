
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
#include "flex/storages/rt_mutable_graph/loader/abstract_arrow_fragment_loader.h"
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

using apsara::odps::sdk::AliyunAccount;
using apsara::odps::sdk::Configuration;
using apsara::odps::sdk::storage_api::ReadRowsReq;
using apsara::odps::sdk::storage_api::SessionReq;
using apsara::odps::sdk::storage_api::SessionStatus;
using apsara::odps::sdk::storage_api::SplitOptions;
using apsara::odps::sdk::storage_api::TableBatchScanReq;
using apsara::odps::sdk::storage_api::TableBatchScanResp;
using apsara::odps::sdk::storage_api::TableBatchWriteReq;
using apsara::odps::sdk::storage_api::TableBatchWriteResp;
using apsara::odps::sdk::storage_api::TableIdentifier;
using apsara::odps::sdk::storage_api::WriteRowsReq;
using apsara::odps::sdk::storage_api::arrow_adapter::ArrowClient;
using apsara::odps::sdk::storage_api::arrow_adapter::Reader;

namespace gs {

class ODPSReadClient {
 public:
  static constexpr const int CONNECTION_TIMEOUT = 5;
  static constexpr const int READ_WRITE_TIMEOUT = 10;
  ODPSReadClient();

  ~ODPSReadClient();

  void init();

  void CreateReadSession(std::string* session_id, int* split_count,
                         const TableIdentifier& table_identifier,
                         const std::vector<std::string>& selected_cols,
                         const std::vector<std::string>& partition_cols,
                         const std::vector<std::string>& selected_partitions);

  std::shared_ptr<arrow::Table> ReadTable(const std::string& session_id,
                                          int split_count,
                                          const TableIdentifier& table_id,
                                          int thread_num) const;

  std::shared_ptr<ArrowClient> GetArrowClient() const;

 private:
  TableBatchScanResp createReadSession(
      const TableIdentifier& table_identifier,
      const std::vector<std::string>& selected_cols,
      const std::vector<std::string>& partition_cols,
      const std::vector<std::string>& selected_partitions);

  TableBatchScanResp getReadSession(std::string session_id,
                                    const TableIdentifier& table_identifier);

  void getReadSessionStatus(const std::string& session_id, int* split_count,
                            const TableIdentifier& table_identifier);

  void producerRoutine(
      const std::string& session_id, const TableIdentifier& table_identifier,
      std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>&
          all_batches_,
      std::vector<int>&& indices) const;

  bool readRows(std::string session_id, const TableIdentifier& table_identifier,
                std::vector<std::shared_ptr<arrow::RecordBatch>>& res_batches,
                int split_index) const;

 private:
  // odps table related
  std::string access_id_;
  std::string access_key_;
  std::string odps_endpoint_;
  std::string tunnel_endpoint_;
  std::string output_directory_;
  std::shared_ptr<ArrowClient> arrow_client_ptr_;
  size_t MAX_PRODUCER_NUM = 8;
  size_t MAX_RETRY = 5;
};

class ODPSStreamRecordBatchSupplier : public IRecordBatchSupplier {
 public:
  ODPSStreamRecordBatchSupplier(label_t label_id, const std::string& file_path,
                                const ODPSReadClient& odps_table_reader,
                                const std::string& session_id, int split_count,
                                TableIdentifier table_identifier);

  std::shared_ptr<arrow::RecordBatch> GetNextBatch() override;

 private:
  std::string file_path_;
  const ODPSReadClient& odps_read_client_;
  std::string session_id_;
  int split_count_;
  TableIdentifier table_identifier_;

  int32_t cur_split_index_;
  ReadRowsReq read_rows_req_;
  std::shared_ptr<Reader> cur_batch_reader_;
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
  std::string file_path_;
  const ODPSReadClient& odps_read_client_;
  std::string session_id_;
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

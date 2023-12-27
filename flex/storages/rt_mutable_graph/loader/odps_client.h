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

#ifndef FLEX_STORAGE_RT_MUTABLE_GRAPH_LOADER_ODPS_CLIENT_H_
#define FLEX_STORAGE_RT_MUTABLE_GRAPH_LOADER_ODPS_CLIENT_H_

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/convert.hpp>
#include <boost/convert/strtol.hpp>
#include <boost/lexical_cast.hpp>

#include "nlohmann/json.hpp"
#include "storage_api.hpp"
#include "storage_api_arrow.hpp"

#include "glog/logging.h"

namespace gs {
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

}  // namespace gs

#endif  // FLEX_STORAGE_RT_MUTABLE_GRAPH_LOADER_ODPS_CLIENT_H_

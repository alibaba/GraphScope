
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

#include "flex/storages/rt_mutable_graph/loader/odps_client.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"

namespace gs {

ODPSReadClient::ODPSReadClient() {}
ODPSReadClient::~ODPSReadClient() {}

void ODPSReadClient::init() {
  char* env = std::getenv("ODPS_ACCESS_ID");
  if (env != nullptr) {
    access_id_ = env;
  } else {
    LOG(FATAL) << "ODPS_ACCESS_ID is not set";
  }
  env = std::getenv("ODPS_ACCESS_KEY");
  if (env != nullptr) {
    access_key_ = env;
  } else {
    LOG(FATAL) << "ODPS_ACCESS_KEY is not set";
  }
  env = std::getenv("ODPS_ENDPOINT");
  if (env != nullptr) {
    odps_endpoint_ = env;
  } else {
    LOG(FATAL) << "ODPS_ENDPOINT is not set";
  }
  env = std::getenv("TUNNEL_ENDPOINT");
  if (env != nullptr) {
    tunnel_endpoint_ = env;
  } else {
    LOG(WARNING) << "TUNNEL_ENDPOINT is not set";
  }
  AliyunAccount aliyun_account(access_id_, access_key_);
  Configuration configuration;
  configuration.SetSocketConnectTimeout(CONNECTION_TIMEOUT);
  configuration.SetSocketTimeout(READ_WRITE_TIMEOUT);
  configuration.SetAccount(aliyun_account);
  configuration.SetEndpoint(odps_endpoint_);
  configuration.SetTunnelEndpoint(tunnel_endpoint_);
  arrow_client_ptr_ = std::make_shared<ArrowClient>(configuration);
}

std::shared_ptr<ArrowClient> ODPSReadClient::GetArrowClient() const {
  return arrow_client_ptr_;
}

TableBatchScanResp ODPSReadClient::createReadSession(
    const TableIdentifier& table_identifier,
    const std::vector<std::string>& selected_cols,
    const std::vector<std::string>& partition_cols,
    const std::vector<std::string>& selected_partitions) {
  VLOG(1) << "CreateReadSession:" << table_identifier.project_ << ", "
          << table_identifier.table_;
  VLOG(1) << "Selected cols:" << gs::to_string(selected_cols);
  VLOG(1) << "Partition:" << gs::to_string(partition_cols);
  VLOG(1) << "Selected partitions:" << gs::to_string(selected_partitions);

  TableBatchScanReq req;
  req.table_identifier_ = table_identifier;
  req.split_options_ = SplitOptions::GetDefaultOptions(SplitOptions::SIZE);
  req.split_options_.split_number_ = 64 * 1024 * 1024;

  if (!partition_cols.empty()) {
    req.required_partitions_ = selected_partitions;
  }
  req.required_data_columns_ = selected_cols;
  // req.required_partition_columns_ = ["p1", "p2"];
  // req.required_partition_ = ["p1=t1/p2=t2"];
  // req.required_data_columns_ = ["col1", "col2"];

  TableBatchScanResp resp;
  arrow_client_ptr_->CreateReadSession(req, resp);
  return resp;
}

TableBatchScanResp ODPSReadClient::getReadSession(
    std::string session_id, const TableIdentifier& table_identifier) {
  SessionReq req;
  req.session_id_ = session_id;
  req.table_identifier_ = table_identifier;

  TableBatchScanResp resp;
  arrow_client_ptr_->GetReadSession(req, resp);
  return resp;
}

void ODPSReadClient::CreateReadSession(
    std::string* session_id, int* split_count,
    const TableIdentifier& table_identifier,
    const std::vector<std::string>& selected_cols,
    const std::vector<std::string>& partition_cols,
    const std::vector<std::string>& selected_partitions) {
  auto resp = createReadSession(table_identifier, selected_cols, partition_cols,
                                selected_partitions);
  if (resp.status_ != apsara::odps::sdk::storage_api::Status::OK &&
      resp.status_ != apsara::odps::sdk::storage_api::Status::WAIT) {
    LOG(FATAL) << "CreateReadSession failed" << resp.error_message_;
  }
  *session_id = resp.session_id_;

  getReadSessionStatus(*session_id, split_count, table_identifier);
  VLOG(1) << "Got split_count: " << *split_count;
}

void ODPSReadClient::getReadSessionStatus(
    const std::string& session_id, int* split_count,
    const TableIdentifier& table_identifier) {
  TableBatchScanResp resp;
  while (resp.session_status_ != SessionStatus::NORMAL) {
    resp = getReadSession(session_id, table_identifier);
    *split_count = resp.split_count_;
    LOG(WARNING) << "GetReadSession failed: " << resp.error_message_
                 << ", retrying...";
    if (resp.session_status_ == SessionStatus::CRITICAL) {
      LOG(FATAL) << "CreateReadSession failed: " << resp.error_message_;
    }
    if (resp.session_status_ == SessionStatus::EXPIRED) {
      LOG(FATAL) << "CreateReadSession expired: " << resp.error_message_;
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
}

void ODPSReadClient::producerRoutine(
    const std::string& session_id, const TableIdentifier& table_identifier,
    std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>& all_batches,
    std::vector<int>&& indices) const {
  for (int i : indices) {
    for (auto j = 0; j < MAX_RETRY; ++j) {
      bool st = readRows(session_id, table_identifier, all_batches[i], i);
      if (!st) {
        LOG(ERROR) << "Read split " << i << " error";
        LOG(ERROR) << "Retry " << j << "/ " << MAX_RETRY;
      } else {
        break;
      }
    }
  }
}

bool ODPSReadClient::readRows(
    std::string session_id, const TableIdentifier& table_identifier,
    std::vector<std::shared_ptr<arrow::RecordBatch>>& res_batches,
    int split_index) const {
  ReadRowsReq req;
  req.table_identifier_ = table_identifier;
  req.session_id_ = session_id;
  req.split_index_ = split_index;

  auto reader = arrow_client_ptr_->ReadRows(req);
  std::shared_ptr<arrow::RecordBatch> record_batch;
  while (reader->Read(record_batch)) {
    res_batches.push_back(record_batch);
  }
  if (reader->GetStatus() != apsara::odps::sdk::storage_api::Status::OK) {
    LOG(ERROR) << "read rows error: " << reader->GetErrorMessage();
    return false;
  }
  return true;
}

// Read table from halo, the producer number
std::shared_ptr<arrow::Table> ODPSReadClient::ReadTable(
    const std::string& session_id, int split_count,
    const TableIdentifier& table_id, int thread_num) const {
  std::vector<std::thread> producers;
  int cur_thread_num = std::max(1, (int) MAX_PRODUCER_NUM / thread_num);
  cur_thread_num = std::min(cur_thread_num, split_count);

  auto split_indices = [](int id, int part, int total) -> std::vector<int> {
    int share = total / part;
    std::vector<int> indices;
    int start = share * id;
    int end = std::min(start + share, total);
    for (int i = start; i < end; ++i) {
      indices.push_back(i);
    }
    int index_in_remainder = share * part + id;
    if (index_in_remainder < total) {
      indices.push_back(index_in_remainder);
    }
    return indices;
  };
  VLOG(10) << "Reading table with " << cur_thread_num << " threads";

  std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> all_batches;
  all_batches.resize(split_count);
  for (auto i = 0; i < cur_thread_num; ++i) {
    auto indices = split_indices(i, cur_thread_num, split_count);
    LOG(INFO) << "Thread " << i << " will read " << indices.size()
              << " splits of " << split_count << " splits: " << gs::to_string(indices);
    producers.emplace_back(std::thread(
        &ODPSReadClient::producerRoutine, this, std::cref(session_id),
        std::cref(table_id), std::ref(all_batches), std::move(indices)));
    // sleep for 1 second
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  for (auto& th : producers) {
    if (th.joinable()) {
      th.join();
    }
  }
  LOG(INFO) << "All producers finished";
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  for (auto& sub_batches : all_batches) {
    batches.insert(batches.end(), sub_batches.begin(), sub_batches.end());
  }
  auto result = arrow::Table::FromRecordBatches(batches);
  if (!result.ok()) {
    LOG(FATAL) << "Fail to convert record batches to table";
  }
  auto table = result.ValueOrDie();
  CHECK(table != nullptr);
  all_batches.clear();
  LOG(INFO) << "[table-" << table_id.table_
            << "] contains: " << table->num_rows() << " rows, "
            << table->num_columns() << " columns";
  return table;
}

}  // namespace gs

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

#include "flex/storages/rt_mutable_graph/loader/odps_fragment_loader.h"

#include <arrow/csv/api.h>

#include <filesystem>

#include "arrow/ipc/api.h"
#include "arrow/status.h"
#include "boost/algorithm/algorithm.hpp"
#include "boost/algorithm/string.hpp"
#include "storage_api.hpp"

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
  while (true) {
    resp = getReadSession(session_id, table_identifier);
    if (resp.session_status_ == SessionStatus::NORMAL) {
      *split_count = resp.split_count_;
      break;
    }
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
    for (size_t j = 0; j < MAX_RETRY; ++j) {
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
  for (int32_t i = 0; i < cur_thread_num; ++i) {
    auto indices = split_indices(i, cur_thread_num, split_count);
    LOG(INFO) << "Thread " << i << " will read " << indices.size()
              << " splits of " << split_count << " splits";
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

////////////////ODPSStreamRecordBatchSupplier/////////////////

ODPSStreamRecordBatchSupplier::ODPSStreamRecordBatchSupplier(
    label_t label_id, const std::string& file_path,
    const ODPSReadClient& odps_table_reader, const std::string& session_id,
    int split_count, TableIdentifier table_identifier)
    : file_path_(file_path),
      odps_read_client_(odps_table_reader),
      session_id_(session_id),
      split_count_(split_count),
      table_identifier_(table_identifier),
      cur_split_index_(0) {
  read_rows_req_.table_identifier_ = table_identifier_;
  read_rows_req_.session_id_ = session_id_;
  read_rows_req_.split_index_ = cur_split_index_;
  read_rows_req_.max_batch_rows_ = 32768;
  cur_batch_reader_ =
      odps_read_client_.GetArrowClient()->ReadRows(read_rows_req_);
}

std::shared_ptr<arrow::RecordBatch>
ODPSStreamRecordBatchSupplier::GetNextBatch() {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  if (!cur_batch_reader_) {
    return record_batch;
  }
  while (true) {
    if (!cur_batch_reader_->Read(record_batch)) {
      if (cur_batch_reader_->GetStatus() !=
          apsara::odps::sdk::storage_api::Status::OK) {
        LOG(ERROR) << "read rows error: "
                   << cur_batch_reader_->GetErrorMessage() << ", "
                   << cur_batch_reader_->GetStatus()
                   << ", split id: " << cur_split_index_;
        cur_batch_reader_ =
            odps_read_client_.GetArrowClient()->ReadRows(read_rows_req_);
      } else {
        VLOG(1) << "Read split " << cur_split_index_ << " finished";
        // move to next split
        ++cur_split_index_;
        if (cur_split_index_ >= split_count_) {
          VLOG(1) << "Finish Read all splits";
          cur_batch_reader_.reset();
          break;
        } else {
          VLOG(1) << "Start reading split " << cur_split_index_;
          read_rows_req_.split_index_ = cur_split_index_;
          cur_batch_reader_ =
              odps_read_client_.GetArrowClient()->ReadRows(read_rows_req_);
        }
      }
    } else {
      break;
    }
  }
  return record_batch;
}

////////////////ODPSTableRecordBatchSupplier/////////////////
ODPSTableRecordBatchSupplier::ODPSTableRecordBatchSupplier(
    label_t label_id, const std::string& file_path,
    const ODPSReadClient& odps_table_reader, const std::string& session_id,
    int split_count, TableIdentifier table_identifier, int thread_num)
    : file_path_(file_path),
      odps_read_client_(odps_table_reader),
      session_id_(session_id),
      table_identifier_(table_identifier) {
  // Read the table.
  table_ = odps_read_client_.ReadTable(session_id, split_count,
                                       table_identifier, thread_num);
  reader_ = std::make_shared<arrow::TableBatchReader>(*table_);
}

std::shared_ptr<arrow::RecordBatch>
ODPSTableRecordBatchSupplier::GetNextBatch() {
  std::shared_ptr<arrow::RecordBatch> batch;
  auto status = reader_->ReadNext(&batch);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to read batch from file: " << file_path_
               << " error: " << status.message();
  }
  return batch;
}

////////////////ODPSFragmentLoader/////////////////

std::shared_ptr<IFragmentLoader> ODPSFragmentLoader::Make(
    const std::string& work_dir, const Schema& schema,
    const LoadingConfig& loading_config, int32_t thread_num) {
  return std::shared_ptr<IFragmentLoader>(
      new ODPSFragmentLoader(work_dir, schema, loading_config, thread_num));
}
void ODPSFragmentLoader::init() { odps_read_client_.init(); }

void ODPSFragmentLoader::LoadFragment() {
  init();
  loadVertices();
  loadEdges();

  basic_fragment_loader_.LoadFragment();
}

// odps_table_path is like /project_name/table_name/partition_name
// partition : pt1
// selected partitions pt1=1,
void ODPSFragmentLoader::parseLocation(
    const std::string& odps_table_path, TableIdentifier& table_identifier,
    std::vector<std::string>& res_partitions,
    std::vector<std::string>& selected_partitions) {
  LOG(INFO) << "Parse real path: " << odps_table_path;

  std::vector<std::string> splits;
  boost::split(splits, odps_table_path, boost::is_any_of("/"));
  // the first one is empty
  CHECK(splits.size() >= 2) << "Invalid odps table path: " << odps_table_path;
  table_identifier.project_ = splits[0];
  table_identifier.table_ = splits[1];

  if (splits.size() == 3) {
    boost::split(selected_partitions, splits[2], boost::is_any_of(","));
    std::vector<std::string> partitions;
    for (size_t i = 0; i < selected_partitions.size(); ++i) {
      partitions.emplace_back(selected_partitions[i].substr(
          0, selected_partitions[i].find_first_of("=")));
    }
    // dedup partitions
    std::sort(partitions.begin(), partitions.end());
    partitions.erase(std::unique(partitions.begin(), partitions.end()),
                     partitions.end());
    res_partitions = partitions;
  }
}

void ODPSFragmentLoader::addVertices(label_t v_label_id,
                                     const std::vector<std::string>& v_files) {
  auto record_batch_supplier_creator =
      [this](label_t label_id, const std::string& v_file,
             const LoadingConfig& loading_config) {
        auto vertex_column_mappings =
            loading_config_.GetVertexColumnMappings(label_id);
        std::string session_id;
        int split_count;
        TableIdentifier table_identifier;
        std::vector<std::string> partition_cols;
        std::vector<std::string> selected_partitions;
        parseLocation(v_file, table_identifier, partition_cols,
                      selected_partitions);
        auto selected_cols =
            columnMappingsToSelectedCols(vertex_column_mappings);
        odps_read_client_.CreateReadSession(
            &session_id, &split_count, table_identifier, selected_cols,
            partition_cols, selected_partitions);
        VLOG(1) << "Successfully got session_id: " << session_id
                << ", split count: " << split_count;
        if (loading_config.GetIsBatchReader()) {
          auto res = std::make_shared<ODPSStreamRecordBatchSupplier>(
              label_id, v_file, odps_read_client_, session_id, split_count,
              table_identifier);
          return std::dynamic_pointer_cast<IRecordBatchSupplier>(res);
        } else {
          auto res = std::make_shared<ODPSTableRecordBatchSupplier>(
              label_id, v_file, odps_read_client_, session_id, split_count,
              table_identifier, thread_num_);
          return std::dynamic_pointer_cast<IRecordBatchSupplier>(res);
        }
      };
  return AbstractArrowFragmentLoader::AddVerticesRecordBatch(
      v_label_id, v_files, record_batch_supplier_creator);
}

void ODPSFragmentLoader::loadVertices() {
  auto vertex_sources = loading_config_.GetVertexLoadingMeta();
  if (vertex_sources.empty()) {
    LOG(INFO) << "Skip loading vertices since no vertex source is specified.";
    return;
  }

  if (thread_num_ == 1) {
    LOG(INFO) << "Loading vertices with single thread...";
    for (auto iter = vertex_sources.begin(); iter != vertex_sources.end();
         ++iter) {
      auto v_label_id = iter->first;
      auto v_files = iter->second;
      addVertices(v_label_id, v_files);
    }
  } else {
    // copy vertex_sources and edge sources to vector, since we need to
    // use multi-thread loading.
    std::vector<std::pair<label_t, std::vector<std::string>>> vertex_files;
    for (auto iter = vertex_sources.begin(); iter != vertex_sources.end();
         ++iter) {
      vertex_files.emplace_back(iter->first, iter->second);
    }
    LOG(INFO) << "Parallel loading with " << thread_num_ << " threads, "
              << " " << vertex_files.size() << " vertex files, ";
    std::atomic<size_t> v_ind(0);
    std::vector<std::thread> threads(thread_num_);
    for (int i = 0; i < thread_num_; ++i) {
      threads[i] = std::thread([&]() {
        while (true) {
          size_t cur = v_ind.fetch_add(1);
          if (cur >= vertex_files.size()) {
            break;
          }
          auto v_label_id = vertex_files[cur].first;
          addVertices(v_label_id, vertex_files[cur].second);
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }

    LOG(INFO) << "Finished loading vertices";
  }
}

////////////////Loading edges/////////////////

void ODPSFragmentLoader::addEdges(label_t src_label_i, label_t dst_label_i,
                                  label_t edge_label_i,
                                  const std::vector<std::string>& table_paths) {
  auto lambda = [this](label_t src_label_id, label_t dst_label_id,
                       label_t e_label_id, const std::string& table_path,
                       const LoadingConfig& loading_config) {
    std::string session_id;
    int split_count;
    TableIdentifier table_identifier;
    std::vector<std::string> partition_cols;
    std::vector<std::string> selected_partitions;
    parseLocation(table_path, table_identifier, partition_cols,
                  selected_partitions);
    auto edge_column_mappings = loading_config_.GetEdgeColumnMappings(
        src_label_id, dst_label_id, e_label_id);
    auto selected_props = columnMappingsToSelectedCols(edge_column_mappings);
    // if odps fragment loader, src_column_name and dst_column_name must be
    // specified.
    auto src_dst_col_pair = loading_config_.GetEdgeSrcDstCol(
        src_label_id, dst_label_id, e_label_id);
    auto src_col_name = src_dst_col_pair.first[0].first;
    auto dst_col_name = src_dst_col_pair.second[0].first;
    if (src_col_name.empty()) {
      LOG(FATAL) << "SrcColumnName in edge table should be specified";
    }
    if (dst_col_name.empty()) {
      LOG(FATAL) << "DstColumnName in edge table should be specified";
    }
    std::vector<std::string> selected_cols;
    selected_cols.emplace_back(src_col_name);
    selected_cols.emplace_back(dst_col_name);
    selected_cols.insert(selected_cols.end(), selected_props.begin(),
                         selected_props.end());

    odps_read_client_.CreateReadSession(&session_id, &split_count,
                                        table_identifier, selected_cols,
                                        partition_cols, selected_partitions);
    VLOG(1) << "Successfully got session_id: " << session_id
            << ", split count: " << split_count;
    if (loading_config.GetIsBatchReader()) {
      auto res = std::make_shared<ODPSStreamRecordBatchSupplier>(
          e_label_id, table_path, odps_read_client_, session_id, split_count,
          table_identifier);
      return std::dynamic_pointer_cast<IRecordBatchSupplier>(res);
    } else {
      auto res = std::make_shared<ODPSTableRecordBatchSupplier>(
          e_label_id, table_path, odps_read_client_, session_id, split_count,
          table_identifier, thread_num_);
      return std::dynamic_pointer_cast<IRecordBatchSupplier>(res);
    }
  };

  AbstractArrowFragmentLoader::AddEdgesRecordBatch(
      src_label_i, dst_label_i, edge_label_i, table_paths, lambda);
}

void ODPSFragmentLoader::loadEdges() {
  auto& edge_sources = loading_config_.GetEdgeLoadingMeta();

  if (edge_sources.empty()) {
    LOG(INFO) << "Skip loading edges since no edge source is specified.";
    return;
  }

  if (thread_num_ == 1) {
    LOG(INFO) << "Loading edges with single thread...";
    for (auto iter = edge_sources.begin(); iter != edge_sources.end(); ++iter) {
      auto& src_label_id = std::get<0>(iter->first);
      auto& dst_label_id = std::get<1>(iter->first);
      auto& e_label_id = std::get<2>(iter->first);
      auto& e_files = iter->second;
      addEdges(src_label_id, dst_label_id, e_label_id, e_files);
    }
  } else {
    std::vector<std::pair<typename LoadingConfig::edge_triplet_type,
                          std::vector<std::string>>>
        edge_files;
    for (auto iter = edge_sources.begin(); iter != edge_sources.end(); ++iter) {
      edge_files.emplace_back(iter->first, iter->second);
    }
    LOG(INFO) << "Parallel loading with " << thread_num_ << " threads, "
              << edge_files.size() << " edge files.";
    std::atomic<size_t> e_ind(0);
    std::vector<std::thread> threads(thread_num_);
    for (int i = 0; i < thread_num_; ++i) {
      threads[i] = std::thread([&]() {
        while (true) {
          size_t cur = e_ind.fetch_add(1);
          if (cur >= edge_files.size()) {
            break;
          }
          auto& edge_file = edge_files[cur];
          auto src_label_id = std::get<0>(edge_file.first);
          auto dst_label_id = std::get<1>(edge_file.first);
          auto e_label_id = std::get<2>(edge_file.first);
          auto& file_names = edge_file.second;
          addEdges(src_label_id, dst_label_id, e_label_id, file_names);
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    LOG(INFO) << "Finished loading edges";
  }
}

std::vector<std::string> ODPSFragmentLoader::columnMappingsToSelectedCols(
    const std::vector<std::tuple<size_t, std::string, std::string>>&
        column_mappings) {
  std::vector<std::string> selected_cols;
  for (auto& column_mapping : column_mappings) {
    selected_cols.push_back(std::get<1>(column_mapping));
  }
  return selected_cols;
}

const bool ODPSFragmentLoader::registered_ =
    LoaderFactory::Register("odps", "arrow",
                            static_cast<LoaderFactory::loader_initializer_t>(
                                &ODPSFragmentLoader::Make));

}  // namespace gs

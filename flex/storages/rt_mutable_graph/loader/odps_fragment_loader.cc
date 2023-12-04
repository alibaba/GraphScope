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
using apsara::odps::sdk::storage_api::WriteRowsReq;

namespace gs {

template <typename KEY_T>
struct _add_vertex {
  std::vector<bool> operator()(const std::shared_ptr<arrow::Array>& col,
                               IdIndexer<KEY_T, vid_t>& indexer,
                               std::vector<vid_t>& vids) {
    return {};
  }
};

template <>
struct _add_vertex<int64_t> {
  std::vector<bool> operator()(const std::shared_ptr<arrow::Array>& col,
                               IdIndexer<int64_t, vid_t>& indexer,
                               std::vector<vid_t>& vids) {
    CHECK(col->type() == arrow::int64());
    size_t row_num = col->length();
    std::vector<bool> valid_indices;
    valid_indices.reserve(row_num);

    auto casted_array = std::static_pointer_cast<arrow::Int64Array>(col);
    vid_t vid;
    for (auto i = 0; i < row_num; ++i) {
      if (!indexer.add(casted_array->Value(i), vid)) {
        LOG(FATAL) << "Duplicate vertex id: " << casted_array->Value(i) << "..";
        valid_indices.emplace_back(false);
      } else {
        valid_indices.emplace_back(true);
      }
      vids.emplace_back(vid);
    }
    return valid_indices;
  }
};

template <>
struct _add_vertex<std::string_view> {
  std::vector<bool> operator()(const std::shared_ptr<arrow::Array>& col,
                               IdIndexer<std::string_view, vid_t>& indexer,
                               std::vector<vid_t>& vids) {
    size_t row_num = col->length();
    std::vector<bool> valid_indices;
    valid_indices.reserve(row_num);
    CHECK(col->type() == arrow::utf8() || col->type() == arrow::large_utf8());
    if (col->type() == arrow::utf8()) {
      auto casted_array = std::static_pointer_cast<arrow::StringArray>(col);
      vid_t vid;
      for (auto i = 0; i < row_num; ++i) {
        auto str = casted_array->GetView(i);
        std::string_view str_view(str.data(), str.size());

        if (!indexer.add(str_view, vid)) {
          VLOG(10) << "Duplicate vertex id: " << str_view << ":" << vids.size();
          valid_indices.emplace_back(false);
        } else {
          valid_indices.emplace_back(true);
        }
        vids.emplace_back(vid);
      }
    } else {
      auto casted_array =
          std::static_pointer_cast<arrow::LargeStringArray>(col);
      vid_t vid;
      for (auto i = 0; i < row_num; ++i) {
        auto str = casted_array->GetView(i);
        std::string_view str_view(str.data(), str.size());

        if (!indexer.add(str_view, vid)) {
          VLOG(10) << "Duplicate vertex id: " << str_view << "..";
          valid_indices.emplace_back(false);
        } else {
          valid_indices.emplace_back(true);
        }
        vids.emplace_back(vid);
      }
    }
    return valid_indices;
  }
};

template <typename KEY_T>
struct _add_vertex_chunk {
  std::vector<bool> operator()(const std::shared_ptr<arrow::ChunkedArray>& col,
                               IdIndexer<KEY_T, vid_t>& indexer,
                               std::vector<vid_t>& vids) {
    return {};
  }
};

template <>
struct _add_vertex_chunk<int64_t> {
  std::vector<bool> operator()(const std::shared_ptr<arrow::ChunkedArray>& col,
                               IdIndexer<int64_t, vid_t>& indexer,
                               std::vector<vid_t>& vids) {
    CHECK(col->type() == arrow::int64());
    size_t row_num = col->length();
    std::vector<bool> valid_indices;
    valid_indices.reserve(row_num);

    for (auto i = 0; i < col->num_chunks(); ++i) {
      auto chunk = col->chunk(i);
      auto casted_array = std::static_pointer_cast<arrow::Int64Array>(chunk);
      for (auto j = 0; j < casted_array->length(); ++j) {
        vid_t vid;
        if (!indexer.add(casted_array->Value(j), vid)) {
          VLOG(10) << "Duplicate vertex id: " << casted_array->Value(j)
                   << " .. ";
          valid_indices.emplace_back(false);
        } else {
          valid_indices.emplace_back(true);
        }
        vids.emplace_back(vid);
      }
    }
    return valid_indices;
  }
};

template <>
struct _add_vertex_chunk<std::string_view> {
  std::vector<bool> operator()(const std::shared_ptr<arrow::ChunkedArray>& col,
                               IdIndexer<std::string_view, vid_t>& indexer,
                               std::vector<vid_t>& vids) {
    CHECK(col->type() == arrow::utf8() || col->type() == arrow::large_utf8());
    size_t row_num = col->length();
    std::vector<bool> valid_indices;
    valid_indices.reserve(row_num);

    if (col->type() == arrow::utf8()) {
      for (auto i = 0; i < col->num_chunks(); ++i) {
        auto chunk = col->chunk(i);
        auto casted_array = std::static_pointer_cast<arrow::StringArray>(chunk);
        for (auto j = 0; j < casted_array->length(); ++j) {
          vid_t vid;
          auto str = casted_array->GetView(j);
          std::string_view str_view(str.data(), str.size());

          if (!indexer.add(str_view, vid)) {
            VLOG(10) << "Duplicate vertex id: " << str_view << " .. ";
            valid_indices.emplace_back(false);
          } else {
            valid_indices.emplace_back(true);
          }
          vids.emplace_back(vid);
        }
      }
    } else {
      for (auto i = 0; i < col->num_chunks(); ++i) {
        auto chunk = col->chunk(i);
        auto casted_array =
            std::static_pointer_cast<arrow::LargeStringArray>(chunk);
        for (auto j = 0; j < casted_array->length(); ++j) {
          vid_t vid;
          auto str = casted_array->GetView(j);
          std::string_view str_view(str.data(), str.size());

          if (!indexer.add(str_view, vid)) {
            VLOG(10) << "Duplicate vertex id: " << str_view << " .. ";
            valid_indices.emplace_back(false);
          } else {
            valid_indices.emplace_back(true);
          }
          vids.emplace_back(vid);
        }
      }
    }
    return valid_indices;
  }
};

std::shared_ptr<IFragmentLoader> ODPSFragmentLoader::Make(
    const std::string& work_dir, const Schema& schema,
    const LoadingConfig& loading_config, int32_t thread_num) {
  return std::shared_ptr<IFragmentLoader>(
      new ODPSFragmentLoader(work_dir, schema, loading_config, thread_num));
}

void ODPSFragmentLoader::LoadFragment() {
  init();
  loadVertices();
  loadEdges();

  basic_fragment_loader_.LoadFragment();
}

std::shared_ptr<ArrowClient> ODPSFragmentLoader::getArrowClient(
    int connect_timeout, int rw_timeout) {
  AliyunAccount aliyun_account(access_id_, access_key_);
  Configuration configuration;
  configuration.SetSocketConnectTimeout(connect_timeout);
  configuration.SetSocketTimeout(rw_timeout);
  configuration.SetAccount(aliyun_account);
  configuration.SetEndpoint(odps_endpoint_);
  configuration.SetTunnelEndpoint(tunnel_endpoint_);

  return std::make_shared<ArrowClient>(configuration);
}

void ODPSFragmentLoader::dump_table_to_csv(std::shared_ptr<arrow::Table> table,
                                           const std::string& table_name) {
  // convert / in table_name to _
  auto copied_table_name(table_name);
  std::replace(copied_table_name.begin(), copied_table_name.end(), '/', '_');
  std::replace(copied_table_name.begin(), copied_table_name.end(), '=', '_');
  std::string csv_file = output_directory_ + "/" + copied_table_name + ".csv";
  // if csv_file exists, override
  if (std::filesystem::exists(csv_file)) {
    LOG(WARNING) << "File " << csv_file << " exists, override";
  }
  LOG(INFO) << "Dump table to csv: " << csv_file << ", the table has "
            << table->num_rows() << " rows";
  // output table to csv file in csv format
  auto write_options = arrow::csv::WriteOptions::Defaults();
  auto outstream = arrow::io::FileOutputStream::Open(csv_file);
  auto st =
      arrow::csv::WriteCSV(*table, write_options, outstream.ValueOrDie().get());
  if (!st.ok()) {
    LOG(FATAL) << "WriteCSV failed: " << st.ToString();
  }
  LOG(INFO) << "Dump table to csv done";
}

void ODPSFragmentLoader::init() {
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
  arrow_client_ptr_ = getArrowClient();
}

TableBatchScanResp ODPSFragmentLoader::createReadSession(
    const TableIdentifier& table_identifier,
    const std::vector<std::string>& selected_cols,
    const std::vector<std::string>& partition_cols,
    const std::vector<std::string>& selected_partitions) {
  LOG(INFO) << "CreateReadSession:" << table_identifier.project_ << ", "
            << table_identifier.table_;
  LOG(INFO) << "Selected cols:" << selected_cols.size();
  for (auto& col : selected_cols) {
    LOG(INFO) << col;
  }
  LOG(INFO) << "Partition:" << partition_cols.size();
  for (auto& col : partition_cols) {
    LOG(INFO) << col;
  }
  LOG(INFO) << "Selected partitions:" << selected_partitions.size();
  for (auto& col : selected_partitions) {
    LOG(INFO) << col;
  }

  TableBatchScanReq req;
  req.table_identifier_ = table_identifier;
  req.split_options_ = SplitOptions::GetDefaultOptions(SplitOptions::SIZE);
  req.split_options_.split_number_ = 64 * 1024 * 1024;

  if (!partition_cols.empty()) {
    //    req.required_partition_columns_ = partition_cols;
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

TableBatchScanResp ODPSFragmentLoader::getReadSession(
    std::string session_id, const TableIdentifier& table_identifier) {
  SessionReq req;
  req.session_id_ = session_id;
  req.table_identifier_ = table_identifier;

  TableBatchScanResp resp;
  arrow_client_ptr_->GetReadSession(req, resp);
  return resp;
}

void ODPSFragmentLoader::getReadSessionStatus(
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

void ODPSFragmentLoader::preprocessRead(
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
  LOG(INFO) << "Got split_count: " << *split_count;
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

static void set_vertex_properties(gs::ColumnBase* col,
                                  std::shared_ptr<arrow::ChunkedArray> array,
                                  const std::vector<vid_t>& vids,
                                  const std::vector<bool>& valid_indices) {
  CHECK(valid_indices.size() == vids.size()) << "Invalid valid indices size";
  auto type = array->type();
  auto col_type = col->type();
  size_t cur_ind = 0;
  if (col_type == PropertyType::kInt64) {
    CHECK(type == arrow::int64())
        << "Inconsistent data type, expect int64, but got " << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::Int64Array>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        if (!valid_indices[cur_ind]) {
          ++cur_ind;
        } else {
          col->set_any(
              vids[cur_ind++],
              std::move(AnyConverter<int64_t>::to_any(casted->Value(k))));
        }
      }
    }
  } else if (col_type == PropertyType::kInt32) {
    CHECK(type == arrow::int32())
        << "Inconsistent data type, expect int32, but got " << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::Int32Array>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        if (!valid_indices[cur_ind]) {
          ++cur_ind;
        } else {
          col->set_any(
              vids[cur_ind++],
              std::move(AnyConverter<int32_t>::to_any(casted->Value(k))));
        }
      }
    }
  } else if (col_type == PropertyType::kDouble) {
    CHECK(type == arrow::float64())
        << "Inconsistent data type, expect double, but got "
        << type->ToString();
    for (auto j = 0; j < array->num_chunks(); ++j) {
      auto casted =
          std::static_pointer_cast<arrow::DoubleArray>(array->chunk(j));
      for (auto k = 0; k < casted->length(); ++k) {
        if (!valid_indices[cur_ind]) {
          ++cur_ind;
        } else {
          col->set_any(
              vids[cur_ind++],
              std::move(AnyConverter<double>::to_any(casted->Value(k))));
        }
      }
    }
  } else if (col_type == PropertyType::kString) {
    CHECK(type == arrow::large_utf8() || type == arrow::utf8())
        << "Inconsistent data type, expect string, but got "
        << type->ToString();
    if (type == arrow::large_utf8()) {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::LargeStringArray>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          if (!valid_indices[cur_ind]) {
            ++cur_ind;
          } else {
            auto str = casted->GetView(k);
            std::string_view str_view(str.data(), str.size());
            col->set_any(
                vids[cur_ind++],
                std::move(AnyConverter<std::string_view>::to_any(str_view)));
          }
        }
      }
    } else {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::StringArray>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          if (!valid_indices[cur_ind]) {
            ++cur_ind;
          } else {
            auto str = casted->GetView(k);
            std::string_view str_view(str.data(), str.size());
            col->set_any(
                vids[cur_ind++],
                std::move(AnyConverter<std::string_view>::to_any(str_view)));
          }
        }
      }
    }
  } else if (col_type == PropertyType::kDate) {
    if (type->Equals(arrow::timestamp(arrow::TimeUnit::type::MILLI))) {
      for (auto j = 0; j < array->num_chunks(); ++j) {
        auto casted =
            std::static_pointer_cast<arrow::TimestampArray>(array->chunk(j));
        for (auto k = 0; k < casted->length(); ++k) {
          if (!valid_indices[cur_ind]) {
            ++cur_ind;
          } else {
            col->set_any(
                vids[cur_ind++],
                std::move(AnyConverter<Date>::to_any(casted->Value(k))));
          }
        }
      }
    } else {
      LOG(FATAL) << "Not implemented: converting " << type->ToString() << " to "
                 << col_type;
    }
  } else {
    LOG(FATAL) << "Not support type: " << type->ToString();
  }
}

template <typename KEY_T>
void ODPSFragmentLoader::addVertexBatch(
    label_t v_label_id, IdIndexer<KEY_T, vid_t>& indexer,
    std::shared_ptr<arrow::Array>& primary_key_col,
    const std::vector<std::shared_ptr<arrow::Array>>& property_cols) {
  size_t row_num = primary_key_col->length();
  auto col_num = property_cols.size();
  for (size_t i = 0; i < col_num; ++i) {
    CHECK_EQ(property_cols[i]->length(), row_num);
  }

  double t = -grape::GetCurrentTime();
  vid_t vid;
  std::vector<vid_t> vids;
  vids.reserve(row_num);

  std::vector<bool> valid_indcies =
      _add_vertex<KEY_T>()(primary_key_col, indexer, vids);

  t += grape::GetCurrentTime();
  for (double tmp = convert_to_internal_vertex_time_;
       !convert_to_internal_vertex_time_.compare_exchange_weak(tmp, tmp + t);) {
  }
  VLOG(9) << "Finish adding oids";

  t = -grape::GetCurrentTime();
  for (auto j = 0; j < property_cols.size(); ++j) {
    auto array = property_cols[j];
    auto chunked_array = std::make_shared<arrow::ChunkedArray>(array);
    set_vertex_properties(
        basic_fragment_loader_.GetVertexTable(v_label_id).column_ptrs()[j],
        chunked_array, vids, valid_indcies);
  }

  t += grape::GetCurrentTime();
  for (double tmp = basic_frag_loader_vertex_time_;
       !basic_frag_loader_vertex_time_.compare_exchange_weak(tmp, tmp + t);) {}

  VLOG(10) << "Insert rows: " << row_num;
}

template <typename KEY_T>
void ODPSFragmentLoader::addVertexBatch(
    label_t v_label_id, IdIndexer<KEY_T, vid_t>& indexer,
    std::shared_ptr<arrow::ChunkedArray>& primary_key_col,
    const std::vector<std::shared_ptr<arrow::ChunkedArray>>& property_cols) {
  size_t row_num = primary_key_col->length();
  std::vector<vid_t> vids;
  vids.reserve(row_num);
  // check row num
  auto col_num = property_cols.size();
  for (size_t i = 0; i < col_num; ++i) {
    CHECK_EQ(property_cols[i]->length(), row_num);
  }

  double t = -grape::GetCurrentTime();
  std::vector<bool> valid_indices =
      _add_vertex_chunk<KEY_T>()(primary_key_col, indexer, vids);

  t += grape::GetCurrentTime();
  for (double tmp = convert_to_internal_vertex_time_;
       !convert_to_internal_vertex_time_.compare_exchange_weak(tmp, tmp + t);) {
  }

  t = -grape::GetCurrentTime();
  VLOG(9) << "Setting vertex properies";
  for (auto i = 0; i < property_cols.size(); ++i) {
    auto array = property_cols[i];
    auto& table = basic_fragment_loader_.GetVertexTable(v_label_id);
    auto& col_ptrs = table.column_ptrs();
    set_vertex_properties(col_ptrs[i], array, vids, valid_indices);
  }
  t += grape::GetCurrentTime();
  for (double tmp = basic_frag_loader_vertex_time_;
       !basic_frag_loader_vertex_time_.compare_exchange_weak(tmp, tmp + t);) {}

  VLOG(10) << "Insert rows: " << row_num;
}

// Read record batch and add vertices to fragment
template <typename KEY_T>
void ODPSFragmentLoader::addVerticesImplWithStreamReader(
    const std::string& v_file, label_t v_label_id,
    IdIndexer<KEY_T, vid_t>& indexer) {
  auto vertex_column_mappings =
      loading_config_.GetVertexColumnMappings(v_label_id);
  auto primary_key = schema_.get_vertex_primary_key(v_label_id)[0];
  auto primary_key_name = std::get<1>(primary_key);
  size_t primary_key_ind = std::get<2>(primary_key);

  std::string session_id;
  int split_count;
  TableIdentifier table_identifier_;
  std::vector<std::string> partition_cols;
  std::vector<std::string> selected_partitions;
  parseLocation(v_file, table_identifier_, partition_cols, selected_partitions);
  auto selected_cols = columnMappingsToSelectedCols(vertex_column_mappings);

  // create reader session, split table.
  preprocessRead(&session_id, &split_count, table_identifier_, selected_cols,
                 partition_cols, selected_partitions);
  LOG(INFO) << "Successfully got session_id: " << session_id
            << ", split count: " << split_count;

  // for each split, read record batch and parse.
  std::shared_ptr<arrow::RecordBatch> record_batch;
  bool first_batch = true;
  for (auto i = 0; i < split_count; ++i) {
    VLOG(1) << "Reading split " << i << " of " << split_count;
    ReadRowsReq req;
    req.table_identifier_ = table_identifier_;
    req.session_id_ = session_id;
    req.split_index_ = i;
    req.max_batch_rows_ = 32768;

    auto reader = arrow_client_ptr_->ReadRows(req);
    while (reader->Read(record_batch)) {
      if (first_batch) {
        auto header = record_batch->schema()->field_names();
        auto schema_column_names =
            schema_.get_vertex_property_names(v_label_id);
        CHECK(schema_column_names.size() + 1 <= header.size());
        VLOG(10) << "Find header of size: " << header.size();
        first_batch = false;
      }

      auto columns = record_batch->columns();
      CHECK(primary_key_ind < columns.size());
      auto primary_key_column = columns[primary_key_ind];
      auto other_columns_array = columns;
      other_columns_array.erase(other_columns_array.begin() + primary_key_ind);
      VLOG(5) << "Reading record batch of size: " << record_batch->num_rows();
      addVertexBatch(v_label_id, indexer, primary_key_column,
                     other_columns_array);
      record_batch.reset();
    }
    if (reader->GetStatus() != apsara::odps::sdk::storage_api::Status::OK) {
      LOG(ERROR) << "read rows error: " << reader->GetErrorMessage() << ", "
                 << reader->GetStatus() << ", split id: " << i;
    }
  }
}

template <typename KEY_T>
void ODPSFragmentLoader::addVerticesImplWithTableReader(
    const std::string& v_file, label_t v_label_id,
    IdIndexer<KEY_T, vid_t>& indexer) {
  auto vertex_column_mappings =
      loading_config_.GetVertexColumnMappings(v_label_id);
  auto primary_key = schema_.get_vertex_primary_key(v_label_id)[0];
  size_t primary_key_ind = std::get<2>(primary_key);

  std::string session_id;
  int split_count;
  TableIdentifier table_identifier_;
  std::vector<std::string> partition_cols;
  std::vector<std::string> selected_partitions;
  parseLocation(v_file, table_identifier_, partition_cols, selected_partitions);
  auto selected_cols = columnMappingsToSelectedCols(vertex_column_mappings);

  // create reader session, split table.
  preprocessRead(&session_id, &split_count, table_identifier_, selected_cols,
                 partition_cols, selected_partitions);
  LOG(INFO) << "Successfully got session_id: " << session_id
            << ", split count: " << split_count;

  // ReadTable with halo API
  std::shared_ptr<arrow::Table> table =
      readTable(session_id, split_count, table_identifier_);
  CHECK(table) << "Fail to read vertex table";
  if (dump_to_csv_) {
    LOG(INFO) << "Dumping table to csv";
    dump_table_to_csv(table, v_file);
  }

  auto header = table->schema()->field_names();
  auto schema_column_names = schema_.get_vertex_property_names(v_label_id);
  CHECK(schema_column_names.size() + 1 == header.size());
  VLOG(10) << "Find header of size: " << header.size();

  auto columns = table->columns();
  CHECK(primary_key_ind < columns.size());
  auto primary_key_column = columns[primary_key_ind];
  auto other_columns_array = columns;
  other_columns_array.erase(other_columns_array.begin() + primary_key_ind);
  VLOG(10) << "Reading record batch of size: " << table->num_rows();
  addVertexBatch(v_label_id, indexer, primary_key_column, other_columns_array);
}

template <typename KEY_T>
void ODPSFragmentLoader::addVerticesImpl(label_t v_label_id,
                                         const std::string& v_label_name,
                                         const std::vector<std::string> v_files,
                                         IdIndexer<KEY_T, vid_t>& indexer) {
  VLOG(10) << "Parsing vertex file:" << v_files.size() << " for label "
           << v_label_name;

  auto batch_reader = loading_config_.GetMetaData("batch_reader");
  for (auto& v_file : v_files) {
    if (batch_reader == "true") {
      addVerticesImplWithStreamReader<KEY_T>(v_file, v_label_id, indexer);
    } else {
      addVerticesImplWithTableReader<KEY_T>(v_file, v_label_id, indexer);
    }
  }
}

void ODPSFragmentLoader::addVertices(label_t v_label_id,
                                     const std::vector<std::string>& v_files) {
  auto primary_keys = schema_.get_vertex_primary_key(v_label_id);

  if (primary_keys.size() != 1) {
    LOG(FATAL) << "Only support one primary key for vertex.";
  }

  auto type = std::get<0>(primary_keys[0]);
  if (type != PropertyType::kInt64 && type != PropertyType::kString) {
    LOG(FATAL)
        << "Only support int64_t and string_view primary key for vertex.";
  }

  std::string v_label_name = schema_.get_vertex_label_name(v_label_id);
  VLOG(10) << "Start init vertices for label " << v_label_name << " with "
           << v_files.size() << " files.";

  if (type == PropertyType::kInt64) {
    IdIndexer<int64_t, vid_t> indexer;

    addVerticesImpl<int64_t>(v_label_id, v_label_name, v_files, indexer);

    if (indexer.bucket_count() == 0) {
      indexer._rehash(schema_.get_max_vnum(v_label_name));
    }
    basic_fragment_loader_.FinishAddingVertex<int64_t>(v_label_id, indexer);
  } else if (type == PropertyType::kString) {
    IdIndexer<std::string_view, vid_t> indexer;

    addVerticesImpl<std::string_view>(v_label_id, v_label_name, v_files,
                                      indexer);

    if (indexer.bucket_count() == 0) {
      indexer._rehash(schema_.get_max_vnum(v_label_name));
    }
    basic_fragment_loader_.FinishAddingVertex<std::string_view>(v_label_id,
                                                                indexer);
  }

  VLOG(10) << "Finish init vertices for label " << v_label_name;
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

static void check_edge_invariant(
    const Schema& schema,
    const std::vector<std::tuple<size_t, std::string, std::string>>&
        column_mappings,
    size_t src_col_ind, size_t dst_col_ind, label_t src_label_i,
    label_t dst_label_i, label_t edge_label_i) {
  // TODO(zhanglei): Check column mappings after multiple property on edge is
  // supported
  if (column_mappings.size() > 1) {
    LOG(FATAL) << "Edge column mapping must be less than 1";
  }
  if (column_mappings.size() > 0) {
    auto& mapping = column_mappings[0];
    if (std::get<0>(mapping) == src_col_ind ||
        std::get<0>(mapping) == dst_col_ind) {
      LOG(FATAL) << "Edge column mappings must not contain src_col_ind or "
                    "dst_col_ind";
    }
    auto src_label_name = schema.get_vertex_label_name(src_label_i);
    auto dst_label_name = schema.get_vertex_label_name(dst_label_i);
    auto edge_label_name = schema.get_edge_label_name(edge_label_i);
    // check property exists in schema
    if (!schema.edge_has_property(src_label_name, dst_label_name,
                                  edge_label_name, std::get<2>(mapping))) {
      LOG(FATAL) << "property " << std::get<2>(mapping)
                 << " not exists in schema for edge triplet " << src_label_name
                 << " -> " << edge_label_name << " -> " << dst_label_name;
    }
  }
}
template <typename EDATA_T>
static void append_edges(
    std::shared_ptr<arrow::Array> src_col,
    std::shared_ptr<arrow::Array> dst_col, const LFIndexer<vid_t>& src_indexer,
    const LFIndexer<vid_t>& dst_indexer,
    std::vector<std::shared_ptr<arrow::Array>>& edata_cols,
    std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& parsed_edges,
    std::vector<int32_t>& ie_degree, std::vector<int32_t>& oe_degree) {
  CHECK(src_col->length() == dst_col->length());
  if (src_indexer.get_type() == PropertyType::kInt64) {
    CHECK(src_col->type() == arrow::int64());
  } else if (src_indexer.get_type() == PropertyType::kString) {
    CHECK(src_col->type() == arrow::utf8() ||
          src_col->type() == arrow::large_utf8());
  }

  if (dst_indexer.get_type() == PropertyType::kInt64) {
    CHECK(dst_col->type() == arrow::int64());
  } else if (dst_indexer.get_type() == PropertyType::kString) {
    CHECK(dst_col->type() == arrow::utf8() ||
          dst_col->type() == arrow::large_utf8());
  }

  auto old_size = parsed_edges.size();
  // parsed_edges.resize(old_size + src_col->length());
  // VLOG(10) << "resize parsed_edges from" << old_size << " to "
  //          << parsed_edges.size();
  std::vector<bool> active_flag(src_col->length(), true);
  std::vector<std::tuple<vid_t, vid_t, EDATA_T>> new_edges;
  new_edges.resize(src_col->length());

  auto _append = [&](bool is_dst) {
    size_t cur_ind = 0;
    const auto& col = is_dst ? dst_col : src_col;
    const auto& indexer = is_dst ? dst_indexer : src_indexer;
    vid_t vid;
    if (col->type() == arrow::int64()) {
      auto casted = std::static_pointer_cast<arrow::Int64Array>(col);
      for (auto j = 0; j < casted->length(); ++j) {
        if (indexer.get_index(Any::From(casted->Value(j)), vid)) {
          if (is_dst) {
            std::get<1>(new_edges[cur_ind++]) = vid;
          } else {
            std::get<0>(new_edges[cur_ind++]) = vid;
          }
          is_dst ? ie_degree[vid]++ : oe_degree[vid]++;
        } else {
          active_flag[cur_ind++] = false;
        }
      }
    } else if (col->type() == arrow::utf8()) {
      auto casted = std::static_pointer_cast<arrow::StringArray>(col);
      for (auto j = 0; j < casted->length(); ++j) {
        auto str = casted->GetView(j);
        std::string_view str_view(str.data(), str.size());
        if (indexer.get_index(Any::From(str_view), vid)) {
          if (is_dst) {
            std::get<1>(new_edges[cur_ind++]) = vid;
          } else {
            std::get<0>(new_edges[cur_ind++]) = vid;
          }
          is_dst ? ie_degree[vid]++ : oe_degree[vid]++;
        } else {
          active_flag[cur_ind++] = false;
        }
      }
    } else if (col->type() == arrow::large_utf8()) {
      auto casted = std::static_pointer_cast<arrow::LargeStringArray>(col);
      for (auto j = 0; j < casted->length(); ++j) {
        auto str = casted->GetView(j);
        std::string_view str_view(str.data(), str.size());
        if (indexer.get_index(Any::From(str_view), vid)) {
          auto vid = indexer.get_index(Any::From(str_view));
          if (is_dst) {
            std::get<1>(new_edges[cur_ind++]) = vid;
          } else {
            std::get<0>(new_edges[cur_ind++]) = vid;
          }
          is_dst ? ie_degree[vid]++ : oe_degree[vid]++;
        } else {
          active_flag[cur_ind++] = false;
        }
      }
    }
  };
  auto src_col_thread = std::thread([&]() { _append(false); });
  auto dst_col_thread = std::thread([&]() { _append(true); });
  src_col_thread.join();
  dst_col_thread.join();

  // if EDATA_T is grape::EmptyType, no need to read columns
  if constexpr (!std::is_same<EDATA_T, grape::EmptyType>::value) {
    CHECK(edata_cols.size() == 1);
    auto edata_col = edata_cols[0];
    CHECK(src_col->length() == edata_col->length());
    size_t cur_ind = 0;
    auto type = edata_col->type();

    using arrow_array_type =
        typename gs::TypeConverter<EDATA_T>::ArrowArrayType;
    // cast chunk to EDATA_T array
    auto data = std::static_pointer_cast<arrow_array_type>(edata_col);
    if (type->Equals(arrow::timestamp(arrow::TimeUnit::MILLI))) {
      auto casted = std::static_pointer_cast<arrow_array_type>(edata_col);
      for (auto j = 0; j < casted->length(); ++j) {
        if (active_flag[cur_ind]) {
          std::get<2>(new_edges[cur_ind]) = casted->Value(j);
        }
        cur_ind++;
      }
    } else if (type->Equals(arrow::large_utf8())) {
      // For edge with string properties, we convert to EDATA_T
      if constexpr (std::is_same_v<double, EDATA_T>) {
        auto casted_chunk =
            std::static_pointer_cast<arrow::LargeStringArray>(edata_col);
        for (auto j = 0; j < casted_chunk->length(); ++j) {
          auto tmp = casted_chunk->GetView(j);
          std::string_view view(tmp.data(), tmp.size());
          if (active_flag[cur_ind]) {
            std::get<2>(new_edges[cur_ind]) = ConvertTo<EDATA_T>::convert(view);
          }
          cur_ind++;
        }
      }
    } else if (type->Equals(arrow::utf8())) {
      // For edge with string properties, we convert to EDATA_T
      if constexpr (std::is_same_v<double, EDATA_T>) {
        auto casted_chunk =
            std::static_pointer_cast<arrow::StringArray>(edata_col);
        for (auto j = 0; j < casted_chunk->length(); ++j) {
          auto tmp = casted_chunk->GetView(j);
          std::string_view view(tmp.data(), tmp.size());
          if (active_flag[cur_ind]) {
            std::get<2>(new_edges[cur_ind]) = ConvertTo<EDATA_T>::convert(view);
          }
          cur_ind++;
        }
      }
    } else {
      auto casted_chunk = std::static_pointer_cast<arrow_array_type>(edata_col);
      for (auto j = 0; j < casted_chunk->length(); ++j) {
        if (active_flag[cur_ind]) {
          std::get<2>(new_edges[cur_ind]) = casted_chunk->Value(j);
        }
        cur_ind++;
      }
    }
    VLOG(10) << "Finish inserting:  " << src_col->length() << " edges";
  }
}

template <typename EDATA_T>
static void append_edges(
    std::shared_ptr<arrow::ChunkedArray> src_col,
    std::shared_ptr<arrow::ChunkedArray> dst_col,
    const LFIndexer<vid_t>& src_indexer, const LFIndexer<vid_t>& dst_indexer,
    std::vector<std::shared_ptr<arrow::ChunkedArray>>& edata_cols,
    std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& parsed_edges,
    std::vector<int32_t>& ie_degree, std::vector<int32_t>& oe_degree) {
  CHECK(src_col->length() == dst_col->length());
  if (src_indexer.get_type() == PropertyType::kInt64) {
    CHECK(src_col->type() == arrow::int64());
  } else if (src_indexer.get_type() == PropertyType::kString) {
    CHECK(src_col->type() == arrow::utf8() ||
          src_col->type() == arrow::large_utf8());
  }

  if (dst_indexer.get_type() == PropertyType::kInt64) {
    CHECK(dst_col->type() == arrow::int64());
  } else if (dst_indexer.get_type() == PropertyType::kString) {
    CHECK(dst_col->type() == arrow::utf8() ||
          dst_col->type() == arrow::large_utf8());
  }

  auto old_size = parsed_edges.size();
  // parsed_edges.resize(old_size + src_col->length());
  // VLOG(10) << "resize parsed_edges from" << old_size << " to "
  //          << parsed_edges.size();

  std::vector<bool> active_flag(src_col->length(), true);
  std::vector<std::tuple<vid_t, vid_t, EDATA_T>> new_edges;
  new_edges.resize(src_col->length());

  auto _append = [&](bool is_dst) {
    size_t cur_ind = 0;
    const auto& col = is_dst ? dst_col : src_col;
    const auto& indexer = is_dst ? dst_indexer : src_indexer;
    vid_t vid;
    for (auto i = 0; i < col->num_chunks(); ++i) {
      auto chunk = col->chunk(i);
      CHECK(chunk->type() == col->type());
      if (col->type() == arrow::int64()) {
        auto casted_chunk = std::static_pointer_cast<arrow::Int64Array>(chunk);
        for (auto j = 0; j < casted_chunk->length(); ++j) {
          if (indexer.get_index(Any::From(casted_chunk->Value(j)), vid)) {
            if (is_dst) {
              std::get<1>(new_edges[cur_ind++]) = vid;
            } else {
              std::get<0>(new_edges[cur_ind++]) = vid;
            }
            is_dst ? ie_degree[vid]++ : oe_degree[vid]++;
          } else {
            active_flag[cur_ind++] = false;
          }
        }
      } else if (col->type() == arrow::utf8()) {
        auto casted_chunk = std::static_pointer_cast<arrow::StringArray>(chunk);
        for (auto j = 0; j < casted_chunk->length(); ++j) {
          auto str = casted_chunk->GetView(j);
          std::string_view str_view(str.data(), str.size());
          if (indexer.get_index(Any::From(str_view), vid)) {
            if (is_dst) {
              std::get<1>(new_edges[cur_ind++]) = vid;
            } else {
              std::get<0>(new_edges[cur_ind++]) = vid;
            }
            is_dst ? ie_degree[vid]++ : oe_degree[vid]++;
          } else {
            active_flag[cur_ind++] = false;
          }
        }
      } else if (col->type() == arrow::large_utf8()) {
        auto casted_chunk =
            std::static_pointer_cast<arrow::LargeStringArray>(chunk);
        for (auto j = 0; j < casted_chunk->length(); ++j) {
          auto str = casted_chunk->GetView(j);
          std::string_view str_view(str.data(), str.size());
          if (indexer.get_index(Any::From(str_view), vid)) {
            auto vid = indexer.get_index(Any::From(str_view));
            if (is_dst) {
              std::get<1>(new_edges[cur_ind++]) = vid;
            } else {
              std::get<0>(new_edges[cur_ind++]) = vid;
            }
            is_dst ? ie_degree[vid]++ : oe_degree[vid]++;
          } else {
            active_flag[cur_ind++] = false;
          }
        }
      } else {
        LOG(FATAL) << "Unsupported type: " << col->type()->ToString();
      }
    }
  };
  auto src_col_thread = std::thread([&]() { _append(false); });
  auto dst_col_thread = std::thread([&]() { _append(true); });

  // if EDATA_T is grape::EmptyType, no need to read columns
  auto edata_col_thread = std::thread([&]() {
    if constexpr (!std::is_same<EDATA_T, grape::EmptyType>::value) {
      CHECK(edata_cols.size() == 1);
      auto edata_col = edata_cols[0];
      CHECK(src_col->length() == edata_col->length());
      // iterate and put data
      size_t cur_ind = 0;
      auto type = edata_col->type();

      using arrow_array_type =
          typename gs::TypeConverter<EDATA_T>::ArrowArrayType;
      if (type->Equals(arrow::timestamp(arrow::TimeUnit::MILLI))) {
        for (auto i = 0; i < edata_col->num_chunks(); ++i) {
          auto chunk = edata_col->chunk(i);
          auto casted_chunk = std::static_pointer_cast<arrow_array_type>(chunk);
          for (auto j = 0; j < casted_chunk->length(); ++j) {
            if (active_flag[cur_ind]) {
              std::get<2>(new_edges[cur_ind]) = casted_chunk->Value(j);
            }
            cur_ind++;
          }
        }
      } else if (type->Equals(arrow::large_utf8())) {
        // For edge with string properties, we convert to EDATA_T
        if constexpr (std::is_same_v<double, EDATA_T>) {
          for (auto i = 0; i < edata_col->num_chunks(); ++i) {
            auto chunk = edata_col->chunk(i);
            auto casted_chunk =
                std::static_pointer_cast<arrow::LargeStringArray>(chunk);
            for (auto j = 0; j < casted_chunk->length(); ++j) {
              auto tmp = casted_chunk->GetView(j);
              std::string_view view(tmp.data(), tmp.size());
              if (active_flag[cur_ind]) {
                std::get<2>(new_edges[cur_ind]) =
                    ConvertTo<EDATA_T>::convert(view);
              }
              cur_ind++;
            }
          }
        }
      } else if (type->Equals(arrow::utf8())) {
        // For edge with string properties, we convert to EDATA_T
        if constexpr (std::is_same_v<double, EDATA_T>) {
          for (auto i = 0; i < edata_col->num_chunks(); ++i) {
            auto chunk = edata_col->chunk(i);
            auto casted_chunk =
                std::static_pointer_cast<arrow::StringArray>(chunk);
            for (auto j = 0; j < casted_chunk->length(); ++j) {
              auto tmp = casted_chunk->GetView(j);
              std::string_view view(tmp.data(), tmp.size());
              if (active_flag[cur_ind]) {
                std::get<2>(new_edges[cur_ind]) =
                    ConvertTo<EDATA_T>::convert(view);
              }
              cur_ind++;
            }
          }
        }
      } else {
        for (auto i = 0; i < edata_col->num_chunks(); ++i) {
          auto chunk = edata_col->chunk(i);
          auto casted_chunk = std::static_pointer_cast<arrow_array_type>(chunk);
          for (auto j = 0; j < casted_chunk->length(); ++j) {
            if (active_flag[cur_ind]) {
              std::get<2>(new_edges[cur_ind]) = casted_chunk->Value(j);
            }
            cur_ind++;
          }
        }
      }
    }
  });
  src_col_thread.join();
  dst_col_thread.join();
  edata_col_thread.join();
  // emplace valid new_edges to parsed_edges
  for (auto i = 0; i < active_flag.size(); ++i) {
    if (active_flag[i]) {
      parsed_edges.emplace_back(new_edges[i]);
    }
  }

  VLOG(10) << "Finish inserting:  " << src_col->length() << " edges";
}

template <typename EDATA_T>
void ODPSFragmentLoader::addEdgesImplWithStreamReader(
    const std::string& filename, label_t src_label_id, label_t dst_label_id,
    label_t e_label_id, std::vector<int32_t>& ie_degree,
    std::vector<int32_t>& oe_degree,
    std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& parsed_edges) {
  const auto& src_indexer = basic_fragment_loader_.GetLFIndexer(src_label_id);
  const auto& dst_indexer = basic_fragment_loader_.GetLFIndexer(dst_label_id);
  std::shared_ptr<arrow::RecordBatch> record_batch;
  // read first batch
  std::string session_id;
  int split_count;
  TableIdentifier table_identifier_;
  std::vector<std::string> partition_cols;
  std::vector<std::string> selected_partitions;
  parseLocation(filename, table_identifier_, partition_cols,
                selected_partitions);
  auto edge_column_mappings = loading_config_.GetEdgeColumnMappings(
      src_label_id, dst_label_id, e_label_id);
  auto selected_props = columnMappingsToSelectedCols(edge_column_mappings);
  // if odps fragment loader, src_column_name and dst_column_name must be
  // specified.
  auto src_dst_col_pair =
      loading_config_.GetEdgeSrcDstCol(src_label_id, dst_label_id, e_label_id);
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

  preprocessRead(&session_id, &split_count, table_identifier_, selected_cols,
                 partition_cols, selected_partitions);

  bool first_batch = true;

  for (auto i = 0; i < split_count; ++i) {
    LOG(INFO) << "Reading split " << i << " of " << split_count;
    ReadRowsReq req;
    req.table_identifier_ = table_identifier_;
    req.session_id_ = session_id;
    req.split_index_ = i;

    auto reader = arrow_client_ptr_->ReadRows(req);
    while (reader->Read(record_batch)) {
      if (first_batch) {
        auto header = record_batch->schema()->field_names();
        auto schema_column_names = schema_.get_edge_property_names(
            src_label_id, dst_label_id, e_label_id);
        auto schema_column_types =
            schema_.get_edge_properties(src_label_id, dst_label_id, e_label_id);
        CHECK(schema_column_names.size() + 2 == header.size())
            << "schema size: " << schema_column_names.size()
            << " header size: " << header.size();
        CHECK(schema_column_types.size() + 2 == header.size())
            << "schema size: " << schema_column_types.size()
            << " header size: " << header.size();
        VLOG(10) << "Find header of size: " << header.size();
        first_batch = false;
      }

      // copy the table to csr.
      auto columns = record_batch->columns();
      // We assume the src_col and dst_col will always be put at front.
      CHECK(columns.size() >= 2);
      auto src_col = columns[0];
      auto dst_col = columns[1];
      CHECK(src_col->type() == arrow::int64() ||
            src_col->type() == arrow::utf8() ||
            src_col->type() == arrow::large_utf8())
          << "src_col type: " << src_col->type()->ToString();
      CHECK(dst_col->type() == arrow::int64() ||
            dst_col->type() == arrow::utf8() ||
            dst_col->type() == arrow::large_utf8())
          << "dst_col type: " << dst_col->type()->ToString();

      std::vector<std::shared_ptr<arrow::Array>> property_cols;
      for (auto i = 2; i < columns.size(); ++i) {
        property_cols.emplace_back(columns[i]);
      }
      CHECK(property_cols.size() <= 1)
          << "Currently only support at most one property on edge";
      {
        // add edges to vector
        CHECK(src_col->length() == dst_col->length());
        append_edges(src_col, dst_col, src_indexer, dst_indexer, property_cols,
                     parsed_edges, ie_degree, oe_degree);
      }
    }
    if (reader->GetStatus() != apsara::odps::sdk::storage_api::Status::OK) {
      LOG(ERROR) << "read rows error: " << reader->GetErrorMessage() << ", "
                 << reader->GetStatus() << ", split id: " << i;
    }
  }
}

template <typename EDATA_T>
void ODPSFragmentLoader::addEdgesImplWithTableReader(
    const std::string& filename, label_t src_label_id, label_t dst_label_id,
    label_t e_label_id, std::vector<int32_t>& ie_degree,
    std::vector<int32_t>& oe_degree,
    std::vector<std::tuple<vid_t, vid_t, EDATA_T>>& parsed_edges) {
  const auto& src_indexer = basic_fragment_loader_.GetLFIndexer(src_label_id);
  const auto& dst_indexer = basic_fragment_loader_.GetLFIndexer(dst_label_id);

  std::string session_id;
  int split_count;
  TableIdentifier table_identifier_;
  std::vector<std::string> partition_cols;
  std::vector<std::string> selected_partitions;
  parseLocation(filename, table_identifier_, partition_cols,
                selected_partitions);
  auto edge_column_mappings = loading_config_.GetEdgeColumnMappings(
      src_label_id, dst_label_id, e_label_id);
  auto selected_props = columnMappingsToSelectedCols(edge_column_mappings);
  // if odps fragment loader, src_column_name and dst_column_name must be
  // specified.
  auto src_dst_col_pair =
      loading_config_.GetEdgeSrcDstCol(src_label_id, dst_label_id, e_label_id);
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

  preprocessRead(&session_id, &split_count, table_identifier_, selected_cols,
                 partition_cols, selected_partitions);

  // ReadTable with halo API
  std::shared_ptr<arrow::Table> table =
      readTable(session_id, split_count, table_identifier_);
  CHECK(table) << "Fail to read Edge table";
  if (dump_to_csv_) {
    LOG(INFO) << "Dumping table to csv";
    dump_table_to_csv(table, filename);
  }

  auto header = table->schema()->field_names();
  auto schema_column_names =
      schema_.get_edge_property_names(src_label_id, dst_label_id, e_label_id);
  auto schema_column_types =
      schema_.get_edge_properties(src_label_id, dst_label_id, e_label_id);
  CHECK(schema_column_names.size() + 2 == header.size());
  CHECK(schema_column_types.size() + 2 == header.size());
  VLOG(10) << "Find header of size: " << header.size();

  auto columns = table->columns();
  CHECK(columns.size() >= 2);
  auto src_col = columns[0];
  auto dst_col = columns[1];
  CHECK(src_col->type() == arrow::int64() || src_col->type() == arrow::utf8() ||
        src_col->type() == arrow::large_utf8())
      << "src_col type: " << src_col->type()->ToString();
  CHECK(dst_col->type() == arrow::int64() || dst_col->type() == arrow::utf8() ||
        dst_col->type() == arrow::large_utf8())
      << "dst_col type: " << dst_col->type()->ToString();

  std::vector<std::shared_ptr<arrow::ChunkedArray>> property_cols;
  for (auto i = 2; i < columns.size(); ++i) {
    property_cols.emplace_back(columns[i]);
  }
  CHECK(property_cols.size() <= 1)
      << "Currently only support at most one property on edge";
  {
    CHECK(src_col->length() == dst_col->length());
    append_edges(src_col, dst_col, src_indexer, dst_indexer, property_cols,
                 parsed_edges, ie_degree, oe_degree);
  }
}

template <typename EDATA_T>
void ODPSFragmentLoader::addEdgesImpl(label_t src_label_id,
                                      label_t dst_label_id, label_t e_label_id,
                                      const std::vector<std::string>& e_files) {
  auto edge_column_mappings = loading_config_.GetEdgeColumnMappings(
      src_label_id, dst_label_id, e_label_id);
  auto src_dst_col_pair =
      loading_config_.GetEdgeSrcDstCol(src_label_id, dst_label_id, e_label_id);
  if (src_dst_col_pair.first.size() != 1 ||
      src_dst_col_pair.second.size() != 1) {
    LOG(FATAL) << "We currently only support one src primary key and one "
                  "dst primary key";
  }
  size_t src_col_ind = src_dst_col_pair.first[0].second;
  size_t dst_col_ind = src_dst_col_pair.second[0].second;
  CHECK(src_col_ind != dst_col_ind);

  check_edge_invariant(schema_, edge_column_mappings, src_col_ind, dst_col_ind,
                       src_label_id, dst_label_id, e_label_id);

  std::vector<std::tuple<vid_t, vid_t, EDATA_T>> parsed_edges;
  std::vector<int32_t> ie_degree, oe_degree;
  const auto& src_indexer = basic_fragment_loader_.GetLFIndexer(src_label_id);
  const auto& dst_indexer = basic_fragment_loader_.GetLFIndexer(dst_label_id);
  ie_degree.resize(dst_indexer.size());
  oe_degree.resize(src_indexer.size());
  VLOG(10) << "src indexer size: " << src_indexer.size()
           << " dst indexer size: " << dst_indexer.size();

  auto batch_reader = loading_config_.GetMetaData("batch_reader");
  for (auto filename : e_files) {
    VLOG(10) << "processing " << filename << " with src_col_id " << src_col_ind
             << " and dst_col_id " << dst_col_ind;
    if (batch_reader == "true") {
      VLOG(1) << "Using batch reader";
      addEdgesImplWithStreamReader(filename, src_label_id, dst_label_id,
                                   e_label_id, ie_degree, oe_degree,
                                   parsed_edges);
    } else {
      addEdgesImplWithTableReader(filename, src_label_id, dst_label_id,
                                  e_label_id, ie_degree, oe_degree,
                                  parsed_edges);
    }
  }
  double t = -grape::GetCurrentTime();
  basic_fragment_loader_.PutEdges(src_label_id, dst_label_id, e_label_id,
                                  parsed_edges, ie_degree, oe_degree);
  t += grape::GetCurrentTime();
  // basic_frag_loader_edge_time_.fetch_add(t);
  for (double tmp = basic_frag_loader_edge_time_;
       !basic_frag_loader_edge_time_.compare_exchange_weak(tmp, tmp + t);) {}
  VLOG(10) << "Finish putting: " << parsed_edges.size() << " edges";
}

void ODPSFragmentLoader::addEdges(label_t src_label_i, label_t dst_label_i,
                                  label_t edge_label_i,
                                  const std::vector<std::string>& filenames) {
  auto src_label_name = schema_.get_vertex_label_name(src_label_i);
  auto dst_label_name = schema_.get_vertex_label_name(dst_label_i);
  auto edge_label_name = schema_.get_edge_label_name(edge_label_i);
  if (filenames.size() <= 0) {
    LOG(FATAL) << "No edge files found for src label: " << src_label_name
               << " dst label: " << dst_label_name
               << " edge label: " << edge_label_name;
  }
  if (filenames.size() <= 0) {
    LOG(FATAL) << "No edge files found for src label: " << src_label_name
               << " dst label: " << dst_label_name
               << " edge label: " << edge_label_name;
  }
  VLOG(10) << "Init edges src label: " << src_label_name
           << " dst label: " << dst_label_name
           << " edge label: " << edge_label_name
           << " filenames: " << filenames.size();
  auto& property_types = schema_.get_edge_properties(
      src_label_name, dst_label_name, edge_label_name);
  size_t col_num = property_types.size();
  CHECK_LE(col_num, 1) << "Only single or no property is supported for edge.";

  if (col_num == 0) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<grape::EmptyType>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesImpl<grape::EmptyType>(src_label_i, dst_label_i, edge_label_i,
                                     filenames);
    }
  } else if (property_types[0] == PropertyType::kDate) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<Date>(src_label_i, dst_label_i,
                                                      edge_label_i);
    } else {
      addEdgesImpl<Date>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else if (property_types[0] == PropertyType::kInt32) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<int>(src_label_i, dst_label_i,
                                                     edge_label_i);
    } else {
      addEdgesImpl<int>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else if (property_types[0] == PropertyType::kInt64) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<int64_t>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesImpl<int64_t>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else if (property_types[0] == PropertyType::kString) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<std::string>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      LOG(FATAL) << "Unsupported edge property type.";
    }
  } else if (property_types[0] == PropertyType::kDouble) {
    if (filenames.empty()) {
      basic_fragment_loader_.AddNoPropEdgeBatch<double>(
          src_label_i, dst_label_i, edge_label_i);
    } else {
      addEdgesImpl<double>(src_label_i, dst_label_i, edge_label_i, filenames);
    }
  } else {
    LOG(FATAL) << "Unsupported edge property type." << property_types[0];
  }
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

// Read table from halo, the producer number
std::shared_ptr<arrow::Table> ODPSFragmentLoader::readTable(
    const std::string& session_id, int split_count,
    const TableIdentifier& table_id) {
  std::vector<std::thread> producers;
  int cur_thread_num = std::max(1, (int) MAX_PRODUCER_NUM / thread_num_);
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
              << " splits of " << split_count << " splits";
    producers.emplace_back(&ODPSFragmentLoader::producerRoutine, this,
                           session_id, table_id, std::ref(all_batches),
                           indices);
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

void ODPSFragmentLoader::producerRoutine(
    const std::string& session_id, const TableIdentifier& table_identifier,
    std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>>& all_batches,
    std::vector<int> indices) {
  int max_retry = 5;
  for (int i : indices) {
    for (auto j = 0; j < max_retry; ++j) {
      bool st = readRows(session_id, table_identifier, all_batches[i], i);
      if (!st) {
        LOG(ERROR) << "Read split " << i << " error";
        LOG(ERROR) << "Retry " << j;
      } else {
        break;
      }
    }
  }
}

bool ODPSFragmentLoader::readRows(
    std::string session_id, const TableIdentifier& table_identifier,
    std::vector<std::shared_ptr<arrow::RecordBatch>>& res_batches,
    int split_index) {
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

const bool ODPSFragmentLoader::registered_ =
    LoaderFactory::Register("odps", "arrow",
                            static_cast<LoaderFactory::loader_initializer_t>(
                                &ODPSFragmentLoader::Make));

}  // namespace gs

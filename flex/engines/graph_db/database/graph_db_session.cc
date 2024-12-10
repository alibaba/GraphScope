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

#include <chrono>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/utils/app_utils.h"

#include "flex/proto_generated_gie/stored_procedure.pb.h"
#include "service_utils.h"

#include <rapidjson/document.h>

namespace gs {

ReadTransaction GraphDBSession::GetReadTransaction() const {
  uint32_t ts = db_.version_manager_.acquire_read_timestamp();
  return ReadTransaction(*this, db_.graph_, db_.version_manager_, ts);
}

InsertTransaction GraphDBSession::GetInsertTransaction() {
  uint32_t ts = db_.version_manager_.acquire_insert_timestamp();
  return InsertTransaction(db_.graph_, alloc_, logger_, db_.version_manager_,
                           ts);
}

SingleVertexInsertTransaction
GraphDBSession::GetSingleVertexInsertTransaction() {
  uint32_t ts = db_.version_manager_.acquire_insert_timestamp();
  return SingleVertexInsertTransaction(db_.graph_, alloc_, logger_,
                                       db_.version_manager_, ts);
}

SingleEdgeInsertTransaction GraphDBSession::GetSingleEdgeInsertTransaction() {
  uint32_t ts = db_.version_manager_.acquire_insert_timestamp();
  return SingleEdgeInsertTransaction(db_.graph_, alloc_, logger_,
                                     db_.version_manager_, ts);
}

UpdateTransaction GraphDBSession::GetUpdateTransaction() {
  uint32_t ts = db_.version_manager_.acquire_update_timestamp();
  return UpdateTransaction(db_.graph_, alloc_, work_dir_, logger_,
                           db_.version_manager_, ts);
}

bool GraphDBSession::BatchUpdate(UpdateBatch& batch) {
  GetUpdateTransaction().batch_commit(batch);
  return true;
}

const MutablePropertyFragment& GraphDBSession::graph() const {
  return db_.graph();
}

const GraphDB& GraphDBSession::db() const { return db_; }

MutablePropertyFragment& GraphDBSession::graph() { return db_.graph(); }

const Schema& GraphDBSession::schema() const { return db_.schema(); }

std::shared_ptr<ColumnBase> GraphDBSession::get_vertex_property_column(
    uint8_t label, const std::string& col_name) const {
  return db_.get_vertex_property_column(label, col_name);
}

std::shared_ptr<RefColumnBase> GraphDBSession::get_vertex_id_column(
    uint8_t label) const {
  return db_.get_vertex_id_column(label);
}

Result<std::vector<char>> GraphDBSession::Eval(const std::string& input) {
  const auto start = std::chrono::high_resolution_clock::now();

  if (input.size() < 2) {
    return Result<std::vector<char>>(
        StatusCode::INVALID_ARGUMENT,
        "Invalid input, input size: " + std::to_string(input.size()),
        std::vector<char>());
  }

  auto type_res = parse_query_type(input);
  if (!type_res.ok()) {
    LOG(ERROR) << "Fail to parse query type";
    return Result<std::vector<char>>(type_res.status(), std::vector<char>());
  }

  uint8_t type;
  std::string_view sv;
  std::tie(type, sv) = type_res.value();

  std::vector<char> result_buffer;

  Encoder encoder(result_buffer);
  Decoder decoder(sv.data(), sv.size());

  AppBase* app = GetApp(type);
  if (!app) {
    return Result<std::vector<char>>(
        StatusCode::NOT_FOUND,
        "Procedure not found, id:" + std::to_string((int) type), result_buffer);
  }

  for (size_t i = 0; i < MAX_RETRY; ++i) {
    result_buffer.clear();
    if (app->run(*this, decoder, encoder)) {
      const auto end = std::chrono::high_resolution_clock::now();
      app_metrics_[type].add_record(
          std::chrono::duration_cast<std::chrono::microseconds>(end - start)
              .count());
      eval_duration_.fetch_add(
          std::chrono::duration_cast<std::chrono::microseconds>(end - start)
              .count());
      ++query_num_;
      return result_buffer;
    }

    LOG(INFO) << "[Query-" << (int) type << "][Thread-" << thread_id_
              << "] retry - " << i << " / " << MAX_RETRY;
    if (i + 1 < MAX_RETRY) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    decoder.reset(sv.data(), sv.size());
  }

  const auto end = std::chrono::high_resolution_clock::now();
  eval_duration_.fetch_add(
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count());
  ++query_num_;
  // When query failed, we assume the user may put the error message in the
  // output buffer.
  // For example, for adhoc_app.cc, if the query failed, the error info will
  // be put in the output buffer.
  if (result_buffer.size() > 4) {
    return Result<std::vector<char>>(
        StatusCode::QUERY_FAILED,
        std::string{result_buffer.data() + 4,
                    result_buffer.size() -
                        4},  // The first 4 bytes are the length of the message.
        result_buffer);
  } else {
    return Result<std::vector<char>>(
        StatusCode::QUERY_FAILED,
        "Query failed for procedure id:" + std::to_string((int) type),
        result_buffer);
  }
}

void GraphDBSession::GetAppInfo(Encoder& result) { db_.GetAppInfo(result); }

int GraphDBSession::SessionId() const { return thread_id_; }

CompactTransaction GraphDBSession::GetCompactTransaction() {
  timestamp_t ts = db_.version_manager_.acquire_update_timestamp();
  return CompactTransaction(db_.graph_, logger_, db_.version_manager_, ts);
}

bool GraphDBSession::Compact() {
  auto txn = GetCompactTransaction();
  if (txn.timestamp() > db_.GetLastCompactionTimestamp() + 100000) {
    db_.UpdateCompactionTimestamp(txn.timestamp());
    txn.Commit();
    return true;
  } else {
    txn.Abort();
    return false;
  }
}

double GraphDBSession::eval_duration() const {
  return static_cast<double>(eval_duration_.load()) / 1000000.0;
}

int64_t GraphDBSession::query_num() const { return query_num_.load(); }

AppBase* GraphDBSession::GetApp(const std::string& app_name) {
  auto& app_name_to_path_index = db_.schema().GetPlugins();
  if (app_name_to_path_index.count(app_name) <= 0) {
    LOG(ERROR) << "Query name is not registered: " << app_name;
    return nullptr;
  }
  return GetApp(app_name_to_path_index.at(app_name).second);
}

#define likely(x) __builtin_expect(!!(x), 1)

AppBase* GraphDBSession::GetApp(int type) {
  // create if not exist
  if (type >= GraphDBSession::MAX_PLUGIN_NUM) {
    LOG(ERROR) << "Query type is out of range: " << type << " > "
               << GraphDBSession::MAX_PLUGIN_NUM;
    return nullptr;
  }
  AppBase* app = nullptr;
  if (likely(apps_[type] != nullptr)) {
    app = apps_[type];
  } else {
    app_wrappers_[type] = db_.CreateApp(type, thread_id_);
    if (app_wrappers_[type].app() == NULL) {
      LOG(ERROR) << "[Query-" + std::to_string((int) type)
                 << "] is not registered...";
      return nullptr;
    } else {
      apps_[type] = app_wrappers_[type].app();
      app = apps_[type];
    }
  }
  return app;
}

#undef likely  // likely

Result<std::pair<uint8_t, std::string_view>>
GraphDBSession::parse_query_type_from_cypher_json(
    const std::string_view& str_view) {
  VLOG(10) << "string view: " << str_view;
  rapidjson::Document j;
  if (j.Parse(std::string(str_view.data(), str_view.size() - 1))
          .HasParseError()) {
    LOG(ERROR) << "Fail to parse json from input content";
    return Result<std::pair<uint8_t, std::string_view>>(gs::Status(
        StatusCode::INTERNAL_ERROR, "Fail to parse json from input content"));
  }
  std::string query_name = j["query_name"].GetString();
  const auto& app_name_to_path_index = schema().GetPlugins();
  if (app_name_to_path_index.count(query_name) <= 0) {
    LOG(ERROR) << "Query name is not registered: " << query_name;
    return Result<std::pair<uint8_t, std::string_view>>(gs::Status(
        StatusCode::NOT_FOUND, "Query name is not registered: " + query_name));
  }
  if (j.HasMember("arguments")) {
    for (auto& arg : j["arguments"].GetArray()) {
      VLOG(10) << "arg: " << jsonToString(arg);
    }
  }
  VLOG(10) << "Query name: " << query_name;
  return std::make_pair(app_name_to_path_index.at(query_name).second, str_view);
}

Result<std::pair<uint8_t, std::string_view>>
GraphDBSession::parse_query_type_from_cypher_internal(
    const std::string_view& str_view) {
  procedure::Query cur_query;
  if (!cur_query.ParseFromArray(str_view.data(), str_view.size() - 1)) {
    LOG(ERROR) << "Fail to parse query from input content";
    return Result<std::pair<uint8_t, std::string_view>>(gs::Status(
        StatusCode::INTERNAL_ERROR, "Fail to parse query from input content"));
  }
  auto query_name = cur_query.query_name().name();
  if (query_name.empty()) {
    LOG(ERROR) << "Query name is empty";
    return Result<std::pair<uint8_t, std::string_view>>(
        gs::Status(StatusCode::NOT_FOUND, "Query name is empty"));
  }
  const auto& app_name_to_path_index = schema().GetPlugins();
  // First check whether the query name is builtin query
  for (int i = 0; i < Schema::BUILTIN_PLUGIN_NUM; ++i) {
    std::string builtin_query_name = Schema::BUILTIN_PLUGIN_NAMES[i];
    if (query_name == builtin_query_name) {
      return std::make_pair(Schema::BUILTIN_PLUGIN_IDS[i], str_view);
    }
  }
  if (app_name_to_path_index.count(query_name) <= 0) {
    LOG(ERROR) << "Query name is not registered: " << query_name;
    return Result<std::pair<uint8_t, std::string_view>>(gs::Status(
        StatusCode::NOT_FOUND, "Query name is not registered: " + query_name));
  }
  return std::make_pair(app_name_to_path_index.at(query_name).second, str_view);
}

const AppMetric& GraphDBSession::GetAppMetric(int idx) const {
  return app_metrics_[idx];
}

}  // namespace gs

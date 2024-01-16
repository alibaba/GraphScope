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

#ifdef MONITOR_SESSIONS
#include <chrono>
#endif

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/utils/app_utils.h"

namespace gs {

void put_argment(Encoder& encoder, const query::Argument& argment) {
  auto& value = argment.value();
  auto item_case = value.item_case();
  switch (item_case) {
  case common::Value::kI32:
    encoder.put_int(value.i32());
    break;
  case common::Value::kI64:
    encoder.put_long(value.i64());
    break;
  case common::Value::kF64:
    encoder.put_double(value.f64());
    break;
  case common::Value::kStr:
    encoder.put_string(value.str());
    break;
  default:
    LOG(ERROR) << "Not recognizable param type" << static_cast<int>(item_case);
  }
}

ReadTransaction GraphDBSession::GetReadTransaction() {
  uint32_t ts = db_.version_manager_.acquire_read_timestamp();
  return ReadTransaction(db_.graph_, db_.version_manager_, ts);
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

MutablePropertyFragment& GraphDBSession::graph() { return db_.graph(); }

const Schema& GraphDBSession::schema() const { return db_.schema(); }

std::shared_ptr<ColumnBase> GraphDBSession::get_vertex_property_column(
    uint8_t label, const std::string& col_name) const {
  return db_.get_vertex_property_column(label, col_name);
}

std::shared_ptr<RefColumnBase> GraphDBSession::get_vertex_id_column(
    uint8_t label) const {
  if (db_.graph().lf_indexers_[label].get_type() == PropertyType::kInt64) {
    return std::make_shared<TypedRefColumn<int64_t>>(
        dynamic_cast<const TypedColumn<int64_t>&>(
            db_.graph().lf_indexers_[label].get_keys()));
  } else if (db_.graph().lf_indexers_[label].get_type() ==
             PropertyType::kInt32) {
    return std::make_shared<TypedRefColumn<int32_t>>(
        dynamic_cast<const TypedColumn<int32_t>&>(
            db_.graph().lf_indexers_[label].get_keys()));
  } else if (db_.graph().lf_indexers_[label].get_type() ==
             PropertyType::kUInt64) {
    return std::make_shared<TypedRefColumn<uint64_t>>(
        dynamic_cast<const TypedColumn<uint64_t>&>(
            db_.graph().lf_indexers_[label].get_keys()));
  } else if (db_.graph().lf_indexers_[label].get_type() ==
             PropertyType::kUInt32) {
    return std::make_shared<TypedRefColumn<uint32_t>>(
        dynamic_cast<const TypedColumn<uint32_t>&>(
            db_.graph().lf_indexers_[label].get_keys()));
  } else if (db_.graph().lf_indexers_[label].get_type() ==
             PropertyType::kString) {
    return std::make_shared<TypedRefColumn<std::string_view>>(
        dynamic_cast<const TypedColumn<std::string_view>&>(
            db_.graph().lf_indexers_[label].get_keys()));
  } else {
    return nullptr;
  }
}

#define likely(x) __builtin_expect(!!(x), 1)

Result<std::vector<char>> GraphDBSession::Eval(const std::string& input) {
#ifdef MONITOR_SESSIONS
  const auto start = std::chrono::high_resolution_clock::now();
#endif
  uint8_t type = input.back();
  const char* str_data = input.data();
  size_t str_len = input.size() - 1;

  std::vector<char> result_buffer;

  Decoder decoder(str_data, str_len);
  Encoder encoder(result_buffer);

  AppBase* app = nullptr;
  if (likely(apps_[type] != nullptr)) {
    app = apps_[type];
  } else {
    app_wrappers_[type] = db_.CreateApp(type, thread_id_);
    if (app_wrappers_[type].app() == NULL) {
      LOG(ERROR) << "[Query-" + std::to_string((int) type)
                 << "] is not registered...";
      return Result<std::vector<char>>(
          StatusCode::NotExists,
          "Query:" + std::to_string((int) type) + " is not registere",
          result_buffer);
    } else {
      apps_[type] = app_wrappers_[type].app();
      app = apps_[type];
    }
  }

  for (size_t i = 0; i < MAX_RETRY; ++i) {
    if (app->Query(decoder, encoder)) {
#ifdef MONITOR_SESSIONS
      const auto end = std::chrono::high_resolution_clock::now();
      eval_duration_.fetch_add(
          std::chrono::duration_cast<std::chrono::microseconds>(end - start)
              .count());
#endif
      ++query_num_;
      return result_buffer;
    }

    LOG(INFO) << "[Query-" << (int) type << "][Thread-" << thread_id_
              << "] retry - " << i << " / " << MAX_RETRY;
    if (i + 1 < MAX_RETRY) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    decoder.reset(str_data, str_len);
    result_buffer.clear();
  }

#ifdef MONITOR_SESSIONS
  const auto end = std::chrono::high_resolution_clock::now();
  eval_duration_.fetch_add(
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count());
#endif
  ++query_num_;
  return Result<std::vector<char>>(
      StatusCode::QueryFailed,
      "Query failed for procedure id:" + std::to_string((int) type),
      result_buffer);
}

// Evaluating stored procedure for hqps adhoc query, the dynamic lib is closed
// immediately after the query
Result<std::vector<char>> GraphDBSession::EvalAdhoc(
    const std::string& input_lib_path) {
  std::vector<char> result_buffer;
  std::vector<char> input_buffer;  // empty. Adhoc query receives no input
  Decoder decoder(input_buffer.data(), input_buffer.size());
  Encoder encoder(result_buffer);

  // the dynamic library will automatically be closed after the query
  auto app_factory = std::make_shared<SharedLibraryAppFactory>(input_lib_path);
  AppWrapper app_wrapper;  // wrapper should be destroyed before the factory

  if (app_factory) {
    app_wrapper = app_factory->CreateApp(*this);
    if (app_wrapper.app() == NULL) {
      LOG(ERROR) << "Fail to create app for adhoc query: " << input_lib_path;
      return Result<std::vector<char>>(
          StatusCode::InternalError,
          "Fail to create app for: " + input_lib_path, result_buffer);
    }
  } else {
    LOG(ERROR) << "Fail to evaluate adhoc query: " << input_lib_path;
    return Result<std::vector<char>>(
        StatusCode::NotExists,
        "Fail to open dynamic lib for: " + input_lib_path, result_buffer);
  }

  for (size_t i = 0; i < MAX_RETRY; ++i) {
    if (app_wrapper.app()->Query(decoder, encoder)) {
      return result_buffer;
    }

    LOG(INFO) << "[Query-" << input_lib_path << "][Thread-" << thread_id_
              << "] retry - " << i << " / " << MAX_RETRY;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    result_buffer.clear();
  }
  return Result<std::vector<char>>(
      StatusCode::QueryFailed,
      "Query failed for adhoc query: " + input_lib_path, result_buffer);
}

Result<std::vector<char>> GraphDBSession::EvalHqpsProcedure(
    const query::Query& query_pb) {
  auto query_name = query_pb.query_name().name();
  if (query_name.empty()) {
    LOG(ERROR) << "Query name is empty";
    return Result<std::vector<char>>(StatusCode::InValidArgument,
                                     "Query name is empty", {});
  }
  auto& app_name_to_path_index = db_.schema().GetPlugins();
  // get procedure id from name.
  if (app_name_to_path_index.count(query_name) <= 0) {
    LOG(ERROR) << "Query name is not registered: " << query_name;
    return Result<std::vector<char>>(
        StatusCode::NotExists, "Query name is not registered: " + query_name,
        {});
  }

  // get app
  auto type = app_name_to_path_index.at(query_name).second;
  if (type >= apps_.size()) {
    LOG(ERROR) << "Query type is not registered: " << type;
    return Result<std::vector<char>>(
        StatusCode::NotExists,
        "Query type is not registered: " + std::to_string(type), {});
  }
  AppBase* app = nullptr;
  if (likely(apps_[type] != nullptr)) {
    app = apps_[type];
  } else {
    app_wrappers_[type] = db_.CreateApp(type, thread_id_);
    if (app_wrappers_[type].app() == NULL) {
      LOG(ERROR) << "[Query-" + std::to_string((int) type)
                 << "] is not registered...";
      return Result<std::vector<char>>(
          StatusCode::NotExists,
          "Query:" + std::to_string((int) type) + " is not registered", {});
    } else {
      apps_[type] = app_wrappers_[type].app();
      app = apps_[type];
    }
  }

  if (app == nullptr) {
    LOG(ERROR) << "Query type is not registered: " << type
               << ", query name: " << query_name;
    return Result<std::vector<char>>(
        StatusCode::NotExists,
        "Query type is not registered: " + std::to_string(type), {});
  }

  std::vector<char> input_buffer;
  gs::Encoder input_encoder(input_buffer);
  auto& args = query_pb.arguments();
  for (int32_t i = 0; i < args.size(); ++i) {
    auto& arg = args[i];
    put_argment(input_encoder, arg);
  }
  const char* str_data = input_buffer.data();
  size_t str_len = input_buffer.size();
  gs::Decoder input_decoder(input_buffer.data(), input_buffer.size());

  for (size_t i = 0; i < MAX_RETRY; ++i) {
    std::vector<char> result_buffer;
    gs::Encoder result_encoder(result_buffer);
    if (app->Query(input_decoder, result_encoder)) {
      return Result<std::vector<char>>(std::move(result_buffer));
    }
    LOG(INFO) << "[Query-" << query_name << "][Thread-" << thread_id_
              << "] retry - " << i << " / " << MAX_RETRY;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    input_decoder.reset(str_data, str_len);
  }
  return Result<std::vector<char>>(
      StatusCode::QueryFailed, "Query failed for procedure: " + query_name, {});
}

#undef likely

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

#ifdef MONITOR_SESSIONS
double GraphDBSession::eval_duration() const {
  return static_cast<double>(eval_duration_.load()) / 1000000.0;
}

#endif

int64_t GraphDBSession::query_num() const { return query_num_.load(); }

}  // namespace gs

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

#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/app/app_utils.h"
#include "flex/engines/graph_db/database/graph_db.h"

namespace gs {

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
  return UpdateTransaction(db_.graph_, alloc_, logger_, db_.version_manager_,
                           ts);
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

#define likely(x) __builtin_expect(!!(x), 1)

std::vector<char> GraphDBSession::Eval(const std::string& input) {
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
      return result_buffer;
    } else {
      apps_[type] = app_wrappers_[type].app();
      app = apps_[type];
    }
  }

  if (app->Query(decoder, encoder)) {
    return result_buffer;
  }

  LOG(INFO) << "[Query-" << (int) type << "][Thread-" << thread_id_
            << "] retry - 1 / 3";
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  decoder.reset(str_data, str_len);
  result_buffer.clear();
  if (app->Query(decoder, encoder)) {
    return result_buffer;
  }

  LOG(INFO) << "[Query-" << (int) type << "][Thread-" << thread_id_
            << "] retry - 2 / 3";
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  decoder.reset(str_data, str_len);
  result_buffer.clear();
  if (app->Query(decoder, encoder)) {
    return result_buffer;
  }

  LOG(INFO) << "[Query-" << (int) type << "][Thread-" << thread_id_
            << "] retry - 3 / 3";
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  decoder.reset(str_data, str_len);
  result_buffer.clear();
  if (app->Query(decoder, encoder)) {
    return result_buffer;
  }
  LOG(INFO) << "[Query-" << (int) type << "][Thread-" << thread_id_
            << "] failed after 3 retries";

  result_buffer.clear();
  return result_buffer;
}

#undef likely

void GraphDBSession::GetAppInfo(Encoder& result) { db_.GetAppInfo(result); }

int GraphDBSession::SessionId() const { return thread_id_; }

}  // namespace gs

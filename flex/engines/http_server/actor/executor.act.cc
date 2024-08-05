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

#include "flex/engines/http_server/actor/executor.act.h"

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_operations.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/http_server/graph_db_service.h"
#include "graph_db_service.h"
#include "nlohmann/json.hpp"

#include <seastar/core/print.hh>

namespace server {

executor::~executor() {
  // finalization
  // ...
}

executor::executor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr)
    : hiactor::actor(exec_ctx, addr) {
  set_max_concurrency(1);  // set max concurrency for task reentrancy (stateful)
  // initialization
  // ...
  auto& graph_db_service = GraphDBService::get();
  // meta_data_ should be thread safe.
  metadata_store_ = graph_db_service.get_metadata_store();
}

seastar::future<query_result> executor::run_graph_db_query(
    query_param&& param) {
  auto ret = gs::GraphDB::get()
                 .GetSession(hiactor::local_shard_id())
                 .Eval(param.content);
  if (!ret.ok()) {
    LOG(ERROR) << "Eval failed: " << ret.status().error_message();
    return seastar::make_exception_future<query_result>(
        "Query failed: " + ret.status().error_message());
  }

  auto result = ret.value();
  seastar::sstring content(result.data(), result.size());
  return seastar::make_ready_future<query_result>(std::move(content));
}

seastar::future<admin_query_result> executor::create_vertex(
    graph_management_param&& param) {
  std::string&& graph_id = std::move(param.content.first);
  auto running_graph_res = metadata_store_->GetRunningGraph();

  if (!running_graph_res.ok() || running_graph_res.value() != graph_id)
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::Status(gs::StatusCode::NotFound,
                       "The queried graph is not running: " + graph_id)));
  nlohmann::json input_json;
  // Parse the input json
  try {
    input_json = nlohmann::json::parse(param.content.second);
  } catch (const std::exception& e) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::Status(gs::StatusCode::InvalidSchema,
                       "Bad input json : " + std::string(e.what()))));
  }
  auto result = gs::GraphDBOperations::CreateVertex(
      gs::GraphDB::get().GetSession(hiactor::local_shard_id()),
      std::move(input_json));

  if (result.ok()) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(result.value()));
  }
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(result.status()));
}

seastar::future<admin_query_result> executor::create_edge(
    graph_management_param&& param) {
  std::string&& graph_id = std::move(param.content.first);
  auto running_graph_res = metadata_store_->GetRunningGraph();
  if (!running_graph_res.ok() || running_graph_res.value() != graph_id)
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::Status(gs::StatusCode::NotFound,
                       "The queried graph is not running: " + graph_id)));
  nlohmann::json input_json;
  // Parse the input json
  try {
    input_json = nlohmann::json::parse(param.content.second);
  } catch (const std::exception& e) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::Status(gs::StatusCode::InvalidSchema,
                       "Bad input json : " + std::string(e.what()))));
  }
  auto result = gs::GraphDBOperations::CreateEdge(
      gs::GraphDB::get().GetSession(hiactor::local_shard_id()),
      std::move(input_json));
  if (result.ok()) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(result.value()));
  }
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(result.status()));
}

seastar::future<admin_query_result> executor::update_vertex(
    graph_management_param&& param) {
  std::string&& graph_id = std::move(param.content.first);
  auto running_graph_res = metadata_store_->GetRunningGraph();

  if (!running_graph_res.ok() || running_graph_res.value() != graph_id)
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::Status(gs::StatusCode::NotFound,
                       "The queried graph is not running: " + graph_id)));
  nlohmann::json input_json;
  // Parse the input json
  try {
    input_json = nlohmann::json::parse(param.content.second);
  } catch (const std::exception& e) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::Status(gs::StatusCode::InvalidSchema,
                       "Bad input json : " + std::string(e.what()))));
  }
  auto result = gs::GraphDBOperations::UpdateVertex(
      gs::GraphDB::get().GetSession(hiactor::local_shard_id()),
      std::move(input_json));

  if (result.ok()) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(result.value()));
  }
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(result.status()));
}
seastar::future<admin_query_result> executor::update_edge(
    graph_management_param&& param) {
  std::string&& graph_id = std::move(param.content.first);
  auto running_graph_res = metadata_store_->GetRunningGraph();

  if (!running_graph_res.ok() || running_graph_res.value() != graph_id)
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::Status(gs::StatusCode::NotFound,
                       "The queried graph is not running: " + graph_id)));
  nlohmann::json input_json;
  // Parse the input json
  try {
    input_json = nlohmann::json::parse(param.content.second);
  } catch (const std::exception& e) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::Status(gs::StatusCode::InvalidSchema,
                       "Bad input json : " + std::string(e.what()))));
  }
  auto result = gs::GraphDBOperations::UpdateEdge(
      gs::GraphDB::get().GetSession(hiactor::local_shard_id()),
      std::move(input_json));

  if (result.ok()) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(result.value()));
  }
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(result.status()));
}

seastar::future<admin_query_result> executor::get_vertex(
    graph_management_query_param&& param) {
  std::string&& graph_id = std::move(param.content.first);
  auto running_graph_res = metadata_store_->GetRunningGraph();

  if (!running_graph_res.ok() || running_graph_res.value() != graph_id)
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::Status(gs::StatusCode::NotFound,
                       "The queried graph is not running: " + graph_id)));

  std::unordered_map<std::string, std::string> params;
  for (auto& [key, value] : param.content.second) {
    params[std::string(key)] = std::string(value);
  }
  auto result = gs::GraphDBOperations::GetVertex(
      gs::GraphDB::get().GetSession(hiactor::local_shard_id()),
      std::move(params));

  if (result.ok()) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(result.value()));
  }
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(result.status()));
}

seastar::future<admin_query_result> executor::get_edge(
    graph_management_query_param&& param) {
  std::string&& graph_id = std::move(param.content.first);
  auto running_graph_res = metadata_store_->GetRunningGraph();

  if (!running_graph_res.ok() || running_graph_res.value() != graph_id)
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::Status(gs::StatusCode::NotFound,
                       "The queried graph is not running: " + graph_id)));
  std::unordered_map<std::string, std::string> params;
  for (auto& [key, value] : param.content.second) {
    params[std::string(key)] = std::string(value);
  }
  auto result = gs::GraphDBOperations::GetEdge(
      gs::GraphDB::get().GetSession(hiactor::local_shard_id()),
      std::move(params));

  if (result.ok()) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(result.value()));
  }
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(result.status()));
}

seastar::future<admin_query_result> executor::delete_vertex(
    graph_management_param&& param) {
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(gs::Status(
          gs::StatusCode::Unimplemented, "delete_vertex is not implemented")));
}

seastar::future<admin_query_result> executor::delete_edge(
    graph_management_param&& param) {
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(gs::Status(
          gs::StatusCode::Unimplemented, "delete_edge is not implemented")));
}

}  // namespace server

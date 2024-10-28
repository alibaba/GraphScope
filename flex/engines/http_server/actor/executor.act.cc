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

#include <rapidjson/document.h>
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
    query_param&& param) {
  rapidjson::Document input_json;
  // Parse the input json
  if (input_json.Parse(param.content.c_str()).HasParseError()) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(gs::Status(
            gs::StatusCode::INVALID_SCHEMA,
            "Bad input json : " + std::to_string(input_json.GetParseError()))));
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

seastar::future<admin_query_result> executor::create_edge(query_param&& param) {
  rapidjson::Document input_json;
  if (input_json.Parse(param.content.c_str()).HasParseError()) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(gs::Status(
            gs::StatusCode::INVALID_SCHEMA,
            "Bad input json : " + std::to_string(input_json.GetParseError()))));
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
    query_param&& param) {
  rapidjson::Document input_json;
  // Parse the input json
  if (input_json.Parse(param.content.c_str()).HasParseError()) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(gs::Status(
            gs::StatusCode::INVALID_SCHEMA,
            "Bad input json : " + std::to_string(input_json.GetParseError()))));
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
seastar::future<admin_query_result> executor::update_edge(query_param&& param) {
  rapidjson::Document input_json;
  // Parse the input json
  if (input_json.Parse(param.content.c_str()).HasParseError()) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(gs::Status(
            gs::StatusCode::INVALID_SCHEMA,
            "Bad input json : " + std::to_string(input_json.GetParseError()))));
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
  std::unordered_map<std::string, std::string> params;
  for (auto& [key, value] : param.content) {
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
  std::unordered_map<std::string, std::string> params;
  for (auto& [key, value] : param.content) {
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
    query_param&& param) {
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(gs::Status(
          gs::StatusCode::UNIMPLEMENTED, "delete_vertex is not implemented")));
}

seastar::future<admin_query_result> executor::delete_edge(query_param&& param) {
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(gs::Status(
          gs::StatusCode::UNIMPLEMENTED, "delete_edge is not implemented")));
}

}  // namespace server

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
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/http_server/codegen_proxy.h"
#include "flex/engines/http_server/workdir_manipulator.h"
#include "flex/proto_generated_gie/stored_procedure.pb.h"
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
}

seastar::future<query_result> executor::run_graph_db_query(
    query_param&& param) {
  auto ret = gs::GraphDB::get()
                 .GetSession(hiactor::local_shard_id())
                 .Eval(param.content);
  if (!ret.ok()) {
    LOG(ERROR) << "Eval failed: " << ret.status().error_message();
  }
  auto result = ret.value();
  seastar::sstring content(result.data(), result.size());
  return seastar::make_ready_future<query_result>(std::move(content));
}

// run_query_for stored_procedure
seastar::future<query_result> executor::run_hqps_procedure_query(
    query_param&& param) {
  auto& str = param.content;
  const char* str_data = str.data();
  size_t str_length = str.size();
  LOG(INFO) << "Receive pay load: " << str_length << " bytes";

  query::Query cur_query;
  if (!cur_query.ParseFromArray(str_data, str_length)) {
    LOG(ERROR) << "Fail to parse query from pay load";
    return seastar::make_ready_future<query_result>(
        seastar::sstring("Fail to parse query from pay load"));
  }

  auto ret = gs::GraphDB::get()
                 .GetSession(hiactor::local_shard_id())
                 .EvalHqpsProcedure(cur_query);
  if (!ret.ok()) {
    LOG(ERROR) << "Eval failed: " << ret.status().error_message();
    return seastar::make_exception_future<query_result>(
        seastar::sstring(ret.status().error_message()));
  }
  auto result = ret.value();
  if (result.size() < 4) {
    return seastar::make_exception_future<query_result>(seastar::sstring(
        "Internal Error, more than 4 bytes should be returned"));
  }
  seastar::sstring content(
      result.data() + 4,
      result.size() - 4);  // skip 4 bytes, since the first 4
                           // bytes is the size of the result
  return seastar::make_ready_future<query_result>(std::move(content));
}

seastar::future<query_result> executor::run_hqps_adhoc_query(
    adhoc_result&& param) {
  LOG(INFO) << "Run adhoc query";
  // The received query's pay load shoud be able to deserialze to physical plan
  // 1. load and run.
  auto& content = param.content;
  LOG(INFO) << "Okay, try to run the query of lib path: " << content.second
            << ", job id: " << content.first
            << "local shard id: " << hiactor::local_shard_id();
  // seastar::sstring result = server::load_and_run(content.first,
  // content.second);
  auto ret = gs::GraphDB::get()
                 .GetSession(hiactor::local_shard_id())
                 .EvalAdhoc(content.second);
  if (!ret.ok()) {
    LOG(ERROR) << "Eval failed: " << ret.status().error_message();
    return seastar::make_exception_future<query_result>(
        seastar::sstring(ret.status().error_message()));
  }
  auto ret_value = ret.value();
  LOG(INFO) << "Adhoc query result size: " << ret_value.size();
  if (ret_value.size() < 4) {
    return seastar::make_exception_future<query_result>(seastar::sstring(
        "Internal Error, more than 4 bytes should be returned"));
  }

  seastar::sstring result(
      ret_value.data() + 4,
      ret_value.size() - 4);  // skip 4 bytes, since the first 4
                              // bytes is the size of the result
  return seastar::make_ready_future<query_result>(std::move(result));
}

}  // namespace server

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
  auto& input_content = param.content;
  if (input_content.size() < 1) {
    return seastar::make_exception_future<query_result>(
        seastar::sstring("Invalid status"));
  }
  // get the last byte
  char type = input_content[input_content.size() - 1];
  input_content.resize(input_content.size() - 1);
  LOG(INFO) << "Run graph db query, type: " << type;
  if (type == '\0') {  // graph_db query
    auto ret = gs::GraphDB::get()
                   .GetSession(hiactor::local_shard_id())
                   .Eval(input_content);
    if (!ret.ok()) {
      LOG(ERROR) << "Eval failed: " << ret.status().error_message();
    }
    auto result = ret.value();
    seastar::sstring content(result.data(), result.size());
    return seastar::make_ready_future<query_result>(std::move(content));
  } else if (type == '\1') {  // hqps procedure query.
#ifndef BUILD_HQPS
    return seastar::make_exception_future<query_result>(
        seastar::sstring("HQPS is disabled, please recompile with "
                         "BUILD_HQPS=ON to enable HQPS"));
#else
    auto ret = gs::GraphDB::get()
                   .GetSession(hiactor::local_shard_id())
                   .EvalHqpsProcedure(input_content);
    if (!ret.ok()) {
      LOG(ERROR) << "Eval failed: " << ret.status().error_message();
      return seastar::make_exception_future<query_result>(
          seastar::sstring(ret.status().error_message()));
    }
    auto result = ret.value();
    if (result.size() < 4) {
      return seastar::make_exception_future<query_result>(
          seastar::sstring("Internal Error when calling procedure, more than 4 "
                           "bytes should be returned"));
    }
    // skip 4 bytes, since the first 4 bytes is the size of the result
    seastar::sstring content(result.data() + 4, result.size() - 4);
    return seastar::make_ready_future<query_result>(std::move(content));
#endif                        // BUILD_HQPS
  } else if (type == '\2') {  // hqp adhoc query
#ifndef BUILD_HQPS
    return seastar::make_exception_future<query_result>(
        seastar::sstring("HQPS is disabled, please recompile with "
                         "BUILD_HQPS=ON to enable HQPS"));
#else
    auto ret = gs::GraphDB::get()
                   .GetSession(hiactor::local_shard_id())
                   .EvalAdhoc(input_content);
    if (!ret.ok()) {
      LOG(ERROR) << "Eval failed: " << ret.status().error_message();
      return seastar::make_exception_future<query_result>(
          seastar::sstring(ret.status().error_message()));
    }
    auto ret_value = ret.value();
    if (ret_value.size() < 4) {
      return seastar::make_exception_future<query_result>(
          seastar::sstring("Internal Error when running Adhoc query, more than "
                           "4 bytes should be returned"));
    }
    // skip 4 bytes, since the first 4 bytes is the size of the result
    seastar::sstring result(ret_value.data() + 4, ret_value.size() - 4);
    return seastar::make_ready_future<query_result>(std::move(result));
#endif  // BUILD_HQPS
  } else {
    seastar::sstring error_msg = "Invalid query type: " + std::to_string(type);
    return seastar::make_exception_future<query_result>(error_msg);
  }
}

}  // namespace server

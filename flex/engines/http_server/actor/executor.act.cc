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
#ifdef BUILD_HQPS
#include "flex/proto_generated_gie/stored_procedure.pb.h"
#endif

#include <seastar/core/print.hh>

namespace server {
#ifdef BUILD_HQPS
void put_argment(gs::Encoder& encoder, const query::Argument& argment) {
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
#endif

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
        seastar::sstring("HQPS is not disabled, please recompile with "
                         "BUILD_HQPS=ON to enable HQPS"));
#else
    query::Query cur_query;
    if (!cur_query.ParseFromArray(input_content.data(), input_content.size())) {
      return seastar::make_exception_future<query_result>(
          seastar::sstring("Can not parse the query"));
    }
    auto query_name = cur_query.query_name().name();

    std::vector<char> input_buffer;
    gs::Encoder input_encoder(input_buffer);
    auto& args = cur_query.arguments();
    for (int32_t i = 0; i < args.size(); ++i) {
      put_argment(input_encoder, args[i]);
    }
    VLOG(10) << "Query name: " << query_name
             << ", args: " << input_buffer.size() << " bytes";
    gs::Decoder decoder(input_buffer.data(), input_buffer.size());
    auto ret = gs::GraphDB::get()
                   .GetSession(hiactor::local_shard_id())
                   .EvalHqpsProcedure(query_name, decoder);
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
#endif                        // BUILD_HQPS
  } else if (type == '\2') {  // hqp adhoc query
#ifndef BUILD_HQPS
    return seastar::make_exception_future<query_result>(
        seastar::sstring("HQPS is not disabled, please recompile with "
                         "BUILD_HQPS=ON to enable HQPS"));
#else
    LOG(INFO) << "Okay, try to run adhoc query of lib path: " << input_content;
    // seastar::sstring result = server::load_and_run(content.first,
    // content.second);
    auto ret = gs::GraphDB::get()
                   .GetSession(hiactor::local_shard_id())
                   .EvalAdhoc(input_content);
    if (!ret.ok()) {
      LOG(ERROR) << "Eval failed: " << ret.status().error_message();
      return seastar::make_exception_future<query_result>(
          seastar::sstring(ret.status().error_message()));
    }
    auto ret_value = ret.value();
    VLOG(10) << "Adhoc query result size: " << ret_value.size();
    if (ret_value.size() < 4) {
      return seastar::make_exception_future<query_result>(seastar::sstring(
          "Internal Error, more than 4 bytes should be returned"));
    }
    seastar::sstring result(
        ret_value.data() + 4,
        ret_value.size() - 4);  // skip 4 bytes, since the first 4
                                // bytes is the size of the result
    return seastar::make_ready_future<query_result>(std::move(result));
#endif  // BUILD_HQPS
  } else {
    seastar::sstring error_msg = "Invalid query type: " + std::to_string(type);
    return seastar::make_exception_future<query_result>(error_msg);
  }
}

}  // namespace server

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

#include "flex/engines/http_server/actor/codegen_actor.act.h"

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/http_server/codegen_proxy.h"
#include "flex/proto_generated_gie/physical.pb.h"

#include <seastar/core/print.hh>

namespace server {

codegen_actor::~codegen_actor() {
  // finalization
  // ...
}

codegen_actor::codegen_actor(hiactor::actor_base* exec_ctx,
                             const hiactor::byte_t* addr)
    : hiactor::actor(exec_ctx, addr) {
  set_max_concurrency(1);  // set max concurrency for task reentrancy
  // (stateful) initialization
  // ...
}

seastar::future<query_result> codegen_actor::do_codegen(query_param&& param) {
  LOG(INFO) << "Running codegen for " << param.content.size();
  // The received query's pay load should be able to deserialize to physical plan
  auto& str = param.content;
  if (str.size() <= 0) {
    LOG(INFO) << "Empty query";
    return seastar::make_exception_future<query_result>(
        std::runtime_error("Empty query string"));
  }

  const char* str_data = str.data();
  size_t str_length = str.size();
  LOG(INFO) << "Deserialize physical job request" << str_length;

  physical::PhysicalPlan plan;
  bool ret = plan.ParseFromArray(str_data, str_length);
  if (ret) {
    VLOG(10) << "Parse physical plan: " << plan.DebugString();
  } else {
    LOG(ERROR) << "Fail to parse physical plan";
    return seastar::make_exception_future<query_result>(
        std::runtime_error("Fail to parse physical plan"));
  }

  // 0. do codegen gen.
  std::string lib_path = "";
  int32_t job_id = -1;
  auto& codegen_proxy = server::CodegenProxy::get();
  if (codegen_proxy.Initialized()) {
    return codegen_proxy.DoGen(plan).then(
        [](std::pair<int32_t, std::string>&& job_id_and_lib_path) {
          if (job_id_and_lib_path.first == -1) {
            return seastar::make_exception_future<query_result>(
                std::runtime_error("Fail to parse job id from codegen proxy"));
          }
          // 1. load and run.
          LOG(INFO) << "Okay, try to run the query of lib path: "
                    << job_id_and_lib_path.second
                    << ", job id: " << job_id_and_lib_path.first
                    << "local shard id: " << hiactor::local_shard_id();
          return seastar::make_ready_future<query_result>(
              std::move(job_id_and_lib_path.second));
        });
  } else {
    return seastar::make_exception_future<query_result>(
        std::runtime_error("Codegen proxy not initialized"));
  }
}

}  // namespace server

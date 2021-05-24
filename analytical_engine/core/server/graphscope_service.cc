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

#include <unordered_map>

#include "core/server/graphscope_service.h"

#include "core/server/rpc_utils.h"

namespace gs {
namespace rpc {

::grpc::Status GraphScopeService::HeartBeat(::grpc::ServerContext* context,
                                            const HeartBeatRequest* request,
                                            HeartBeatResponse* response) {
  ResponseStatus* res_status = response->mutable_status();
  res_status->set_code(rpc::Code::OK);

  return ::grpc::Status::OK;
}

::grpc::Status GraphScopeService::RunStep(::grpc::ServerContext* context,
                                          const RunStepRequest* request,
                                          RunStepResponse* response) {
  CHECK(request->has_dag_def());
  const DagDef& dag_def = request->dag_def();
  auto* res_status = response->mutable_status();
  std::unordered_map<std::string, OpResult*> op_key_to_result;

  for (const auto& op : dag_def.op()) {
    OpResult* op_result = response->add_results();
    op_result->set_key(op.key());
    op_key_to_result.emplace(op.key(), op_result);
    CommandDetail cmd = OpToCmd(op);

    bool success = true;
    std::string error_msgs;
    auto result = dispatcher_->Dispatch(cmd);
    auto policy = result[0].aggregate_policy();

    // First pass: make sure all result are valid and check the consistency
    for (auto& e : result) {
      auto ok = (e.error_code() == rpc::Code::OK);
      if (ok) {
        CHECK_EQ(e.aggregate_policy(), policy);
        auto& graph_def = e.graph_def();

        if (!graph_def.key().empty()) {
          if (op_result->graph_def().key().empty()) {
            op_result->mutable_graph_def()->CopyFrom(graph_def);
          } else if (graph_def.SerializeAsString() !=
                     op_result->graph_def().SerializeAsString()) {
            LOG(FATAL) << "BUG: Multiple workers return different graph def.";
          }
        }
      } else {
        error_msgs += e.message() + "\n";
        op_result->set_code(e.error_code());
      }
      success &= ok;
    }

    if (!success) {
      res_status->set_code(rpc::Code::ANALYTICAL_ENGINE_INTERNAL_ERROR);
      res_status->set_error_msg(error_msgs);
      op_result->set_error_msg(error_msgs);
      // break dag exection flow
      break;
    }

    // Second pass: aggregate result according to the policy
    switch (policy) {
    case DispatchResult::AggregatePolicy::kPickFirst: {
      op_result->mutable_result()->assign(result[0].data());
      break;
    }
    case DispatchResult::AggregatePolicy::kPickFirstNonEmpty: {
      for (auto& e : result) {
        auto& data = e.data();

        if (!data.empty()) {
          op_result->mutable_result()->assign(data.begin(), data.end());
          break;
        }
      }
      break;
    }
    case DispatchResult::AggregatePolicy::kRequireConsistent: {
      for (auto& e : result) {
        auto& data = e.data();

        if (op_result->result().empty()) {
          op_result->mutable_result()->assign(data.begin(), data.end());
        } else if (op_result->result() != data) {
          std::stringstream ss;

          ss << "Error: Multiple workers return different data."
             << " Current worker id: " << e.worker_id() << " " << data
             << " vs the previous: " << op_result->result();

          op_result->set_code(rpc::Code::WORKER_RESULTS_INCONSISTENT_ERROR);
          res_status->set_error_msg(ss.str());
          LOG(ERROR) << ss.str();
          success &= false;
          break;
        }
      }
      break;
    }
    case DispatchResult::AggregatePolicy::kConcat: {
      for (auto& e : result) {
        op_result->mutable_result()->append(e.data());
      }
      break;
    }
    }

    if (!success) {
      res_status->set_code(rpc::Code::ANALYTICAL_ENGINE_INTERNAL_ERROR);
      // break dag exection flow
      break;
    }
  }

  return ::grpc::Status::OK;
}

}  // namespace rpc
}  // namespace gs

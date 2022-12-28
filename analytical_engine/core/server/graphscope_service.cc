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

#include "core/server/graphscope_service.h"

#include <glog/logging.h>

#include <cassert>
#include <ostream>
#include <queue>
#include <string>
#include <vector>

#include "grpcpp/support/sync_stream.h"

#include "core/server/rpc_utils.h"
#include "graphscope/proto/attr_value.pb.h"
#include "graphscope/proto/error_codes.pb.h"
#include "graphscope/proto/graph_def.pb.h"
#include "graphscope/proto/message.pb.h"
#include "graphscope/proto/op_def.pb.h"

namespace gs {
struct CommandDetail;

namespace rpc {

Status GraphScopeService::HeartBeat(ServerContext* context,
                                    const HeartBeatRequest* request,
                                    HeartBeatResponse* response) {
  return Status::OK;
}

::grpc::Status GraphScopeService::RunStep(
    ServerContext* context,
    ServerReaderWriter<RunStepResponse, RunStepRequest>* stream) {
  DagDef dag_def;
  std::queue<std::string> chunks;
  RunStepRequest request;
  bool has_next = true;
  // read stream request and join the chunk
  while (stream->Read(&request)) {
    if (request.has_head()) {
      // head is always the first in the stream
      // get a copy of 'dag_def' and set the 'large_attr' from body later.
      dag_def = request.head().dag_def();
    } else {
      // body
      if (chunks.empty() || has_next == false) {
        chunks.push("");
      }
      auto& chunk = chunks.back();
      chunk += request.body().chunk();
      has_next = request.body().has_next();
    }
  }
  // fill the chunks into dag_def
  auto* ops = dag_def.mutable_op();
  for (auto& op : *ops) {
    LargeAttrValue large_attr = op.large_attr();
    if (large_attr.has_chunk_meta_list()) {
      auto* mutable_large_attr = op.mutable_large_attr();
      auto* chunk_list = mutable_large_attr->mutable_chunk_list();
      for (const auto& chunk_meta : large_attr.chunk_meta_list().items()) {
        auto* chunk = chunk_list->add_items();
        if (chunk_meta.size() > 0) {
          // set buffer
          chunk->set_buffer(std::move(chunks.front()));
          chunks.pop();
        }
        // copy attr from chunk_meta
        auto* mutable_attr = chunk->mutable_attr();
        for (auto& attr : chunk_meta.attr()) {
          (*mutable_attr)[attr.first].CopyFrom(attr.second);
        }
      }
    }
  }
  assert(chunks.empty());

  // construct ops result
  RunStepResponse response_head;
  auto* head = response_head.mutable_head();
  // a list of chunks as response body for large result
  std::vector<RunStepResponse> response_bodies;

  // execute the dag
  for (const auto& op : dag_def.op()) {
    OpResult* op_result = head->add_results();
    op_result->set_key(op.key());
    std::shared_ptr<CommandDetail> cmd = OpToCmd(op);

    bool success = true;
    std::string error_msgs;
    auto result = dispatcher_->Dispatch(cmd);
    auto policy = result[0].aggregate_policy();

    // First pass: make sure all result are valid and check the consistency
    for (auto& e : result) {
      auto ok = (e.error_code() == rpc::Code::OK);
      if (ok) {
        CHECK_EQ(e.aggregate_policy(), policy);
      } else {
        error_msgs += e.message() + "\n";
        op_result->set_code(e.error_code());
      }
      success &= ok;
    }

    if (!success) {
      op_result->set_error_msg(error_msgs);
      // break dag exection flow
      stream->Write(response_head);
      return Status(StatusCode::INTERNAL, error_msgs);
    }

    // Second pass: aggregate graph def or data result according to the policy
    switch (policy) {
    case DispatchResult::AggregatePolicy::kPickFirst: {
      splitOpResult(op_result, result[0], response_bodies);
      break;
    }
    case DispatchResult::AggregatePolicy::kPickFirstNonEmpty: {
      for (auto& e : result) {
        auto& data = e.data();

        if (!data.empty()) {
          splitOpResult(op_result, e, response_bodies);
          break;
        }
      }
      break;
    }
    case DispatchResult::AggregatePolicy::kRequireConsistent: {
      for (auto& e : result) {
        if (e.has_large_data()) {
          std::string error_msg =
              "Error: Result require consistenct among multiple workers can "
              "not be large data.";
          op_result->set_code(rpc::Code::WORKER_RESULTS_INCONSISTENT_ERROR);
          op_result->set_error_msg(error_msg);
          LOG(ERROR) << error_msg;
          stream->Write(response_head);
          return Status(StatusCode::INTERNAL, error_msg);
        }

        auto& data = e.data();
        if (op_result->result().empty()) {
          op_result->mutable_result()->assign(data.begin(), data.end());
        } else if (op_result->result() != data) {
          std::stringstream ss;

          ss << "Error: Multiple workers return different data."
             << " Current worker id: " << e.worker_id() << " " << data
             << " vs the previous: " << op_result->result();

          op_result->set_code(rpc::Code::WORKER_RESULTS_INCONSISTENT_ERROR);
          op_result->set_error_msg(ss.str());
          LOG(ERROR) << ss.str();
          stream->Write(response_head);
          return Status(StatusCode::INTERNAL, ss.str());
        }
      }
      break;
    }
    case DispatchResult::AggregatePolicy::kConcat: {
      for (auto& e : result) {
        splitOpResult(op_result, e, response_bodies);
      }
      break;
    }
    case DispatchResult::AggregatePolicy::kPickFirstNonEmptyGraphDef: {
      for (auto& e : result) {
        auto& graph_def = e.graph_def();
        if (!graph_def.key().empty()) {
          op_result->mutable_graph_def()->CopyFrom(graph_def);
          break;
        }
      }
      break;
    }
    case DispatchResult::AggregatePolicy::kMergeGraphDef: {
      rpc::graph::GraphDefPb merged_graph_def = result[0].graph_def();
      // aggregate the is_multigraph status of all fragments.
      for (auto& e : result) {
        auto& graph_def = e.graph_def();
        bool update =
            (merged_graph_def.is_multigraph() || graph_def.is_multigraph());
        merged_graph_def.set_is_multigraph(update);
      }
      op_result->mutable_graph_def()->CopyFrom(merged_graph_def);
      break;
    }
    }
  }

  // write responses as stream
  stream->Write(response_head);
  for (auto& response_body : response_bodies) {
    stream->Write(response_body);
  }

  return ::grpc::Status::OK;
}

}  // namespace rpc
}  // namespace gs

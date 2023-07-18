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

#ifndef ANALYTICAL_ENGINE_CORE_SERVER_GRAPHSCOPE_SERVICE_H_
#define ANALYTICAL_ENGINE_CORE_SERVER_GRAPHSCOPE_SERVICE_H_

#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "boost/lexical_cast.hpp"
#include "grpcpp/server_context.h"
#include "grpcpp/support/status.h"
#include "grpcpp/support/status_code_enum.h"
#include "grpcpp/support/sync_stream.h"

#include "core/server/dispatcher.h"
#include "proto/engine_service.grpc.pb.h"
#include "proto/message.pb.h"
#include "proto/op_def.pb.h"

namespace gs {
namespace rpc {

using grpc::ServerContext;
using ::grpc::ServerReaderWriter;
using grpc::Status;
using grpc::StatusCode;

/**
 * @brief GraphScopeService's responsibility is to listen to the request from
 * the client(coordinator) to perform operations.
 */
class GraphScopeService final : public EngineService::Service {
 public:
  explicit GraphScopeService(std::shared_ptr<Dispatcher> dispatcher)
      : dispatcher_(std::move(dispatcher)) {
    if (getenv("GS_GRPC_CHUNK_SIZE")) {
      chunk_size_ = boost::lexical_cast<size_t>(
          std::string(getenv("GS_GRPC_CHUNK_SIZE")));
    } else {
      // chunk_size defaults to 256MB
      chunk_size_ = 256 * 1024 * 1024 - 1;
    }
  }

  ::grpc::Status RunStep(
      ::grpc::ServerContext* context,
      ServerReaderWriter<RunStepResponse, RunStepRequest>* stream) override;

  ::grpc::Status HeartBeat(::grpc::ServerContext* context,
                           const HeartBeatRequest* request,
                           HeartBeatResponse* response) override;

 private:
  void splitOpResult(OpResult* op_result, const DispatchResult& result,
                     std::vector<RunStepResponse>& response_bodies) {
    auto policy = result.aggregate_policy();
    const std::string& data = result.data();

    // has_large_result
    op_result->mutable_meta()->set_has_large_result(result.has_large_data());
    if (op_result->meta().has_large_result()) {
      // split
      for (size_t i = 0; i < data.size(); i += chunk_size_) {
        RunStepResponse response_body;
        auto* body = response_body.mutable_body();
        if ((i + chunk_size_) >= data.size()) {
          body->mutable_chunk()->assign(data.begin() + i, data.end());
          body->set_has_next(false);
        } else {
          body->mutable_chunk()->assign(data.begin() + i,
                                        data.begin() + i + chunk_size_);
          body->set_has_next(true);
        }
        response_bodies.push_back(std::move(response_body));
      }
    } else {
      if (policy == DispatchResult::AggregatePolicy::kConcat) {
        op_result->mutable_result()->append(data);
      } else {
        op_result->mutable_result()->assign(data.begin(), data.end());
      }
    }
  }

 private:
  std::shared_ptr<Dispatcher> dispatcher_;
  size_t chunk_size_;
};
}  // namespace rpc
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_SERVER_GRAPHSCOPE_SERVICE_H_

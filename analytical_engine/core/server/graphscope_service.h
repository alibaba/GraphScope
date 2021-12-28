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

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "core/server/dispatcher.h"
#include "proto/graphscope/proto/engine_service.grpc.pb.h"
#include "proto/graphscope/proto/graph_def.pb.h"
#include "proto/graphscope/proto/op_def.pb.h"

namespace gs {
namespace rpc {

using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

/**
 * @brief GraphScopeService's responsibility is to listen to the request from
 * the client(coordinator) to perform operations.
 */
class GraphScopeService final : public EngineService::Service {
 public:
  explicit GraphScopeService(std::shared_ptr<Dispatcher> dispatcher)
      : dispatcher_(std::move(dispatcher)) {}

  ::grpc::Status RunStep(::grpc::ServerContext* context,
                         const RunStepRequest* request,
                         RunStepResponse* response) override;

  ::grpc::Status HeartBeat(::grpc::ServerContext* context,
                           const HeartBeatRequest* request,
                           HeartBeatResponse* response) override;

 private:
  std::shared_ptr<Dispatcher> dispatcher_;
};
}  // namespace rpc
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_SERVER_GRAPHSCOPE_SERVICE_H_

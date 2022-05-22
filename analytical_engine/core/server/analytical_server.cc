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
#include "core/server/analytical_server.h"

#include <limits>
#include <ostream>
#include <string>

#include "glog/logging.h"
#include "grpcpp/security/server_credentials.h"
#include "grpcpp/server.h"
#include "grpcpp/server_builder.h"

#include "core/server/graphscope_service.h"

namespace gs {

namespace rpc {
void AnalyticalServer::StartServer() {
  std::string server_address = host_ + ":" + std::to_string(port_);
  GraphScopeService service(dispatcher_);

  grpc::ServerBuilder builder;
  builder.SetMaxReceiveMessageSize(std::numeric_limits<int>::max());
  builder.SetMaxSendMessageSize(std::numeric_limits<int>::max());

  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  grpc_server_ = builder.BuildAndStart();
  LOG(INFO) << "Analytical server is listening on " << server_address;

  grpc_server_->Wait();
}

void AnalyticalServer::StopServer() { grpc_server_->Shutdown(); }
}  // namespace rpc
}  // namespace gs

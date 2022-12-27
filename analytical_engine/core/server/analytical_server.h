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

#ifndef ANALYTICAL_ENGINE_CORE_SERVER_ANALYTICAL_SERVER_H_
#define ANALYTICAL_ENGINE_CORE_SERVER_ANALYTICAL_SERVER_H_

#include <memory>
#include <string>
#include <utility>

#include "grpcpp/server.h"

namespace gs {
class Dispatcher;

namespace rpc {

/**
 * @brief AnalyticalServer is responsible for create and start the gRPC service
 */
class AnalyticalServer {
 public:
  AnalyticalServer(std::shared_ptr<Dispatcher> dispatcher, std::string host,
                   int port)
      : dispatcher_(std::move(dispatcher)),
        host_(std::move(host)),
        port_(port) {}

  void StartServer();

  void StopServer();

 private:
  std::shared_ptr<Dispatcher> dispatcher_;
  std::string host_;
  int port_;
  std::unique_ptr<grpc::Server> grpc_server_;
};
}  // namespace rpc
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_SERVER_ANALYTICAL_SERVER_H_

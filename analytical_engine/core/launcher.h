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

#ifndef ANALYTICAL_ENGINE_CORE_LAUNCHER_H_
#define ANALYTICAL_ENGINE_CORE_LAUNCHER_H_

#include <memory>
#include <string>

#include "boost/process/detail/child_decl.hpp"
#include "core/flags.h"
#include "grape/worker/comm_spec.h"

namespace vineyard {
class Client;
}  // namespace vineyard

namespace gs {

/**
 * @brief VineyardServer is a launcher for vineyardd
 */
class VineyardServer {
 public:
  explicit VineyardServer(const grape::CommSpec& comm_spec)
      : comm_spec_(comm_spec), vineyard_socket_(FLAGS_vineyard_socket) {}

  ~VineyardServer() { this->Stop(); }

  const std::string& vineyard_socket() const { return vineyard_socket_; }

  void Start();

  void Stop();

 private:
  grape::CommSpec comm_spec_;
  std::string vineyard_socket_;
  std::unique_ptr<boost::process::child> proc_;
};

void EnsureClient(std::shared_ptr<vineyard::Client>& client,
                  const std::string& vineyard_socket);

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_LAUNCHER_H_

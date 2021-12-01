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

#include <chrono>
#include <memory>
#include <string>

#include "grape/worker/comm_spec.h"
#include "vineyard/common/util/functions.h"

#include "core/flags.h"
#include "core/launcher.h"

namespace bp = boost::process;

namespace gs {

void VineyardServer::Start() {
  if (!vineyard_socket_.empty()) {
    return;
  }

  // Use a unique timestamp as the etcd prefix to avoid contention between
  // unrelated vineyardd processes.
  uint64_t ts = 0;
  if (comm_spec_.worker_id() == 0) {
    ts = std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
             .count();
    grape::BcastSend(ts, comm_spec_.comm());
  } else {
    grape::BcastRecv(ts, comm_spec_.comm(), 0);
  }

  if (comm_spec_.local_id() != 0) {
    // Only launch one vineyard instance at each machine.
    grape::BcastRecv(vineyard_socket_, comm_spec_.local_comm(), 0);
    return;
  }

  if (getenv("VINEYARD_IPC_SOCKET")) {
    vineyard_socket_ =
        vineyard::ExpandEnvironmentVariables("$VINEYARD_IPC_SOCKET");
  } else {
    // random vineyard socket
    vineyard_socket_ = "/tmp/vineyard.sock." + std::to_string(ts);
  }

  std::string vineyardd_path;
  if (getenv("VINEYARD_HOME")) {
    vineyardd_path =
        vineyard::ExpandEnvironmentVariables("$VINEYARD_HOME/vineyardd");
  }
  if (vineyardd_path.empty()) {
    vineyardd_path = boost::process::search_path("vineyardd").string();
  }
  if (vineyardd_path.empty()) {
    vineyardd_path = "/usr/bin/env python3 -m vineyard";
  }
  std::string cmd = vineyardd_path + " --socket " + vineyard_socket_ +
                    " --size " + FLAGS_vineyard_shared_mem +
                    " --etcd_endpoint " + FLAGS_etcd_endpoint +
                    " --etcd_prefix vineyard.gsa." + std::to_string(ts);
  auto env = boost::this_process::environment();
  // Set verbosity level to 2 can get rid of most of vineyard server's
  // debugging output
  env["GLOG_v"] = "2";
  std::error_code ec;
  proc_ = std::make_unique<bp::child>(cmd, bp::std_out > stdout,
                                      bp::std_err > stderr, env, ec);
  if (ec) {
    // NB: currently we just leave a error message, and don't require the
    // vineyard instance is successfully launched.
    // This may change in the future, we can return a boolean value to
    // indicates if the vineyardd succeeds.
    LOG(FATAL) << "Failed to launch vineyard: " << ec.message();
  } else {
    LOG(INFO) << "vineyardd launched: pid = " << proc_->id()
              << ", listening on " << vineyard_socket_;
  }

  grape::BcastSend(vineyard_socket_, comm_spec_.local_comm());
}

void VineyardServer::Stop() {
  if (proc_ && proc_->valid()) {
    kill(proc_->id(), SIGTERM);
    proc_->wait();
  }
}

void EnsureClient(std::shared_ptr<vineyard::Client>& client,
                  const std::string& vineyard_socket) {
  if (client == nullptr) {
    client = std::make_shared<vineyard::Client>();
    VINEYARD_CHECK_OK(client->Connect(vineyard_socket));
  }
}

}  // namespace gs

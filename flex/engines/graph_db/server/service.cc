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

#include "flex/engines/graph_db/server/service.h"
#include "flex/engines/graph_db/server/options.h"
namespace server {

void service::init(uint32_t num_shards, uint16_t http_port, bool dpdk_mode) {
  actor_sys_ = std::make_unique<actor_system>(num_shards, dpdk_mode);
  http_hdl_ = std::make_unique<http_handler>(http_port);
}

void service::run_and_wait_for_exit() {
  if (!actor_sys_ || !http_hdl_) {
    std::cerr << "Service has not been inited!" << std::endl;
    return;
  }
  actor_sys_->launch();
  http_hdl_->start();
  running_.store(true);
  while (running_.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  http_hdl_->stop();
  actor_sys_->terminate();
}

void service::set_exit_state() { running_.store(false); }

}  // namespace server

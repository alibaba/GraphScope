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

#include "flex/engines/http_server/service/proxy_service.h"

namespace server {

gs::Status ProxyService::init(
    uint32_t num_shards, uint16_t http_port,
    const std::vector<std::pair<std::string, uint16_t>>& endpoints,
    bool enable_heartbeat, int32_t heart_beat_interval,
    bool hang_until_success) {
  proxy_port_ = http_port;
  endpoints_ = endpoints;
  actor_sys_ = std::make_unique<actor_system>(num_shards, false);
  http_hdl_ = std::make_unique<proxy_http_handler>(http_port);
  auto init_res = client.init(endpoints, enable_heartbeat, heart_beat_interval,
                              hang_until_success);
  if (!init_res.ok()) {
    LOG(ERROR) << "Failed to init HttpProxy";
    return gs::Status(gs::StatusCode::InternalError,
                      "Failed to init HttpProxy" + init_res.error_message());
  }
  return gs::Status::OK();
}

void ProxyService::run_and_wait_for_exit() {
  if (!actor_sys_ || !http_hdl_) {
    std::cerr << "GraphDB service has not been inited!" << std::endl;
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

const std::vector<std::pair<std::string, uint16_t>>&
ProxyService::get_endpoints() const {
  return endpoints_;
}

HttpProxy& ProxyService::get_client() { return client; }

void ProxyService::set_exit_state() { running_.store(false); }

}  // namespace server

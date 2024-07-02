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

#ifndef ENGINES_HTTP_SERVER_SERVICE_PROXY_SERVICE_H_
#define ENGINES_HTTP_SERVER_SERVICE_PROXY_SERVICE_H_

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "flex/engines/http_server/actor_system.h"
#include "flex/engines/http_server/handler/http_proxy.h"
#include "flex/engines/http_server/handler/proxy_http_handler.h"
#include "flex/utils/result.h"
#include "flex/utils/service_utils.h"

namespace server {
class ProxyService {
 public:
  static ProxyService& get() {
    static ProxyService instance;
    return instance;
  }

  ~ProxyService() = default;

  gs::Status init(
      uint32_t num_shards, uint16_t http_port,
      const std::vector<std::pair<std::string, uint16_t>>& endpoints,
      bool enable_heartbeat = false,
      int32_t heart_beat_interval =
          HeartBeatChecker::DEFAULT_HEART_BEAT_INTERVAL,
      bool hang_until_success = true);
  void run_and_wait_for_exit();
  const std::vector<std::pair<std::string, uint16_t>>& get_endpoints() const;
  void set_exit_state();

  HttpProxy& get_client();

 private:
  ProxyService() = default;

 private:
  uint32_t proxy_port_;
  std::vector<std::pair<std::string, uint16_t>> endpoints_;
  std::unique_ptr<actor_system> actor_sys_;
  std::unique_ptr<proxy_http_handler> http_hdl_;
  std::atomic<bool> running_{false};
  HttpProxy client;
};
}  // namespace server

#endif  // ENGINES_HTTP_SERVER_SERVICE_PROXY_SERVICE_H_

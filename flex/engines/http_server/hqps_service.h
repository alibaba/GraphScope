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
#ifndef ENGINES_HTTP_SERVER_HQPS_SERVICE_H_
#define ENGINES_HTTP_SERVER_HQPS_SERVICE_H_

#include <string>

#include "flex/engines/http_server/actor_system.h"
#include "flex/engines/http_server/hqps_http_handler.h"

namespace server {

class HQPSService {
 public:
  static HQPSService& get() {
    static HQPSService instance;
    return instance;
  }
  ~HQPSService();

  // the store procedure contains <query_id, query_name, store_path>
  void init(uint32_t num_shards, uint16_t http_port, bool dpdk_mode,
            bool enable_thread_resource_pool, unsigned external_thread_num);

  void run_and_wait_for_exit();
  void set_exit_state();

 private:
  HQPSService() = default;

 private:
  std::unique_ptr<actor_system> actor_sys_;
  std::unique_ptr<hqps_http_handler> http_hdl_;
  std::atomic<bool> running_{false};
};

}  // namespace server

#endif  // ENGINES_HTTP_SERVER_HQPS_SERVICE_H_

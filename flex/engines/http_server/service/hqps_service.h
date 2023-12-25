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

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/http_server/actor_system.h"
#include "flex/engines/http_server/handler/admin_http_handler.h"
#include "flex/engines/http_server/handler/hqps_http_handler.h"
#include "flex/utils/result.h"
#include "flex/utils/service_utils.h"

namespace server {

class HQPSService {
 public:
  static const std::string DEFAULT_GRAPH_NAME;
  static HQPSService& get();
  ~HQPSService();

  // only start the query service.
  void init(uint32_t num_shards, uint16_t query_port, bool dpdk_mode,
            bool enable_thread_resource_pool, unsigned external_thread_num);

  // start both admin and query service.
  void init(uint32_t num_shards, uint16_t admin_port, uint16_t query_port,
            bool dpdk_mode, bool enable_thread_resource_pool,
            unsigned external_thread_num);

  bool is_initialized() const;

  bool is_running() const;

  uint16_t get_query_port() const;

  gs::Result<seastar::sstring> service_status();

  void run_and_wait_for_exit();

  void set_exit_state();

  // Actually stop the actors, the service is still on, but returns error code
  // for each request.
  seastar::future<> stop_query_actors();

  // Actually create new actors with a different scope_id,
  // Because we don't know whether the previous scope_id can be reused.
  void start_query_actors();

 private:
  HQPSService() = default;

 private:
  std::unique_ptr<actor_system> actor_sys_;
  std::unique_ptr<admin_http_handler> admin_hdl_;
  std::unique_ptr<hqps_http_handler> query_hdl_;
  std::atomic<bool> running_{false};
  std::atomic<bool> initialized_{false};
};

}  // namespace server

#endif  // ENGINES_HTTP_SERVER_HQPS_SERVICE_H_

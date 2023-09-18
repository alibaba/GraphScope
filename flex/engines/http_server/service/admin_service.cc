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
#include "flex/engines/http_server/service/admin_service.h"
#include "flex/engines/http_server/options.h"
namespace server {

void InteractiveAdminService::init(uint32_t num_shards, uint16_t admin_port,
                                   uint16_t query_port, bool dpdk_mode = false,
                                   bool enable_thread_resource_pool = true,
                                   unsigned external_thread_num = 1) {
  actor_sys_ = std::make_unique<actor_system>(
      num_shards, dpdk_mode, enable_thread_resource_pool, external_thread_num);
  http_hdl_ = std::make_unique<admin_http_handler>(admin_port);
  query_handler_ = std::make_unique<hqps_http_handler>(query_port);
  gs::init_cpu_usage_watch();
}

InteractiveAdminService::~InteractiveAdminService() {
  if (actor_sys_) {
    actor_sys_->terminate();
  }
}

void InteractiveAdminService::stop_query_service() {
  if (query_handler_) {
    LOG(INFO) << "Stopping query service...";
    query_handler_->stop();
  }
}

void InteractiveAdminService::start_query_service() {
  if (query_handler_) {
    query_handler_->start();
  }
}

seastar::sstring InteractiveAdminService::get_query_service_status() {
  nlohmann::json res;
  if (query_handler_) {
    auto& workspace_manager = server::WorkspaceManager::Get();
    res["status"] = "running";
    res["query_port"] = query_handler_->get_port();
    res["graph_name"] = workspace_manager.GetRunningGraph();
  } else {
    LOG(INFO) << "Query service has not been inited!";
    res["status"] = "Query service has not been inited!";
  }
  return res.dump();
}

void InteractiveAdminService::run_and_wait_for_exit() {
  if (!actor_sys_ || !http_hdl_) {
    std::cerr << "Interactive admin service has not been inited!" << std::endl;
    return;
  }
  actor_sys_->launch();
  http_hdl_->start();
  query_handler_->start();
  running_.store(true);
  while (running_.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  http_hdl_->stop();
  query_handler_->stop();
  actor_sys_->terminate();
}

void InteractiveAdminService::set_exit_state() { running_.store(false); }

}  // namespace server

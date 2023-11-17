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
#include "flex/engines/http_server/service/hqps_service.h"
#include "flex/engines/http_server/options.h"
namespace server {

const std::string HQPSService::DEFAULT_GRAPH_NAME = "modern_graph";

HQPSService& HQPSService::get() {
  static HQPSService instance;
  return instance;
}

void HQPSService::init(uint32_t num_shards, uint16_t http_port, bool dpdk_mode,
                       bool enable_thread_resource_pool,
                       unsigned external_thread_num) {
  if (initialized_.load(std::memory_order_relaxed)) {
    std::cerr << "High QPS service has been already initialized!" << std::endl;
    return;
  }
  actor_sys_ = std::make_unique<actor_system>(
      num_shards, dpdk_mode, enable_thread_resource_pool, external_thread_num);
  http_hdl_ = std::make_unique<hqps_http_handler>(http_port);
  LOG(INFO) << "Creating http handler";
  initialized_.store(true);
}

bool HQPSService::is_initialized() const {
  return initialized_.load(std::memory_order_relaxed);
}

bool HQPSService::is_running() const {
  return running_.load(std::memory_order_relaxed);
}

gs::Result<seastar::sstring> HQPSService::service_status() {
  if (!is_initialized()) {
    return gs::Result<seastar::sstring>(
        gs::StatusCode::OK, "High QPS service has not been inited!", "");
  }
  if (!is_running()) {
    return gs::Result<seastar::sstring>(
        gs::StatusCode::OK, "High QPS service has not been started!", "");
  }
  return gs::Result<seastar::sstring>(
      seastar::sstring("High QPS service is running ..."));
}

gs::Result<bool> HQPSService::start_service(const gs::Schema& schema,
                                            const std::string& data_dir,
                                            int32_t thread_num) {
  // Use graph_name to update the graph, and then start the service.
  auto& db = gs::GraphDB::get();
  auto result = db.LoadFromDataDirectory(schema, data_dir, thread_num);
  if (!result.ok()) {
    return result;
  }
  if (!schema.Equals(db.schema())) {
    return gs::Result<bool>(
        gs::StatusCode::InternalError,
        "Schema in graph config file is not consistent with existing graph!",
        false);
  }
  if (!is_initialized()) {
    return gs::Result<bool>(gs::StatusCode::InternalError,
                            "High QPS service has not been inited!", false);
  }
  if (is_running()) {
    return gs::Result<bool>(gs::StatusCode::IllegalOperation,
                            "High QPS service has been already started!",
                            false);
  }
  actor_sys_->launch();
  LOG(INFO) << "Actor system is launched";
  http_hdl_->start();
  LOG(INFO) << "Http handler is started";
  running_.store(true);
  LOG(INFO) << "High QPS service is running ...";
  return gs::Result<bool>(true);
}

HQPSService::~HQPSService() {
  if (actor_sys_) {
    actor_sys_->terminate();
  }
}

void HQPSService::run_and_wait_for_exit() {
  if (!actor_sys_ || !http_hdl_) {
    std::cerr << "High QPS service has not been inited!" << std::endl;
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

void HQPSService::set_exit_state() { running_.store(false); }

}  // namespace server

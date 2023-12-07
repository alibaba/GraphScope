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
#include "flex/engines/http_server/workdir_manipulator.h"
#include "flex/utils/result.h"
#include "flex/utils/service_utils.h"

#include <yaml-cpp/yaml.h>
#include <boost/process.hpp>

namespace server {
/* Stored service configuration, read from engine_config.yaml
 */
struct ServiceConfig {
  static constexpr const uint32_t DEFAULT_SHARD_NUM = 1;
  static constexpr const uint32_t DEFAULT_QUERY_PORT = 10000;
  static constexpr const uint32_t DEFAULT_ADMIN_PORT = 7777;
  static constexpr const uint32_t DEFAULT_BOLT_PORT = 7687;

  // Those has default value
  uint32_t bolt_port;
  uint32_t admin_port;
  uint32_t query_port;
  uint32_t shard_num;
  bool dpdk_mode;
  bool enable_thread_resource_pool;
  unsigned external_thread_num;
  bool start_admin_service;  // Whether to start the admin service or only
                             // start the query service.
  bool start_compiler;

  // Those has not default value
  std::string default_graph;
  std::string engine_config_path;  // used for codegen.
  ServiceConfig();
};

class HQPSService {
 public:
  static const std::string DEFAULT_GRAPH_NAME;
  static const std::string DEFAULT_INTERACTIVE_HOME;
  static const std::string COMPILER_SERVER_CLASS_NAME;
  static HQPSService& get();
  ~HQPSService();

  // only start the query service.
  void init(const ServiceConfig& config);

  const ServiceConfig& get_service_config() const;

  bool is_initialized() const;

  bool is_running() const;

  uint16_t get_query_port() const;

  gs::Result<seastar::sstring> service_status();

  void run_and_wait_for_exit();

  void set_exit_state();

  // Actually stop the actors, the service is still on, but returns error code
  // for each request.
  seastar::future<> stop_query_actors();

  bool is_actors_running() const;

  // Actually create new actors with a different scope_id,
  // Because we don't know whether the previous scope_id can be reused.
  void start_query_actors();

  bool start_compiler_subprocess(const std::string& graph_schema_path = "");

  bool stop_compiler_subprocess();

 private:
  HQPSService() = default;

  std::string find_interactive_class_path();

 private:
  std::unique_ptr<actor_system> actor_sys_;
  std::unique_ptr<admin_http_handler> admin_hdl_;
  std::unique_ptr<hqps_http_handler> query_hdl_;
  std::atomic<bool> running_{false};
  std::atomic<bool> initialized_{false};
  std::mutex mtx_;

  ServiceConfig service_config_;
  boost::process::child compiler_process_;
};

}  // namespace server

namespace YAML {

template <>
struct convert<server::ServiceConfig> {
  // The encode function is not defined, since we don't need to write the config
  static bool decode(const Node& config,
                     server::ServiceConfig& service_config) {
    if (!config.IsMap()) {
      LOG(ERROR) << "ServiceConfig should be a map";
      return false;
    }
    auto engine_node = config["compute_engine"];
    if (engine_node) {
      auto engine_type = engine_node["type"];
      if (engine_type) {
        auto engine_type_str = engine_type.as<std::string>();
        if (engine_type_str != "hiactor" && engine_type_str != "Hiactor") {
          LOG(ERROR) << "compute_engine type should be hiactor, found: "
                     << engine_type_str;
          return false;
        }
      }
      auto shard_num_node = engine_node["thread_num_per_worker"];
      if (shard_num_node) {
        service_config.shard_num = shard_num_node.as<uint32_t>();
      } else {
        LOG(INFO) << "shard_num not found, use default value "
                  << service_config.shard_num;
      }
    } else {
      LOG(ERROR) << "Fail to find compute_engine configuration";
      return false;
    }
    auto http_service_node = config["http_service"];
    if (http_service_node) {
      auto query_port_node = http_service_node["query_port"];
      if (query_port_node) {
        service_config.query_port = query_port_node.as<uint32_t>();
      } else {
        LOG(INFO) << "query_port not found, use default value "
                  << service_config.query_port;
      }
      auto admin_port_node = http_service_node["admin_port"];
      if (admin_port_node) {
        service_config.admin_port = admin_port_node.as<uint32_t>();
      } else {
        LOG(INFO) << "admin_port not found, use default value "
                  << service_config.admin_port;
      }
    } else {
      LOG(ERROR) << "Fail to find http_service configuration";
      return false;
    }
    auto default_graph_node = config["default_graph"];
    std::string default_graph;
    if (default_graph_node) {
      service_config.default_graph = default_graph_node.as<std::string>();
    } else {
      LOG(WARNING) << "Fail to find default_graph configuration";
    }
    return true;
  }
};
}  // namespace YAML

#endif  // ENGINES_HTTP_SERVER_HQPS_SERVICE_H_

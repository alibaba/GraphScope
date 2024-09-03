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

#include <cctype>
#include <memory>
#include <string>

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/http_server/actor_system.h"
#include "flex/engines/http_server/handler/admin_http_handler.h"
#include "flex/engines/http_server/handler/graph_db_http_handler.h"
#include "flex/engines/http_server/workdir_manipulator.h"
#include "flex/storages/metadata/graph_meta_store.h"
#include "flex/storages/metadata/metadata_store_factory.h"
#include "flex/utils/result.h"
#include "flex/utils/service_utils.h"

#include <yaml-cpp/yaml.h>
#include <boost/process.hpp>

namespace server {
/* Stored service configuration, read from interactive_config.yaml
 */
struct ServiceConfig {
  static constexpr const uint32_t DEFAULT_SHARD_NUM = 1;
  static constexpr const uint32_t DEFAULT_QUERY_PORT = 10000;
  static constexpr const uint32_t DEFAULT_ADMIN_PORT = 7777;
  static constexpr const uint32_t DEFAULT_BOLT_PORT = 7687;
  static constexpr const uint32_t DEFAULT_GREMLIN_PORT = 8182;
  static constexpr const uint32_t DEFAULT_VERBOSE_LEVEL = 0;
  static constexpr const uint32_t DEFAULT_LOG_LEVEL =
      0;  // 0 = INFO, 1 = WARNING, 2 = ERROR, 3 = FATAL

  // Those has default value
  uint32_t bolt_port;
  uint32_t gremlin_port;
  uint32_t admin_port;
  uint32_t query_port;
  uint32_t shard_num;
  uint32_t memory_level;
  bool enable_adhoc_handler;  // Whether to enable adhoc handler.
  bool dpdk_mode;
  bool enable_thread_resource_pool;
  unsigned external_thread_num;
  bool start_admin_service;  // Whether to start the admin service or only
                             // start the query service.
  bool start_compiler;
  bool enable_gremlin;
  bool enable_bolt;
  gs::MetadataStoreType metadata_store_type_;
  // verbose log level. should be a int
  // could also be set from command line: GLOG_v={}.
  // If we found GLOG_v in the environment, we will at the first place.
  int log_level;
  int verbose_level;

  // Those has not default value
  std::string default_graph;
  std::string engine_config_path;  // used for codegen.

  ServiceConfig();
};

class GraphDBService {
 public:
  static const std::string DEFAULT_GRAPH_NAME;
  static const std::string DEFAULT_INTERACTIVE_HOME;
  static const std::string COMPILER_SERVER_CLASS_NAME;
  static GraphDBService& get();
  ~GraphDBService();

  void init(const ServiceConfig& config);

  const ServiceConfig& get_service_config() const;

  bool is_initialized() const;

  bool is_running() const;

  uint16_t get_query_port() const;

  uint64_t get_start_time() const;

  void reset_start_time();

  std::shared_ptr<gs::IGraphMetaStore> get_metadata_store() const;

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

  bool check_compiler_ready() const;

 private:
  GraphDBService() = default;

  std::string find_interactive_class_path();
  // Insert graph meta into metadata store.
  gs::GraphId insert_default_graph_meta();
  void open_default_graph();
  void clear_running_graph();

 private:
  std::unique_ptr<actor_system> actor_sys_;
  std::unique_ptr<admin_http_handler> admin_hdl_;
  std::unique_ptr<graph_db_http_handler> query_hdl_;
  std::atomic<bool> running_{false};
  std::atomic<bool> initialized_{false};
  std::atomic<uint64_t> start_time_{0};
  std::mutex mtx_;

  ServiceConfig service_config_;
  boost::process::child compiler_process_;
  // handler for metadata store
  std::shared_ptr<gs::IGraphMetaStore> metadata_store_;
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
    // log level: INFO=0, WARNING=1, ERROR=2, FATAL=3
    if (config["log_level"]) {
      auto level_str = gs::toUpper(config["log_level"].as<std::string>());

      if (level_str == "INFO") {
        service_config.log_level = 0;
      } else if (level_str == "WARNING") {
        service_config.log_level = 1;
      } else if (level_str == "ERROR") {
        service_config.log_level = 2;
      } else if (level_str == "FATAL") {
        service_config.log_level = 3;
      } else {
        LOG(ERROR) << "Unsupported log level: " << level_str;
        return false;
      }
    } else {
      LOG(INFO) << "log_level not found, use default value "
                << service_config.log_level;
    }

    // verbose log level
    if (config["verbose_level"]) {
      service_config.verbose_level = config["verbose_level"].as<int>();
    } else {
      LOG(INFO) << "verbose_level not found, use default value "
                << service_config.verbose_level;
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

      auto metadata_store_node = engine_node["metadata_store"];
      if (metadata_store_node) {
        auto metadata_store_type = metadata_store_node["type"];
        if (metadata_store_type) {
          auto metadata_store_type_str = metadata_store_type.as<std::string>();
          if (metadata_store_type_str == "file") {
            service_config.metadata_store_type_ =
                gs::MetadataStoreType::kLocalFile;
          } else {
            LOG(ERROR) << "Unsupported metadata store type: "
                       << metadata_store_type_str;
            return false;
          }
        }
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

    auto compiler_node = config["compiler"];
    if (compiler_node) {
      auto endpoint_node = compiler_node["endpoint"];
      if (endpoint_node) {
        auto bolt_node = endpoint_node["bolt_connector"];
        if (bolt_node && bolt_node["disabled"]) {
          service_config.enable_bolt = !bolt_node["disabled"].as<bool>();
        } else {
          service_config.enable_bolt = true;
        }
        if (bolt_node && bolt_node["port"]) {
          service_config.bolt_port = bolt_node["port"].as<uint32_t>();
        } else {
          LOG(INFO) << "bolt_port not found, or disabled";
        }
        auto gremlin_node = endpoint_node["gremlin_connector"];
        if (gremlin_node && gremlin_node["disabled"]) {
          service_config.enable_gremlin = !gremlin_node["disabled"].as<bool>();
        } else {
          service_config.enable_gremlin = true;
        }
        if (gremlin_node && gremlin_node["port"]) {
          service_config.gremlin_port = gremlin_node["port"].as<uint32_t>();
        } else {
          LOG(INFO) << "gremlin_port not found, use default value "
                    << service_config.gremlin_port;
        }
      }
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

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

#include <filesystem>
#include <iostream>
#include "stdlib.h"

#include "flex/engines/http_server/hqps_service.h"

#include "flex/engines/hqps_db/database/mutable_csr_interface.h"
#include "flex/engines/http_server/codegen_proxy.h"
#include "flex/engines/http_server/stored_procedure.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"

#include <yaml-cpp/yaml.h>
#include <boost/program_options.hpp>

namespace bpo = boost::program_options;

static constexpr const char* CODEGEN_BIN = "load_plan_and_run.sh";

std::string find_codegen_bin() {
  // first check whether flex_home env exists
  std::string flex_home;
  std::string codegen_bin;
  char* flex_home_char = getenv("FLEX_HOME");
  if (flex_home_char == nullptr) {
    // infer flex_home from current binary' directory
    char* bin_path = realpath("/proc/self/exe", NULL);
    std::string bin_path_str(bin_path);
    // flex home should be bin_path/../../
    std::string flex_home_str =
        bin_path_str.substr(0, bin_path_str.find_last_of("/"));
    // usr/loca/bin/
    flex_home_str = flex_home_str.substr(0, flex_home_str.find_last_of("/"));
    // usr/local/

    LOG(INFO) << "infer flex_home as installed, flex_home: " << flex_home_str;
    // check codege_bin_path exists
    codegen_bin = flex_home_str + "/bin/" + CODEGEN_BIN;
    // if flex_home exists, return flex_home
    if (std::filesystem::exists(codegen_bin)) {
      return codegen_bin;
    } else {
      // if not found, try as if it is in build directory
      // flex/build/
      flex_home_str = flex_home_str.substr(0, flex_home_str.find_last_of("/"));
      // flex/
      LOG(INFO) << "infer flex_home as build, flex_home: " << flex_home_str;
      codegen_bin = flex_home_str + "/bin/" + CODEGEN_BIN;
      if (std::filesystem::exists(codegen_bin)) {
        return codegen_bin;
      } else {
        LOG(FATAL) << "codegen bin not exists: ";
        return "";
      }
    }
  } else {
    flex_home = std::string(flex_home_char);
    LOG(INFO) << "flex_home env exists, flex_home: " << flex_home;
    codegen_bin = flex_home + "/bin/" + CODEGEN_BIN;
    if (std::filesystem::exists(codegen_bin)) {
      return codegen_bin;
    } else {
      LOG(FATAL) << "codegen bin not exists: ";
      return "";
    }
  }
}

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help,h", "Display help messages")(
      "server-config,c", bpo::value<std::string>(),
      "path to server config yaml")(
      "codegen-dir,d",
      bpo::value<std::string>()->default_value("/tmp/codegen/"),
      "codegen working directory")("codegen-bin,b", bpo::value<std::string>(),
                                   "codegen binary path")(
      "db-home", bpo::value<std::string>(), "db home path")(
      "graph-config,g", bpo::value<std::string>(), "graph schema config file")(
      "data-path,d", bpo::value<std::string>(), "data directory path")(
      "bulk-load,l", bpo::value<std::string>(), "bulk-load config file");

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  uint32_t shard_num = 1;
  uint16_t http_port = 10000;
  std::string plugin_dir;
  if (vm.count("server-config") != 0) {
    std::string server_config_path = vm["server-config"].as<std::string>();
    // check file exists
    if (!std::filesystem::exists(server_config_path)) {
      LOG(ERROR) << "server-config not exists: " << server_config_path;
      return 0;
    }
    YAML::Node config = YAML::LoadFile(server_config_path);
    auto dbms_node = config["dbms"];
    if (dbms_node) {
      auto server_node = dbms_node["server"];
      if (!server_node) {
        LOG(ERROR) << "dbms.server config not found";
        return 0;
      }
      auto shard_num_node = server_node["shared_num"];
      if (shard_num_node) {
        shard_num = shard_num_node.as<uint32_t>();
      } else {
        LOG(INFO) << "shared_num not found, use default value 1";
      }
      auto http_port_node = server_node["port"];
      if (http_port_node) {
        http_port = http_port_node.as<uint16_t>();
      } else {
        LOG(INFO) << "http_port not found, use default value 10000";
      }
      auto plugin_dir_node = server_node["plugin_dir"];
      if (plugin_dir_node) {
        plugin_dir = plugin_dir_node.as<std::string>();
      } else {
        LOG(INFO) << "plugin_dir not found";
      }
    } else {
      LOG(ERROR) << "dbms config not found";
      return 0;
    }
  } else {
    LOG(INFO) << "server-config is not specified, use default config";
  }
  LOG(INFO) << "shard_num: " << shard_num;
  LOG(INFO) << "http_port: " << http_port;
  LOG(INFO) << "plugin_dir: " << plugin_dir;

  std::string codegen_dir = vm["codegen-dir"].as<std::string>();

  LOG(INFO) << "codegen dir: " << codegen_dir;

  std::string codegen_bin;
  if (vm.count("codegen-bin") == 0) {
    LOG(INFO) << "codegen-bin is not specified";
    LOG(INFO) << "Try to find with relative path: ";
    codegen_bin = find_codegen_bin();
  } else {
    LOG(INFO) << "codegen-bin is specified";
    codegen_bin = vm["codegen-bin"].as<std::string>();
  }

  LOG(INFO) << "codegen bin: " << codegen_bin;

  // check codegen bin exists
  if (!std::filesystem::exists(codegen_bin)) {
    LOG(ERROR) << "codegen bin not exists: " << codegen_bin;
    return 0;
  }

  // clear codegen dir
  if (std::filesystem::exists(codegen_dir)) {
    LOG(INFO) << "codegen dir exists, clear directory";
    std::filesystem::remove_all(codegen_dir);
  } else {
    // create codegen_dir
    LOG(INFO) << "codegen dir not exists, create directory";
    std::filesystem::create_directory(codegen_dir);
  }
  // init graph
  std::string graph_schema_path = "";
  std::string data_path = "";
  std::string bulk_load_config_path = "";

  if (!vm.count("graph-config")) {
    LOG(ERROR) << "graph-config is required";
    return -1;
  }
  graph_schema_path = vm["graph-config"].as<std::string>();
  if (!vm.count("data-path")) {
    LOG(ERROR) << "data-path is required";
    return -1;
  }
  data_path = vm["data-path"].as<std::string>();
  if (vm.count("bulk-load")) {
    bulk_load_config_path = vm["bulk-load"].as<std::string>();
  }

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();

  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  auto loading_config =
      gs::LoadingConfig::ParseFromYaml(schema, bulk_load_config_path);
  db.Init(schema, loading_config, data_path, shard_num);

  t0 += grape::GetCurrentTime();

  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";

  // loading plugin
  if (!plugin_dir.empty()) {
    LOG(INFO) << "Load plugins from dir: " << plugin_dir;
    server::StoredProcedureManager::get().LoadFromPluginDir(plugin_dir, 0);
  }

  // db-home
  std::string db_home;
  if (vm.count("db-home") == 0) {
    LOG(FATAL) << "db-home is not specified" << std::endl;
  } else {
    db_home = vm["db-home"].as<std::string>();
    LOG(INFO) << "db-home: " << db_home;
  }

  server::CodegenProxy::get().Init(codegen_dir, codegen_bin, db_home);

  server::HQPSService::get().init(shard_num, http_port, false);
  server::HQPSService::get().run_and_wait_for_exit();

  return 0;
}

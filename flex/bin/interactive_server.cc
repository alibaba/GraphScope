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

#include "flex/engines/http_server/codegen_proxy.h"
#include "flex/engines/http_server/service/hqps_service.h"
#include "flex/engines/http_server/workdir_manipulator.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"
#include "flex/utils/service_utils.h"

#include <yaml-cpp/yaml.h>
#include <boost/program_options.hpp>

#include <glog/logging.h>

namespace bpo = boost::program_options;

namespace gs {
static constexpr const uint32_t DEFAULT_SHARD_NUM = 1;
static constexpr const uint32_t DEFAULT_QUERY_PORT = 10000;
static constexpr const uint32_t DEFAULT_ADMIN_PORT = 7777;

std::string parse_codegen_dir(const bpo::variables_map& vm) {
  std::string codegen_dir;

  if (vm.count("codegen-dir") == 0) {
    LOG(INFO) << "codegen-dir is not specified";
    codegen_dir = server::CodegenProxy::DEFAULT_CODEGEN_DIR;
  } else {
    codegen_dir = vm["codegen-dir"].as<std::string>();
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
  return codegen_dir;
}

// parse from yaml
std::tuple<uint32_t, uint32_t, uint32_t> parse_from_server_config(
    const std::string& server_config_path) {
  YAML::Node config = YAML::LoadFile(server_config_path);
  uint32_t shard_num = DEFAULT_SHARD_NUM;
  uint32_t query_port = DEFAULT_QUERY_PORT;
  uint32_t admin_port = DEFAULT_ADMIN_PORT;
  auto engine_node = config["compute_engine"];
  if (engine_node) {
    auto engine_type = engine_node["type"];
    if (engine_type) {
      auto engine_type_str = engine_type.as<std::string>();
      if (engine_type_str != "hiactor" && engine_type_str != "Hiactor") {
        LOG(FATAL) << "compute_engine type should be hiactor, found: "
                   << engine_type_str;
      }
    }
    auto shard_num_node = engine_node["thread_num_per_worker"];
    if (shard_num_node) {
      shard_num = shard_num_node.as<uint32_t>();
    } else {
      LOG(INFO) << "shard_num not found, use default value "
                << DEFAULT_SHARD_NUM;
    }
  } else {
    LOG(FATAL) << "Fail to find compute_engine configuration";
  }
  auto http_service_node = config["http_service"];
  if (http_service_node) {
    auto query_port_node = http_service_node["query_port"];
    if (query_port_node) {
      query_port = query_port_node.as<uint32_t>();
    } else {
      LOG(INFO) << "query_port not found, use default value "
                << DEFAULT_QUERY_PORT;
    }
    auto admin_port_node = http_service_node["admin_port"];
    if (admin_port_node) {
      admin_port = admin_port_node.as<uint32_t>();
    } else {
      LOG(INFO) << "admin_port not found, use default value "
                << DEFAULT_ADMIN_PORT;
    }
  } else {
    LOG(FATAL) << "Fail to find http_service configuration";
  }
  return std::make_tuple(shard_num, admin_port, query_port);
}

void init_codegen_proxy(const bpo::variables_map& vm,
                        const std::string& graph_schema_file,
                        const std::string& engine_config_file) {
  std::string codegen_dir = parse_codegen_dir(vm);
  std::string codegen_bin;
  if (vm.count("codegen-bin") == 0) {
    LOG(INFO) << "codegen-bin is not specified";
    codegen_bin = find_codegen_bin();
  } else {
    LOG(INFO) << "codegen-bin is specified";
    codegen_bin = vm["codegen-bin"].as<std::string>();
    if (!std::filesystem::exists(codegen_bin)) {
      LOG(FATAL) << "codegen bin not exists: " << codegen_bin;
    }
  }
  server::CodegenProxy::get().Init(codegen_dir, codegen_bin, engine_config_file,
                                   graph_schema_file);
}

void parse_args(bpo::variables_map& vm, int32_t& admin_port,
                int32_t& query_port, std::string& workspace) {
  if (vm.count("admin-port")) {
    admin_port = vm["admin-port"].as<int32_t>();
  }
  if (vm.count("query-port")) {
    query_port = vm["query-port"].as<int32_t>();
  }
  if (vm.count("workspace")) {
    workspace = vm["workspace"].as<std::string>();
  }
}

void initWorkspace(const std::string workspace, int32_t thread_num) {
  // If workspace directory not exists, create.

  auto default_graph = server::HQPSService::DEFAULT_GRAPH_NAME;
  if (!std::filesystem::exists(workspace)) {
    std::filesystem::create_directory(workspace);
  }
  // Create subdirectories
  std::filesystem::create_directory(workspace + "/" +
                                    server::WorkDirManipulator::DATA_DIR_NAME);
  std::filesystem::create_directory(workspace + "/conf");
  std::filesystem::create_directory(workspace + "/log");

  LOG(INFO) << "Finish creating workspace directory " << workspace;
  // Get current executable path

  std::string exe_dir = gs::get_current_dir();
  LOG(INFO) << "Executable directory: " << exe_dir;
  std::string interactive_home = exe_dir + "/../../interactive";
  // check exists
  if (!std::filesystem::exists(interactive_home)) {
    LOG(FATAL) << "Interactive home directory " << interactive_home
               << " not exists, exit.";
  }
  LOG(INFO) << "Interactive home: " << interactive_home;
  // copy conf files
  std::filesystem::copy_file(
      interactive_home + "/conf/" +
          server::WorkDirManipulator::CONF_ENGINE_CONFIG_FILE_NAME,
      workspace + "/conf/" +
          server::WorkDirManipulator::CONF_ENGINE_CONFIG_FILE_NAME,
      std::filesystem::copy_options::overwrite_existing);
  std::filesystem::copy_file(interactive_home + "/conf/interactive.yaml",
                             workspace + "/conf/interactive.yaml",
                             std::filesystem::copy_options::overwrite_existing);
  // create modern_graph directory
  std::filesystem::create_directory(workspace + "/data/" + default_graph);
  auto bulk_loading_file =
      interactive_home + "/examples/" + default_graph + "/bulk_load.yaml";
  // copy modern_graph files
  if (!std::filesystem::exists(bulk_loading_file)) {
    LOG(FATAL) << "Bulk loading file " << bulk_loading_file
               << " not exists, exit.";
  }

  std::filesystem::copy_file(
      interactive_home + "/examples/modern_graph/modern_graph.yaml",
      workspace + "/data/modern_graph/graph.yaml",
      std::filesystem::copy_options::overwrite_existing);

  server::WorkDirManipulator::SetWorkspace(workspace);

  // gs::run_graph_loading(schema_path, bulk_loading_file, data_dir);
  auto schema_path =
      server::WorkDirManipulator::GetGraphSchemaPath(default_graph);

  auto res = server::WorkDirManipulator::LoadGraph(bulk_loading_file,
                                                   default_graph, 1);
  if (!res.ok()) {
    LOG(FATAL) << "Fail to load graph: " << res.status().error_message();
  }

  VLOG(1) << "Finish init workspace";

  auto& db = gs::GraphDB::get();

  gs::Schema schema = gs::Schema::LoadFromYaml(schema_path);
  auto data_dir_res =
      server::WorkDirManipulator::GetDataDirectory(default_graph);
  if (!data_dir_res.ok()) {
    LOG(FATAL) << "Fail to get data directory for default graph: "
               << data_dir_res.status().error_message();
  }
  auto data_dir = data_dir_res.value();
  db.Close();
  if (!db.Open(schema, data_dir, thread_num).ok()) {
    LOG(FATAL) << "Fail to load graph from data directory: " << data_dir;
  }
  LOG(INFO) << "Successfully init graph db for default graph: "
            << server::HQPSService::DEFAULT_GRAPH_NAME;

  server::WorkDirManipulator::SetRunningGraph(
      server::HQPSService::DEFAULT_GRAPH_NAME);
}

}  // namespace gs

/**
 * The main entrance for InteractiveServer.
 */
int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help,h", "Display help messages")(
      "enable-admin-service,e", bpo::value<bool>()->default_value(false),
      "whether or not to start admin service")("server-config,c",
                                               bpo::value<std::string>(),
                                               "path to server config yaml")(
      "codegen-dir,d",
      bpo::value<std::string>()->default_value("/tmp/codegen/"),
      "codegen working directory")("shard-num,s", bpo::value<int32_t>(),
                                   "shard number")(
      "workspace,w",
      bpo::value<std::string>()->default_value("/tmp/workspace/"),
      "directory to interactive workspace")(
      "graph-config,g", bpo::value<std::string>(), "graph schema config file")(
      "data-path,a", bpo::value<std::string>(), "data directory path")(
      "open-thread-resource-pool", bpo::value<bool>()->default_value(true),
      "open thread resource pool")("worker-thread-number",
                                   bpo::value<unsigned>()->default_value(2),
                                   "worker thread number");

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  //// declare vars
  int32_t shard_num = 1;
  int32_t admin_port = gs::DEFAULT_ADMIN_PORT;
  int32_t query_port = gs::DEFAULT_QUERY_PORT;
  bool start_admin_service;

  if (vm.count("shard-num")) {
    shard_num = vm["shard-num"].as<int32_t>();
  }
  VLOG(10) << "Set shard num to " << shard_num;
  start_admin_service = vm["enable-admin-service"].as<bool>();

  std::string workspace;
  gs::parse_args(vm, admin_port, query_port, workspace);
  auto& db = gs::GraphDB::get();

  if (start_admin_service) {
    // When start admin service, we need a workspace to put all the meta data
    // and graph indices. We will initiate the query service with default graph.
    if (vm.count("graph-config") || vm.count("data-path")) {
      LOG(FATAL) << "To start admin service, graph-config and "
                    "data-path should NOT be specified";
    }
    gs::initWorkspace(workspace, shard_num);  // the default graph is loaded.
    LOG(INFO) << "Finish init workspace";

    server::HQPSService::get().init(shard_num, admin_port, query_port, false,
                                    vm["open-thread-resource-pool"].as<bool>(),
                                    vm["worker-thread-number"].as<unsigned>());
    server::HQPSService::get().run_and_wait_for_exit();
  } else {
    LOG(INFO) << "Start query service only";
    std::string graph_schema_path, data_path;
    if (!vm.count("server-config")) {
      LOG(FATAL) << "server-config is needed";
    }
    auto engine_config_file = vm["server-config"].as<std::string>();
    // When only starting query service.
    std::tie(shard_num, admin_port, query_port) =
        gs::parse_from_server_config(engine_config_file);

    // init graph
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

    auto schema = gs::Schema::LoadFromYaml(graph_schema_path);

    // Ths schema is loaded just to get the plugin dir and plugin list
    gs::init_codegen_proxy(vm, graph_schema_path, engine_config_file);
    db.Close();
    auto load_res = db.Open(schema, data_path, shard_num);
    if (!load_res.ok()) {
      LOG(FATAL) << "Failed to load graph from data directory: "
                 << load_res.status().error_message();
    }

    server::HQPSService::get().init(shard_num, query_port, false,
                                    vm["open-thread-resource-pool"].as<bool>(),
                                    vm["worker-thread-number"].as<unsigned>());
    server::HQPSService::get().run_and_wait_for_exit();
  }

  return 0;
}

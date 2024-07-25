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
#include "flex/engines/http_server/graph_db_service.h"
#include "flex/engines/http_server/workdir_manipulator.h"
#include "flex/otel/otel.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"
#include "flex/utils/service_utils.h"

#include <yaml-cpp/yaml.h>
#include <boost/program_options.hpp>

#include <glog/logging.h>

namespace bpo = boost::program_options;

namespace gs {

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

void blockSignal(int sig) {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, sig);
  if (pthread_sigmask(SIG_BLOCK, &set, NULL) != 0) {
    perror("pthread_sigmask");
  }
}

// When graph_schema is not specified, codegen proxy will use the running graph
// schema in graph_db_service
void init_codegen_proxy(const bpo::variables_map& vm,
                        const std::string& engine_config_file,
                        const std::string& graph_schema_file = "") {
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

void openDefaultGraph(const std::string workspace, int32_t thread_num,
                      const std::string& default_graph, uint32_t memory_level) {
  if (!std::filesystem::exists(workspace)) {
    LOG(ERROR) << "Workspace directory not exists: " << workspace;
  }
  auto data_dir_path =
      workspace + "/" + server::WorkDirManipulator::DATA_DIR_NAME;
  if (!std::filesystem::exists(data_dir_path)) {
    LOG(ERROR) << "Data directory not exists: " << data_dir_path;
    return;
  }

  // Get current executable path

  server::WorkDirManipulator::SetWorkspace(workspace);

  VLOG(1) << "Finish init workspace";

  if (default_graph.empty()) {
    LOG(FATAL) << "No Default graph is specified";
    return;
  }

  auto& db = gs::GraphDB::get();
  auto schema_path =
      server::WorkDirManipulator::GetGraphSchemaPath(default_graph);
  auto schema_res = gs::Schema::LoadFromYaml(schema_path);
  if (!schema_res.ok()) {
    LOG(FATAL) << "Fail to load graph schema from yaml file: " << schema_path;
  }
  auto data_dir_res =
      server::WorkDirManipulator::GetDataDirectory(default_graph);
  if (!data_dir_res.ok()) {
    LOG(FATAL) << "Fail to get data directory for default graph: "
               << data_dir_res.status().error_message();
  }
  std::string data_dir = data_dir_res.value();
  if (!std::filesystem::exists(data_dir)) {
    LOG(FATAL) << "Data directory not exists: " << data_dir
               << ", for graph: " << default_graph;
  }
  db.Close();
  gs::GraphDBConfig config(schema_res.value(), data_dir, thread_num);
  config.memory_level = memory_level;
  if (config.memory_level >= 2) {
    config.enable_auto_compaction = true;
  }
  if (!db.Open(config).ok()) {
    LOG(FATAL) << "Fail to load graph from data directory: " << data_dir;
  }
  LOG(INFO) << "Successfully init graph db for default graph: "
            << default_graph;
}

}  // namespace gs

/**
 * The main entrance for InteractiveServer.
 */
int main(int argc, char** argv) {
  // block sigint and sigterm in main thread, let seastar handle it
  gs::blockSignal(SIGINT);
  gs::blockSignal(SIGTERM);

  bpo::options_description desc("Usage:");
  desc.add_options()("help,h", "Display help messages")(
      "enable-admin-service,e", bpo::value<bool>()->default_value(false),
      "whether or not to start admin service")("server-config,c",
                                               bpo::value<std::string>(),
                                               "path to server config yaml")(
      "codegen-dir,d",
      bpo::value<std::string>()->default_value("/tmp/codegen/"),
      "codegen working directory")(
      "workspace,w",
      bpo::value<std::string>()->default_value("/tmp/workspace/"),
      "directory to interactive workspace")(
      "graph-config,g", bpo::value<std::string>(), "graph schema config file")(
      "data-path,a", bpo::value<std::string>(), "data directory path")(
      "open-thread-resource-pool", bpo::value<bool>()->default_value(true),
      "open thread resource pool")("worker-thread-number",
                                   bpo::value<unsigned>()->default_value(2),
                                   "worker thread number")(
      "enable-trace", bpo::value<bool>()->default_value(false),
      "whether to enable opentelemetry tracing")(
      "start-compiler", bpo::value<bool>()->default_value(false),
      "whether or not to start compiler")(
      "memory-level,m", bpo::value<unsigned>()->default_value(1),
      "memory allocation strategy");

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
  std::string workspace, engine_config_file;
  if (vm.count("workspace")) {
    workspace = vm["workspace"].as<std::string>();
  }

  if (!vm.count("server-config")) {
    LOG(FATAL) << "server-config is needed";
  }
  engine_config_file = vm["server-config"].as<std::string>();

  YAML::Node node = YAML::LoadFile(engine_config_file);
  // Parse service config
  server::ServiceConfig service_config = node.as<server::ServiceConfig>();
  service_config.engine_config_path = engine_config_file;
  service_config.start_admin_service = vm["enable-admin-service"].as<bool>();
  service_config.start_compiler = vm["start-compiler"].as<bool>();
  service_config.memory_level = vm["memory-level"].as<unsigned>();
  service_config.enable_adhoc_handler = true;

  auto& db = gs::GraphDB::get();

  if (vm["enable-trace"].as<bool>()) {
#ifdef HAVE_OPENTELEMETRY_CPP
    LOG(INFO) << "Initialize opentelemetry...";
    otel::initTracer();
    otel::initMeter();
    otel::initLogger();
#else
    LOG(WARNING) << "OpenTelemetry is not enabled in this build";
#endif
  }

  if (service_config.start_admin_service) {
    // When start admin service, we need a workspace to put all the meta data
    // and graph indices. We will initiate the query service with default graph.
    if (vm.count("graph-config") || vm.count("data-path")) {
      LOG(FATAL) << "To start admin service, graph-config and "
                    "data-path should NOT be specified";
    }

    gs::openDefaultGraph(workspace, service_config.shard_num,
                         service_config.default_graph,
                         service_config.memory_level);
    // Suppose the default_graph is already loaded.
    LOG(INFO) << "Finish init workspace";
    auto schema_file = server::WorkDirManipulator::GetGraphSchemaPath(
        service_config.default_graph);
    gs::init_codegen_proxy(vm, engine_config_file);
  } else {
    LOG(INFO) << "Start query service only";
    std::string graph_schema_path, data_path;

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

    auto schema_res = gs::Schema::LoadFromYaml(graph_schema_path);
    if (!schema_res.ok()) {
      LOG(FATAL) << "Fail to load graph schema from yaml file: "
                 << graph_schema_path;
    }

    // The schema is loaded just to get the plugin dir and plugin list
    gs::init_codegen_proxy(vm, engine_config_file, graph_schema_path);
    db.Close();
    auto load_res =
        db.Open(schema_res.value(), data_path, service_config.shard_num);
    if (!load_res.ok()) {
      LOG(FATAL) << "Failed to load graph from data directory: "
                 << load_res.status().error_message();
    }
  }

  server::GraphDBService::get().init(service_config);
  server::GraphDBService::get().run_and_wait_for_exit();

#ifdef HAVE_OPENTELEMETRY_CPP
  otel::cleanUpTracer();
#endif

  return 0;
}

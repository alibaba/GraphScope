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
#ifdef BUILD_WITH_OSS
#include "flex/utils/remote/oss_storage.h"
#endif

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

void config_log_level(int log_level, int verbose_level) {
  if (getenv("GLOG_minloglevel") != nullptr) {
    FLAGS_stderrthreshold = atoi(getenv("GLOG_minloglevel"));
  } else {
    if (log_level == 0) {
      FLAGS_minloglevel = 0;
    } else if (log_level == 1) {
      FLAGS_minloglevel = 1;
    } else if (log_level == 2) {
      FLAGS_minloglevel = 2;
    } else if (log_level == 3) {
      FLAGS_minloglevel = 3;
    } else {
      LOG(ERROR) << "Unsupported log level: " << log_level;
    }
  }

  // If environment variable is set, we will use it
  if (getenv("GLOG_v") != nullptr) {
    FLAGS_v = atoi(getenv("GLOG_v"));
  } else {
    if (verbose_level >= 0) {
      FLAGS_v = verbose_level;
    } else {
      LOG(ERROR) << "Unsupported verbose level: " << verbose_level;
    }
  }
}

#ifdef BUILD_WITH_OSS

Status unzip(const std::string& zip_file, const std::string& dest_dir) {
  std::string cmd = "unzip -o " + zip_file + " -d " + dest_dir;
  boost::process::child process(cmd);
  process.wait();
  if (process.exit_code() != 0) {
    return Status(StatusCode::IO_ERROR,
                  "Fail to unzip file: " + zip_file +
                      ", error code: " + std::to_string(process.exit_code()));
  }
  return Status::OK();
}

std::string download_data_from_oss(const std::string& graph_name,
                                   const std::string& remote_data_path,
                                   const std::string& local_data_dir) {
  if (std::filesystem::exists(local_data_dir)) {
    LOG(INFO) << "Data directory exists";
  } else {
    LOG(INFO) << "Data directory not exists, create directory";
    std::filesystem::create_directories(local_data_dir);
  }
  gs::OSSConf conf;
  conf.load_conf_from_env();
  gs::OSSRemoteStorageDownloader downloader(conf);
  auto open_res = downloader.Open();
  if (!open_res.ok()) {
    LOG(FATAL) << "Fail to open oss client: " << open_res.error_message();
  }

  auto data_dir_zip_path = local_data_dir + "/data.zip";
  // if data_path start with conf.bucket_name_, remove it
  std::string data_path_no_bucket = remote_data_path;
  if (data_path_no_bucket.find(conf.bucket_name_) == 0) {
    data_path_no_bucket =
        data_path_no_bucket.substr(conf.bucket_name_.size() + 1);
  }
  LOG(INFO) << "Download data from oss: " << data_path_no_bucket << " to "
            << data_dir_zip_path;

  auto download_res = downloader.Get(data_path_no_bucket, data_dir_zip_path);
  if (!download_res.ok()) {
    LOG(FATAL) << "Fail to download data from oss: "
               << download_res.error_message();
  }
  if (std::filesystem::exists(data_dir_zip_path)) {
    LOG(INFO) << "Data zip file exists, start to unzip";
    auto unzip_res = gs::unzip(data_dir_zip_path, local_data_dir);
    if (!unzip_res.ok()) {
      LOG(FATAL) << "Fail to unzip data file: " << unzip_res.error_message();
    }
    // remove zip file
    std::filesystem::remove(data_dir_zip_path);
  } else {
    LOG(FATAL) << "Data zip file not exists: " << data_dir_zip_path;
  }
  return local_data_dir;
}
#endif

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
      "memory allocation strategy")("enable-adhoc-handler",
                                    bpo::value<bool>()->default_value(false),
                                    "whether to enable adhoc handler");

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
  server::WorkDirManipulator::SetWorkspace(workspace);

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
  service_config.enable_adhoc_handler = vm["enable-adhoc-handler"].as<bool>();

  // Config log level
  gs::config_log_level(service_config.log_level, service_config.verbose_level);

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

    // Suppose the default_graph is already loaded.
    LOG(INFO) << "Finish init workspace";
    auto schema_file = server::WorkDirManipulator::GetGraphSchemaPath(
        service_config.default_graph);
    if (service_config.enable_adhoc_handler) {
      gs::init_codegen_proxy(vm, engine_config_file);
    }
  } else {
    LOG(INFO) << "Start query service only";
    std::string graph_schema_path, data_path;

    // init graph
    if (!vm.count("graph-config")) {
      LOG(ERROR) << "graph-config is required";
      return -1;
    }
    graph_schema_path = vm["graph-config"].as<std::string>();
    auto schema_res = gs::Schema::LoadFromYaml(graph_schema_path);
    if (!schema_res.ok()) {
      LOG(FATAL) << "Fail to load graph schema from yaml file: "
                 << graph_schema_path;
    }

    if (!vm.count("data-path")) {
      LOG(FATAL) << "data-path is required";
    }
    data_path = vm["data-path"].as<std::string>();

    auto remote_path = schema_res.value().GetRemotePath();

    // If data_path starts with oss, download the data from oss
    if (!remote_path.empty() && remote_path.find("oss://") == 0) {
#ifdef BUILD_WITH_OSS
      auto down_dir = gs::download_data_from_oss(
          "default_graph", remote_path.substr(6), data_path);
      LOG(INFO) << "Download data from oss to local path: " << remote_path
                << ", " << data_path;
      CHECK(down_dir == data_path);
#else
      LOG(FATAL) << "OSS is not supported in this build";
#endif
    }

    // The schema is loaded just to get the plugin dir and plugin list
    if (service_config.enable_adhoc_handler) {
      gs::init_codegen_proxy(vm, engine_config_file, graph_schema_path);
    }
    db.Close();
    gs::GraphDBConfig config(schema_res.value(), data_path, "",
                             service_config.shard_num);
    config.wal_uri = service_config.wal_uri;
    auto load_res = db.Open(config);
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

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
#include "flex/engines/http_server/graph_db_service.h"
#include "flex/engines/http_server/options.h"
#include "flex/engines/http_server/workdir_manipulator.h"
namespace server {

bool check_port_occupied(uint16_t port) {
  VLOG(10) << "Check port " << port << " is occupied or not.";
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd == -1) {
    return false;
  }
  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  int ret = bind(sockfd, (struct sockaddr*) &addr, sizeof(addr));
  close(sockfd);
  return ret < 0;
}

ServiceConfig::ServiceConfig()
    : bolt_port(DEFAULT_BOLT_PORT),
      admin_port(DEFAULT_ADMIN_PORT),
      query_port(DEFAULT_QUERY_PORT),
      shard_num(DEFAULT_SHARD_NUM),
      enable_adhoc_handler(false),
      dpdk_mode(false),
      enable_thread_resource_pool(true),
      external_thread_num(2),
      start_admin_service(false),
      start_compiler(false),
      enable_gremlin(false),
      enable_bolt(false),
      metadata_store_type_(gs::MetadataStoreType::kLocalFile),
      log_level(DEFAULT_LOG_LEVEL),
      verbose_level(DEFAULT_VERBOSE_LEVEL) {}

const std::string GraphDBService::DEFAULT_GRAPH_NAME = "modern_graph";
const std::string GraphDBService::DEFAULT_INTERACTIVE_HOME = "/opt/flex/";
const std::string GraphDBService::COMPILER_SERVER_CLASS_NAME =
    "com.alibaba.graphscope.GraphServer";

GraphDBService& GraphDBService::get() {
  static GraphDBService instance;
  return instance;
}

void openGraph(const gs::GraphId& graph_id,
               const ServiceConfig& service_config) {
  auto workspace = server::WorkDirManipulator::GetWorkspace();
  if (!std::filesystem::exists(workspace)) {
    LOG(ERROR) << "Workspace directory not exists: " << workspace;
  }
  if (graph_id.empty()) {
    LOG(FATAL) << "No graph is specified";
    return;
  }
  auto data_dir_path =
      workspace + "/" + server::WorkDirManipulator::DATA_DIR_NAME;
  if (!std::filesystem::exists(data_dir_path)) {
    LOG(ERROR) << "Data directory not exists: " << data_dir_path;
    return;
  }

  auto& db = gs::GraphDB::get();
  auto schema_path = server::WorkDirManipulator::GetGraphSchemaPath(graph_id);
  auto schema_res = gs::Schema::LoadFromYaml(schema_path);
  if (!schema_res.ok()) {
    LOG(FATAL) << "Fail to load graph schema from yaml file: " << schema_path;
  }
  auto data_dir_res = server::WorkDirManipulator::GetDataDirectory(graph_id);
  if (!data_dir_res.ok()) {
    LOG(FATAL) << "Fail to get data directory for default graph: "
               << data_dir_res.status().error_message();
  }
  std::string data_dir = data_dir_res.value();
  if (!std::filesystem::exists(data_dir)) {
    LOG(FATAL) << "Data directory not exists: " << data_dir
               << ", for graph: " << graph_id;
  }
  db.Close();
  gs::GraphDBConfig config(schema_res.value(), data_dir,
                           service_config.shard_num);
  config.memory_level = service_config.memory_level;
  if (config.memory_level >= 2) {
    config.enable_auto_compaction = true;
  }
  if (!db.Open(config).ok()) {
    LOG(FATAL) << "Fail to load graph from data directory: " << data_dir;
  }
  LOG(INFO) << "Successfully init graph db for graph: " << graph_id;
}

void GraphDBService::init(const ServiceConfig& config) {
  if (initialized_.load(std::memory_order_relaxed)) {
    std::cerr << "High QPS service has been already initialized!" << std::endl;
    return;
  }
  actor_sys_ = std::make_unique<actor_system>(
      config.shard_num, config.dpdk_mode, config.enable_thread_resource_pool,
      config.external_thread_num, [this]() { set_exit_state(); });
  query_hdl_ = std::make_unique<graph_db_http_handler>(
      config.query_port, config.shard_num, config.enable_adhoc_handler);
  if (config.start_admin_service) {
    admin_hdl_ = std::make_unique<admin_http_handler>(config.admin_port);
  }

  initialized_.store(true);
  service_config_ = config;
  gs::init_cpu_usage_watch();
  if (config.start_admin_service) {
    metadata_store_ = gs::MetadataStoreFactory::Create(
        config.metadata_store_type_, WorkDirManipulator::GetWorkspace());

    auto res = metadata_store_->Open();
    if (!res.ok()) {
      std::cerr << "Failed to open metadata store: "
                << res.status().error_message() << std::endl;
      return;
    }
    LOG(INFO) << "Metadata store opened successfully.";
    // If there is no graph in the metadata store, insert the default graph.
    auto graph_metas_res = metadata_store_->GetAllGraphMeta();
    if (!graph_metas_res.ok()) {
      LOG(FATAL) << "Failed to get graph metas: "
                 << graph_metas_res.status().error_message();
    }
    gs::GraphId cur_graph_id = "";
    // Try to launch service on the previous running graph.
    auto running_graph_res = metadata_store_->GetRunningGraph();
    if (running_graph_res.ok() && !running_graph_res.value().empty()) {
      cur_graph_id = running_graph_res.value();
      // make sure the cur_graph_id is in the graph_metas_res.
      auto it = std::find_if(graph_metas_res.value().begin(),
                             graph_metas_res.value().end(),
                             [&cur_graph_id](const gs::GraphMeta& meta) {
                               return meta.id == cur_graph_id;
                             });
      if (it == graph_metas_res.value().end()) {
        LOG(ERROR) << "The running graph: " << cur_graph_id
                   << " is not in the metadata store, maybe the metadata is "
                      "corrupted.";
        cur_graph_id = "";
      }
    }
    if (cur_graph_id.empty()) {
      if (!graph_metas_res.value().empty()) {
        LOG(INFO) << "There are already " << graph_metas_res.value().size()
                  << " graph metas in the metadata store.";
        // return the graph id with the smallest value.
        cur_graph_id =
            (std::min_element(
                 graph_metas_res.value().begin(), graph_metas_res.value().end(),
                 [](const gs::GraphMeta& a, const gs::GraphMeta& b) {
                   return a.id < b.id;
                 }))
                ->id;
      } else {
        cur_graph_id = insert_default_graph_meta();
      }
    }
    // open the graph with the default graph id.
    openGraph(cur_graph_id, service_config_);
    auto set_res = metadata_store_->SetRunningGraph(cur_graph_id);
    if (!set_res.ok()) {
      LOG(FATAL) << "Failed to set running graph: "
                 << res.status().error_message();
      return;
    }

    auto lock_res = metadata_store_->LockGraphIndices(cur_graph_id);
    if (!lock_res.ok()) {
      LOG(FATAL) << lock_res.status().error_message();
      return;
    }
  }
}

GraphDBService::~GraphDBService() {
  if (actor_sys_) {
    actor_sys_->terminate();
  }
  stop_compiler_subprocess();
  if (metadata_store_) {
    metadata_store_->Close();
  }
}

const ServiceConfig& GraphDBService::get_service_config() const {
  return service_config_;
}

bool GraphDBService::is_initialized() const {
  return initialized_.load(std::memory_order_relaxed);
}

bool GraphDBService::is_running() const {
  return running_.load(std::memory_order_relaxed);
}

uint16_t GraphDBService::get_query_port() const {
  if (query_hdl_) {
    return query_hdl_->get_port();
  }
  return 0;
}

uint64_t GraphDBService::get_start_time() const {
  return start_time_.load(std::memory_order_relaxed);
}

void GraphDBService::reset_start_time() {
  start_time_.store(gs::GetCurrentTimeStamp());
}

std::shared_ptr<gs::IGraphMetaStore> GraphDBService::get_metadata_store()
    const {
  return metadata_store_;
}

gs::Result<seastar::sstring> GraphDBService::service_status() {
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

void GraphDBService::run_and_wait_for_exit() {
  if (!is_initialized()) {
    std::cerr << "High QPS service has not been inited!" << std::endl;
    return;
  }
  actor_sys_->launch();
  query_hdl_->start();
  if (admin_hdl_) {
    admin_hdl_->start();
  }
  if (service_config_.start_compiler) {
    if (!start_compiler_subprocess()) {
      LOG(FATAL) << "Failed to start compiler subprocess! exiting...";
    }
  }
  start_time_.store(gs::GetCurrentTimeStamp());
  running_.store(true);
  while (running_.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  query_hdl_->stop();
  if (admin_hdl_) {
    admin_hdl_->stop();
  }
  actor_sys_->terminate();
}

void GraphDBService::set_exit_state() { running_.store(false); }

bool GraphDBService::is_actors_running() const {
  if (query_hdl_) {
    return query_hdl_->is_actors_running();
  } else
    return false;
}

seastar::future<> GraphDBService::stop_query_actors() {
  std::unique_lock<std::mutex> lock(mtx_);
  if (query_hdl_) {
    return query_hdl_->stop_query_actors();
  } else {
    std::cerr << "Query handler has not been inited!" << std::endl;
    return seastar::make_exception_future<>(
        std::runtime_error("Query handler has not been inited!"));
  }
}

void GraphDBService::start_query_actors() {
  std::unique_lock<std::mutex> lock(mtx_);
  if (query_hdl_) {
    query_hdl_->start_query_actors();
  } else {
    std::cerr << "Query handler has not been inited!" << std::endl;
    return;
  }
}

bool GraphDBService::check_compiler_ready() const {
  if (service_config_.start_compiler) {
    if (service_config_.enable_gremlin) {
      if (check_port_occupied(service_config_.gremlin_port)) {
        return true;
      } else {
        LOG(ERROR) << "Gremlin server is not ready!";
        return false;
      }
    }
    if (service_config_.enable_bolt) {
      if (check_port_occupied(service_config_.bolt_port)) {
        return true;
      } else {
        LOG(ERROR) << "Bolt server is not ready!";
        return false;
      }
    }
  }
  return true;
}

bool GraphDBService::start_compiler_subprocess(
    const std::string& graph_schema_path) {
  if (!service_config_.start_compiler) {
    return true;
  }
  LOG(INFO) << "Start compiler subprocess";
  stop_compiler_subprocess();
  auto java_bin_path = boost::process::search_path("java");
  if (java_bin_path.empty()) {
    std::cerr << "Java binary not found in PATH!" << std::endl;
    return false;
  }
  // try to find compiler jar from env.
  auto interactive_class_path = find_interactive_class_path();
  if (interactive_class_path.empty()) {
    std::cerr << "Interactive home not found!" << std::endl;
    return false;
  }
  std::stringstream ss;
  ss << "java -cp " << interactive_class_path;
  if (!graph_schema_path.empty()) {
    ss << " -Dgraph.schema=http://localhost:" << service_config_.admin_port
       << "/v1/service/status";
  }
  ss << " " << COMPILER_SERVER_CLASS_NAME;
  ss << " " << service_config_.engine_config_path;
  auto cmd_str = ss.str();
  LOG(INFO) << "Start compiler with command: " << cmd_str;
  auto compiler_log = WorkDirManipulator::GetCompilerLogFile();

  compiler_process_ =
      boost::process::child(cmd_str, boost::process::std_out > compiler_log,
                            boost::process::std_err > compiler_log);
  LOG(INFO) << "Compiler process started with pid: " << compiler_process_.id();
  // sleep for a maximum 30 seconds to wait for the compiler process to start
  int32_t sleep_time = 0;
  int32_t max_sleep_time = 30;
  int32_t sleep_interval = 4;
  while (sleep_time < max_sleep_time) {
    std::this_thread::sleep_for(std::chrono::seconds(sleep_interval));
    if (!compiler_process_.running()) {
      LOG(ERROR) << "Compiler process failed to start!";
      return false;
    }
    // check query server port is ready
    if (check_compiler_ready()) {
      LOG(INFO) << "Compiler server is ready!";
      // sleep another 2 seconds to make sure the server is ready
      std::this_thread::sleep_for(std::chrono::seconds(2));
      return true;
    }
    sleep_time += sleep_interval;
    LOG(INFO) << "Sleep " << sleep_time << " seconds to wait for compiler "
              << "server to start.";
  }
  LOG(ERROR) << "Max sleep time reached, fail to start compiler server!";
  return false;
}

bool GraphDBService::stop_compiler_subprocess() {
  if (compiler_process_.running()) {
    LOG(INFO) << "Terminate previous compiler process with pid: "
              << compiler_process_.id();
    auto pid = compiler_process_.id();
    ::kill(pid, SIGINT);
    int32_t sleep_time = 0;
    int32_t max_sleep_time = 10;
    int32_t sleep_interval = 2;
    bool sub_process_exited = false;
    // sleep for a maximum 10 seconds to wait for the compiler process to stop
    while (sleep_time < max_sleep_time) {
      std::this_thread::sleep_for(std::chrono::seconds(sleep_interval));
      // check if the compiler process is still running
      if (compiler_process_.running() == false) {
        sub_process_exited = true;
        break;
      }
      sleep_time += sleep_interval;
    }
    // if the compiler process is still running, force to kill it with SIGKILL
    if (sub_process_exited == false) {
      LOG(ERROR) << "Fail to stop compiler process! Force to kill it!";
      ::kill(pid, SIGKILL);
      std::this_thread::sleep_for(std::chrono::seconds(sleep_interval));
    } else {
      LOG(INFO) << "Compiler process stopped successfully in " << sleep_time
                << " seconds.";
    }
  }
  return true;
}

std::string GraphDBService::find_interactive_class_path() {
  std::string interactive_home = DEFAULT_INTERACTIVE_HOME;
  if (std::getenv("INTERACTIVE_HOME")) {
    // try to use DEFAULT_INTERACTIVE_HOME
    interactive_home = std::getenv("INTERACTIVE_HOME");
  }

  // check compiler*.jar in DEFAULT_INTERACTIVE_HOME/lib/
  LOG(INFO) << "try to find compiler*.jar in " << interactive_home << "/lib/";
  auto lib_path = interactive_home + "/lib/";
  if (boost::filesystem::exists(lib_path)) {
    for (auto& p : boost::filesystem::directory_iterator(lib_path)) {
      if (p.path().filename().string().find("compiler") != std::string::npos &&
          p.path().extension() == ".jar") {
        return lib_path + "* -Djna.library.path=" + lib_path;
      }
    }
  }
  // if not, try the relative path from current binary's path
  auto current_binary_dir = gs::get_current_binary_directory();

  auto ir_core_lib_path =
      current_binary_dir /
      "../../../interactive_engine/executor/ir/target/release/";
  if (!boost::filesystem::exists(ir_core_lib_path)) {
    LOG(ERROR) << "ir_core_lib_path not found";
    return "";
  }
  // compiler*.jar in
  // current_binary_dir/../../interactive_engine/compiler/target/
  auto compiler_path =
      current_binary_dir / "../../../interactive_engine/compiler/target/";
  LOG(INFO) << "try to find compiler*.jar in " << compiler_path;
  if (boost::filesystem::exists(compiler_path)) {
    for (auto& p : boost::filesystem::directory_iterator(compiler_path)) {
      if (p.path().filename().string().find("compiler") != std::string::npos &&
          p.path().extension() == ".jar") {
        auto libs_path = compiler_path / "libs";
        // combine the path with the libs folder
        if (boost::filesystem::exists(libs_path)) {
          return p.path().string() + ":" + libs_path.string() +
                 "/* -Djna.library.path=" + ir_core_lib_path.string();
        }
      }
    }
  }
  LOG(ERROR) << "Compiler jar not found";
  return "";
}

gs::GraphId GraphDBService::insert_default_graph_meta() {
  auto default_graph_name = this->service_config_.default_graph;
  auto schema_str_res =
      WorkDirManipulator::GetGraphSchemaString(default_graph_name);
  if (!schema_str_res.ok()) {
    LOG(FATAL) << "Failed to get graph schema string: "
               << schema_str_res.status().error_message();
  }
  auto request_res =
      gs::CreateGraphMetaRequest::FromJson(schema_str_res.value());
  if (!request_res.ok()) {
    LOG(FATAL) << "Failed to parse graph schema string: "
               << request_res.status().error_message();
  }
  auto request = request_res.value();
  request.data_update_time = gs::GetCurrentTimeStamp();

  auto res = metadata_store_->CreateGraphMeta(request);
  if (!res.ok()) {
    LOG(FATAL) << "Failed to insert default graph meta: "
               << res.status().error_message();
  }

  auto dst_graph_dir = WorkDirManipulator::GetGraphDir(res.value());
  auto src_graph_dir = WorkDirManipulator::GetGraphDir(default_graph_name);
  if (std::filesystem::exists(dst_graph_dir)) {
    // if the dst_graph_dir is already existed, we do nothing.
    LOG(INFO) << "Graph dir " << dst_graph_dir << " already exists.";
  } else {
    // create soft link
    std::filesystem::create_symlink(src_graph_dir, dst_graph_dir);
    LOG(INFO) << "Create soft link from " << src_graph_dir << " to "
              << dst_graph_dir;
  }

  LOG(INFO) << "Insert default graph meta successfully, graph_id: "
            << res.value();
  return res.value();
}

}  // namespace server

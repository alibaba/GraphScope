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

#include <atomic>
#include <boost/process.hpp>

#include "flex/engines/http_server/codegen_proxy.h"
#include "flex/engines/http_server/service/hqps_service.h"
#include "flex/engines/http_server/workdir_manipulator.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"

// Write a macro to define the function, to check whether a filed presents in a
// json object.
#define CHECK_JSON_FIELD(json, field)                                         \
  if (!json.contains(field)) {                                                \
    return gs::Result<seastar::sstring>(                                      \
        gs::Status(gs::StatusCode::InValidArgument,                           \
                   "Procedure " + std::string(field) + " is not specified")); \
  }

namespace server {

LockFile::LockFile(const std::string& graph_name, const std::string& lock_path)
    : graph_name(graph_name), lock_path(lock_path) {}
LockFile::~LockFile() {
  if (std::filesystem::exists(lock_path)) {
    std::filesystem::remove(lock_path);
  }
}

LockFile::LockFile(LockFile&& other)
    : graph_name(std::move(other.graph_name)),
      lock_path(std::move(other.lock_path)) {}

AtomicIntDecrementer::AtomicIntDecrementer(std::atomic<int32_t>& count)
    : count_(&count) {}

AtomicIntDecrementer::~AtomicIntDecrementer() {
  if (count_) {
    CHECK(*count_ > 0);
    (*count_)--;
  }
}

AtomicIntDecrementer::AtomicIntDecrementer(AtomicIntDecrementer&& other)
    : count_(other.count_) {
  other.count_ = nullptr;
}

std::string WorkDirManipulator::workspace = ".";  // default to .

static void open_and_write_content(const std::string& job_dir,
                                   const std::string& file_name,
                                   const std::string& content) {
  // write content to file job_dir/file_name
  std::ofstream ofs(job_dir + "/" + file_name);
  ofs << content;
  ofs.close();
}

static gs::Result<seastar::sstring> get_json_sstring_from_yaml(
    const YAML::Node& node) {
  auto json_str_res = gs::get_json_string_from_yaml(node);
  return gs::Result<seastar::sstring>(json_str_res.status(),
                                      json_str_res.value());
}

void WorkDirManipulator::SetWorkspace(const std::string& path) {
  workspace = path;
}

void WorkDirManipulator::SetRunningGraph(const std::string& name) {
  // clear the old RUNNING_GRAPH_FILE_NAME, and write the new one.
  auto running_graph_file = workspace + "/" + RUNNING_GRAPH_FILE_NAME;
  try {
    std::ofstream ofs(running_graph_file,
                      std::ofstream::out | std::ofstream::trunc);
    ofs << name;
    ofs.close();
    LOG(INFO) << "Successfully set running graph: " << name;
  } catch (const std::exception& e) {
    LOG(ERROR) << "Fail to set running graph: " << name
               << ", error: " << e.what();
  }
}

void WorkDirManipulator::ClearRunningGraph() {
  auto running_graph_file = workspace + "/" + RUNNING_GRAPH_FILE_NAME;
  // If the file exists, rm
  if (std::filesystem::exists(running_graph_file)) {
    try {
      std::filesystem::remove(running_graph_file);
      LOG(INFO) << "Successfully clear running graph";
    } catch (const std::exception& e) {
      LOG(ERROR) << "Fail to clear running graph, error: " << e.what();
    }
  }
}

void WorkDirManipulator::ClearLockFile() {
  // for each graph under data_workspace, check whether lock file exists, if
  // exists, remove it.
  auto data_workspace = workspace + "/" + DATA_DIR_NAME;
  for (const auto& entry :
       std::filesystem::directory_iterator(data_workspace)) {
    if (entry.is_directory()) {
      auto graph_name = entry.path().filename().string();
      auto lock_file = get_graph_lock_file(graph_name);
      if (std::filesystem::exists(lock_file)) {
        try {
          std::filesystem::remove(lock_file);
          LOG(INFO) << "Successfully clear lock file for graph: " << graph_name;
        } catch (const std::exception& e) {
          LOG(ERROR) << "Fail to clear lock file for graph: " << graph_name
                     << ", error: " << e.what();
        }
      }
    }
  }
}

std::string WorkDirManipulator::GetRunningGraph() {
  auto running_graph_file = workspace + "/" + RUNNING_GRAPH_FILE_NAME;
  std::ifstream ifs(running_graph_file);
  if (!ifs.is_open()) {
    LOG(ERROR) << "Fail to open running graph file: " << running_graph_file;
    return "";
  }
  std::string line;
  std::getline(ifs, line);
  return line;
}

// GraphName can be specified in the config file or in the argument.
gs::Result<seastar::sstring> WorkDirManipulator::CreateGraph(
    YAML::Node& yaml_config) {
  // First check graph exits
  if (!yaml_config["name"]) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InvalidSchema,
                   "Graph name is not specified"),
        seastar::sstring("Graph name is not specified"));
  }
  auto graph_name = yaml_config["name"].as<std::string>();
  // Set some default values before parsing and dump to file
  if (!yaml_config["stored_procedures"]) {
    // create map for stored_procedures
    yaml_config["stored_procedures"] = YAML::Node(YAML::NodeType::Map);
    yaml_config["stored_procedures"]["directory"] =
        WorkDirManipulator::GRAPH_PLUGIN_DIR_NAME;
  }

  if (is_graph_exist(graph_name)) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::AlreadyExists, "Graph already exists"),
        seastar::sstring("graph " + graph_name + " already exists"));
  }

  // First check whether yaml is valid
  auto schema_result = gs::Schema::LoadFromYamlNode(yaml_config);
  if (!schema_result.ok()) {
    return gs::Result<seastar::sstring>(
        seastar::sstring(schema_result.status().error_message()));
  }
  auto& schema = schema_result.value();
  // dump schema to file.
  RETURN_IF_NOT_OK(dump_graph_schema(yaml_config, graph_name));
  VLOG(10) << "Successfully dump graph schema to file: " << graph_name << ", "
           << GetGraphSchemaPath(graph_name);

  return gs::Result<seastar::sstring>(
      seastar::sstring("successfully created graph "));
}

gs::Result<seastar::sstring> WorkDirManipulator::GetGraphSchemaString(
    const std::string& graph_name) {
  if (!is_graph_exist(graph_name)) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::NotExists,
                   "Graph not exists: " + graph_name),
        seastar::sstring());
  }
  auto schema_file = GetGraphSchemaPath(graph_name);
  if (!std::filesystem::exists(schema_file)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists,
        "Graph schema file is expected, but not exists: " + schema_file));
  }
  // read schema file and output to string
  try {
    auto schema_node = YAML::LoadFile(schema_file);
    if (schema_node["schema"]) {
      FLEX_AUTO(schema_res, get_json_sstring_from_yaml(schema_node["schema"]));
      return gs::Result<seastar::sstring>(schema_res);
    } else {
      return gs::Result<seastar::sstring>(gs::Status(
          gs::StatusCode::InvalidSchema,
          "Schema field not found in schema file for " + graph_name));
    }
  } catch (const std::exception& e) {
    LOG(ERROR) << "Fail to load graph schema from file: " << schema_file
               << ", error: " << e.what();
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to load graph schema from file: " + schema_file +
                       ", for graph: " + graph_name + e.what()));
  }
}

gs::Result<gs::Schema> WorkDirManipulator::GetGraphSchema(
    const std::string& graph_name) {
  LOG(INFO) << "Get graph schema: " << graph_name;
  gs::Schema schema;
  if (!is_graph_exist(graph_name)) {
    return gs::Result<gs::Schema>(gs::Status(
        gs::StatusCode::NotExists, "Graph not exists: " + graph_name));
  }
  auto schema_file = GetGraphSchemaPath(graph_name);
  if (!std::filesystem::exists(schema_file)) {
    return gs::Result<gs::Schema>(gs::Status(
        gs::StatusCode::NotExists,
        "Graph schema file is expected, but not exists: " + schema_file));
  }
  // Load schema from schema_file
  try {
    LOG(INFO) << "Load graph schema from file: " << schema_file;
    auto schema_res = gs::Schema::LoadFromYaml(schema_file);
    if (!schema_res.ok()) {
      return gs::Result<gs::Schema>(schema_res.status(), schema);
    }
    schema = schema_res.value();
  } catch (const std::exception& e) {
    LOG(ERROR) << "Fail to load graph schema: " << schema_file
               << ", error: " << e.what();
    return gs::Result<gs::Schema>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to load graph schema: " + schema_file +
                       ", for graph: " + graph_name + e.what()));
  }
  return gs::Result<gs::Schema>(schema);
}

gs::Result<seastar::sstring> WorkDirManipulator::GetDataDirectory(
    const std::string& graph_name) {
  if (!is_graph_exist(graph_name)) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::NotExists,
                   "Graph not exists: " + graph_name),
        seastar::sstring());
  }
  auto data_dir = GetGraphIndicesDir(graph_name);
  if (!std::filesystem::exists(data_dir)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists,
        "Graph data directory is expected, but not exists: " + data_dir));
  }
  return gs::Result<seastar::sstring>(data_dir);
}

gs::Result<seastar::sstring> WorkDirManipulator::ListGraphs() {
  // list all graph schema files under data_workspace
  YAML::Node yaml_list;
  auto data_workspace = workspace + "/" + DATA_DIR_NAME;
  for (const auto& entry :
       std::filesystem::directory_iterator(data_workspace)) {
    if (entry.is_directory()) {
      auto graph_name = entry.path().filename().string();
      // visit graph.yaml under data/graph_name/graph.yaml
      auto graph_path = GetGraphSchemaPath(graph_name);
      VLOG(10) << "Check graph path: " << graph_path;
      if (!std::filesystem::exists(graph_path)) {
        continue;
      }
      try {
        auto graph_schema_str_res = YAML::LoadFile(graph_path);
        yaml_list.push_back(graph_schema_str_res);
      } catch (const std::exception& e) {
        LOG(ERROR) << "Fail to parse graph schema file: " << graph_path
                   << ", error: " << e.what();
      }
    }
  }
  FLEX_AUTO(json_str, get_json_sstring_from_yaml(yaml_list));
  return gs::Result<seastar::sstring>(json_str);
}

gs::Result<seastar::sstring> WorkDirManipulator::DeleteGraph(
    const std::string& raw_graph_name) {
  auto graph_name = trim_string(raw_graph_name);

  if (!is_graph_exist(graph_name)) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::NotExists,
                   "Graph not exists: " + graph_name),
        seastar::sstring("graph " + graph_name + " not exists"));
  }
  if (is_graph_running(graph_name)) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::IllegalOperation,
                   "Can not remove a running " + graph_name),
        seastar::sstring("graph " + graph_name +
                         " is running, can not be removed"));
  }
  if (is_graph_locked(graph_name)) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::IllegalOperation,
                   "Can not remove graph " + graph_name +
                       ", since data loading ongoing"),
        seastar::sstring("Can not remove graph " + graph_name +
                         ", since data loading ongoing"));
  }
  // remove the graph directory
  try {
    auto graph_path = get_graph_dir(graph_name);
    std::filesystem::remove_all(graph_path);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to remove graph directory: " + graph_name),
        seastar::sstring("Fail to remove graph directory: " + graph_name));
  }
  return gs::Result<seastar::sstring>(
      gs::Status::OK(), "Successfully delete graph: " + graph_name);
}

gs::Result<seastar::sstring> WorkDirManipulator::LoadGraph(
    const std::string& graph_name, const YAML::Node& yaml_node,
    int32_t loading_thread_num, AtomicIntDecrementer&& job_count_decrementer) {
  // First check whether graph exists
  if (!is_graph_exist(graph_name)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists, "Graph not exists: " + graph_name));
  }
  if (is_graph_locked(graph_name)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::IllegalOperation,
        "Graph is locked: " + graph_name +
            ", either service is running on graph, or graph is loading"));
  }
  // Then check graph is already loaded
  if (is_graph_loaded(graph_name)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::IllegalOperation,
        "Graph is already loaded, can not be loaded twice: " + graph_name));
  }
  // check is graph locked
  if (is_graph_running(graph_name)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::IllegalOperation,
        "Graph is already running, can not be loaded: " + graph_name));
  }
  auto lock_res = try_lock_graph(graph_name);
  if (!lock_res.ok()) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::IllegalOperation, "Fail to lock graph: " + graph_name));
  }
  // We use a local object to ensure the lock is released when the function
  // returns.

  // No need to check whether graph exists, because it is checked in LoadGraph
  // First load schema
  auto schema_file = GetGraphSchemaPath(graph_name);
  gs::Schema schema;
  try {
    auto schema_res = gs::Schema::LoadFromYaml(schema_file);
    if (!schema_res.ok()) {
      return gs::Result<seastar::sstring>(schema_res.status());
    }
    schema = schema_res.value();
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to load graph schema: " + schema_file +
                       ", for graph: " + graph_name));
  }
  VLOG(1) << "Loaded schema, vertex label num: " << schema.vertex_label_num()
          << ", edge label num: " << schema.edge_label_num();

  auto loading_config_res =
      gs::LoadingConfig::ParseFromYamlNode(schema, yaml_node);
  if (!loading_config_res.ok()) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   loading_config_res.status().error_message()));
  }
  // dump to file
  auto loading_config = loading_config_res.value();
  std::string temp_file_name = graph_name + "_bulk_loading_config.yaml";
  auto temp_file_path = TMP_DIR + "/" + temp_file_name;
  RETURN_IF_NOT_OK(dump_yaml_to_file(yaml_node, temp_file_path));

  bool overwrite = loading_config.GetMethod() == gs::BulkLoadMethod::kOverwrite
                       ? true
                       : false;
  return load_graph_impl(
      temp_file_path, graph_name, loading_thread_num, overwrite,
      std::forward<AtomicIntDecrementer>(job_count_decrementer),
      std::forward<LockFile>(lock_res.move_value()));
}

gs::Result<seastar::sstring> WorkDirManipulator::GetProceduresByGraphName(
    const std::string& graph_name) {
  if (!is_graph_exist(graph_name)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists, "Graph not exists: " + graph_name));
  }
  bool is_graph_running = WorkDirManipulator::GetRunningGraph() == graph_name;
  bool is_service_running = HQPSService::get().is_actors_running();
  // get graph schema file, and get procedure lists.
  std::vector<std::string> runnable_procedures;
  if (is_service_running && is_graph_running) {
    runnable_procedures = get_runnable_procedures();
    LOG(INFO) << "The graph is running, get procedures from graph db: "
              << graph_name << ", runnable procedure list: "
              << gs::to_string(runnable_procedures);
  }

  auto schema_file = GetGraphSchemaPath(graph_name);
  if (!std::filesystem::exists(schema_file)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists,
        "Graph schema file is expected, but not exists: " + schema_file));
  }
  YAML::Node schema_node;
  try {
    schema_node = YAML::LoadFile(schema_file);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::InternalError,
        "Fail to load graph schema: " + schema_file + ", error: " + e.what()));
  }
  if (schema_node["stored_procedures"]) {
    auto procedure_node = schema_node["stored_procedures"];
    if (procedure_node["enable_lists"]) {
      auto procedures = procedure_node["enable_lists"];
      if (procedures.IsSequence()) {
        std::vector<std::string> procedure_list;
        for (const auto& procedure : procedures) {
          procedure_list.push_back(procedure.as<std::string>());
        }
        VLOG(1) << "Enabled procedures found: " << graph_name
                << ", schema file: " << schema_file
                << ", procedure list: " << gs::to_string(procedure_list);
        return get_all_procedure_yamls(graph_name, procedure_list,
                                       runnable_procedures);
      }
    }
  }
  VLOG(1) << "No enabled procedures found: " << graph_name
          << ", schema file: " << schema_file;
  return get_all_procedure_yamls(
      graph_name,
      runnable_procedures);  // should be all procedures, not enabled only.
}

gs::Result<seastar::sstring>
WorkDirManipulator::GetProcedureByGraphAndProcedureName(
    const std::string& graph_name, const std::string& procedure_name) {
  if (!is_graph_exist(graph_name)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists, "Graph not exists: " + graph_name));
  }
  // get graph schema file, and get procedure lists.
  auto schema_file = GetGraphSchemaPath(graph_name);
  if (!std::filesystem::exists(schema_file)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists,
        "Graph schema file is expected, but not exists: " + schema_file));
  }
  YAML::Node schema_node;
  try {
    schema_node = YAML::LoadFile(schema_file);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::InternalError,
        "Fail to load graph schema: " + schema_file + ", error: " + e.what()));
  }
  // get yaml file in plugin directory.
  auto plugin_dir = get_graph_plugin_dir(graph_name);
  if (!std::filesystem::exists(plugin_dir)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists,
        "Graph plugin directory is expected, but not exists: " + plugin_dir));
  }
  auto plugin_file = plugin_dir + "/" + procedure_name + ".yaml";
  if (!std::filesystem::exists(plugin_file)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists, "plugin not found " + plugin_file));
  }
  // check whether procedure is enabled.
  YAML::Node plugin_node;
  try {
    plugin_node = YAML::LoadFile(plugin_file);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::InternalError,
        "Fail to load graph plugin: " + plugin_file + ", error: " + e.what()));
  }
  plugin_node["enable"] = false;
  plugin_node["bound_graph"] = graph_name;

  if (schema_node["stored_procedures"]) {
    auto procedure_node = schema_node["stored_procedures"];
    if (procedure_node["enable_lists"]) {
      auto procedures = procedure_node["enable_lists"];
      if (procedures.IsSequence()) {
        std::vector<std::string> procedure_list;
        for (const auto& procedure : procedures) {
          procedure_list.push_back(procedure.as<std::string>());
        }
        if (std::find(procedure_list.begin(), procedure_list.end(),
                      procedure_name) != procedure_list.end()) {
          // add enabled: true to the plugin yaml.
          plugin_node["enable"] = true;
        }
      }
    } else {
      VLOG(1) << "No enabled procedures found: " << graph_name
              << ", schema file: " << schema_file;
    }
  }
  bool is_graph_running = WorkDirManipulator::GetRunningGraph() == graph_name;
  bool is_service_running = HQPSService::get().is_actors_running();
  // check runnabled
  if (is_service_running && is_graph_running) {
    auto runnable_procedures = get_runnable_procedures();
    VLOG(1) << "The graph is running, get procedures from graph db: "
            << graph_name << ", runnable procedure list: "
            << gs::to_string(runnable_procedures);
    if (std::find(runnable_procedures.begin(), runnable_procedures.end(),
                  procedure_name) != runnable_procedures.end()) {
      // add runnable: true to the plugin yaml.
      plugin_node["runnable"] = true;
    } else {
      plugin_node["runnable"] = false;
    }
  } else {
    plugin_node["runnable"] = false;
  }

  // yaml_list to string
  FLEX_AUTO(json_str, get_json_sstring_from_yaml(plugin_node));
  return gs::Result<seastar::sstring>(json_str);
}

seastar::future<seastar::sstring> WorkDirManipulator::CreateProcedure(
    const std::string& graph_name, const std::string& parameter,
    const std::string& engine_config_path) {
  if (!is_graph_exist(graph_name)) {
    return seastar::make_exception_future<seastar::sstring>(
        std::runtime_error("Graph not exists: " + graph_name));
  }
  // check procedure exits
  auto plugin_dir = get_graph_plugin_dir(graph_name);
  if (!std::filesystem::exists(plugin_dir)) {
    try {
      std::filesystem::create_directory(plugin_dir);
    } catch (const std::exception& e) {
      return seastar::make_ready_future<seastar::sstring>(
          "Fail to create plugin directory: " + plugin_dir);
    }
  }
  // load parameter as json, and do some check
  nlohmann::json json;
  try {
    json = nlohmann::json::parse(parameter);
  } catch (const std::exception& e) {
    return seastar::make_exception_future<seastar::sstring>(
        std::runtime_error("Fail to parse parameter as json: " + parameter));
  }
  // check required fields is give.
  auto res = create_procedure_sanity_check(json);
  if (!res.ok()) {
    return seastar::make_exception_future<seastar::sstring>(
        std::runtime_error(res.status().error_message()));
  }
  LOG(INFO) << "Pass sanity check for procedure: "
            << json["name"].get<std::string>();
  // get procedure name
  auto procedure_name = json["name"].get<std::string>();
  // check whether procedure already exists.
  auto plugin_file = plugin_dir + "/" + procedure_name + ".yaml";
  if (std::filesystem::exists(plugin_file)) {
    return seastar::make_exception_future<seastar::sstring>(
        std::runtime_error("Procedure already exists: " + procedure_name));
  }
  return generate_procedure(json, engine_config_path)
      .then_wrapped([json](auto&& fut) {
        try {
          auto res = fut.get();
          bool enable = true;  // default enable.
          if (json.contains("enable")) {
            if (json["enable"].is_boolean()) {
              enable = json["enable"].get<bool>();
            } else if (json["enable"].is_string()) {
              auto enable_str = json["enable"].get<std::string>();
              if (enable_str == "true" || enable_str == "True" ||
                  enable_str == "TRUE") {
                enable = true;
              } else {
                enable = false;
              }
            } else {
              return seastar::make_exception_future<seastar::sstring>(
                  std::runtime_error("Fail to parse enable field: " +
                                     json["enable"].dump()));
            }
          }

          // If create procedure success, update graph schema (dump to file)
          // and add to plugin list. this is critical, and should be
          // transactional.
          if (enable) {
            LOG(INFO)
                << "Procedure is enabled, add to graph schema and plugin list.";
            return add_procedure_to_graph(json, res);
          } else {
            // Not enabled, do nothing.
            VLOG(10) << "Procedure is not enabled, do nothing.";
          }

          return seastar::make_ready_future<seastar::sstring>(
              seastar::sstring("Successfully create procedure"));
        } catch (const std::exception& e) {
          return seastar::make_ready_future<seastar::sstring>(
              "Fail to generate procedure: " + std::string(e.what()));
        }
        return seastar::make_ready_future<seastar::sstring>(
            "Fail to generate procedure");
      });
}

gs::Result<seastar::sstring> WorkDirManipulator::DeleteProcedure(
    const std::string& graph_name, const std::string& procedure_name) {
  LOG(INFO) << "Delete procedure: " << procedure_name
            << " on graph: " << graph_name;
  if (!is_graph_exist(graph_name)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists, "Graph not exists: " + graph_name));
  }
  // delete from graph schema
  auto schema_file = GetGraphSchemaPath(graph_name);
  if (!std::filesystem::exists(schema_file)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists,
        "Graph schema file is expected, but not exists: " + schema_file));
  }
  YAML::Node schema_node;
  try {
    schema_node = YAML::LoadFile(schema_file);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::InternalError,
        "Fail to load graph schema: " + schema_file + ", error: " + e.what()));
  }

  if (schema_node["stored_procedures"]) {
    auto procedure_node = schema_node["stored_procedures"];
    if (procedure_node["enable_lists"]) {
      auto procedures = procedure_node["enable_lists"];
      if (procedures.IsSequence()) {
        std::vector<std::string> procedure_list;
        for (const auto& procedure : procedures) {
          procedure_list.push_back(procedure.as<std::string>());
        }
        auto it = std::find(procedure_list.begin(), procedure_list.end(),
                            procedure_name);
        if (it != procedure_list.end()) {
          procedure_list.erase(it);
          procedures = procedure_list;
          VLOG(1) << "Successfully removed " << procedure_name
                  << " from procedure list" << gs::to_string(procedure_list);
          procedure_node["enable_lists"] = procedures;
          // dump to file.
          RETURN_IF_NOT_OK(dump_yaml_to_file(schema_node, schema_file));
        }
      } else {
        VLOG(10) << "No enabled procedures found: " << graph_name
                 << ", schema file: " << schema_file;
      }
    } else {
      VLOG(10) << "No enabled procedures found: " << graph_name
               << ", schema file: " << schema_file;
    }
  }
  // remove the plugin file and dynamic lib
  auto plugin_dir = get_graph_plugin_dir(graph_name);
  if (!std::filesystem::exists(plugin_dir)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists,
        "Graph plugin directory is expected, but not exists: " + plugin_dir));
  }
  auto plugin_file = plugin_dir + "/" + procedure_name + ".yaml";
  if (!std::filesystem::exists(plugin_file)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists, "plugin not found " + plugin_file));
  }
  try {
    std::filesystem::remove(plugin_file);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::InternalError,
        "Fail to remove plugin file: " + plugin_file + ", error: " + e.what()));
  }
  auto plugin_lib = plugin_dir + "/lib" + procedure_name + ".so";
  if (!std::filesystem::exists(plugin_lib)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists, "plugin lib not found " + plugin_lib));
  }
  try {
    std::filesystem::remove(plugin_lib);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::InternalError,
        "Fail to remove plugin lib: " + plugin_lib + ", error: " + e.what()));
  }
  // delete from gs schema's plugin list
  auto& mutable_schema = gs::GraphDB::get().graph().mutable_schema();
  mutable_schema.RemovePlugin(procedure_name);
  LOG(INFO) << "Successfully delete procedure: " << procedure_name
            << " on graph: " << graph_name;

  return gs::Result<seastar::sstring>(gs::Status::OK(),
                                      "Successfully delete procedure");
}

// we only support update the description and enable status.
gs::Result<seastar::sstring> WorkDirManipulator::UpdateProcedure(
    const std::string& graph_name, const std::string& procedure_name,
    const std::string& parameters) {
  if (!is_graph_exist(graph_name)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists, "Graph not exists: " + graph_name));
  }
  // check procedure exits.
  auto plugin_dir = get_graph_plugin_dir(graph_name);
  if (!std::filesystem::exists(plugin_dir)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists,
        "Graph plugin directory is expected, but not exists: " + plugin_dir));
  }
  auto plugin_file = plugin_dir + "/" + procedure_name + ".yaml";
  if (!std::filesystem::exists(plugin_file)) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::NotExists,
                   "plugin not found when update procedure:" + plugin_file));
  }
  // load parameter as json, and do some check
  nlohmann::json json;
  try {
    json = nlohmann::json::parse(parameters);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to parse parameter as json: " + parameters));
  }
  VLOG(1) << "Successfully parse json parameters: " << json.dump();
  // load plugin_file as yaml
  YAML::Node plugin_node;
  try {
    plugin_node = YAML::LoadFile(plugin_file);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::InternalError,
        "Fail to load graph plugin: " + plugin_file + ", error: " + e.what()));
  }
  // update description and enable status.
  if (json.contains("description")) {
    auto new_description = json["description"];
    VLOG(10) << "Update description: "
             << new_description;  // update description
    plugin_node["description"] = new_description.get<std::string>();
  }

  bool enabled;
  if (json.contains("enable")) {
    VLOG(1) << "Enable is specified in the parameter:" << json["enable"].dump();
    if (json["enable"].is_boolean()) {
      enabled = json["enable"].get<bool>();
    } else if (json["enable"].is_string()) {
      auto enable_str = json["enable"].get<std::string>();
      if (enable_str == "true" || enable_str == "True" ||
          enable_str == "TRUE") {
        enabled = true;
      } else {
        enabled = false;
      }
    } else {
      return gs::Result<seastar::sstring>(
          gs::Status(gs::StatusCode::InternalError,
                     "Fail to parse enable field: " + json["enable"].dump()));
    }
    plugin_node["enable"] = enabled;
  }

  // dump to file.
  RETURN_IF_NOT_OK(dump_yaml_to_file(plugin_node, plugin_file));
  VLOG(10) << "Dump plugin yaml to file: " << plugin_file;
  // if enable is specified in the parameter, update graph schema file.
  if (enabled) {
    return enable_procedure_on_graph(graph_name, procedure_name);
  } else {
    return disable_procedure_on_graph(graph_name, procedure_name);
  }
}

gs::Result<seastar::sstring> WorkDirManipulator::GetProcedureLibPath(
    const std::string& graph_name, const std::string& procedure_name) {
  if (!is_graph_exist(graph_name)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists, "Graph not exists: " + graph_name));
  }
  // get the plugin dir and append procedure_name
  auto plugin_dir = get_graph_plugin_dir(graph_name);
  if (!std::filesystem::exists(plugin_dir)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists,
        "Graph plugin directory is expected, but not exists: " + plugin_dir));
  }
  auto plugin_so_path = plugin_dir + "/lib" + procedure_name + ".so";
  if (!std::filesystem::exists(plugin_so_path)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists,
        "Graph plugin so file is expected, but not exists: " + plugin_so_path));
  }
  return gs::Result<seastar::sstring>(plugin_so_path);
}

std::string WorkDirManipulator::GetGraphSchemaPath(
    const std::string& graph_name) {
  return get_graph_dir(graph_name) + "/" + GRAPH_SCHEMA_FILE_NAME;
}

std::string WorkDirManipulator::get_graph_lock_file(
    const std::string& graph_name) {
  return get_graph_dir(graph_name) + "/" + LOCK_FILE;
}

std::string WorkDirManipulator::GetGraphIndicesDir(
    const std::string& graph_name) {
  return get_graph_dir(graph_name) + "/" + GRAPH_INDICES_DIR_NAME;
}

std::string WorkDirManipulator::GetLogDir() {
  auto log_dir = workspace + "/logs/";
  if (!std::filesystem::exists(log_dir)) {
    std::filesystem::create_directory(log_dir);
  }
  return log_dir;
}

std::string WorkDirManipulator::GetCompilerLogFile() {
  // with timestamp
  auto time_stamp = std::to_string(
      std::chrono::system_clock::now().time_since_epoch().count());
  auto log_path = GetLogDir() + "/compiler.log";
  // Check if the log file exists
  if (std::filesystem::exists(log_path)) {
    // Backup the previous log file
    std::string backupPath = GetLogDir() + "/compiler.log." + time_stamp;
    std::filesystem::rename(log_path, backupPath);
    std::cout << "Backed up the previous log file to: " << backupPath
              << std::endl;
  }
  return log_path;
}

std::string WorkDirManipulator::get_graph_indices_file(
    const std::string& graph_name) {
  return get_graph_dir(graph_name) + GRAPH_INDICES_DIR_NAME + "/" +
         GRAPH_INDICES_FILE_NAME;
}

std::string WorkDirManipulator::get_graph_plugin_dir(
    const std::string& graph_name) {
  return get_graph_dir(graph_name) + "/" + GRAPH_PLUGIN_DIR_NAME;
}

// graph_name can be a path, first try as it is absolute path, or
// relative path
std::string WorkDirManipulator::get_graph_dir(const std::string& graph_name) {
  if (std::filesystem::exists(graph_name)) {
    return graph_name;
  }
  return workspace + "/" + DATA_DIR_NAME + "/" + graph_name;
}

bool WorkDirManipulator::is_graph_exist(const std::string& graph_name) {
  auto graph_path = GetGraphSchemaPath(graph_name);
  return std::filesystem::exists(graph_path);
}

bool WorkDirManipulator::is_graph_loaded(const std::string& graph_name) {
  return std::filesystem::exists(get_graph_indices_file(graph_name));
}

bool WorkDirManipulator::is_graph_running(const std::string& graph_name) {
  return GetRunningGraph() == graph_name;
}

bool WorkDirManipulator::is_graph_locked(const std::string& graph_name) {
  auto lock_file = get_graph_lock_file(graph_name);
  return std::filesystem::exists(lock_file);
}

gs::Result<LockFile> WorkDirManipulator::try_lock_graph(
    const std::string& graph_name) {
  auto lock_file = get_graph_lock_file(graph_name);
  if (std::filesystem::exists(lock_file)) {
    return gs::Result<LockFile>(gs::Status(gs::StatusCode::InternalError,
                                           "Graph is locked: " + graph_name));
  }
  std::ofstream fout(lock_file);
  if (!fout.is_open()) {
    return gs::Result<LockFile>(gs::Status(
        gs::StatusCode::InternalError, "Fail to open lock file: " + lock_file));
  }
  fout.close();
  return gs::Result<LockFile>(LockFile(graph_name, lock_file));
}

std::string WorkDirManipulator::trim_string(const std::string& str) {
  auto res = str;
  if (res.back() == '/') {
    res.pop_back();
  }
  if (res.front() == '/') {
    res = str.substr(1);
  }
  return res;
}

bool WorkDirManipulator::ensure_graph_dir_exists(
    const std::string& graph_name) {
  auto graph_path = get_graph_dir(graph_name);
  if (!std::filesystem::exists(graph_path)) {
    std::filesystem::create_directory(graph_path);
  }
  return std::filesystem::exists(graph_path);
}

gs::Result<seastar::sstring> WorkDirManipulator::dump_graph_schema(
    const YAML::Node& yaml_config, const std::string& graph_name) {
  if (!ensure_graph_dir_exists(graph_name)) {
    return {gs::Status(gs::StatusCode::PermissionError,
                       "Fail to create graph directory")};
  }
  auto graph_path = GetGraphSchemaPath(graph_name);
  VLOG(10) << "Dump graph schema to file: " << graph_path;
  RETURN_IF_NOT_OK(dump_yaml_to_file(yaml_config, graph_path));

  VLOG(10) << "Successfully dump graph schema to file: " << graph_path;
  return gs::Result<seastar::sstring>(gs::Status::OK());
}

gs::Result<seastar::sstring> WorkDirManipulator::load_graph_impl(
    const std::string& config_file_path, const std::string& graph_name,
    int32_t loading_thread_num, bool overwrite,
    AtomicIntDecrementer&& decrementer, LockFile&& lock_file) {
  auto schema_file = GetGraphSchemaPath(graph_name);
  auto final_indices_dir = GetGraphIndicesDir(graph_name);
  std::string tmp_indices_dir;
  if (overwrite) {
    tmp_indices_dir = final_indices_dir + "_tmp";
    // remove tmp_indices_dir if exists
    if (std::filesystem::exists(tmp_indices_dir)) {
      std::filesystem::remove_all(tmp_indices_dir);
    }
  } else {
    tmp_indices_dir = final_indices_dir;
  }
  auto bulk_loading_job_log = get_tmp_bulk_loading_job_log_path(graph_name);
  VLOG(10) << "Bulk loading job log: " << bulk_loading_job_log;
  std::stringstream ss;
  ss << GRAPH_LOADER_BIN << " -g " << schema_file << " -l " << config_file_path
     << " -d " << tmp_indices_dir << " -p "
     << std::to_string(loading_thread_num);
  auto cmd_string = ss.str();
  VLOG(10) << "Call graph_loader: " << cmd_string;

  std::string job_id;
  auto fut =
      hiactor::thread_resource_pool::submit_work(
          [&job_id, copied_graph_name = graph_name,
           cmd_string_copied = cmd_string,
           tmp_indices_dir_copied = tmp_indices_dir,
           final_indices_dir_copied = final_indices_dir, overwrite,
           bulk_loading_job_log_copied = bulk_loading_job_log]() mutable {
            boost::process::child child_handle(
                cmd_string_copied,
                boost::process::std_out > bulk_loading_job_log_copied,
                boost::process::std_err > bulk_loading_job_log_copied);
            int32_t pid = child_handle.id();

            auto create_time =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch());
            ASSIGN_AND_RETURN_IF_NOT_OK(
                job_id, create_job(copied_graph_name, create_time.count(), pid,
                                   bulk_loading_job_log_copied));
            auto internal_job_id = job_id;
            child_handle.wait();
            auto res = child_handle.exit_code();
            VLOG(10) << "Graph loader finished, job_id: " << internal_job_id
                     << ", res: " << res;

            update_job_meta(internal_job_id, bulk_loading_job_log_copied, res);
            if (res == 0 && overwrite) {
              VLOG(10) << "Overwrite is true, rename tmp_indices_dir to "
                          "final_indices_dir: "
                       << tmp_indices_dir_copied << " -> "
                       << final_indices_dir_copied;
              CHECK(std::filesystem::exists(tmp_indices_dir_copied));
              if (std::filesystem::exists(final_indices_dir_copied)) {
                std::filesystem::remove_all(final_indices_dir_copied);
              }
              std::filesystem::rename(tmp_indices_dir_copied,
                                      final_indices_dir_copied);
            }
            return gs::Result<seastar::sstring>(gs::Status::OK());
          })
          .then_wrapped(
              [lock_file_copied = std::move(lock_file),
               decrementer_copied = std::move(decrementer)](auto&& f) {
                // the destructor of lock_file will unlock the graph.
                // the destructor of decrementer will decrement the job count.
                return gs::Result<seastar::sstring>(gs::Status::OK());
              });

  while (job_id.empty()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  LOG(INFO) << "Successfully created job: " << job_id;

  return gs::Result<seastar::sstring>(job_id);
}

std::vector<std::string> WorkDirManipulator::get_runnable_procedures() {
  std::vector<std::string> runnable_procedures;

  auto& db = gs::GraphDB::get();
  auto& schema = db.schema();
  auto procedures = schema.GetPlugins();
  // insert keys to vector
  VLOG(10) << "Num of runnable procedures read from schema: "
           << procedures.size();
  for (const auto& procedure : procedures) {
    runnable_procedures.push_back(procedure.first);
  }
  return runnable_procedures;
}

std::string WorkDirManipulator::get_job_dir(const std::string& job_id) {
  // if workspace + "/jobs" not exists, create it.
  auto job_dir = workspace + "/jobs";
  if (!std::filesystem::exists(job_dir)) {
    std::filesystem::create_directory(job_dir);
  }
  return workspace + "/jobs/" + job_id;
}

std::string WorkDirManipulator::get_log_dir() {
  auto log_dir = workspace + "/logs/";
  if (!std::filesystem::exists(log_dir)) {
    std::filesystem::create_directory(log_dir);
  }
  return log_dir;
}

gs::Result<seastar::sstring> WorkDirManipulator::create_job(
    const std::string& graph_name, int64_t time_stamp, int32_t pid,
    const std::string& tmp_log_file) {
  // job_{graph_name}_{time_stamp}_{pid}
  auto job_id = std::string("job_") + graph_name + "_" +
                std::to_string(time_stamp) + "_" + std::to_string(pid);
  auto job_dir = get_job_dir(job_id);
  if (!std::filesystem::create_directory(job_dir)) {
    LOG(ERROR) << "Fail to create job dir: " << job_dir;
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::InternalError,
        "Fail to create job dir: " + job_dir + ", error: " + strerror(errno)));
  }
  open_and_write_content(job_dir, JOB_STATUS_FILE_NAME, "RUNNING");
  // write the path to the tmp log file
  open_and_write_content(job_dir, JOB_TMP_LOG_FILE_NAME, tmp_log_file);
  // write graph_name
  open_and_write_content(job_dir, GRAPH_NAME_FILE_NAME, graph_name);
  return gs::Result<seastar::sstring>(std::move(job_id));
}

gs::Result<int32_t> WorkDirManipulator::get_pid_from_job_id(
    const std::string& job_id) {
  // job_{graph_name}_{create_time}_{pid}
  auto job_id_str = job_id;
  if (job_id_str.find("job_") != 0) {
    return gs::Result<int32_t>(
        gs::Status(gs::StatusCode::InternalError, "Invalid job id: " + job_id));
  }
  // find last _
  auto last_ = job_id_str.find_last_of("_");
  if (last_ == std::string::npos) {
    return gs::Result<int32_t>(
        gs::Status(gs::StatusCode::InternalError, "Invalid job id: " + job_id));
  }
  auto pid_str = job_id_str.substr(last_ + 1);
  try {
    auto pid = std::stoi(pid_str);
    return gs::Result<int32_t>(pid);
  } catch (const std::exception& e) {
    return gs::Result<int32_t>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to parse pid from job id: " + job_id));
  }
  // default return gs::StatusCode::InternalError;
  return gs::Result<int32_t>(
      gs::Status(gs::StatusCode::InternalError,
                 "Fail to parse pid from job id: " + job_id));
}

int64_t WorkDirManipulator::get_start_time_from_job_id(
    const std::string& job_id) {
  // job_{graph_name}_{create_time}_{pid}
  auto job_id_str = job_id;
  // find last _
  auto last_ = job_id_str.find_last_of("_");
  if (last_ == std::string::npos) {
    return -1;
  }
  // find last before last _
  auto last_before_last_ = job_id_str.find_last_of("_", last_ - 1);
  if (last_before_last_ == std::string::npos) {
    return -1;
  }
  auto str =
      job_id_str.substr(last_before_last_ + 1, last_ - last_before_last_ - 1);
  try {
    auto start_time = std::stoll(str);
    return start_time;
  } catch (const std::exception& e) { return -1; }
  return -1;
}

std::string WorkDirManipulator::get_job_meta(const std::string& job_id,
                                             const std::string& file_name,
                                             const std::string& default_value) {
  auto job_dir = get_job_dir(job_id);
  auto file_path = job_dir + "/" + file_name;
  if (!std::filesystem::exists(file_path)) {
    return default_value;
  }
  // read first line from file_path
  std::string res;
  std::ifstream fin(file_path);
  if (fin.is_open()) {
    std::getline(fin, res);
    fin.close();
  } else {
    LOG(ERROR) << "Fail to open file: " << file_path;
    return default_value;
  }
  return res;
}

std::string WorkDirManipulator::get_file_content(const std::string& file_name,
                                                 int32_t tail_lines_limit) {
  // read the last 100 lines of the file
  if (std::filesystem::exists(file_name)) {
    std::ifstream fin(file_name);
    if (fin.is_open()) {
      std::string line;
      std::string res;
      int32_t line_count = 0;
      while (std::getline(fin, line)) {
        if (line_count > tail_lines_limit) {
          break;
        }
        res += line + "\n";
        line_count++;
      }
      fin.close();
      return res;
    } else {
      LOG(ERROR) << "Fail to open file: " << file_name;
      return "";
    }
  } else {
    LOG(ERROR) << "File not exists: " << file_name;
    return "";
  }
}

std::string WorkDirManipulator::get_tmp_bulk_loading_job_log_path(
    const std::string& graph_name) {
  // file_name = graph_name + current_time + ".log";
  auto current_time = std::chrono::system_clock::now();
  auto current_time_str = std::chrono::duration_cast<std::chrono::milliseconds>(
                              current_time.time_since_epoch())
                              .count();
  auto file_name = TMP_DIR + "/" + graph_name + "_" +
                   std::to_string(current_time_str) + ".log";
  return file_name;
}

void WorkDirManipulator::update_job_meta(const std::string& job_id,
                                         const std::string& tmp_log_file,
                                         int32_t exit_code) {
  // first check whether the job is already cancelled.
  if (get_job_meta(job_id, JOB_STATUS_FILE_NAME, "") == "CANCELLED") {
    LOG(INFO) << "Job is already cancelled, do nothing.";
    return;
  }
  auto job_dir = get_job_dir(job_id);
  if (exit_code == 0) {
    open_and_write_content(job_dir, JOB_STATUS_FILE_NAME, "SUCCESS");
  } else {
    open_and_write_content(job_dir, JOB_STATUS_FILE_NAME, "FAILED");
  }
  // rename the tmp log file to final log file
  auto final_file_path = job_dir + "/" + JOB_LOG_FILE_NAME;
  if (std::filesystem::exists(tmp_log_file)) {
    std::filesystem::rename(tmp_log_file, final_file_path);
    // remove tmp log file
    std::filesystem::remove(job_dir + "/" + JOB_TMP_LOG_FILE_NAME);
  } else {
    LOG(ERROR) << "Tmp log file not exists: " << tmp_log_file;
  }

  // write exit_code
  open_and_write_content(job_dir, EXIT_CODE_FILE_NAME,
                         std::to_string(exit_code));
  // write current time to end_time file
  auto current_time = std::chrono::system_clock::now();
  auto current_time_str = std::chrono::duration_cast<std::chrono::milliseconds>(
                              current_time.time_since_epoch())
                              .count();
  open_and_write_content(job_dir, END_TIME_FILE_NAME,
                         std::to_string(current_time_str));

  LOG(INFO) << "Successfully update job meta: " << job_dir
            << ",job_id: " << job_id << ", exit code: " << exit_code;
}

void WorkDirManipulator::update_cancelled_job_meta(const std::string& job_id) {
  auto job_dir = get_job_dir(job_id);
  open_and_write_content(job_dir, JOB_STATUS_FILE_NAME, "CANCELLED");

  // get tmp_log_file_path from JOB_TMP_LOG_FILE_NAME, and rename to final log
  // file
  auto final_log_file_path = job_dir + "/" + JOB_LOG_FILE_NAME;
  auto real_tmp_log_file_path = get_job_meta(job_id, JOB_TMP_LOG_FILE_NAME, "");
  if (!real_tmp_log_file_path.empty()) {
    std::filesystem::rename(real_tmp_log_file_path, final_log_file_path);
  }

  // set EXIT_code to -1
  open_and_write_content(job_dir, EXIT_CODE_FILE_NAME, "-1");
}

gs::Result<seastar::sstring> WorkDirManipulator::create_procedure_sanity_check(
    const nlohmann::json& json) {
  // check required fields is give.
  CHECK_JSON_FIELD(json, "bound_graph");
  CHECK_JSON_FIELD(json, "description");
  CHECK_JSON_FIELD(json, "enable");
  CHECK_JSON_FIELD(json, "name");
  CHECK_JSON_FIELD(json, "query");
  CHECK_JSON_FIELD(json, "type");
  auto type = json["type"].get<std::string>();
  if (type == "cypher" || type == "CYPHER") {
    VLOG(10) << "Cypher procedure, name: " << json["name"].get<std::string>()
             << ", enable: " << json["enable"].get<bool>();
  } else if (type == "CPP" || type == "cpp") {
    CHECK_JSON_FIELD(json, "params");
    CHECK_JSON_FIELD(json, "returns");
    VLOG(10) << "Native procedure, name: " << json["name"].get<std::string>()
             << ", enable: " << json["enable"].get<bool>();
  } else {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InValidArgument,
                   "Procedure type is not supported: " + type));
  }

  return gs::Result<seastar::sstring>(gs::Status::OK());
}

seastar::future<seastar::sstring> WorkDirManipulator::generate_procedure(
    const nlohmann::json& json, const std::string& engine_config_path) {
  VLOG(10) << "Generate procedure: " << json.dump();
  auto codegen_bin = gs::find_codegen_bin();
  auto temp_codegen_directory =
      std::string(server::CodegenProxy::DEFAULT_CODEGEN_DIR);
  // mkdir -p temp_codegen_directory
  if (!std::filesystem::exists(temp_codegen_directory)) {
    std::filesystem::create_directory(temp_codegen_directory);
  }
  // dump json["query"] to file.
  auto query = json["query"].get<std::string>();
  auto name = json["name"].get<std::string>();
  auto type = json["type"].get<std::string>();
  auto bounded_graph = json["bound_graph"].get<std::string>();
  std::string procedure_desc;
  if (json.contains("description")) {
    procedure_desc = json["description"].get<std::string>();
  } else {
    procedure_desc = "";
  }
  std::string query_file;
  if (type == "cypher" || type == "CYPHER") {
    query_file = temp_codegen_directory + "/" + name + ".cypher";
  } else if (type == "CPP" || type == "cpp") {
    query_file = temp_codegen_directory + "/" + name + ".cpp";
  } else {
    return seastar::make_exception_future<seastar::sstring>(
        std::runtime_error("Procedure type is not supported: " + type));
  }
  // dump query string as text to query_file
  try {
    std::ofstream fout(query_file);
    if (!fout.is_open()) {
      return seastar::make_exception_future<seastar::sstring>(
          std::runtime_error("Fail to open query file: " + query_file));
    }
    fout << query;
    fout.close();
  } catch (const std::exception& e) {
    return seastar::make_exception_future<seastar::sstring>(
        std::runtime_error("Fail to dump query to file: " + query_file +
                           ", error: " + std::string(e.what())));
  }

  if (!is_graph_exist(bounded_graph)) {
    return seastar::make_exception_future<seastar::sstring>(
        std::runtime_error("Graph not exists: " + bounded_graph));
  }
  auto output_dir = get_graph_plugin_dir(bounded_graph);
  if (!std::filesystem::exists(output_dir)) {
    std::filesystem::create_directory(output_dir);
  }
  auto schema_path = GetGraphSchemaPath(bounded_graph);

  return CodegenProxy::CallCodegenCmd(
             codegen_bin, query_file, name, temp_codegen_directory, output_dir,
             schema_path, engine_config_path, procedure_desc)
      .then_wrapped([name, output_dir](auto&& f) {
        try {
          auto res = f.get();
          std::string so_file;
          {
            std::stringstream ss;
            ss << output_dir << "/lib" << name << ".so";
            so_file = ss.str();
          }
          VLOG(10) << "Check so file: " << so_file;

          if (!std::filesystem::exists(so_file)) {
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error(
                    "Fail to generate procedure, so file not exists: " +
                    so_file));
          }
          std::string yaml_file;
          {
            std::stringstream ss;
            ss << output_dir << "/" << name << ".yaml";
            yaml_file = ss.str();
          }
          LOG(INFO) << "Check yaml file: " << yaml_file;
          if (!std::filesystem::exists(yaml_file)) {
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error(
                    "Fail to generate procedure, yaml file not exists: " +
                    yaml_file));
          }
          return seastar::make_ready_future<seastar::sstring>(
              seastar::sstring{yaml_file});
        } catch (const std::exception& e) {
          LOG(ERROR) << "Fail to generate procedure, error: " << e.what();
          return seastar::make_exception_future<seastar::sstring>(
              std::runtime_error("Fail to generate procedure, error: " +
                                 std::string(e.what())));
        } catch (...) {
          LOG(ERROR) << "Fail to generate procedure, unknown error";
          return seastar::make_exception_future<seastar::sstring>(
              std::runtime_error("Fail to generate procedure, unknown error"));
        }
      });
}

seastar::future<seastar::sstring> WorkDirManipulator::add_procedure_to_graph(
    const nlohmann::json& json, const std::string& proc_yaml_config_file) {
  try {
    YAML::Node proc_config_node;
    proc_config_node = YAML::LoadFile(proc_yaml_config_file);
  } catch (const std::exception& e) {
    return seastar::make_exception_future<seastar::sstring>(gs::Status(
        gs::StatusCode::InternalError,
        "Fail to load procedure config file: " + proc_yaml_config_file +
            ", error: " + e.what()));
  }
  // get graph_name from json
  auto graph_name = json["bound_graph"].get<std::string>();
  auto proc_name = json["name"].get<std::string>();
  if (proc_name.empty()) {
    return seastar::make_exception_future<seastar::sstring>(std::runtime_error(
        "Procedure name is empty, can not add to graph: " + graph_name));
  }
  // get graph schema file
  auto graph_schema_file = GetGraphSchemaPath(graph_name);
  // load graph schema
  YAML::Node schema_node;
  try {
    schema_node = YAML::LoadFile(graph_schema_file);
  } catch (const std::exception& e) {
    return seastar::make_exception_future<seastar::sstring>(
        std::runtime_error("Fail to load graph schema: " + graph_schema_file +
                           ", error: " + e.what()));
  }
  // get plugin list
  if (!schema_node) {
    return seastar::make_exception_future<seastar::sstring>(std::runtime_error(
        "Graph schema is empty, can not add procedure to graph: " +
        graph_name));
  }
  if (!schema_node["stored_procedures"]) {
    schema_node["stored_procedures"] = YAML::Node(YAML::NodeType::Map);
  }
  auto stored_procedures = schema_node["stored_procedures"];
  if (!stored_procedures["enable_lists"]) {
    stored_procedures["enable_lists"] = YAML::Node(YAML::NodeType::Sequence);
  }
  auto enable_lists = stored_procedures["enable_lists"];
  // check whether procedure is already in the list, if so, then we raise
  // error: procedure already exists.
  for (const auto& item : enable_lists) {
    if (item.as<std::string>() == proc_name) {
      return seastar::make_exception_future<seastar::sstring>(
          std::runtime_error("Procedure " + proc_name +
                             " already exists in graph: " + graph_name));
    }
  }
  enable_lists.push_back(proc_name);
  // dump schema to file
  try {
    std::ofstream fout(graph_schema_file);
    if (!fout.is_open()) {
      return seastar::make_exception_future<seastar::sstring>(
          std::runtime_error("Fail to open graph schema file: " +
                             graph_schema_file));
    }
    fout << schema_node;
    fout.close();
  } catch (const std::exception& e) {
    return seastar::make_exception_future<seastar::sstring>(std::runtime_error(
        "Fail to dump graph schema to file: " + graph_schema_file +
        ", error: " + e.what()));
  }
  return seastar::make_ready_future<seastar::sstring>(
      seastar::sstring("Successfully create procedure"));
}

gs::Result<seastar::sstring> WorkDirManipulator::get_all_procedure_yamls(
    const std::string& graph_name,
    const std::vector<std::string>& procedure_names,
    const std::vector<std::string>& runnable_procedures) {
  YAML::Node yaml_list;
  auto plugin_dir = get_graph_plugin_dir(graph_name);
  // iterate all .yamls in plugin_dir
  if (std::filesystem::exists(plugin_dir)) {
    for (const auto& entry : std::filesystem::directory_iterator(plugin_dir)) {
      if (entry.path().extension() == ".yaml") {
        auto procedure_yaml_file = entry.path().string();
        try {
          auto procedure_yaml_node = YAML::LoadFile(procedure_yaml_file);
          procedure_yaml_node["enable"] = false;
          procedure_yaml_node["runnable"] = false;
          procedure_yaml_node["bound_graph"] = graph_name;
          if (!procedure_yaml_node["name"]) {
            LOG(ERROR) << "Procedure yaml file not contains name: "
                       << procedure_yaml_file;
            return gs::Result<seastar::sstring>(
                gs::Status(gs::StatusCode::InternalError,
                           "Procedure yaml file not contains name: " +
                               procedure_yaml_file));
          }
          auto proc_name = procedure_yaml_node["name"].as<std::string>();
          if (std::find(procedure_names.begin(), procedure_names.end(),
                        proc_name) != procedure_names.end()) {
            // only add the procedure yaml file that is in procedure_names.
            procedure_yaml_node["enable"] = true;
          }
          if (std::find(runnable_procedures.begin(), runnable_procedures.end(),
                        proc_name) != runnable_procedures.end()) {
            // only add the procedure yaml file that is in
            // runnable_procedures.
            procedure_yaml_node["runnable"] = true;
          }

          yaml_list.push_back(procedure_yaml_node);
        } catch (const std::exception& e) {
          LOG(ERROR) << "Fail to load procedure yaml file: "
                     << procedure_yaml_file << ", error: " << e.what();
          return gs::Result<seastar::sstring>(gs::Status(
              gs::StatusCode::InternalError,
              "Fail to load procedure yaml file: " + procedure_yaml_file +
                  ", error: " + e.what()));
        }
      }
    }
  }
  FLEX_AUTO(res, get_json_sstring_from_yaml(yaml_list));
  return gs::Result<seastar::sstring>(std::move(res));
}

// get all procedures for graph, all set to disabled.
gs::Result<seastar::sstring> WorkDirManipulator::get_all_procedure_yamls(
    const std::string& graph_name,
    const std::vector<std::string>& runnable_procedures) {
  YAML::Node yaml_list;
  auto plugin_dir = get_graph_plugin_dir(graph_name);
  // iterate all .yamls in plugin_dir
  if (std::filesystem::exists(plugin_dir)) {
    for (const auto& entry : std::filesystem::directory_iterator(plugin_dir)) {
      if (entry.path().extension() == ".yaml") {
        auto procedure_yaml_file = entry.path().string();
        try {
          auto procedure_yaml_node = YAML::LoadFile(procedure_yaml_file);
          procedure_yaml_node["enable"] = false;
          procedure_yaml_node["runnable"] = false;
          procedure_yaml_node["bound_graph"] = graph_name;
          auto proc_name = procedure_yaml_node["name"].as<std::string>();
          if (std::find(runnable_procedures.begin(), runnable_procedures.end(),
                        proc_name) != runnable_procedures.end()) {
            // only add the procedure yaml file that is in
            // runnable_procedures.
            procedure_yaml_node["runnable"] = true;
          }
          yaml_list.push_back(procedure_yaml_node);
        } catch (const std::exception& e) {
          LOG(ERROR) << "Fail to load procedure yaml file: "
                     << procedure_yaml_file << ", error: " << e.what();
          return gs::Result<seastar::sstring>(gs::Status(
              gs::StatusCode::InternalError,
              "Fail to load procedure yaml file: " + procedure_yaml_file +
                  ", error: " + e.what()));
        }
      }
    }
  }
  FLEX_AUTO(res, get_json_sstring_from_yaml(yaml_list));
  return gs::Result<seastar::sstring>(std::move(res));
}

gs::Result<seastar::sstring> WorkDirManipulator::get_procedure_yaml(
    const std::string& graph_name, const std::string& procedure_name) {
  auto procedure_yaml_file =
      get_graph_plugin_dir(graph_name) + "/" + procedure_name + ".yaml";
  if (!std::filesystem::exists(procedure_yaml_file)) {
    LOG(ERROR) << "Procedure yaml file not exists: " << procedure_yaml_file;
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Procedure yaml file not exists: " + procedure_yaml_file));
  }
  try {
    auto procedure_yaml_node = YAML::LoadFile(procedure_yaml_file);
    FLEX_AUTO(str, get_json_sstring_from_yaml(procedure_yaml_node));
    return gs::Result<seastar::sstring>(std::move(str));
  } catch (const std::exception& e) {
    LOG(ERROR) << "Fail to load procedure yaml file: " << procedure_yaml_file
               << ", error: " << e.what();
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to load procedure yaml file: " + procedure_yaml_file +
                       ", error: " + e.what()));
  }
  return gs::Result<seastar::sstring>(
      gs::Status(gs::StatusCode::InternalError, "Unknown error"));
}

gs::Result<seastar::sstring> WorkDirManipulator::enable_procedure_on_graph(
    const std::string& graph_name, const std::string& procedure_name) {
  VLOG(10) << "Enabling procedure " << procedure_name << " on graph "
           << graph_name;

  auto schema_file = GetGraphSchemaPath(graph_name);
  if (!std::filesystem::exists(schema_file)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists, "Graph schema file not exists: " +
                                       schema_file + ", graph: " + graph_name));
  }
  YAML::Node schema_node;
  try {
    schema_node = YAML::LoadFile(schema_file);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::InternalError,
        "Fail to load graph schema: " + schema_file + ", error: " + e.what()));
  }
  if (!schema_node["stored_procedures"]) {
    schema_node["stored_procedures"] = YAML::Node(YAML::NodeType::Map);
  }
  auto stored_procedures = schema_node["stored_procedures"];
  if (!stored_procedures["enable_lists"]) {
    stored_procedures["enable_lists"] = YAML::Node(YAML::NodeType::Sequence);
  }
  auto enable_lists = stored_procedures["enable_lists"];
  // check whether procedure is already in the list, if so, then we raise
  // error: procedure already exists.
  for (const auto& item : enable_lists) {
    if (item.as<std::string>() == procedure_name) {
      return gs::Result<seastar::sstring>(
          gs::Status(gs::StatusCode::OK,
                     "Procedure already exists in graph: " + graph_name));
    }
  }
  enable_lists.push_back(procedure_name);
  // dump schema to file
  RETURN_IF_NOT_OK(dump_yaml_to_file(schema_node, schema_file));
  return gs::Result<seastar::sstring>(gs::Status::OK(), "Success");
}

gs::Result<seastar::sstring> WorkDirManipulator::disable_procedure_on_graph(
    const std::string& graph_name, const std::string& procedure_name) {
  VLOG(10) << "Disabling procedure " << procedure_name << " on graph "
           << graph_name;

  auto schema_file = GetGraphSchemaPath(graph_name);
  if (!std::filesystem::exists(schema_file)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists, "Graph schema file not exists: " +
                                       schema_file + ", graph: " + graph_name));
  }
  YAML::Node schema_node;
  try {
    schema_node = YAML::LoadFile(schema_file);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::InternalError,
        "Fail to load graph schema: " + schema_file + ", error: " + e.what()));
  }
  if (!schema_node["stored_procedures"]) {
    schema_node["stored_procedures"] = YAML::Node(YAML::NodeType::Map);
  }
  auto stored_procedures = schema_node["stored_procedures"];
  if (!stored_procedures["enable_lists"]) {
    stored_procedures["enable_lists"] = YAML::Node(YAML::NodeType::Sequence);
  }
  auto enable_lists = stored_procedures["enable_lists"];
  // check whether procedure is already in the list, if so, then we raise
  // error: procedure already exists.
  // remove procedure from enable_lists
  auto new_enable_list = YAML::Node(YAML::NodeType::Sequence);
  for (auto iter = enable_lists.begin(); iter != enable_lists.end(); iter++) {
    if (iter->as<std::string>() == procedure_name) {
      VLOG(10) << "Found procedure " << procedure_name << " in enable_lists";
      break;
    } else {
      new_enable_list.push_back(*iter);
    }
  }

  VLOG(10) << "after remove: " << enable_lists;
  stored_procedures["enable_lists"] = new_enable_list;
  schema_node["stored_procedures"] = stored_procedures;
  // dump schema to file
  RETURN_IF_NOT_OK(dump_yaml_to_file(schema_node, schema_file));
  return gs::Result<seastar::sstring>(gs::Status::OK(), "Success");
}

gs::Result<seastar::sstring> WorkDirManipulator::dump_yaml_to_file(
    const YAML::Node& yaml_node, const std::string& yaml_file) {
  try {
    YAML::Emitter emitter;
    auto status = gs::write_yaml_node_to_yaml_string(yaml_node, emitter);
    if (!status.ok()) {
      return {status};
    }
    std::ofstream fout(yaml_file);
    if (!fout.is_open()) {
      return gs::Result<seastar::sstring>(
          gs::Status(gs::StatusCode::InternalError,
                     "Fail to open file: " + yaml_file +
                         ", error: " + std::string(std::strerror(errno))));
    }
    fout << emitter.c_str();
    fout.close();
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to dump yaml to file: " + yaml_file +
                       ", error: " + std::string(e.what())));
  } catch (...) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::InternalError,
        "Fail to dump yaml to file: " + yaml_file + ", unknown error"));
  }
  VLOG(10) << "Successfully dump yaml to file: " << yaml_file;
  return gs::Result<seastar::sstring>(gs::Status::OK(), "Success");
}

gs::Result<seastar::sstring> WorkDirManipulator::GetJob(
    const seastar::sstring& job_id_sstring) {
  std::string job_id = job_id_sstring;
  auto job_dir = workspace + "/jobs/" + job_id;
  if (!std::filesystem::exists(job_dir)) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::NotExists, "Job not exists: " + job_id));
  }
  // try to construct a json string
  /*
  {
    "job_id:" : "xxxx",
    "type" : "bulk_loading",
    "status" : "RUNNING/SUCCESS/FAILED",
    "start_time" : "xxxx",
    "end_time" : "xxxx",
    "detail" : {
      "graph_name" : "xxxx"
    }
    "log" : "xxxx" // the last five lines.
  }
  */
  nlohmann::json json;
  json["job_id"] = job_id;
  json["type"] = "bulk_loading";
  json["status"] = get_job_meta(job_id, JOB_STATUS_FILE_NAME, "UNKNOWN");
  json["start_time"] = get_start_time_from_job_id(job_id);
  auto end_time = get_job_meta(job_id, END_TIME_FILE_NAME, "");
  if (!end_time.empty()) {
    // try to convert to int64_t, if failed, use -1;
    try {
      int64_t end_time_int = std::stoll(end_time);
      json["end_time"] = end_time_int;
    } catch (const std::exception& e) { json["end_time"] = -1; }
  }
  json["detail"]["graph_name"] =
      get_job_meta(job_id, GRAPH_NAME_FILE_NAME, "UNKOWN");
  // if the job is running, we should redirect to the tmp log file.
  if (json["status"] == "RUNNING") {
    auto tmp_log_path = get_job_meta(job_id, JOB_TMP_LOG_FILE_NAME, "");
    VLOG(10) << "Tmp log path: " << tmp_log_path;
    // read last five lines from tmp_log_path
    json["log"] = get_file_content(tmp_log_path, 200);
  } else {
    auto log_path = get_job_dir(job_id) + "/" + JOB_LOG_FILE_NAME;
    json["log"] = get_file_content(log_path, 200);
  }
  return gs::Result<seastar::sstring>(json.dump(2));
}

gs::Result<seastar::sstring> WorkDirManipulator::ListJobs() {
  // list all jobs in workspace/jobs
  nlohmann::json json;
  json["jobs"] = nlohmann::json::array();
  // get all sub directories in workspace/jobs
  auto job_dir = workspace + "/jobs";
  std::vector<std::string> job_ids;
  // if job_dir not exists, we return empty json, and create the directory.
  if (!std::filesystem::exists(job_dir)) {
    std::filesystem::create_directory(job_dir);
    return gs::Result<seastar::sstring>(json["jobs"].dump(2));
  }
  for (const auto& entry : std::filesystem::directory_iterator(job_dir)) {
    if (entry.is_directory()) {
      auto job_id = entry.path().filename().string();
      // if job_id start with job_ , we add it to job_ids
      if (job_id.find("job_") == 0) {
        try {
          job_ids.push_back(job_id);
        } catch (const std::exception& e) {
          LOG(ERROR) << "Fail to convert job_id to int32_t: " << job_id
                     << ", error: " << e.what();
          continue;
        }
      }
    }
  }
  VLOG(10) << "collect job ids: " << job_ids.size();
  // for each job_id, we get the job meta
  for (const auto& job_id : job_ids) {
    auto job_res = GetJob(job_id);
    if (!job_res.ok()) {
      LOG(ERROR) << "Fail to get job: " << job_id
                 << ", error: " << job_res.status().error_message();
      continue;
    }
    auto job_json = nlohmann::json::parse(job_res.value());
    json["jobs"].push_back(job_json);
  }
  return gs::Result<seastar::sstring>(json["jobs"].dump(2));
}

gs::Result<seastar::sstring> WorkDirManipulator::CancelJob(
    const seastar::sstring& job_id_sstring) {
  std::string job_id = job_id_sstring;
  // check whether job exists
  auto job_dir = workspace + "/jobs/" + job_id;
  if (!std::filesystem::exists(job_dir)) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::NotExists, "Job not exists: " + job_id));
  }
  // check whether job is running
  auto status = get_job_meta(job_id, JOB_STATUS_FILE_NAME, "UNKNOWN");
  if (status != "RUNNING") {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Job is not running, can not cancel: " + job_id));
  }
  // get pid from job_id
  auto res = get_pid_from_job_id(job_id);
  if (!res.ok()) {
    return gs::Result<seastar::sstring>(res.status());
  }
  auto pid = res.value();

  boost::process::child::child_handle child(pid);
  std::error_code ec;
  boost::process::detail::api::terminate(child, ec);

  VLOG(10) << "Killing process: " << pid << ", res: " << ec.message();
  if (ec.value() != 0) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::InternalError,
        "Fail to kill process: " + std::to_string(pid) +
            ", error: " + std::to_string(ec.value()) + ", " + ec.message()));
  }
  update_cancelled_job_meta(job_id);

  return gs::Result<seastar::sstring>(gs::Status::OK(),
                                      "Successfully cancelled job: " + job_id);
}

const std::string WorkDirManipulator::LOCK_FILE = ".lock";
const std::string WorkDirManipulator::DATA_DIR_NAME = "data";
const std::string WorkDirManipulator::GRAPH_SCHEMA_FILE_NAME = "graph.yaml";
const std::string WorkDirManipulator::GRAPH_INDICES_FILE_NAME =
    "init_snapshot.bin";
const std::string WorkDirManipulator::GRAPH_INDICES_DIR_NAME = "indices";
const std::string WorkDirManipulator::GRAPH_PLUGIN_DIR_NAME = "plugins";
const std::string WorkDirManipulator::CONF_ENGINE_CONFIG_FILE_NAME =
    "engine_config.yaml";
const std::string WorkDirManipulator::RUNNING_GRAPH_FILE_NAME = "RUNNING";
const std::string WorkDirManipulator::TMP_DIR = "/tmp";
const std::string WorkDirManipulator::GRAPH_LOADER_BIN = "bulk_loader";
const std::string WorkDirManipulator::EXIT_CODE_FILE_NAME = "EXIT_CODE";
const std::string WorkDirManipulator::GRAPH_NAME_FILE_NAME = "GRAPH_NAME";
const std::string WorkDirManipulator::JOB_STATUS_FILE_NAME = "STATUS";
const std::string WorkDirManipulator::JOB_LOG_FILE_NAME = "LOG";
const std::string WorkDirManipulator::JOB_TMP_LOG_FILE_NAME = "TMP_LOG";
const std::string WorkDirManipulator::START_TIME_FILE_NAME = "START_TIME";
const std::string WorkDirManipulator::END_TIME_FILE_NAME = "END_TIME";

}  // namespace server
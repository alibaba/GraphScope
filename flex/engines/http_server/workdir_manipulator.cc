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

#include "flex/engines/http_server/workdir_manipulator.h"
#include "flex/engines/http_server/codegen_proxy.h"

// Write a macro to define the function, to check whether a filed presents in a
// json object.
#define CHECK_JSON_FIELD(json, field)                                         \
  if (!json.contains(field)) {                                                \
    return gs::Result<seastar::sstring>(                                      \
        gs::Status(gs::StatusCode::InValidArgument,                           \
                   "Procedure " + std::string(field) + " is not specified")); \
  }

namespace server {
std::string WorkDirManipulator::workspace = ".";  // default to .

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
    const YAML::Node& yaml_config) {
  // First check graph exits
  if (!yaml_config["name"]) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InvalidSchema,
                   "Graph name is not specified"),
        seastar::sstring("Graph name is not specified"));
  }
  auto graph_name = yaml_config["name"].as<std::string>();

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
  auto dump_res = dump_graph_schema(yaml_config, graph_name);
  if (!dump_res.ok()) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::PermissionError,
        "Fail to dump graph schema: " + dump_res.status().error_message()));
  }
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
  auto schema_str_res = gs::get_string_from_yaml(schema_file);
  if (!schema_str_res.ok()) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::NotExists,
                   "Failed to read schema file: " + schema_file +
                       ", error: " + schema_str_res.status().error_message()));
  } else {
    return gs::Result<seastar::sstring>(schema_str_res.value());
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
    schema = gs::Schema::LoadFromYaml(schema_file);
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
  auto json_str = gs::get_string_from_yaml(yaml_list);
  if (!json_str.ok()) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::InternalError,
        "Fail to convert yaml to json: " + json_str.status().error_message()));
  }
  return gs::Result<seastar::sstring>(json_str.value());
}

gs::Result<seastar::sstring> WorkDirManipulator::DeleteGraph(
    const std::string& graph_name) {
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
    int32_t loading_thread_num) {
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
  if (!try_lock_graph(graph_name)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::IllegalOperation, "Fail to lock graph: " + graph_name));
  }

  // No need to check whether graph exists, because it is checked in LoadGraph
  // First load schema
  auto schema_file = GetGraphSchemaPath(graph_name);
  gs::Schema schema;
  try {
    schema = gs::Schema::LoadFromYaml(schema_file);
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
  auto dump_res = dump_yaml_to_file(yaml_node, temp_file_path);
  if (!dump_res.ok()) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to dump loading config to file: " + temp_file_path +
                       ", error: " + dump_res.status().error_message()));
  }

  auto res = LoadGraph(temp_file_path, graph_name, loading_thread_num);
  if (!res.ok()) {
    return gs::Result<seastar::sstring>(res.status());
  }
  // unlock graph
  unlock_graph(graph_name);

  return gs::Result<seastar::sstring>(res.status(), res.value());
}

gs::Result<seastar::sstring> WorkDirManipulator::GetProceduresByGraphName(
    const std::string& graph_name) {
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
  if (schema_node["stored_procedures"]) {
    auto procedure_node = schema_node["stored_procedures"];
    if (procedure_node["enable_lists"]) {
      auto procedures = procedure_node["enable_lists"];
      if (procedures.IsSequence()) {
        std::vector<std::string> procedure_list;
        for (const auto& procedure : procedures) {
          procedure_list.push_back(procedure.as<std::string>());
        }
        LOG(INFO) << "Enabled procedures found: " << graph_name
                  << ", schema file: " << schema_file
                  << ", procedure list: " << gs::to_string(procedure_list);
        return get_all_procedure_yamls(graph_name, procedure_list);
      }
    }
  }
  LOG(INFO) << "No enabled procedures found: " << graph_name
            << ", schema file: " << schema_file;
  return get_all_procedure_yamls(
      graph_name);  // should be all procedures, not enabled only.
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
  plugin_node["enabled"] = false;

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
          plugin_node["enabled"] = true;
        }
      }
    } else {
      LOG(INFO) << "No enabled procedures found: " << graph_name
                << ", schema file: " << schema_file;
    }
  }
  // yaml_list to string
  YAML::Emitter emitter;
  emitter << plugin_node;
  auto str = emitter.c_str();
  return gs::Result<seastar::sstring>(std::move(str));
}

seastar::future<seastar::sstring> WorkDirManipulator::CreateProcedure(
    const std::string& graph_name, const std::string& parameter) {
  if (!is_graph_exist(graph_name)) {
    return seastar::make_ready_future<seastar::sstring>("Graph not exists: " +
                                                        graph_name);
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
        "Fail to parse parameter as json: " + parameter);
  }
  // check required fields is give.
  auto res = create_procedure_sanity_check(json);
  if (!res.ok()) {
    return seastar::make_exception_future<seastar::sstring>(
        res.status().error_message());
  }
  LOG(INFO) << "Pass sanity check for procedure: "
            << json["name"].get<std::string>();
  // get procedure name
  auto procedure_name = json["name"].get<std::string>();
  // check whether procedure already exists.
  auto plugin_file = plugin_dir + "/" + procedure_name + ".yaml";
  if (std::filesystem::exists(plugin_file)) {
    return seastar::make_exception_future<seastar::sstring>(
        "Procedure already exists: " + procedure_name);
  }
  return generate_procedure(json).then_wrapped([json](auto&& fut) {
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
          return seastar::make_ready_future<seastar::sstring>(
              "Fail to parse enable field: " + json["enable"].dump());
        }
      }
      LOG(INFO) << "Enable: " << std::to_string(enable);

      // If create procedure success, update graph schema (dump to file)
      // and add to plugin list. this is critical, and should be
      // transactional.
      if (enable) {
        LOG(INFO)
            << "Procedure is enabled, add to graph schema and plugin list.";
        return add_procedure_to_graph(json, res);
      } else {
        // Not enabled, do nothing.
        LOG(INFO) << "Procedure is not enabled, do nothing.";
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
          auto dump_res = dump_yaml_to_file(schema_node, schema_file);
          if (dump_res.ok()) {
            LOG(INFO) << "Dump graph schema to file: " << schema_file;
          } else {
            return gs::Result<seastar::sstring>(gs::Status(
                gs::StatusCode::InternalError,
                "Fail to dump graph schema: " + schema_file +
                    ", error: " + dump_res.status().error_message()));
          }
        }
      } else {
        VLOG(10) << "No enabled procedures found: " << graph_name
                 << ", schema file: " << schema_file;
      }
    } else {
      LOG(INFO) << "No enabled procedures found: " << graph_name
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
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NotExists, "plugin not found " + plugin_file));
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
  auto dump_res = dump_yaml_to_file(plugin_node, plugin_file);
  if (!dump_res.ok()) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to dump plugin yaml to file: " + plugin_file +
                       ", error: " + dump_res.status().error_message()));
  }
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

bool WorkDirManipulator::try_lock_graph(const std::string& graph_name) {
  auto lock_file = get_graph_lock_file(graph_name);
  if (std::filesystem::exists(lock_file)) {
    return false;
  }
  std::ofstream fout(lock_file);
  if (!fout.is_open()) {
    return false;
  }
  fout.close();
  return true;
}

void WorkDirManipulator::unlock_graph(const std::string& graph_name) {
  auto lock_file = get_graph_lock_file(graph_name);
  if (std::filesystem::exists(lock_file)) {
    std::filesystem::remove(lock_file);
  }
}

std::string WorkDirManipulator::get_engine_config_path() {
  return workspace + "/conf/" + CONF_ENGINE_CONFIG_FILE_NAME;
}

bool WorkDirManipulator::ensure_graph_dir_exists(
    const std::string& graph_name) {
  auto graph_path = get_graph_dir(graph_name);
  if (!std::filesystem::exists(graph_path)) {
    std::filesystem::create_directory(graph_path);
  }
  return std::filesystem::exists(graph_path);
}

gs::Result<std::string> WorkDirManipulator::dump_graph_schema(
    const YAML::Node& yaml_config, const std::string& graph_name) {
  if (!ensure_graph_dir_exists(graph_name)) {
    return {gs::Status(gs::StatusCode::PermissionError,
                       "Fail to create graph directory")};
  }
  auto graph_path = GetGraphSchemaPath(graph_name);
  VLOG(10) << "Dump graph schema to file: " << graph_path;
  std::ofstream fout(graph_path);
  if (!fout.is_open()) {
    return {gs::Status(gs::StatusCode::PermissionError, "Fail to open file")};
  }
  fout << yaml_config;
  fout.close();
  VLOG(10) << "Successfully dump graph schema to file: " << graph_path;
  return gs::Result<std::string>(gs::Status::OK());
}

gs::Result<std::string> WorkDirManipulator::LoadGraph(
    const std::string& config_file_path, const std::string& graph_name,
    int32_t loading_thread_num) {
  // TODO: call GRAPH_LOADER_BIN.
  auto schema_file = GetGraphSchemaPath(graph_name);
  auto cur_indices_dir = GetGraphIndicesDir(graph_name);
  // system call to GRAPH_LOADER_BIN schema_file, loading_config,
  // cur_indices_dir
  std::string cmd_string = GRAPH_LOADER_BIN + " -g " + schema_file + " -l " +
                           config_file_path + " -d " + cur_indices_dir + " " +
                           std::to_string(loading_thread_num);
  LOG(INFO) << "Call graph_loader: " << cmd_string;
  auto res = std::system(cmd_string.c_str());
  if (res != 0) {
    return gs::Result<std::string>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to load graph: " + graph_name +
                       ", error code: " + std::to_string(res)));
  }

  return gs::Result<std::string>(
      gs::Status::OK(), "Successfully load data to graph: " + graph_name);
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
    LOG(INFO) << "Cypher procedure, name: " << json["name"].get<std::string>()
              << ", enable: " << json["enable"].get<bool>();
  } else if (type == "CPP" || type == "cpp") {
    CHECK_JSON_FIELD(json, "params");
    CHECK_JSON_FIELD(json, "returns");
    LOG(INFO) << "Native procedure, name: " << json["name"].get<std::string>()
              << ", enable: " << json["enable"].get<bool>();
  } else {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InValidArgument,
                   "Procedure type is not supported: " + type));
  }

  return gs::Result<seastar::sstring>(gs::Status::OK());
}

seastar::future<seastar::sstring> WorkDirManipulator::generate_procedure(
    const nlohmann::json& json) {
  LOG(INFO) << "Generate procedure: " << json.dump();
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
  std::string query_file;
  if (type == "cypher" || type == "CYPHER") {
    query_file = temp_codegen_directory + "/" + name + ".cypher";
  } else if (type == "CPP" || type == "cpp") {
    query_file = temp_codegen_directory + "/" + name + ".cpp";
  } else {
    return seastar::make_exception_future<seastar::sstring>(
        "Procedure type is not supported: " + type);
  }
  // dump query string as text to query_file
  try {
    std::ofstream fout(query_file);
    if (!fout.is_open()) {
      return seastar::make_exception_future<seastar::sstring>(
          "Fail to open query file: " + query_file);
    }
    fout << query;
    fout.close();
  } catch (const std::exception& e) {
    return seastar::make_exception_future<seastar::sstring>(
        "Fail to dump query to file: " + query_file +
        ", error: " + std::string(e.what()));
  }

  if (!is_graph_exist(bounded_graph)) {
    return seastar::make_exception_future<seastar::sstring>(
        "Graph not exists: " + bounded_graph);
  }
  auto output_dir = get_graph_plugin_dir(bounded_graph);
  if (!std::filesystem::exists(output_dir)) {
    std::filesystem::create_directory(output_dir);
  }
  auto schema_path = GetGraphSchemaPath(bounded_graph);
  auto engine_config = get_engine_config_path();

  return CodegenProxy::CallCodegenCmd(query_file, name, temp_codegen_directory,
                                      output_dir, schema_path, engine_config,
                                      codegen_bin)
      .then_wrapped([name, output_dir](auto&& f) {
        try {
          auto res = f.get();
          std::string so_file;
          {
            std::stringstream ss;
            ss << output_dir << "/lib" << name << ".so";
            so_file = ss.str();
          }
          LOG(INFO) << "Check so file: " << so_file;

          if (!std::filesystem::exists(so_file)) {
            return seastar::make_exception_future<seastar::sstring>(
                "Fail to generate procedure, so file not exists: " + so_file);
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
                "Fail to generate procedure, yaml file not exists: " +
                yaml_file);
          }
          return seastar::make_ready_future<seastar::sstring>(
              seastar::sstring{yaml_file});
        } catch (const std::exception& e) {
          LOG(ERROR) << "Fail to generate procedure, error: " << e.what();
          return seastar::make_exception_future<seastar::sstring>(
              "Fail to generate procedure, error: " + std::string(e.what()));
        } catch (...) {
          LOG(ERROR) << "Fail to generate procedure, unknown error";
          return seastar::make_exception_future<seastar::sstring>(
              "Fail to generate procedure, unknown error");
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
    return seastar::make_exception_future<seastar::sstring>(
        "Procedure name is empty, can not add to graph: " + graph_name);
  }
  // get graph schema file
  auto graph_schema_file = GetGraphSchemaPath(graph_name);
  // load graph schema
  YAML::Node schema_node;
  try {
    schema_node = YAML::LoadFile(graph_schema_file);
  } catch (const std::exception& e) {
    return seastar::make_exception_future<seastar::sstring>(
        "Fail to load graph schema: " + graph_schema_file +
        ", error: " + e.what());
  }
  // get plugin list
  if (!schema_node) {
    return seastar::make_exception_future<seastar::sstring>(
        "Graph schema is empty, can not add procedure to graph: " + graph_name);
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
          "Procedure already exists in graph: " + graph_name);
    }
  }
  enable_lists.push_back(proc_name);
  // dump schema to file
  try {
    std::ofstream fout(graph_schema_file);
    if (!fout.is_open()) {
      return seastar::make_exception_future<seastar::sstring>(
          "Fail to open graph schema file: " + graph_schema_file);
    }
    fout << schema_node;
    fout.close();
  } catch (const std::exception& e) {
    return seastar::make_exception_future<seastar::sstring>(
        "Fail to dump graph schema to file: " + graph_schema_file +
        ", error: " + e.what());
  }
  return seastar::make_ready_future<seastar::sstring>(
      seastar::sstring("Successfully create procedure"));
}

gs::Result<seastar::sstring> WorkDirManipulator::get_all_procedure_yamls(
    const std::string& graph_name,
    const std::vector<std::string>& procedure_names) {
  YAML::Node yaml_list;
  auto plugin_dir = get_graph_plugin_dir(graph_name);
  // iterate all .yamls in plugin_dir
  if (std::filesystem::exists(plugin_dir)) {
    for (const auto& entry : std::filesystem::directory_iterator(plugin_dir)) {
      if (entry.path().extension() == ".yaml") {
        auto procedure_yaml_file = entry.path().string();
        try {
          auto procedure_yaml_node = YAML::LoadFile(procedure_yaml_file);
          procedure_yaml_node["enabled"] = false;
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
            procedure_yaml_node["enabled"] = true;
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
  // dump to json
  auto res = gs::get_string_from_yaml(yaml_list);
  if (!res.ok()) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to dump procedure yaml list to json, error: " +
                       res.status().error_message()));
  }
  return gs::Result<seastar::sstring>(std::move(res.value()));
}

// get all procedures for graph, all set to disabled.
gs::Result<seastar::sstring> WorkDirManipulator::get_all_procedure_yamls(
    const std::string& graph_name) {
  YAML::Node yaml_list;
  auto plugin_dir = get_graph_plugin_dir(graph_name);
  // iterate all .yamls in plugin_dir
  if (std::filesystem::exists(plugin_dir)) {
    for (const auto& entry : std::filesystem::directory_iterator(plugin_dir)) {
      if (entry.path().extension() == ".yaml") {
        auto procedure_yaml_file = entry.path().string();
        try {
          auto procedure_yaml_node = YAML::LoadFile(procedure_yaml_file);
          procedure_yaml_node["enabled"] = false;
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
  // dump to json
  auto res = gs::get_string_from_yaml(yaml_list);
  if (!res.ok()) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to dump procedure yaml list to json, error: " +
                       res.status().error_message()));
  }
  return gs::Result<seastar::sstring>(std::move(res.value()));
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
    // dump to json
    YAML::Emitter emitter;
    emitter << procedure_yaml_node;
    auto str = emitter.c_str();
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
  LOG(INFO) << "Enabling procedure " << procedure_name << " on graph "
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
  auto dump_res = dump_yaml_to_file(schema_node, schema_file);
  if (!dump_res.ok()) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to dump graph schema: " + schema_file +
                       ", error: " + dump_res.status().error_message()));
  }
  return gs::Result<seastar::sstring>(gs::Status::OK(), "Success");
}

gs::Result<seastar::sstring> WorkDirManipulator::disable_procedure_on_graph(
    const std::string& graph_name, const std::string& procedure_name) {
  LOG(INFO) << "Disabling procedure " << procedure_name << " on graph "
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
      LOG(INFO) << "Found procedure " << procedure_name << " in enable_lists";
      break;
    } else {
      new_enable_list.push_back(*iter);
    }
  }

  LOG(INFO) << "after remove: " << enable_lists;
  stored_procedures["enable_lists"] = new_enable_list;
  schema_node["stored_procedures"] = stored_procedures;
  // dump schema to file
  auto dump_res = dump_yaml_to_file(schema_node, schema_file);
  if (!dump_res.ok()) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to dump graph schema: " + schema_file +
                       ", error: " + dump_res.status().error_message()));
  }
  return gs::Result<seastar::sstring>(gs::Status::OK(), "Success");
}

gs::Result<seastar::sstring> WorkDirManipulator::dump_yaml_to_file(
    const YAML::Node& yaml_node, const std::string& procedure_yaml_file) {
  try {
    YAML::Emitter emitter;
    emitter << yaml_node;
    auto str = emitter.c_str();
    std::ofstream fout(procedure_yaml_file);
    if (!fout.is_open()) {
      return gs::Result<seastar::sstring>(
          gs::Status(gs::StatusCode::InternalError,
                     "Fail to open file: " + procedure_yaml_file +
                         ", error: " + std::string(std::strerror(errno))));
    }
    fout << str;
    fout.close();
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to dump yaml to file: " + procedure_yaml_file +
                       ", error: " + std::string(e.what())));
  } catch (...) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::InternalError,
                   "Fail to dump yaml to file: " + procedure_yaml_file +
                       ", unknown error"));
  }
  LOG(INFO) << "Successfully dump yaml to file: " << procedure_yaml_file;
  return gs::Result<seastar::sstring>(gs::Status::OK(), "Success");
}

// define LOCK_FILE
// define DATA_DIR_NAME
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

}  // namespace server
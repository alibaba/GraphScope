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
#include <rapidjson/document.h>
#include "flex/engines/http_server/codegen_proxy.h"
#include "service_utils.h"

#include <boost/uuid/uuid.hpp>             // uuid class
#include <boost/uuid/uuid_generators.hpp>  // generators
#include <boost/uuid/uuid_io.hpp>          // streaming operators etc.

// Write a macro to define the function, to check whether a filed presents in a
// json object.
#define CHECK_JSON_FIELD(json, field)                                         \
  if (!json.HasMember(field)) {                                               \
    return gs::Result<seastar::sstring>(                                      \
        gs::Status(gs::StatusCode::INVALID_ARGUMENT,                          \
                   "Procedure " + std::string(field) + " is not specified")); \
  }

namespace server {
std::string WorkDirManipulator::workspace = ".";  // default to .

void WorkDirManipulator::SetWorkspace(const std::string& path) {
  workspace = path;
}

std::string WorkDirManipulator::GetWorkspace() { return workspace; }

gs::Result<seastar::sstring> WorkDirManipulator::DumpGraphSchema(
    const gs::GraphId& graph_id, const std::string& json_str) {
  YAML::Node yaml_node;
  try {
    yaml_node = YAML::Load(json_str);

  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::INVALID_SCHEMA,
        "Fail to parse graph schema: " + json_str + ", error: " + e.what()));
  }
  return DumpGraphSchema(graph_id, yaml_node);
}

// GraphName can be specified in the config file or in the argument.
gs::Result<seastar::sstring> WorkDirManipulator::DumpGraphSchema(
    const gs::GraphId& graph_id, const YAML::Node& yaml_config) {
  // First check graph exits
  if (!yaml_config["name"]) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::INVALID_SCHEMA,
                   "Graph name is not specified"),
        seastar::sstring("Graph name is not specified"));
  }

  if (is_graph_exist(graph_id)) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::ALREADY_EXISTS, "Graph already exists"),
        seastar::sstring("graph " + graph_id + " already exists"));
  }

  // First check whether yaml is valid
  auto schema_result = gs::Schema::LoadFromYamlNode(yaml_config);
  if (!schema_result.ok()) {
    return gs::Result<seastar::sstring>(
        seastar::sstring(schema_result.status().error_message()));
  }
  auto& schema = schema_result.value();
  // dump schema to file.
  auto dump_res = dump_graph_schema(yaml_config, graph_id);
  if (!dump_res.ok()) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::PERMISSION_DENIED,
        "Fail to dump graph schema: " + dump_res.status().error_message()));
  }
  VLOG(10) << "Successfully dump graph schema to file: " << graph_id << ", "
           << GetGraphSchemaPath(graph_id);

  return gs::Result<seastar::sstring>(
      seastar::sstring("successfully created graph "));
}

gs::Result<bool> WorkDirManipulator::DumpGraphSchema(
    const gs::GraphMeta& graph_meta,
    const std::vector<gs::PluginMeta>& plugin_metas) {
  auto graph_id = graph_meta.id;
  if (!is_graph_exist(graph_id)) {
    return gs::Result<bool>(
        gs::Status(gs::StatusCode::NOT_FOUND, "Graph not exists: " + graph_id),
        false);
  }
  auto graph_schema = graph_meta.ToJson();
  YAML::Node yaml_node;
  try {
    yaml_node = YAML::Load(graph_schema);
  } catch (const std::exception& e) {
    return gs::Result<bool>(
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
                   "Fail to parse graph schema: " + graph_schema +
                       ", error: " + e.what()),
        false);
  }
  if (!yaml_node["stored_procedures"]) {
    yaml_node["stored_procedures"] = YAML::Node(YAML::NodeType::Sequence);
  }
  if (!yaml_node["version"]) {
    yaml_node["version"] = "v0.1";
  }
  auto procedures_node = yaml_node["stored_procedures"];
  for (auto& plugin : plugin_metas) {
    if (plugin.enable) {
      // push back to sequence
      YAML::Node plugin_node;
      plugin_node["name"] = plugin.name;
      plugin_node["library"] = plugin.library;
      // quote the description, since it may contain space.
      plugin_node["description"] = "\"" + plugin.description + "\"";
      if (plugin.params.size() > 0) {
        YAML::Node params_node;
        for (auto& param : plugin.params) {
          params_node.push_back(YAML::convert<gs::Parameter>::encode(
              param));  // convert to YAML::Node via encode function
        }
        plugin_node["params"] = params_node;
      }
      if (plugin.returns.size() > 0) {
        YAML::Node returns_node;
        for (auto& ret : plugin.returns) {
          returns_node.push_back(YAML::convert<gs::Parameter>::encode(
              ret));  // convert to YAML::Node via encode function
        }
        plugin_node["returns"] = returns_node;
      }
      procedures_node.push_back(plugin_node);
      VLOG(10) << "Add enabled plugin: " << plugin.name;
    } else {
      VLOG(10) << "Plugin is not enabled: " << plugin.name;
    }
  }
  yaml_node["stored_procedures"] = procedures_node;
  auto dump_res = dump_graph_schema(yaml_node, graph_id);
  if (!dump_res.ok()) {
    return gs::Result<bool>(gs::Status(gs::StatusCode::PERMISSION_DENIED,
                                       "Fail to dump graph schema: " +
                                           dump_res.status().error_message()),
                            false);
  }
  VLOG(10) << "Successfully dump graph schema to file: " << graph_id << ", "
           << GetGraphSchemaPath(graph_id);
  return gs::Result<bool>(true);
}

gs::Result<seastar::sstring> WorkDirManipulator::GetGraphSchemaString(
    const std::string& graph_name) {
  if (!is_graph_exist(graph_name)) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::NOT_FOUND,
                   "Graph not exists: " + graph_name),
        seastar::sstring());
  }
  auto schema_file = GetGraphSchemaPath(graph_name);
  if (!std::filesystem::exists(schema_file)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND,
        "Graph schema file is expected, but not exists: " + schema_file));
  }
  // read schema file and output to string
  auto schema_str_res = gs::get_json_string_from_yaml(schema_file);
  if (!schema_str_res.ok()) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::NOT_FOUND,
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
        gs::StatusCode::NOT_FOUND, "Graph not exists: " + graph_name));
  }
  auto schema_file = GetGraphSchemaPath(graph_name);
  if (!std::filesystem::exists(schema_file)) {
    return gs::Result<gs::Schema>(gs::Status(
        gs::StatusCode::NOT_FOUND,
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
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
                   "Fail to load graph schema: " + schema_file +
                       ", for graph: " + graph_name + e.what()));
  }
  return gs::Result<gs::Schema>(schema);
}

gs::Result<seastar::sstring> WorkDirManipulator::GetDataDirectory(
    const std::string& graph_name) {
  if (!is_graph_exist(graph_name)) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::NOT_FOUND,
                   "Graph not exists: " + graph_name),
        seastar::sstring());
  }
  auto data_dir = GetGraphIndicesDir(graph_name);
  if (!std::filesystem::exists(data_dir)) {
    std::filesystem::create_directory(data_dir);
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
  auto json_str = gs::get_json_string_from_yaml(yaml_list);
  if (!json_str.ok()) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::INTERNAL_ERROR,
        "Fail to convert yaml to json: " + json_str.status().error_message()));
  }
  return gs::Result<seastar::sstring>(json_str.value());
}

gs::Result<seastar::sstring> WorkDirManipulator::DeleteGraph(
    const std::string& graph_name) {
  // remove the graph directory
  try {
    auto graph_path = get_graph_dir(graph_name);
    std::filesystem::remove_all(graph_path);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
                   "Fail to remove graph directory: " + graph_name),
        seastar::sstring("Fail to remove graph directory: " + graph_name));
  }
  return gs::Result<seastar::sstring>(
      gs::Status::OK(), "Successfully delete graph: " + graph_name);
}

gs::Result<seastar::sstring> WorkDirManipulator::LoadGraph(
    const std::string& graph_name, const YAML::Node& yaml_node,
    int32_t loading_thread_num, const std::string& dst_indices_dir,
    std::shared_ptr<gs::IGraphMetaStore> metadata_store) {
  // First check whether graph exists
  if (!is_graph_exist(graph_name)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND, "Graph not exists: " + graph_name));
  }

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
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
                   "Fail to load graph schema: " + schema_file +
                       ", for graph: " + graph_name));
  }
  VLOG(1) << "Loaded schema, vertex label num: " << schema.vertex_label_num()
          << ", edge label num: " << schema.edge_label_num();

  auto loading_config_res =
      gs::LoadingConfig::ParseFromYamlNode(schema, yaml_node);
  if (!loading_config_res.ok()) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
                   loading_config_res.status().error_message()));
  }
  // dump to file
  auto loading_config = loading_config_res.value();
  std::string temp_file_name = graph_name + "_bulk_loading_config.yaml";
  auto temp_file_path = TMP_DIR + "/" + temp_file_name;
  RETURN_IF_NOT_OK(dump_yaml_to_file(yaml_node, temp_file_path));

  auto loading_config_json_str_res = gs::get_json_string_from_yaml(yaml_node);
  if (!loading_config_json_str_res.ok()) {
    return loading_config_json_str_res.status();
  }

  return load_graph_impl(temp_file_path, graph_name, loading_thread_num,
                         dst_indices_dir, loading_config_json_str_res.value(),
                         metadata_store);
}

gs::Result<seastar::sstring> WorkDirManipulator::GetProceduresByGraphName(
    const std::string& graph_name) {
  if (!is_graph_exist(graph_name)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND, "Graph not exists: " + graph_name));
  }
  // get graph schema file, and get procedure lists.
  auto schema_file = GetGraphSchemaPath(graph_name);
  if (!std::filesystem::exists(schema_file)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND,
        "Graph schema file is expected, but not exists: " + schema_file));
  }
  YAML::Node schema_node;
  try {
    schema_node = YAML::LoadFile(schema_file);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::INTERNAL_ERROR,
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
    const std::string& graph_id, const std::string& procedure_id) {
  if (!is_graph_exist(graph_id)) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::NOT_FOUND, "Graph not exists: " + graph_id));
  }
  // get graph schema file, and get procedure lists.
  auto schema_file = GetGraphSchemaPath(graph_id);
  if (!std::filesystem::exists(schema_file)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND,
        "Graph schema file is expected, but not exists: " + schema_file));
  }
  YAML::Node schema_node;
  try {
    schema_node = YAML::LoadFile(schema_file);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::INTERNAL_ERROR,
        "Fail to load graph schema: " + schema_file + ", error: " + e.what()));
  }
  // get yaml file in plugin directory.
  auto plugin_dir = GetGraphPluginDir(graph_id);
  if (!std::filesystem::exists(plugin_dir)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND,
        "Graph plugin directory is expected, but not exists: " + plugin_dir));
  }
  auto plugin_file = plugin_dir + "/" + procedure_id + ".yaml";
  if (!std::filesystem::exists(plugin_file)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND, "plugin not found " + plugin_file));
  }
  // check whether procedure is enabled.
  YAML::Node plugin_node;
  try {
    plugin_node = YAML::LoadFile(plugin_file);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::INTERNAL_ERROR,
        "Fail to load graph plugin: " + plugin_file + ", error: " + e.what()));
  }
  return gs::Result<seastar::sstring>(
      gs::get_json_string_from_yaml(plugin_node).value());
}

seastar::future<seastar::sstring> WorkDirManipulator::CreateProcedure(
    const std::string& graph_name, const std::string& plugin_id,
    const rapidjson::Value& json, const std::string& engine_config_path) {
  LOG(INFO) << "Create procedure: " << plugin_id << " on graph: " << graph_name;
  if (!is_graph_exist(graph_name)) {
    return seastar::make_ready_future<seastar::sstring>("Graph not exists: " +
                                                        graph_name);
  }
  // check procedure exits
  auto plugin_dir = GetGraphPluginDir(graph_name);
  if (!std::filesystem::exists(plugin_dir)) {
    try {
      std::filesystem::create_directory(plugin_dir);
    } catch (const std::exception& e) {
      return seastar::make_ready_future<seastar::sstring>(
          "Fail to create plugin directory: " + plugin_dir);
    }
  }
  // load parameter as json, and do some check
  // check required fields is give.
  auto res = create_procedure_sanity_check(json);
  if (!res.ok()) {
    return seastar::make_exception_future<seastar::sstring>(
        res.status().error_message());
  }

  LOG(INFO) << "Pass sanity check for procedure: " << json["name"].GetString();
  // get procedure name
  // check whether procedure already exists.
  auto plugin_file = plugin_dir + "/" + plugin_id + ".yaml";
  if (std::filesystem::exists(plugin_file)) {
    return seastar::make_exception_future<seastar::sstring>(
        "Procedure already exists: " + plugin_id);
  }
  return generate_procedure(graph_name, plugin_id, json, engine_config_path);
}

gs::Result<seastar::sstring> WorkDirManipulator::DeleteProcedure(
    const std::string& graph_name, const std::string& procedure_name) {
  LOG(INFO) << "Delete procedure: " << procedure_name
            << " on graph: " << graph_name;
  if (!is_graph_exist(graph_name)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND, "Graph not exists: " + graph_name));
  }
  // remove the plugin file and dynamic lib
  auto plugin_dir = GetGraphPluginDir(graph_name);
  if (!std::filesystem::exists(plugin_dir)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND,
        "Graph plugin directory is expected, but not exists: " + plugin_dir));
  }
  auto plugin_file = plugin_dir + "/" + procedure_name + ".yaml";
  if (!std::filesystem::exists(plugin_file)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND, "plugin not found " + plugin_file));
  }
  try {
    std::filesystem::remove(plugin_file);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::INTERNAL_ERROR,
        "Fail to remove plugin file: " + plugin_file + ", error: " + e.what()));
  }
  auto plugin_lib = plugin_dir + "/lib" + procedure_name + ".so";
  if (!std::filesystem::exists(plugin_lib)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND, "plugin lib not found " + plugin_lib));
  }
  try {
    std::filesystem::remove(plugin_lib);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::INTERNAL_ERROR,
        "Fail to remove plugin lib: " + plugin_lib + ", error: " + e.what()));
  }
  return gs::Result<seastar::sstring>(gs::Status::OK(),
                                      "Successfully delete procedure");
}

// we only support update the description and enable status.
gs::Result<seastar::sstring> WorkDirManipulator::UpdateProcedure(
    const std::string& graph_name, const std::string& procedure_name,
    const std::string& parameters) {
  // check procedure exits.
  auto plugin_dir = GetGraphPluginDir(graph_name);
  if (!std::filesystem::exists(plugin_dir)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND,
        "Graph plugin directory is expected, but not exists: " + plugin_dir));
  }
  auto plugin_file = plugin_dir + "/" + procedure_name + ".yaml";
  if (!std::filesystem::exists(plugin_file)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND, "plugin not found " + plugin_file));
  }
  // load parameter as json, and do some check
  rapidjson::Document json;
  if (json.Parse(parameters.c_str()).HasParseError()) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
                   "Fail to parse parameter as json: " + parameters));
  }
  VLOG(1) << "Successfully parse json parameters: "
          << gs::rapidjson_stringify(json);
  // load plugin_file as yaml
  YAML::Node plugin_node;
  try {
    plugin_node = YAML::LoadFile(plugin_file);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::INTERNAL_ERROR,
        "Fail to load graph plugin: " + plugin_file + ", error: " + e.what()));
  }
  // update description and enable status.
  if (json.HasMember("description")) {
    auto& new_description = json["description"];
    VLOG(10) << "Update description: "
             << gs::jsonToString(new_description);  // update description
    // quote the description, since it may contain space.
    plugin_node["description"] =
        "\"" + std::string(new_description.GetString()) + "\"";
  }

  bool enabled;
  if (json.HasMember("enable")) {
    VLOG(1) << "Enable is specified in the parameter:"
            << gs::jsonToString(json["enable"]);
    if (json["enable"].IsBool()) {
      enabled = json["enable"].GetBool();
    } else if (json["enable"].IsString()) {
      std::string enable_str = json["enable"].GetString();
      if (enable_str == "true" || enable_str == "True" ||
          enable_str == "TRUE") {
        enabled = true;
      } else {
        enabled = false;
      }
    } else {
      return gs::Result<seastar::sstring>(gs::Status(
          gs::StatusCode::INTERNAL_ERROR,
          "Fail to parse enable field: " + gs::jsonToString(json["enable"])));
    }
    plugin_node["enable"] = enabled;
  }

  // dump to file.
  auto dump_res = dump_yaml_to_file(plugin_node, plugin_file);
  if (!dump_res.ok()) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
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
        gs::StatusCode::NOT_FOUND, "Graph not exists: " + graph_name));
  }
  // get the plugin dir and append procedure_name
  auto plugin_dir = GetGraphPluginDir(graph_name);
  if (!std::filesystem::exists(plugin_dir)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND,
        "Graph plugin directory is expected, but not exists: " + plugin_dir));
  }
  auto plugin_so_path = plugin_dir + "/lib" + procedure_name + ".so";
  if (!std::filesystem::exists(plugin_so_path)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND,
        "Graph plugin so file is expected, but not exists: " + plugin_so_path));
  }
  return gs::Result<seastar::sstring>(plugin_so_path);
}

std::string WorkDirManipulator::GetGraphSchemaPath(
    const std::string& graph_name) {
  return get_graph_dir(graph_name) + "/" + GRAPH_SCHEMA_FILE_NAME;
}

std::string WorkDirManipulator::GetGraphDir(const std::string& graph_name) {
  return get_graph_dir(graph_name);
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

std::string WorkDirManipulator::GetUploadDir() {
  auto upload_dir = workspace + UPLOAD_DIR;
  if (!std::filesystem::exists(upload_dir)) {
    std::filesystem::create_directory(upload_dir);
  }
  return upload_dir;
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

gs::Result<std::string> WorkDirManipulator::CommitTempIndices(
    const std::string& graph_id) {
  auto temp_indices_dir = GetTempIndicesDir(graph_id);
  auto indices_dir = GetGraphIndicesDir(graph_id);
  if (std::filesystem::exists(indices_dir)) {
    std::filesystem::remove_all(indices_dir);
  }
  if (!std::filesystem::exists(temp_indices_dir)) {
    return {
        gs::Status(gs::StatusCode::NOT_FOUND, "Temp indices dir not found")};
  }
  std::filesystem::rename(temp_indices_dir, indices_dir);
  return indices_dir;
}

gs::Result<std::string> WorkDirManipulator::CreateFile(
    const seastar::sstring& content) {
  if (content.size() == 0) {
    return {gs::Status(gs::StatusCode::INVALID_ARGUMENT, "Content is empty")};
  }
  if (content.size() > MAX_CONTENT_SIZE) {
    return {
        gs::Status(gs::StatusCode::INVALID_ARGUMENT,
                   "Content is too large" + std::to_string(content.size()))};
  }

  // get the timestamp as the file name
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  auto file_name = GetUploadDir() + "/" + boost::uuids::to_string(uuid);
  std::ofstream fout(file_name);
  if (!fout.is_open()) {
    return {gs::Status(gs::StatusCode::PERMISSION_DENIED, "Fail to open file")};
  }
  fout << content;
  fout.close();
  LOG(INFO) << "Successfully create file: " << file_name;
  return file_name;
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

std::string WorkDirManipulator::GetTempIndicesDir(
    const std::string& graph_name) {
  return get_graph_dir(graph_name) + "/" + GRAPH_TEMP_INDICES_DIR_NAME;
}

std::string WorkDirManipulator::CleanTempIndicesDir(
    const std::string& graph_name) {
  auto temp_indices_dir = GetTempIndicesDir(graph_name);
  if (std::filesystem::exists(temp_indices_dir)) {
    std::filesystem::remove_all(temp_indices_dir);
  }
  return temp_indices_dir;
}

std::string WorkDirManipulator::get_graph_indices_file(
    const std::string& graph_name) {
  return get_graph_dir(graph_name) + GRAPH_INDICES_DIR_NAME + "/" +
         GRAPH_INDICES_FILE_NAME;
}

std::string WorkDirManipulator::GetGraphPluginDir(
    const std::string& graph_name) {
  return get_graph_dir(graph_name) + "/" + GRAPH_PLUGIN_DIR_NAME;
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
    return {gs::Status(gs::StatusCode::PERMISSION_DENIED,
                       "Fail to create graph directory")};
  }
  auto graph_path = GetGraphSchemaPath(graph_name);
  VLOG(10) << "Dump graph schema to file: " << graph_path;
  std::ofstream fout(graph_path);
  if (!fout.is_open()) {
    return {gs::Status(gs::StatusCode::PERMISSION_DENIED, "Fail to open file")};
  }
  std::string yaml_str;
  ASSIGN_AND_RETURN_IF_RESULT_NOT_OK(
      yaml_str, gs::get_yaml_string_from_yaml_node(yaml_config));
  fout << yaml_str;
  fout.close();
  VLOG(10) << "Successfully dump graph schema to file: " << graph_path;
  return gs::Result<std::string>(gs::Status::OK());
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

gs::Result<seastar::sstring> WorkDirManipulator::load_graph_impl(
    const std::string& config_file_path, const std::string& graph_id,
    int32_t loading_thread_num, const std::string& dst_indices_dir,
    const std::string& loading_config_json_str,
    std::shared_ptr<gs::IGraphMetaStore> metadata_store) {
  auto schema_file = GetGraphSchemaPath(graph_id);
  auto final_indices_dir = GetGraphIndicesDir(graph_id);
  auto bulk_loading_job_log = get_tmp_bulk_loading_job_log_path(graph_id);
  VLOG(10) << "Bulk loading job log: " << bulk_loading_job_log;
  std::stringstream ss;
  std::string graph_loader_bin;
  ASSIGN_AND_RETURN_IF_RESULT_NOT_OK(graph_loader_bin, GetGraphLoaderBin());
  ss << graph_loader_bin << " -g " << schema_file << " -l " << config_file_path
     << " -d " << dst_indices_dir << " -p "
     << std::to_string(loading_thread_num);
  auto cmd_string = ss.str();
  VLOG(10) << "Call graph_loader: " << cmd_string;

  gs::JobId job_id;
  auto fut =
      hiactor::thread_resource_pool::submit_work(
          [&job_id, copied_graph_id = graph_id, cmd_string_copied = cmd_string,
           tmp_indices_dir_copied = dst_indices_dir,
           final_indices_dir_copied = final_indices_dir,
           bulk_loading_job_log_copied = bulk_loading_job_log,
           loading_config_json_str_copied = loading_config_json_str,
           metadata_store = metadata_store]() mutable {
            boost::process::child child_handle(
                cmd_string_copied,
                boost::process::std_out > bulk_loading_job_log_copied,
                boost::process::std_err > bulk_loading_job_log_copied);
            int32_t pid = child_handle.id();

            auto create_job_req = gs::CreateJobMetaRequest::NewRunning(
                copied_graph_id, pid, bulk_loading_job_log_copied,
                "BULK_LOADING");
            auto create_job_res = metadata_store->CreateJobMeta(create_job_req);
            if (!create_job_res.ok()) {
              LOG(ERROR) << "Fail to create job meta for graph: "
                         << copied_graph_id;
              return gs::Result<seastar::sstring>(create_job_res.status());
            }
            job_id = create_job_res.value();
            LOG(INFO) << "Successfully created job: " << job_id;
            auto internal_job_id = job_id;
            LOG(INFO) << "Waiting exiting...";

            child_handle.wait();
            auto res = child_handle.exit_code();
            VLOG(10) << "Graph loader finished, job_id: " << internal_job_id
                     << ", res: " << res;

            LOG(INFO) << "Updating job meta and graph meta";
            auto exit_request = gs::UpdateJobMetaRequest::NewFinished(res);
            auto update_exit_res =
                metadata_store->UpdateJobMeta(internal_job_id, exit_request);
            if (!update_exit_res.ok()) {
              LOG(ERROR) << "Fail to update job status to finished, job_id: "
                         << internal_job_id;
              return gs::Result<seastar::sstring>(update_exit_res.status());
            }

            gs::UpdateGraphMetaRequest update_graph_meta_req(
                gs::GetCurrentTimeStamp(), loading_config_json_str_copied);
            // Note that this call is also transactional
            auto update_graph_meta_res = metadata_store->UpdateGraphMeta(
                copied_graph_id, update_graph_meta_req);

            if (!update_graph_meta_res.ok()) {
              LOG(INFO) << "Fail to update graph meta for graph: "
                        << copied_graph_id;
              WorkDirManipulator::CleanTempIndicesDir(copied_graph_id);
              return gs::Result<seastar::sstring>(
                  update_graph_meta_res.status());
            }

            LOG(INFO) << "Committing temp indices for graph: "
                      << copied_graph_id;
            auto commit_res =
                WorkDirManipulator::CommitTempIndices(copied_graph_id);
            if (!commit_res.ok()) {
              LOG(ERROR) << "Fail to commit temp indices for graph: "
                         << copied_graph_id;
              return gs::Result<seastar::sstring>(commit_res.status());
            }
            return gs::Result<seastar::sstring>(
                "Finish Loading and commit temp "
                "indices");
          })
          .then_wrapped([copied_graph_id = graph_id,
                         metadata_store = metadata_store](auto&& f) {
            // the destructor of lock_file will unlock the graph.
            // the destructor of decrementer will decrement the job count.
            metadata_store->UnlockGraphIndices(copied_graph_id);
            return gs::Result<seastar::sstring>("Finish unlock graph");
          });

  while (job_id.empty()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  LOG(INFO) << "Successfully created job: " << job_id;

  return gs::Result<seastar::sstring>(job_id);
}

gs::Result<seastar::sstring> WorkDirManipulator::create_procedure_sanity_check(
    const rapidjson::Value& json) {
  // check required fields is give.
  CHECK_JSON_FIELD(json, "bound_graph");
  CHECK_JSON_FIELD(json, "description");
  CHECK_JSON_FIELD(json, "enable");
  CHECK_JSON_FIELD(json, "name");
  CHECK_JSON_FIELD(json, "query");
  CHECK_JSON_FIELD(json, "type");
  std::string type = json["type"].GetString();
  if (type == "cypher" || type == "CYPHER") {
    LOG(INFO) << "Cypher procedure, name: " << json["name"].GetString()
              << ", enable: " << json["enable"].GetBool();
  } else if (type == "CPP" || type == "cpp") {
    LOG(INFO) << "Native procedure, name: " << json["name"].GetString()
              << ", enable: " << json["enable"].GetBool();
  } else {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::INVALID_ARGUMENT,
                   "Procedure type is not supported: " + type));
  }

  return gs::Result<seastar::sstring>(gs::Status::OK());
}

seastar::future<seastar::sstring> WorkDirManipulator::generate_procedure(
    const std::string& graph_id, const std::string& plugin_id,
    const rapidjson::Value& json, const std::string& engine_config_path) {
  VLOG(10) << "Generate procedure: " << gs::rapidjson_stringify(json);
  auto codegen_bin = gs::find_codegen_bin();
  auto temp_codegen_directory =
      std::string(server::CodegenProxy::DEFAULT_CODEGEN_DIR);
  // mkdir -p temp_codegen_directory
  if (!std::filesystem::exists(temp_codegen_directory)) {
    std::filesystem::create_directory(temp_codegen_directory);
  }
  // dump json["query"] to file.
  auto query = json["query"].GetString();
  // auto name = json["name"].GetString();
  std::string type = json["type"].GetString();
  std::string query_name = plugin_id;
  std::string procedure_desc;
  if (json.HasMember("description")) {
    procedure_desc = json["description"].GetString();
  } else {
    procedure_desc = "";
  }
  std::string query_file;
  if (type == "cypher" || type == "CYPHER") {
    query_file = temp_codegen_directory + "/" + plugin_id + ".cypher";
  } else if (type == "CPP" || type == "cpp") {
    query_file = temp_codegen_directory + "/" + plugin_id + ".cc";
  } else {
    return seastar::make_exception_future<seastar::sstring>(
        "Procedure type is not supported: " + type);
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

  if (!is_graph_exist(graph_id)) {
    return seastar::make_exception_future<seastar::sstring>(
        std::runtime_error("Graph not exists: " + graph_id));
  }
  auto output_dir = GetGraphPluginDir(graph_id);
  if (!std::filesystem::exists(output_dir)) {
    std::filesystem::create_directory(output_dir);
  }
  auto schema_path = GetGraphSchemaPath(graph_id);

  return CodegenProxy::CallCodegenCmd(
             codegen_bin, query_file, query_name, temp_codegen_directory,
             output_dir, schema_path, engine_config_path, procedure_desc)
      .then_wrapped([plugin_id = plugin_id, output_dir](auto&& f) {
        try {
          auto res = f.get();
          if (!res.ok()) {
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("Fail to generate procedure, error: " +
                                   res.status().error_message()));
          }
          std::string so_file;
          {
            std::stringstream ss;
            ss << output_dir << "/lib" << plugin_id << ".so";
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
            ss << output_dir << "/" << plugin_id << ".yaml";
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
              seastar::sstring(plugin_id));
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

gs::Result<seastar::sstring> WorkDirManipulator::get_all_procedure_yamls(
    const std::string& graph_name,
    const std::vector<std::string>& procedure_names) {
  YAML::Node yaml_list;
  auto plugin_dir = GetGraphPluginDir(graph_name);
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
                gs::Status(gs::StatusCode::INTERNAL_ERROR,
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
              gs::StatusCode::INTERNAL_ERROR,
              "Fail to load procedure yaml file: " + procedure_yaml_file +
                  ", error: " + e.what()));
        }
      }
    }
  }
  // dump to json
  auto res = gs::get_json_string_from_yaml(yaml_list);
  if (!res.ok()) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
                   "Fail to dump procedure yaml list to json, error: " +
                       res.status().error_message()));
  }
  return gs::Result<seastar::sstring>(std::move(res.value()));
}

// get all procedures for graph, all set to disabled.
gs::Result<seastar::sstring> WorkDirManipulator::get_all_procedure_yamls(
    const std::string& graph_name) {
  YAML::Node yaml_list;
  auto plugin_dir = GetGraphPluginDir(graph_name);
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
              gs::StatusCode::INTERNAL_ERROR,
              "Fail to load procedure yaml file: " + procedure_yaml_file +
                  ", error: " + e.what()));
        }
      }
    }
  }
  // dump to json
  auto res = gs::get_json_string_from_yaml(yaml_list);
  if (!res.ok()) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
                   "Fail to dump procedure yaml list to json, error: " +
                       res.status().error_message()));
  }
  return gs::Result<seastar::sstring>(std::move(res.value()));
}

gs::Result<seastar::sstring> WorkDirManipulator::get_procedure_yaml(
    const std::string& graph_name, const std::string& procedure_name) {
  auto procedure_yaml_file =
      GetGraphPluginDir(graph_name) + "/" + procedure_name + ".yaml";
  if (!std::filesystem::exists(procedure_yaml_file)) {
    LOG(ERROR) << "Procedure yaml file not exists: " << procedure_yaml_file;
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
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
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
                   "Fail to load procedure yaml file: " + procedure_yaml_file +
                       ", error: " + e.what()));
  }
  return gs::Result<seastar::sstring>(
      gs::Status(gs::StatusCode::INTERNAL_ERROR, "Unknown error"));
}

gs::Result<seastar::sstring> WorkDirManipulator::enable_procedure_on_graph(
    const std::string& graph_name, const std::string& procedure_name) {
  LOG(INFO) << "Enabling procedure " << procedure_name << " on graph "
            << graph_name;

  auto schema_file = GetGraphSchemaPath(graph_name);
  if (!std::filesystem::exists(schema_file)) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::NOT_FOUND, "Graph schema file not exists: " +
                                       schema_file + ", graph: " + graph_name));
  }
  YAML::Node schema_node;
  try {
    schema_node = YAML::LoadFile(schema_file);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::INTERNAL_ERROR,
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
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
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
        gs::StatusCode::NOT_FOUND, "Graph schema file not exists: " +
                                       schema_file + ", graph: " + graph_name));
  }
  YAML::Node schema_node;
  try {
    schema_node = YAML::LoadFile(schema_file);
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(gs::Status(
        gs::StatusCode::INTERNAL_ERROR,
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
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
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
          gs::Status(gs::StatusCode::INTERNAL_ERROR,
                     "Fail to open file: " + procedure_yaml_file +
                         ", error: " + std::string(std::strerror(errno))));
    }
    fout << str;
    fout.close();
  } catch (const std::exception& e) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
                   "Fail to dump yaml to file: " + procedure_yaml_file +
                       ", error: " + std::string(e.what())));
  } catch (...) {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
                   "Fail to dump yaml to file: " + procedure_yaml_file +
                       ", unknown error"));
  }
  LOG(INFO) << "Successfully dump yaml to file: " << procedure_yaml_file;
  return gs::Result<seastar::sstring>(gs::Status::OK(), "Success");
}

gs::Result<seastar::sstring> WorkDirManipulator::GetGraphLoaderBin() {
  // first via relative path
  std::string graph_loader_bin_path =
      (gs::get_current_binary_directory() / std::string(GRAPH_LOADER_BIN))
          .string();
  if (std::filesystem::exists(graph_loader_bin_path)) {
    return gs::Result<seastar::sstring>(graph_loader_bin_path);
  }
  // test whether executable
  std::string which_cmd =
      std::string("which ") + GRAPH_LOADER_BIN + " > /dev/null";
  if (std::system(which_cmd.c_str()) == 0) {
    return gs::Result<seastar::sstring>(graph_loader_bin_path);
  } else {
    return gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
                   "Fail to find graph loader binary: " + GRAPH_LOADER_BIN));
  }
}

// define LOCK_FILE
// define DATA_DIR_NAME
const std::string WorkDirManipulator::LOCK_FILE = ".lock";
const std::string WorkDirManipulator::DATA_DIR_NAME = "data";
const std::string WorkDirManipulator::GRAPH_SCHEMA_FILE_NAME = "graph.yaml";
const std::string WorkDirManipulator::GRAPH_INDICES_FILE_NAME =
    "init_snapshot.bin";
const std::string WorkDirManipulator::GRAPH_INDICES_DIR_NAME = "indices";
const std::string WorkDirManipulator::GRAPH_TEMP_INDICES_DIR_NAME =
    "temp_indices";
const std::string WorkDirManipulator::GRAPH_PLUGIN_DIR_NAME = "plugins";
const std::string WorkDirManipulator::CONF_ENGINE_CONFIG_FILE_NAME =
    "interactive_config.yaml";
const std::string WorkDirManipulator::RUNNING_GRAPH_FILE_NAME = "RUNNING";
const std::string WorkDirManipulator::TMP_DIR = "/tmp";
const std::string WorkDirManipulator::GRAPH_LOADER_BIN = "bulk_loader";
const std::string WorkDirManipulator::UPLOAD_DIR = "upload";

}  // namespace server
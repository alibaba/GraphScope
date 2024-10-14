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
#include <fstream>
#include <iostream>
#include <string>

#include "flex/storages/metadata/graph_meta_store.h"
#include "flex/storages/rt_mutable_graph/schema.h"
#include "flex/utils/yaml_utils.h"
#include "property/types.h"
#include "service_utils.h"

#include <rapidjson/document.h>
#include <rapidjson/pointer.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace gs {

std::string read_file_to_string(const std::string& file_path) {
  if (std::filesystem::exists(file_path)) {
    std::ifstream fin(file_path);
    if (fin.is_open()) {
      std::string line;
      std::string res;
      while (std::getline(fin, line)) {
        res += line + "\n";
      }
      fin.close();
      return res;
    } else {
      LOG(ERROR) << "Fail to open file: " << file_path;
      return "";
    }
  } else {
    LOG(ERROR) << "File not exists: " << file_path;
    return "";
  }
}
const std::vector<PluginMeta>& get_builtin_plugin_metas() {
  static std::vector<PluginMeta> builtin_plugins;
  static bool initialized = false;
  if (!initialized) {
    // count_vertices
    PluginMeta count_vertices;
    count_vertices.id = "count_vertices";
    count_vertices.name = "count_vertices";
    count_vertices.description = "A builtin plugin to count vertices";
    count_vertices.enable = true;
    count_vertices.runnable = true;
    count_vertices.type = "cypher";
    count_vertices.creation_time = GetCurrentTimeStamp();
    count_vertices.update_time = GetCurrentTimeStamp();
    count_vertices.params.push_back({"labelName", PropertyType::kString});
    count_vertices.returns.push_back({"count", PropertyType::kInt32});
    builtin_plugins.push_back(count_vertices);

    // pagerank
    PluginMeta pagerank;
    pagerank.id = "pagerank";
    pagerank.name = "pagerank";
    pagerank.description = "A builtin plugin to calculate pagerank";
    pagerank.enable = true;
    pagerank.runnable = true;
    pagerank.type = "cypher";
    pagerank.creation_time = GetCurrentTimeStamp();
    pagerank.update_time = GetCurrentTimeStamp();
    pagerank.params.push_back({"vertex_label", PropertyType::kString});
    pagerank.params.push_back({"edge_label", PropertyType::kString});
    pagerank.params.push_back({"damping_factor", PropertyType::kDouble});
    pagerank.params.push_back({"max_iterations", PropertyType::kInt32});
    pagerank.params.push_back({"epsilon", PropertyType::kDouble});
    pagerank.returns.push_back({"label_name", PropertyType::kString});
    pagerank.returns.push_back({"vertex_oid", PropertyType::kInt64});
    pagerank.returns.push_back({"pagerank", PropertyType::kDouble});
    builtin_plugins.push_back(pagerank);

    // k_neighbors
    PluginMeta k_neighbors;
    k_neighbors.id = "k_neighbors";
    k_neighbors.name = "k_neighbors";
    k_neighbors.description = "A builtin plugin to calculate k_neighbors";
    k_neighbors.enable = true;
    k_neighbors.runnable = true;
    k_neighbors.type = "cypher";
    k_neighbors.creation_time = GetCurrentTimeStamp();
    k_neighbors.update_time = GetCurrentTimeStamp();
    k_neighbors.params.push_back({"label_name", PropertyType::kString});
    k_neighbors.params.push_back({"oid", PropertyType::kInt64});
    k_neighbors.params.push_back({"k", PropertyType::kInt32});
    k_neighbors.returns.push_back({"label_name", PropertyType::kString});
    k_neighbors.returns.push_back({"vertex_oid", PropertyType::kInt64});
    builtin_plugins.push_back(k_neighbors);

    // shortest_path_among_three
    PluginMeta shortest_path_among_three;
    shortest_path_among_three.id = "shortest_path_among_three";
    shortest_path_among_three.name = "shortest_path_among_three";
    shortest_path_among_three.description =
        "A builtin plugin to calculate shortest_path_among_three";
    shortest_path_among_three.enable = true;
    shortest_path_among_three.runnable = true;
    shortest_path_among_three.type = "cypher";
    shortest_path_among_three.creation_time = GetCurrentTimeStamp();
    shortest_path_among_three.update_time = GetCurrentTimeStamp();
    shortest_path_among_three.params.push_back(
        {"label_name1", PropertyType::kString});
    shortest_path_among_three.params.push_back({"oid1", PropertyType::kInt64});
    shortest_path_among_three.params.push_back(
        {"label_name2", PropertyType::kString});
    shortest_path_among_three.params.push_back({"oid2", PropertyType::kInt64});
    shortest_path_among_three.params.push_back(
        {"label_name3", PropertyType::kString});
    shortest_path_among_three.params.push_back({"oid3", PropertyType::kInt64});
    shortest_path_among_three.returns.push_back(
        {"shortest_path_among_three (label name, vertex oid)",
         PropertyType::kString});
    builtin_plugins.push_back(shortest_path_among_three);

    initialized = true;
  }
  return builtin_plugins;
}

void append_builtin_plugins(std::vector<PluginMeta>& plugin_metas) {
  auto builtin_plugin_metas = get_builtin_plugin_metas();
  plugin_metas.insert(plugin_metas.end(), builtin_plugin_metas.begin(),
                      builtin_plugin_metas.end());
}

UpdateGraphMetaRequest::UpdateGraphMetaRequest(
    int64_t data_update_time, const std::string& data_import_config)
    : data_update_time(data_update_time),
      data_import_config(data_import_config) {}

std::string Parameter::ToJson() const {
  rapidjson::Document json(rapidjson::kObjectType);
  json.AddMember("name", name, json.GetAllocator());
  json.AddMember("type", to_json(type, &json.GetAllocator()),
                 json.GetAllocator());
  return rapidjson_stringify(json);
}

void GraphMeta::ToJson(rapidjson::Value& json,
                       rapidjson::Document::AllocatorType& allocator) const {
  json.AddMember("version", version, allocator);
  json.AddMember("id", id, allocator);
  json.AddMember("name", name, allocator);
  json.AddMember("description", description, allocator);
  json.AddMember("creation_time", creation_time, allocator);
  json.AddMember("data_update_time", data_update_time, allocator);
  if (!data_import_config.empty()) {
    rapidjson::Document tempDoc(rapidjson::kObjectType, &allocator);
    if (tempDoc.Parse(data_import_config.c_str()).HasParseError()) {
      LOG(ERROR) << "Invalid data_import_config: " << data_import_config;
    } else {
      json.AddMember("data_import_config", tempDoc, allocator);
    }
  }
  {
    rapidjson::Document tempDoc(rapidjson::kObjectType, &allocator);
    if (tempDoc.Parse(schema.c_str()).HasParseError()) {
      LOG(ERROR) << "Invalid schema: " << schema;
    } else {
      json.AddMember("schema", tempDoc, allocator);
    }
  }
  rapidjson::Document stored_procedures(rapidjson::kArrayType, &allocator);
  for (auto& plugin_meta : plugin_metas) {
    rapidjson::Document tempDoc(rapidjson::kObjectType, &allocator);
    plugin_meta.ToJson(tempDoc, allocator);
    stored_procedures.PushBack(tempDoc, allocator);
  }
  json.AddMember("stored_procedures", stored_procedures, allocator);
  return;
}

std::string GraphMeta::ToJson() const {
  rapidjson::Document json(rapidjson::kObjectType);
  ToJson(json, json.GetAllocator());
  return rapidjson_stringify(json);
}

GraphMeta GraphMeta::FromJson(const std::string& json_str) {
  rapidjson::Document json(rapidjson::kObjectType);
  if (json.Parse(json_str.c_str()).HasParseError()) {
    LOG(ERROR) << "Invalid json string: " << json_str;
    return GraphMeta();
  } else {
    return GraphMeta::FromJson(json);
  }
}

GraphMeta GraphMeta::FromJson(const rapidjson::Value& json) {
  GraphMeta meta;
  if (json.HasMember("version")) {
    meta.version = json["version"].GetString();
  } else {
    meta.version = "v0.1";
  }
  if (json.HasMember("id")) {
    if (json["id"].IsInt()) {
      meta.id = json["id"].GetInt();
    } else if (json["id"].IsInt64()) {
      meta.id = json["id"].GetInt64();
    } else {
      meta.id = json["id"].GetString();
    }
  }

  meta.name = json["name"].GetString();
  meta.description = json["description"].GetString();
  meta.creation_time = json["creation_time"].GetInt64();
  meta.schema = rapidjson_stringify(json["schema"]);

  if (json.HasMember("data_update_time")) {
    meta.data_update_time = json["data_update_time"].GetInt64();
  } else {
    meta.data_update_time = 0;
  }
  if (json.HasMember("data_import_config")) {
    meta.data_import_config = rapidjson_stringify(json["data_import_config"]);
  }
  if (json.HasMember("stored_procedures") &&
      json["stored_procedures"].IsArray()) {
    for (auto& plugin : json["stored_procedures"].GetArray()) {
      meta.plugin_metas.push_back(PluginMeta::FromJson(plugin));
    }
  }
  if (json.HasMember("store_type")) {
    meta.store_type = json["store_type"].GetString();
  } else {
    meta.store_type = "mutable_csr";
  }
  return meta;
}

PluginMeta PluginMeta::FromJson(const std::string& json_str) {
  rapidjson::Document json(rapidjson::kObjectType);
  if (json.Parse(json_str.c_str()).HasParseError()) {
    LOG(ERROR) << "Invalid json string: " << json_str;
    return PluginMeta();
  } else {
    return PluginMeta::FromJson(json);
  }
}

PluginMeta PluginMeta::FromJson(const rapidjson::Value& json) {
  PluginMeta meta;
  if (json.HasMember("id")) {
    if (json["id"].IsInt()) {
      meta.id = json["id"].GetInt();
    } else if (json["id"].IsInt64()) {
      meta.id = json["id"].GetInt64();
    } else {
      meta.id = json["id"].GetString();
    }
  }
  if (json.HasMember("name")) {
    meta.name = json["name"].GetString();
    if (meta.id.empty()) {
      meta.id = meta.name;
    }
  }
  if (json.HasMember("bound_graph")) {
    meta.bound_graph = json["bound_graph"].GetString();
  }
  if (json.HasMember("description")) {
    meta.description = json["description"].GetString();
  }
  if (json.HasMember("params")) {
    meta.setParamsFromJsonString((json["params"]));
  }
  if (json.HasMember("returns")) {
    meta.setReturnsFromJsonString(json["returns"]);
  }
  if (json.HasMember("library")) {
    meta.library = json["library"].GetString();
  }
  if (json.HasMember("query")) {
    meta.query = json["query"].GetString();
  }
  if (json.HasMember("type")) {
    meta.type = json["type"].GetString();
  } else {
    meta.type = "cpp";  // default is cpp
  }
  if (json.HasMember("option")) {
    meta.setOptionFromJsonString(rapidjson_stringify(json["option"]));
  }
  if (json.HasMember("creation_time")) {
    meta.creation_time = json["creation_time"].GetInt64();
  }
  if (json.HasMember("update_time")) {
    meta.update_time = json["update_time"].GetInt64();
  }
  if (json.HasMember("enable")) {
    meta.enable = json["enable"].GetBool();
  }
  if (json.HasMember("runnable")) {
    meta.runnable = json["runnable"].GetBool();
  }
  return meta;
}

void PluginMeta::ToJson(rapidjson::Value& json,
                        rapidjson::Document::AllocatorType& allocator) const {
  json.AddMember("id", id, allocator);
  json.AddMember("name", name, allocator);
  json.AddMember("bound_graph", bound_graph, allocator);
  json.AddMember("description", description, allocator);
  rapidjson::Document params_json(rapidjson::kArrayType, &allocator);
  for (auto& param : params) {
    rapidjson::Document tempDoc(rapidjson::kObjectType, &allocator);
    tempDoc.AddMember("name", param.name, allocator);
    tempDoc.AddMember("type", to_json(param.type, &allocator), allocator);
    params_json.PushBack(tempDoc, allocator);
  }
  json.AddMember("params", params_json, allocator);
  rapidjson::Document returns_json(rapidjson::kArrayType, &allocator);
  for (auto& ret : returns) {
    rapidjson::Document tempDoc(rapidjson::kObjectType, &allocator);
    tempDoc.AddMember("name", ret.name, allocator);
    tempDoc.AddMember("type", to_json(ret.type, &allocator), allocator);
    returns_json.PushBack(tempDoc, allocator);
  }
  json.AddMember("returns", returns_json, allocator);

  rapidjson::Document option_json(rapidjson::kObjectType, &allocator);
  for (auto& opt : option) {
    rapidjson::Value key(opt.first.c_str(), allocator);
    rapidjson::Value value(opt.second.c_str(), allocator);
    option_json.AddMember(key, value, allocator);
  }

  json.AddMember("option", option_json, allocator);
  json.AddMember("creation_time", creation_time, allocator);
  json.AddMember("update_time", update_time, allocator);
  json.AddMember("enable", enable, allocator);
  json.AddMember("runnable", runnable, allocator);
  json.AddMember("library", library, allocator);
  json.AddMember("query", query, allocator);
  json.AddMember("type", type, allocator);
}

std::string PluginMeta::ToJson() const {
  rapidjson::Document json(rapidjson::kObjectType);
  ToJson(json, json.GetAllocator());
  return rapidjson_stringify(json);
}

void PluginMeta::setParamsFromJsonString(const rapidjson::Value& document) {
  if (document.IsArray()) {
    for (auto& param : document.GetArray()) {
      Parameter p;
      p.name = param["name"].GetString();
      p.type = from_json(param["type"]);
      params.push_back(p);
    }
  } else {
    LOG(ERROR) << "Invalid params string, expected array: "
               << rapidjson_stringify(document);
  }
}

void PluginMeta::setReturnsFromJsonString(const rapidjson::Value& value) {
  if (value.IsArray()) {
    for (auto& ret : value.GetArray()) {
      Parameter p;
      p.name = ret["name"].GetString();
      p.type = from_json(ret["type"]);
      returns.push_back(p);
    }
  } else {
    LOG(ERROR) << "Invalid returns string, expected array: "
               << rapidjson_stringify(value);
  }
}

void PluginMeta::setOptionFromJsonString(const std::string& json_str) {
  rapidjson::Document document(rapidjson::kObjectType);
  if (document.Parse(json_str.c_str()).HasParseError()) {
    LOG(ERROR) << "Invalid option string: " << json_str;
    return;
  }
  if (document.IsObject()) {
    for (auto& opt : document.GetObject()) {
      option[opt.name.GetString()] = opt.value.GetString();
    }
  } else {
    LOG(ERROR) << "Invalid option string, expected object: " << json_str;
  }
}

std::string JobMeta::ToJson(bool print_log) const {
  rapidjson::Document json(rapidjson::kObjectType);
  json.AddMember("id", id, json.GetAllocator());
  json.AddMember("status", std::to_string(status), json.GetAllocator());
  json.AddMember("start_time", start_time, json.GetAllocator());
  json.AddMember("end_time", end_time, json.GetAllocator());
  if (print_log) {
    json.AddMember("log", read_file_to_string(log_path), json.GetAllocator());
  } else {
    json.AddMember("log_path", log_path, json.GetAllocator());
  }
  json.AddMember("detail", rapidjson::kObjectType, json.GetAllocator());
  json["detail"].AddMember("graph_id", graph_id, json.GetAllocator());
  json["detail"].AddMember("process_id", process_id, json.GetAllocator());
  json.AddMember("type", type, json.GetAllocator());
  return rapidjson_stringify(json);
}

JobMeta JobMeta::FromJson(const std::string& json_str) {
  rapidjson::Document json(rapidjson::kObjectType);
  if (json.Parse(json_str.c_str()).HasParseError()) {
    LOG(ERROR) << "Invalid json string: " << json_str;
    return JobMeta();
  } else {
    return JobMeta::FromJson(json);
  }
}

JobMeta JobMeta::FromJson(const rapidjson::Value& json) {
  JobMeta meta;
  if (json.HasMember("id")) {
    if (json["id"].IsInt64()) {
      meta.id = json["id"].GetInt64();
    } else if (json["id"].IsInt()) {
      meta.id = json["id"].GetInt();
    } else {
      meta.id = json["id"].GetString();
    }
  }
  if (json.HasMember("detail")) {
    const auto& detail = json["detail"];
    if (detail.HasMember("graph_id")) {
      if (detail["graph_id"].IsInt()) {
        meta.graph_id = detail["graph_id"].GetInt();
      } else if (detail["graph_id"].IsInt64()) {
        meta.graph_id = detail["graph_id"].GetInt64();
      } else {
        meta.graph_id = detail["graph_id"].GetString();
      }
    }
    if (detail.HasMember("process_id")) {
      meta.process_id = detail["process_id"].GetInt();
    }
  }

  if (json.HasMember("start_time")) {
    meta.start_time = json["start_time"].GetInt64();
  }
  if (json.HasMember("end_time")) {
    meta.end_time = json["end_time"].GetInt64();
  }
  if (json.HasMember("status")) {
    meta.status = parseFromString(json["status"].GetString());
  }
  if (json.HasMember("log_path")) {
    meta.log_path = json["log_path"].GetString();
    VLOG(10) << "log_path: " << meta.log_path;
  }
  if (json.HasMember("type")) {
    meta.type = json["type"].GetString();
  }
  return meta;
}

gs::Result<YAML::Node> preprocess_vertex_schema(YAML::Node root,
                                                const std::string& type_name) {
  // 1. To support open a empty graph, we should check if the x_csr_params is
  // set for each vertex type, if not set, we set it to a rather small max_vnum,
  // to avoid to much memory usage.
  auto types = root[type_name];
  YAML::Node new_types;
  for (auto type : types) {
    if (!type["x_csr_params"]) {
      type["x_csr_params"]["max_vertex_num"] = 8192;
    }
    new_types.push_back(type);
  }
  root[type_name] = new_types;
  return root;
}

gs::Result<YAML::Node> preprocess_vertex_edge_types(
    YAML::Node root, const std::string& type_name) {
  auto types = root[type_name];
  int32_t cur_type_id = 0;
  YAML::Node new_types;
  for (auto type : types) {
    if (type["type_id"]) {
      auto type_id = type["type_id"].as<int32_t>();
      if (type_id != cur_type_id) {
        return gs::Status(gs::StatusCode::INVALID_SCHEMA,
                          "Invalid " + type_name +
                              " type_id: " + std::to_string(type_id) +
                              ", expect: " + std::to_string(cur_type_id));
      }
    } else {
      type["type_id"] = cur_type_id;
    }
    cur_type_id++;
    int32_t cur_prop_id = 0;
    if (type["properties"]) {
      for (auto prop : type["properties"]) {
        if (prop["property_id"]) {
          auto prop_id = prop["property_id"].as<int32_t>();
          if (prop_id != cur_prop_id) {
            return gs::Status(gs::StatusCode::INVALID_SCHEMA,
                              "Invalid " + type_name + " property_id: " +
                                  type["type_name"].as<std::string>() + " : " +
                                  std::to_string(prop_id) +
                                  ", expect: " + std::to_string(cur_prop_id));
          }
        } else {
          prop["property_id"] = cur_prop_id;
        }
        cur_prop_id++;
      }
    }
    new_types.push_back(type);
  }
  root[type_name] = new_types;
  return root;
}

// Preprocess the schema to be compatible with the current storage.
// 1. check if any property_id or type_id is set for each type, If set, then all
// vertex/edge types should all set.
// 2. If property_id or type_id is not set, then set them according to the order
gs::Result<YAML::Node> preprocess_graph_schema(YAML::Node&& node) {
  if (node["schema"] && node["schema"]["vertex_types"]) {
    // First check whether property_id or type_id is set in the schema
    YAML::Node schema_node = node["schema"];
    ASSIGN_AND_RETURN_IF_RESULT_NOT_OK(
        schema_node, preprocess_vertex_edge_types(schema_node, "vertex_types"));
    ASSIGN_AND_RETURN_IF_RESULT_NOT_OK(
        schema_node, preprocess_vertex_schema(schema_node, "vertex_types"));
    if (node["schema"]["edge_types"]) {
      // edge_type could be optional.
      ASSIGN_AND_RETURN_IF_RESULT_NOT_OK(
          schema_node, preprocess_vertex_edge_types(schema_node, "edge_types"));
    }
    node["schema"] = schema_node;
    return node;
  } else {
    return gs::Status(gs::StatusCode::INVALID_SCHEMA, "Invalid graph schema: ");
  }
}

Result<std::string> preprocess_and_check_schema_json_string(
    const std::string& raw_json_str) {
  YAML::Node yaml;
  try {
    rapidjson::Document doc;
    if (doc.Parse(raw_json_str).HasParseError()) {
      throw std::runtime_error("Fail to parse json: " +
                               std::to_string(doc.GetParseError()));
    }
    std::stringstream json_ss;
    json_ss << raw_json_str;
    yaml = YAML::Load(json_ss);
  } catch (std::exception& e) {
    LOG(ERROR) << "Fail to parse json: " << e.what();
    return gs::Result<std::string>(
        gs::Status(gs::StatusCode::INVALID_SCHEMA,
                   "Fail to parse json: " + std::string(e.what())));
  } catch (...) {
    LOG(ERROR) << "Fail to parse json: " << raw_json_str;
    return gs::Result<std::string>(
        gs::Status(gs::StatusCode::INVALID_SCHEMA, "Fail to parse json: "));
  }
  // preprocess the schema yaml,
  auto res_yaml = preprocess_graph_schema(std::move(yaml));
  if (!res_yaml.ok()) {
    return gs::Result<std::string>(res_yaml.status());
  }
  auto& yaml_value = res_yaml.value();
  // set default value
  if (!yaml_value["store_type"]) {
    yaml_value["store_type"] = "mutable_csr";
  }

  auto parse_schema_res = gs::Schema::LoadFromYamlNode(yaml_value);
  if (!parse_schema_res.ok()) {
    return gs::Result<std::string>(parse_schema_res.status());
  }
  return gs::get_json_string_from_yaml(yaml_value);
}

Result<CreateGraphMetaRequest> CreateGraphMetaRequest::FromJson(
    const std::string& json_str) {
  LOG(INFO) << "CreateGraphMetaRequest::FromJson: " << json_str;

  Result<std::string> real_json_str =
      preprocess_and_check_schema_json_string(json_str);

  CreateGraphMetaRequest request;
  rapidjson::Document json(rapidjson::kObjectType);
  if (json.Parse(real_json_str.value().c_str()).HasParseError()) {
    LOG(ERROR) << "CreateGraphMetaRequest::FromJson error: "
               << real_json_str.value();
    return request;
  }
  if (json.HasMember("version")) {
    request.version = json["version"].GetString();
  } else {
    request.version = "v0.1";
  }
  if (json.HasMember("name")) {
    request.name = json["name"].GetString();
  }
  if (json.HasMember("description")) {
    request.description = json["description"].GetString();
  }
  if (json.HasMember("schema")) {
    request.schema = rapidjson_stringify(json["schema"]);
  }
  if (json.HasMember("data_update_time")) {
    request.data_update_time = json["data_update_time"].GetInt64();
  } else {
    request.data_update_time = 0;
  }
  if (json.HasMember("creation_time")) {
    request.creation_time = json["creation_time"].GetInt64();
  } else {
    request.creation_time = GetCurrentTimeStamp();
  }
  if (json.HasMember("stored_procedures") &&
      json["stored_procedures"].IsArray()) {
    for (auto& plugin : json["stored_procedures"].GetArray()) {
      request.plugin_metas.push_back(PluginMeta::FromJson(plugin));
    }
  }
  // Add builtin plugins
  append_builtin_plugins(request.plugin_metas);
  return request;
}

std::string CreateGraphMetaRequest::ToString() const {
  rapidjson::Document json(rapidjson::kObjectType);
  json.AddMember("version", version, json.GetAllocator());
  json.AddMember("name", name, json.GetAllocator());
  json.AddMember("description", description, json.GetAllocator());
  {
    rapidjson::Document schema_doc(rapidjson::kObjectType,
                                   &json.GetAllocator());
    if (schema_doc.Parse(schema.c_str()).HasParseError()) {
      LOG(ERROR) << "Invalid schema: " << schema;
    } else {
      json.AddMember("schema", schema_doc, json.GetAllocator());
    }
  }
  if (data_update_time.has_value()) {
    json.AddMember("data_update_time", data_update_time.value(),
                   json.GetAllocator());
  } else {
    json.AddMember("data_update_time", 0, json.GetAllocator());
  }
  json.AddMember("creation_time", creation_time, json.GetAllocator());

  rapidjson::Document stored_procedures(rapidjson::kArrayType,
                                        &json.GetAllocator());
  for (auto& plugin_meta : plugin_metas) {
    rapidjson::Document tempDoc(rapidjson::kObjectType, &json.GetAllocator());
    if (tempDoc.Parse(plugin_meta.ToJson().c_str()).HasParseError()) {
      LOG(ERROR) << "Invalid plugin_meta: " << plugin_meta.ToJson();
    } else {
      stored_procedures.PushBack(tempDoc, json.GetAllocator());
    }
  }
  json.AddMember("stored_procedures", stored_procedures, json.GetAllocator());
  return rapidjson_stringify(json);
}

CreatePluginMetaRequest::CreatePluginMetaRequest() : enable(true) {}

std::string CreatePluginMetaRequest::paramsString() const {
  rapidjson::Document json(rapidjson::kArrayType);
  for (auto& param : params) {
    rapidjson::Document param_json(rapidjson::kObjectType,
                                   &json.GetAllocator());
    param_json.AddMember("name", param.name, json.GetAllocator());
    param_json.AddMember("type", to_json(param.type, &json.GetAllocator()),
                         json.GetAllocator());
    json.PushBack(param_json, json.GetAllocator());
  }
  return rapidjson_stringify(json);
}

std::string CreatePluginMetaRequest::returnsString() const {
  rapidjson::Document json(rapidjson::kArrayType);
  for (auto& ret : returns) {
    rapidjson::Document ret_json(rapidjson::kObjectType, &json.GetAllocator());
    ret_json.AddMember("name", ret.name, json.GetAllocator());
    ret_json.AddMember("type", to_json(ret.type, &json.GetAllocator()),
                       json.GetAllocator());
    json.PushBack(ret_json, json.GetAllocator());
  }
  return rapidjson_stringify(json);
}

std::string CreatePluginMetaRequest::optionString() const {
  rapidjson::Document json(rapidjson::kObjectType);
  for (auto& opt : option) {
    json.AddMember(rapidjson::Value(opt.first.c_str(), json.GetAllocator()),
                   rapidjson::Value(opt.second.c_str(), json.GetAllocator()),
                   json.GetAllocator());
  }
  return rapidjson_stringify(json);
}

std::string CreatePluginMetaRequest::ToString() const {
  rapidjson::Document json(rapidjson::kObjectType);
  if (id.has_value()) {
    json.AddMember("id", id.value(), json.GetAllocator());
  }
  json.AddMember("bound_graph", bound_graph, json.GetAllocator());
  json.AddMember("name", name, json.GetAllocator());
  json.AddMember("creation_time", creation_time, json.GetAllocator());
  json.AddMember("description", description, json.GetAllocator());
  json.AddMember("params", rapidjson::kArrayType, json.GetAllocator());
  for (auto& param : params) {
    rapidjson::Document param_json(rapidjson::kObjectType,
                                   &json.GetAllocator());
    param_json.AddMember("name", param.name, json.GetAllocator());
    param_json.AddMember("type", to_json(param.type, &json.GetAllocator()),
                         json.GetAllocator());
    json["params"].PushBack(param_json, json.GetAllocator());
  }
  json.AddMember("returns", rapidjson::kArrayType, json.GetAllocator());
  for (auto& ret : returns) {
    rapidjson::Document ret_json(rapidjson::kObjectType, &json.GetAllocator());
    ret_json.AddMember("name", ret.name, json.GetAllocator());
    ret_json.AddMember("type", to_json(ret.type, &json.GetAllocator()),
                       json.GetAllocator());
    json["returns"].PushBack(ret_json, json.GetAllocator());
  }
  json.AddMember("library", library, json.GetAllocator());
  json.AddMember("option", rapidjson::kObjectType, json.GetAllocator());
  for (auto& opt : option) {
    json["option"].AddMember(
        rapidjson::Value(opt.first.c_str(), json.GetAllocator()),
        rapidjson::Value(opt.second.c_str(), json.GetAllocator()),
        json.GetAllocator());
  }
  json.AddMember("query", query, json.GetAllocator());
  json.AddMember("type", type, json.GetAllocator());
  json.AddMember("enable", enable, json.GetAllocator());
  return rapidjson_stringify(json);
}

CreatePluginMetaRequest CreatePluginMetaRequest::FromJson(
    const std::string& json) {
  rapidjson::Document document(rapidjson::kObjectType);
  if (document.Parse(json.c_str()).HasParseError()) {
    LOG(ERROR) << "CreatePluginMetaRequest::FromJson error: " << json;
    return CreatePluginMetaRequest();
  }
  return CreatePluginMetaRequest::FromJson(document);
}

CreatePluginMetaRequest CreatePluginMetaRequest::FromJson(
    const rapidjson::Value& j) {
  // TODO: make sure this is correct
  CreatePluginMetaRequest request;
  if (j.HasMember("id")) {
    if (j["id"].IsInt()) {
      request.id = std::to_string(j["id"].GetInt());
    } else if (j["id"].IsInt64()) {
      request.id = std::to_string(j["id"].GetInt64());
    } else {
      request.id = j["id"].GetString();
    }
  }
  if (j.HasMember("name")) {
    request.name = j["name"].GetString();
  }
  if (j.HasMember("bound_graph")) {
    if (j["bound_graph"].IsInt()) {
      request.bound_graph = j["bound_graph"].GetInt();
    } else if (j["bound_graph"].IsInt64()) {
      request.bound_graph = j["bound_graph"].GetInt64();
    } else {
      request.bound_graph = j["bound_graph"].GetString();
    }
  }
  if (j.HasMember("creation_time")) {
    request.creation_time = j["creation_time"].GetInt64();
  } else {
    request.creation_time = GetCurrentTimeStamp();
  }
  if (j.HasMember("description")) {
    request.description = j["description"].GetString();
  }
  if (j.HasMember("params")) {
    for (auto& param : j["params"].GetArray()) {
      Parameter p;
      p.name = param["name"].GetString();
      from_json(param["type"], p.type);
      request.params.push_back(p);
    }
  }
  if (j.HasMember("returns")) {
    for (auto& ret : j["returns"].GetArray()) {
      Parameter p;
      p.name = ret["name"].GetString();
      from_json(ret["type"], p.type);
      request.returns.push_back(p);
    }
  }
  if (j.HasMember("library")) {
    request.library = j["library"].GetString();
  }
  if (j.HasMember("option")) {
    for (auto& opt : j["option"].GetObject()) {
      request.option[opt.name.GetString()] = opt.value.GetString();
    }
  }
  if (j.HasMember("query")) {
    request.query = j["query"].GetString();
  }
  if (j.HasMember("type")) {
    request.type = j["type"].GetString();
  }
  if (j.HasMember("enable")) {
    request.enable = j["enable"].GetBool();
  }
  return request;
}

UpdatePluginMetaRequest::UpdatePluginMetaRequest() : enable(true) {}

UpdatePluginMetaRequest UpdatePluginMetaRequest::FromJson(
    const std::string& json) {
  UpdatePluginMetaRequest request;
  rapidjson::Document j(rapidjson::kObjectType);
  if (j.Parse(json.c_str()).HasParseError()) {
    LOG(ERROR) << "UpdatePluginMetaRequest::FromJson error: " << json;
    return request;
  }
  if (j.HasMember("name")) {
    if (j["name"].IsInt()) {
      request.name = std::to_string(j["name"].GetInt());
    } else if (j["name"].IsInt64()) {
      request.name = std::to_string(j["name"].GetInt64());
    } else {
      request.name = j["name"].GetString();
    }
  }
  if (j.HasMember("description")) {
    request.description = j["description"].GetString();
  }
  if (j.HasMember("update_time")) {
    request.update_time = j["update_time"].GetInt64();
  } else {
    request.update_time = GetCurrentTimeStamp();
  }
  if (j.HasMember("params") && j["params"].IsArray()) {
    request.params = std::vector<Parameter>();
    for (auto& param : j["params"].GetArray()) {
      Parameter p;
      p.name = param["name"].GetString();
      p.type = from_json(param["type"]);
      request.params->emplace_back(std::move(p));
    }
  }
  if (j.HasMember("returns") && j["returns"].IsArray()) {
    request.returns = std::vector<Parameter>();
    for (auto& ret : j["returns"].GetArray()) {
      Parameter p;
      p.name = ret["name"].GetString();
      p.type = from_json(ret["type"]);
      request.returns->emplace_back(std::move(p));
    }
  }
  if (j.HasMember("library")) {
    request.library = j["library"].GetString();
  }
  if (j.HasMember("option")) {
    request.option = std::unordered_map<std::string, std::string>();
    for (auto& opt : j["option"].GetObject()) {
      request.option->insert({opt.name.GetString(), opt.value.GetString()});
    }
  }
  if (j.HasMember("enable")) {
    request.enable = j["enable"].GetBool();
  }
  return request;
}

std::string UpdatePluginMetaRequest::paramsString() const {
  rapidjson::Document json(rapidjson::kArrayType);
  if (params.has_value()) {
    for (auto& param : params.value()) {
      rapidjson::Document param_json(rapidjson::kObjectType,
                                     &json.GetAllocator());
      param_json.AddMember("name", param.name, json.GetAllocator());
      param_json.AddMember("type", to_json(param.type, &json.GetAllocator()),
                           json.GetAllocator());
      json.PushBack(param_json, json.GetAllocator());
    }
  }
  return rapidjson_stringify(json);
}

std::string UpdatePluginMetaRequest::returnsString() const {
  rapidjson::Document json(rapidjson::kArrayType);
  if (returns.has_value()) {
    for (auto& ret : returns.value()) {
      rapidjson::Document ret_json(rapidjson::kObjectType,
                                   &json.GetAllocator());
      ret_json.AddMember("name", ret.name, json.GetAllocator());
      ret_json.AddMember("type", to_json(ret.type, &json.GetAllocator()),
                         json.GetAllocator());
      json.PushBack(ret_json, json.GetAllocator());
    }
  }
  return rapidjson_stringify(json);
}

std::string UpdatePluginMetaRequest::optionString() const {
  rapidjson::Document json(rapidjson::kObjectType);
  if (option.has_value()) {
    for (auto& opt : option.value()) {
      json.AddMember(rapidjson::Value(opt.first.c_str(), json.GetAllocator()),
                     rapidjson::Value(opt.second.c_str(), json.GetAllocator()),
                     json.GetAllocator());
    }
  }
  return rapidjson_stringify(json);
}

std::string UpdatePluginMetaRequest::ToString() const {
  rapidjson::Document json(rapidjson::kObjectType);
  if (name.has_value()) {
    json.AddMember("name", name.value(), json.GetAllocator());
  }
  if (bound_graph.has_value()) {
    json.AddMember("bound_graph", bound_graph.value(), json.GetAllocator());
  }
  if (description.has_value()) {
    json.AddMember("description", description.value(), json.GetAllocator());
  }
  if (update_time.has_value()) {
    json.AddMember("update_time", update_time.value(), json.GetAllocator());
  }
  // create array of json objects
  rapidjson::Document params_json(rapidjson::kArrayType, &json.GetAllocator());
  if (params.has_value()) {
    for (auto& param : params.value()) {
      rapidjson::Document param_json(rapidjson::kObjectType,
                                     &json.GetAllocator());
      param_json.AddMember("name", param.name, json.GetAllocator());
      param_json.AddMember("type", to_json(param.type, &json.GetAllocator()),
                           json.GetAllocator());
      params_json.PushBack(param_json, json.GetAllocator());
    }
  }
  json.AddMember("params", params_json, json.GetAllocator());
  rapidjson::Document returns_json(rapidjson::kArrayType, &json.GetAllocator());
  if (returns.has_value()) {
    for (auto& ret : returns.value()) {
      rapidjson::Document ret_json(rapidjson::kObjectType,
                                   &json.GetAllocator());
      ret_json.AddMember("name", ret.name, json.GetAllocator());
      ret_json.AddMember("type", to_json(ret.type, &json.GetAllocator()),
                         json.GetAllocator());
      returns_json.PushBack(ret_json, json.GetAllocator());
    }
  }
  json.AddMember("returns", returns_json, json.GetAllocator());
  if (library.has_value()) {
    json.AddMember("library", library.value(), json.GetAllocator());
  }
  rapidjson::Document option_json(rapidjson::kObjectType, &json.GetAllocator());
  if (option.has_value()) {
    for (auto& opt : option.value()) {
      option_json.AddMember(
          rapidjson::Value(opt.first.c_str(), json.GetAllocator()),
          rapidjson::Value(opt.second.c_str(), json.GetAllocator()),
          json.GetAllocator());
    }
  }
  json.AddMember("option", option_json, json.GetAllocator());
  if (enable.has_value()) {
    json.AddMember("enable", enable.value(), json.GetAllocator());
  }
  auto dumped = rapidjson_stringify(json);
  VLOG(10) << "dump: " << dumped;
  return dumped;
}

CreateJobMetaRequest CreateJobMetaRequest::NewRunning(
    const GraphId& graph_id, int32_t process_id, const std::string& log_path,
    const std::string& type) {
  CreateJobMetaRequest request;
  request.graph_id = graph_id;
  request.process_id = process_id;
  request.start_time = GetCurrentTimeStamp();
  request.status = JobStatus::kRunning;
  request.log_path = log_path;
  request.type = type;
  return request;
}

std::string CreateJobMetaRequest::ToString() const {
  rapidjson::Document json(rapidjson::kObjectType);
  json.AddMember("detail", rapidjson::kObjectType, json.GetAllocator());
  json["detail"].AddMember("graph_id", graph_id, json.GetAllocator());
  json["detail"].AddMember("process_id", process_id, json.GetAllocator());
  json.AddMember("start_time", start_time, json.GetAllocator());
  json.AddMember("status", std::to_string(status), json.GetAllocator());
  json.AddMember("log_path", log_path, json.GetAllocator());
  json.AddMember("type", type, json.GetAllocator());
  return rapidjson_stringify(json);
}

UpdateJobMetaRequest UpdateJobMetaRequest::NewCancel() {
  UpdateJobMetaRequest request;
  request.status = JobStatus::kCancelled;
  request.end_time = GetCurrentTimeStamp();
  return request;
}

UpdateJobMetaRequest UpdateJobMetaRequest::NewFinished(int rc) {
  UpdateJobMetaRequest request;
  if (rc == 0) {
    request.status = JobStatus::kSuccess;
  } else {
    request.status = JobStatus::kFailed;
  }
  request.end_time = GetCurrentTimeStamp();
  return request;
}

JobStatus parseFromString(const std::string& status_string) {
  if (status_string == "RUNNING") {
    return JobStatus::kRunning;
  } else if (status_string == "SUCCESS") {
    return JobStatus::kSuccess;
  } else if (status_string == "FAILED") {
    return JobStatus::kFailed;
  } else if (status_string == "CANCELLED") {
    return JobStatus::kCancelled;
  } else {
    LOG(ERROR) << "Unknown job status: " << status_string;
    return JobStatus::kUnknown;
  }
}

std::string GraphStatistics::ToJson() const {
  rapidjson::Document json(rapidjson::kObjectType);
  json.AddMember("total_vertex_count", total_vertex_count, json.GetAllocator());
  json.AddMember("total_edge_count", total_edge_count, json.GetAllocator());
  json.AddMember("vertex_type_statistics", rapidjson::kArrayType,
                 json.GetAllocator());
  for (auto& type_stat : vertex_type_statistics) {
    rapidjson::Document type_stat_json(rapidjson::kObjectType,
                                       &json.GetAllocator());
    type_stat_json.AddMember("type_id", std::get<0>(type_stat),
                             json.GetAllocator());
    type_stat_json.AddMember("type_name", std::get<1>(type_stat),
                             json.GetAllocator());
    type_stat_json.AddMember("count", std::get<2>(type_stat),
                             json.GetAllocator());
    json["vertex_type_statistics"].PushBack(type_stat_json,
                                            json.GetAllocator());
  }
  json.AddMember("edge_type_statistics", rapidjson::kArrayType,
                 json.GetAllocator());
  for (auto& type_stat : edge_type_statistics) {
    rapidjson::Document type_stat_json(rapidjson::kObjectType,
                                       &json.GetAllocator());
    type_stat_json.AddMember("type_id", std::get<0>(type_stat),
                             json.GetAllocator());
    type_stat_json.AddMember("type_name", std::get<1>(type_stat),
                             json.GetAllocator());
    type_stat_json.AddMember("vertex_type_pair_statistics",
                             rapidjson::kArrayType, json.GetAllocator());
    for (auto& pair_stat : std::get<2>(type_stat)) {
      rapidjson::Document pair_stat_json(rapidjson::kObjectType,
                                         &json.GetAllocator());
      pair_stat_json.AddMember("source_vertex", std::get<0>(pair_stat),
                               json.GetAllocator());
      pair_stat_json.AddMember("destination_vertex", std::get<1>(pair_stat),
                               json.GetAllocator());
      pair_stat_json.AddMember("count", std::get<2>(pair_stat),
                               json.GetAllocator());
      type_stat_json["vertex_type_pair_statistics"].PushBack(
          pair_stat_json, json.GetAllocator());
    }
    json["edge_type_statistics"].PushBack(type_stat_json, json.GetAllocator());
  }
  return rapidjson_stringify(json);
}

Result<GraphStatistics> GraphStatistics::FromJson(const std::string& json_str) {
  rapidjson::Document json(rapidjson::kObjectType);
  if (json.Parse(json_str.c_str()).HasParseError()) {
    LOG(ERROR) << "Invalid json string: " << json_str;
    return Result<GraphStatistics>(Status(
        StatusCode::INTERNAL_ERROR,
        "Invalid json string when parsing graph statistics : " + json_str));
  }
  return GraphStatistics::FromJson(json);
}

Result<GraphStatistics> GraphStatistics::FromJson(
    const rapidjson::Value& json) {
  GraphStatistics stat;
  stat.total_vertex_count = json["total_vertex_count"].GetInt64();
  stat.total_edge_count = json["total_edge_count"].GetInt64();
  for (auto& type_stat : json["vertex_type_statistics"].GetArray()) {
    stat.vertex_type_statistics.push_back({type_stat["type_id"].GetInt(),
                                           type_stat["type_name"].GetString(),
                                           type_stat["count"].GetInt64()});
  }
  for (auto& type_stat : json["edge_type_statistics"].GetArray()) {
    std::vector<typename GraphStatistics::vertex_type_pair_statistic>
        vertex_type_pair_statistics;
    for (auto& pair : type_stat["vertex_type_pair_statistics"].GetArray()) {
      vertex_type_pair_statistics.push_back(
          {pair["source_vertex"].GetString(),
           pair["destination_vertex"].GetString(), pair["count"].GetInt64()});
    }
    stat.edge_type_statistics.push_back({type_stat["type_id"].GetInt(),
                                         type_stat["type_name"].GetString(),
                                         vertex_type_pair_statistics});
  }
  return stat;
}

}  // namespace gs

namespace std {
std::string to_string(const gs::JobStatus& status) {
  switch (status) {
  case gs::JobStatus::kRunning:
    return "RUNNING";
  case gs::JobStatus::kSuccess:
    return "SUCCESS";
  case gs::JobStatus::kFailed:
    return "FAILED";
  case gs::JobStatus::kCancelled:
    return "CANCELLED";
  case gs::JobStatus::kUnknown:
    return "UNKNOWN";
  }
  return "UNKNOWN";
}

std::ostream& operator<<(std::ostream& os, const gs::JobStatus& status) {
  os << to_string(status);
  return os;
}
}  // namespace std

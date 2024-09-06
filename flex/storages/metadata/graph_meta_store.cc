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

UpdateGraphMetaRequest::UpdateGraphMetaRequest(
    int64_t data_update_time, const std::string& data_import_config)
    : data_update_time(data_update_time),
      data_import_config(data_import_config) {}

std::string Parameter::ToJson() const {
  rapidjson::Document json(rapidjson::kObjectType);
  json.AddMember("name", name, json.GetAllocator());
  json.AddMember("type", config_parsing::PrimitivePropertyTypeToString(type),
                 json.GetAllocator());
  return rapidjson_stringify(json);
}

void GraphMeta::ToJson(rapidjson::Value& json,
                       rapidjson::Document::AllocatorType& allocator) const {
  json.AddMember("id", id, allocator);
  json.AddMember("name", name, allocator);
  json.AddMember("description", description, allocator);
  json.AddMember("creation_time", creation_time, allocator);
  json.AddMember("data_update_time", data_update_time, allocator);
  if (!data_import_config.empty()) {
    rapidjson::Document tempDoc;
    if (tempDoc.Parse(data_import_config.c_str()).HasParseError()) {
      LOG(ERROR) << "Invalid data_import_config: " << data_import_config;
    } else {
      json.AddMember("data_import_config", tempDoc, allocator);
    }
  }
  {
    rapidjson::Document tempDoc;
    if (tempDoc.Parse(schema.c_str()).HasParseError()) {
      LOG(ERROR) << "Invalid schema: " << schema;
    } else {
      json.AddMember("schema", tempDoc, allocator);
    }
  }
  rapidjson::Value stored_procedures(rapidjson::kArrayType);
  for (auto& plugin_meta : plugin_metas) {
    rapidjson::Document tempDoc(rapidjson::kObjectType);
    plugin_meta.ToJson(tempDoc, allocator);
    stored_procedures.PushBack(tempDoc, allocator);
  }
  json.AddMember("stored_procedures", stored_procedures, allocator);
  return;
}

std::string GraphMeta::ToJson() const {
  rapidjson::Document json(rapidjson::kObjectType);
  json.AddMember("id", id, json.GetAllocator());
  json.AddMember("name", name, json.GetAllocator());
  json.AddMember("description", description, json.GetAllocator());
  json.AddMember("creation_time", creation_time, json.GetAllocator());
  json.AddMember("data_update_time", data_update_time, json.GetAllocator());
  if (!data_import_config.empty()) {
    rapidjson::Document tempDoc;
    if (tempDoc.Parse(data_import_config.c_str()).HasParseError()) {
      LOG(ERROR) << "Invalid data_import_config: " << data_import_config;
    } else {
      json.AddMember("data_import_config", tempDoc, json.GetAllocator());
    }
  }
  {
    rapidjson::Document tempDoc;
    if (tempDoc.Parse(schema.c_str()).HasParseError()) {
      LOG(ERROR) << "Invalid schema: " << schema;
    } else {
      json.AddMember("schema", tempDoc, json.GetAllocator());
    }
  }
  rapidjson::Value stored_procedures(rapidjson::kArrayType);
  for (auto& plugin_meta : plugin_metas) {
    rapidjson::Document tempDoc(rapidjson::kObjectType);
    plugin_meta.ToJson(tempDoc, json.GetAllocator());
    stored_procedures.PushBack(tempDoc, json.GetAllocator());
  }
  json.AddMember("store_type", store_type, json.GetAllocator());
  return rapidjson_stringify(json);
}

GraphMeta GraphMeta::FromJson(const std::string& json_str) {
  rapidjson::Document json;
  if (json.Parse(json_str.c_str()).HasParseError()) {
    LOG(ERROR) << "Invalid json string: " << json_str;
    return GraphMeta();
  } else {
    return GraphMeta::FromJson(json);
  }
}

GraphMeta GraphMeta::FromJson(const rapidjson::Value& json) {
  GraphMeta meta;
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
  if (json.HasMember("stored_procedures")) {
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
  rapidjson::Document json;
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
    meta.setParamsFromJsonString(rapidjson_stringify(json["params"]));
  }
  if (json.HasMember("returns")) {
    meta.setReturnsFromJsonString(rapidjson_stringify(json["returns"]));
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
  json.AddMember("params", rapidjson::kArrayType, allocator);
  for (auto& param : params) {
    rapidjson::Document tempDoc(rapidjson::kObjectType);
    tempDoc.AddMember("name", param.name, allocator);
    tempDoc.AddMember("type",
                      config_parsing::PrimitivePropertyTypeToString(param.type),
                      allocator);
    json["params"].PushBack(tempDoc, allocator);
  }
  json.AddMember("returns", rapidjson::kArrayType, allocator);
  for (auto& ret : returns) {
    rapidjson::Document tempDoc(rapidjson::kObjectType);
    tempDoc.AddMember("name", ret.name, allocator);
    tempDoc.AddMember("type",
                      config_parsing::PrimitivePropertyTypeToString(ret.type),
                      allocator);
    json["returns"].PushBack(tempDoc, allocator);
  }
  json.AddMember("option", rapidjson::kObjectType, allocator);
  for (auto& opt : option) {
    rapidjson::Value key(opt.first.c_str(), allocator);
    rapidjson::Value value(opt.second.c_str(), allocator);
    json["option"].AddMember(key, value, allocator);
  }
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
  json.AddMember("id", id, json.GetAllocator());
  json.AddMember("name", name, json.GetAllocator());
  json.AddMember("bound_graph", bound_graph, json.GetAllocator());
  json.AddMember("description", description, json.GetAllocator());
  json.AddMember("params", rapidjson::kArrayType, json.GetAllocator());
  for (auto& param : params) {
    rapidjson::Document tempDoc(rapidjson::kObjectType);
    tempDoc.AddMember("name", param.name, json.GetAllocator());
    tempDoc.AddMember("type",
                      config_parsing::PrimitivePropertyTypeToString(param.type),
                      json.GetAllocator());
    json["params"].PushBack(tempDoc, json.GetAllocator());
  }
  json.AddMember("returns", rapidjson::kArrayType, json.GetAllocator());
  for (auto& ret : returns) {
    rapidjson::Document tempDoc(rapidjson::kObjectType);
    tempDoc.AddMember("name", ret.name, json.GetAllocator());
    tempDoc.AddMember("type",
                      config_parsing::PrimitivePropertyTypeToString(ret.type),
                      json.GetAllocator());
    json["returns"].PushBack(tempDoc, json.GetAllocator());
  }
  json.AddMember("option", rapidjson::kObjectType, json.GetAllocator());
  for (auto& opt : option) {
    rapidjson::Value key(opt.first.c_str(), json.GetAllocator());
    rapidjson::Value value(opt.second.c_str(), json.GetAllocator());
    json["option"].AddMember(key, value, json.GetAllocator());
  }
  json.AddMember("creation_time", creation_time, json.GetAllocator());
  json.AddMember("update_time", update_time, json.GetAllocator());
  json.AddMember("enable", enable, json.GetAllocator());
  json.AddMember("runnable", runnable, json.GetAllocator());
  json.AddMember("library", library, json.GetAllocator());
  json.AddMember("query", query, json.GetAllocator());
  json.AddMember("type", type, json.GetAllocator());
  return rapidjson_stringify(json);
}

void PluginMeta::setParamsFromJsonString(const std::string& json_str) {
  if (json_str.empty() || json_str == "[]" || json_str == "{}" ||
      json_str == "nu") {
    return;
  }
  rapidjson::Document document;
  if (document.Parse(json_str.c_str()).HasParseError()) {
    LOG(ERROR) << "Invalid params string: " << json_str;
    return;
  }
  if (document.IsArray()) {
    for (auto& param : document.GetArray()) {
      Parameter p;
      p.name = param["name"].GetString();
      p.type = config_parsing::StringToPrimitivePropertyType(
          param["type"].GetString());
      params.push_back(p);
    }
  } else {
    LOG(ERROR) << "Invalid params string, expected array: " << json_str;
  }
}

void PluginMeta::setReturnsFromJsonString(const std::string& json_str) {
  rapidjson::Document document;
  if (document.Parse(json_str.c_str()).HasParseError()) {
    LOG(ERROR) << "Invalid returns string: " << json_str;
    return;
  }
  if (document.IsArray()) {
    for (auto& ret : document.GetArray()) {
      Parameter p;
      p.name = ret["name"].GetString();
      from_json(ret["type"], p.type);
      returns.push_back(p);
    }
  } else {
    LOG(ERROR) << "Invalid returns string, expected array: " << json_str;
  }
}

void PluginMeta::setOptionFromJsonString(const std::string& json_str) {
  rapidjson::Document document;
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
  rapidjson::Document json;
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

CreateGraphMetaRequest CreateGraphMetaRequest::FromJson(
    const std::string& json_str) {
  LOG(INFO) << "CreateGraphMetaRequest::FromJson: " << json_str;
  CreateGraphMetaRequest request;
  rapidjson::Document json;
  if (json.Parse(json_str.c_str()).HasParseError()) {
    LOG(ERROR) << "CreateGraphMetaRequest::FromJson error: " << json_str;
    return request;
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
  if (json.HasMember("stored_procedures")) {
    for (auto& plugin : json["stored_procedures"].GetArray()) {
      request.plugin_metas.push_back(PluginMeta::FromJson(plugin));
    }
  }
  return request;
}

std::string CreateGraphMetaRequest::ToString() const {
  rapidjson::Document json(rapidjson::kObjectType);
  json.AddMember("name", name, json.GetAllocator());
  json.AddMember("description", description, json.GetAllocator());
  {
    rapidjson::Document schema_doc;
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

  json.AddMember("stored_procedures", rapidjson::kArrayType,
                 json.GetAllocator());
  for (auto& plugin_meta : plugin_metas) {
    rapidjson::Document tempDoc;
    if (tempDoc.Parse(plugin_meta.ToJson().c_str()).HasParseError()) {
      LOG(ERROR) << "Invalid plugin_meta: " << plugin_meta.ToJson();
    } else {
      json["stored_procedures"].PushBack(tempDoc, json.GetAllocator());
    }
  }
  return rapidjson_stringify(json);
}

CreatePluginMetaRequest::CreatePluginMetaRequest() : enable(true) {}

std::string CreatePluginMetaRequest::paramsString() const {
  rapidjson::Document json(rapidjson::kArrayType);
  for (auto& param : params) {
    rapidjson::Document param_json(rapidjson::kObjectType);
    param_json.AddMember("name", param.name, json.GetAllocator());
    param_json.AddMember(
        "type", config_parsing::PrimitivePropertyTypeToString(param.type),
        json.GetAllocator());
    json.PushBack(param_json, json.GetAllocator());
  }
  return rapidjson_stringify(json);
}

std::string CreatePluginMetaRequest::returnsString() const {
  rapidjson::Document json(rapidjson::kArrayType);
  for (auto& ret : returns) {
    rapidjson::Document ret_json(rapidjson::kObjectType);
    ret_json.AddMember("name", ret.name, json.GetAllocator());
    ret_json.AddMember("type",
                       config_parsing::PrimitivePropertyTypeToString(ret.type),
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
    rapidjson::Document param_json(rapidjson::kObjectType);
    param_json.AddMember("name", param.name, json.GetAllocator());
    param_json.AddMember(
        "type", config_parsing::PrimitivePropertyTypeToString(param.type),
        json.GetAllocator());
    json["params"].PushBack(param_json, json.GetAllocator());
  }
  json.AddMember("returns", rapidjson::kArrayType, json.GetAllocator());
  for (auto& ret : returns) {
    rapidjson::Document ret_json(rapidjson::kObjectType);
    ret_json.AddMember("name", ret.name, json.GetAllocator());
    ret_json.AddMember("type",
                       config_parsing::PrimitivePropertyTypeToString(ret.type),
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
  rapidjson::Document document;
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
      p.type = config_parsing::StringToPrimitivePropertyType(
          param["type"].GetString());
      request.params.push_back(p);
    }
  }
  if (j.HasMember("returns")) {
    for (auto& ret : j["returns"].GetArray()) {
      Parameter p;
      p.name = ret["name"].GetString();
      p.type = config_parsing::StringToPrimitivePropertyType(
          ret["type"].GetString());
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
  rapidjson::Document j;
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
      p.type = config_parsing::StringToPrimitivePropertyType(
          param["type"].GetString());

      request.params->emplace_back(std::move(p));
    }
  }
  if (j.HasMember("returns") && j["returns"].IsArray()) {
    request.returns = std::vector<Parameter>();
    for (auto& ret : j["returns"].GetArray()) {
      Parameter p;
      p.name = ret["name"].GetString();
      p.type = config_parsing::StringToPrimitivePropertyType(
          ret["type"].GetString());
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
  // } catch (const std::exception& e) {
  //   LOG(ERROR) << "UpdatePluginMetaRequest::FromJson error: " << e.what() <<
  //   " "
  //              << json;
  // }
  return request;
}

std::string UpdatePluginMetaRequest::paramsString() const {
  rapidjson::Document json(rapidjson::kArrayType);
  if (params.has_value()) {
    for (auto& param : params.value()) {
      rapidjson::Document param_json(rapidjson::kObjectType);
      param_json.AddMember("name", param.name, json.GetAllocator());
      param_json.AddMember(
          "type", config_parsing::PrimitivePropertyTypeToString(param.type),
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
      rapidjson::Document ret_json(rapidjson::kObjectType);
      ret_json.AddMember("name", ret.name, json.GetAllocator());
      ret_json.AddMember(
          "type", config_parsing::PrimitivePropertyTypeToString(ret.type),
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
  json.AddMember("params", rapidjson::kArrayType, json.GetAllocator());
  if (params.has_value()) {
    for (auto& param : params.value()) {
      rapidjson::Document param_json(rapidjson::kObjectType);
      param_json.AddMember("name", param.name, json.GetAllocator());
      param_json.AddMember(
          "type", config_parsing::PrimitivePropertyTypeToString(param.type),
          json.GetAllocator());
      json["params"].PushBack(param_json, json.GetAllocator());
    }
  }
  json.AddMember("returns", rapidjson::kArrayType, json.GetAllocator());
  if (returns.has_value()) {
    for (auto& ret : returns.value()) {
      rapidjson::Document ret_json(rapidjson::kObjectType);
      ret_json.AddMember("name", ret.name, json.GetAllocator());
      ret_json.AddMember(
          "type", config_parsing::PrimitivePropertyTypeToString(ret.type),
          json.GetAllocator());
      json["returns"].PushBack(ret_json, json.GetAllocator());
    }
  }
  if (library.has_value()) {
    json.AddMember("library", library.value(), json.GetAllocator());
  }
  json.AddMember("option", rapidjson::kObjectType, json.GetAllocator());
  if (option.has_value()) {
    for (auto& opt : option.value()) {
      json["option"].AddMember(
          rapidjson::Value(opt.first.c_str(), json.GetAllocator()),
          rapidjson::Value(opt.second.c_str(), json.GetAllocator()),
          json.GetAllocator());
    }
  }
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
    rapidjson::Document type_stat_json(rapidjson::kObjectType);
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
    rapidjson::Document type_stat_json(rapidjson::kObjectType);
    type_stat_json.AddMember("type_id", std::get<0>(type_stat),
                             json.GetAllocator());
    type_stat_json.AddMember("type_name", std::get<1>(type_stat),
                             json.GetAllocator());
    type_stat_json.AddMember("vertex_type_pair_statistics",
                             rapidjson::kArrayType, json.GetAllocator());
    for (auto& pair_stat : std::get<2>(type_stat)) {
      rapidjson::Document pair_stat_json(rapidjson::kObjectType);
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
  rapidjson::Document json;
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

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
  nlohmann::json json;
  json["name"] = name;
  json["type"] = type;  // calls to_json implicitly
  return json.dump();
}

std::string GraphMeta::ToJson() const {
  nlohmann::json json;
  json["id"] = id;
  json["name"] = name;
  json["description"] = description;
  json["creation_time"] = creation_time;
  json["data_update_time"] = data_update_time;
  if (!data_import_config.empty()) {
    try {
      json["data_import_config"] = nlohmann::json::parse(data_import_config);
    } catch (const std::exception& e) {
      LOG(ERROR) << "Invalid data_import_config: " << data_import_config;
    }
  }
  json["schema"] = nlohmann::json::parse(schema);
  json["stored_procedures"] = nlohmann::json::array();
  for (auto& plugin_meta : plugin_metas) {
    json["stored_procedures"].push_back(
        nlohmann::json::parse(plugin_meta.ToJson()));
  }
  json["store_type"] = store_type;
  return json.dump();
}

GraphMeta GraphMeta::FromJson(const std::string& json_str) {
  auto j = nlohmann::json::parse(json_str);
  return GraphMeta::FromJson(j);
}

GraphMeta GraphMeta::FromJson(const nlohmann::json& json) {
  GraphMeta meta;
  if (json.contains("id")) {
    if (json["id"].is_number()) {
      meta.id = json["id"].get<int64_t>();
    } else {
      meta.id = json["id"].get<GraphId>();
    }
  }

  meta.name = json["name"].get<std::string>();
  meta.description = json["description"].get<std::string>();
  meta.creation_time = json["creation_time"].get<int64_t>();
  meta.schema = json["schema"].dump();

  if (json.contains("data_update_time")) {
    meta.data_update_time = json["data_update_time"].get<int64_t>();
  } else {
    meta.data_update_time = 0;
  }
  if (json.contains("data_import_config")) {
    meta.data_import_config = json["data_import_config"].dump();
  }
  if (json.contains("stored_procedures")) {
    for (auto& plugin : json["stored_procedures"]) {
      meta.plugin_metas.push_back(PluginMeta::FromJson(plugin));
    }
  }
  if (json.contains("store_type")) {
    meta.store_type = json["store_type"].get<std::string>();
  } else {
    meta.store_type = "mutable_csr";
  }
  return meta;
}

PluginMeta PluginMeta::FromJson(const std::string& json_str) {
  auto j = nlohmann::json::parse(json_str);
  return PluginMeta::FromJson(j);
}

PluginMeta PluginMeta::FromJson(const nlohmann::json& json) {
  PluginMeta meta;
  if (json.contains("id")) {
    if (json["id"].is_number()) {
      meta.id = json["id"].get<int64_t>();
    } else {
      meta.id = json["id"].get<PluginId>();
    }
  }
  if (json.contains("name")) {
    meta.name = json["name"].get<std::string>();
    if (meta.id.empty()) {
      meta.id = meta.name;
    }
  }
  if (json.contains("bound_graph")) {
    meta.bound_graph = json["bound_graph"].get<GraphId>();
  }
  if (json.contains("description")) {
    meta.description = json["description"].get<std::string>();
  }
  if (json.contains("params")) {
    meta.setParamsFromJsonString(json["params"].dump());
  }
  if (json.contains("returns")) {
    meta.setReturnsFromJsonString(json["returns"].dump());
  }
  if (json.contains("library")) {
    meta.library = json["library"].get<std::string>();
  }
  if (json.contains("query")) {
    meta.query = json["query"].get<std::string>();
  }
  if (json.contains("type")) {
    meta.type = json["type"].get<std::string>();
  } else {
    meta.type = "cpp";  // default is cpp
  }
  if (json.contains("option")) {
    meta.setOptionFromJsonString(json["option"].dump());
  }
  if (json.contains("creation_time")) {
    meta.creation_time = json["creation_time"].get<int64_t>();
  }
  if (json.contains("update_time")) {
    meta.update_time = json["update_time"].get<int64_t>();
  }
  if (json.contains("enable")) {
    meta.enable = json["enable"].get<bool>();
  }
  if (json.contains("runnable")) {
    meta.runnable = json["runnable"].get<bool>();
  }
  return meta;
}

std::string PluginMeta::ToJson() const {
  nlohmann::json json;
  json["id"] = id;
  json["name"] = name;
  json["bound_graph"] = bound_graph;
  json["description"] = description;
  json["params"] = nlohmann::json::array();
  for (auto& param : params) {
    nlohmann::json p;
    p["name"] = param.name;
    p["type"] = param.type;
    json["params"].push_back(p);
  }
  json["returns"] = nlohmann::json::array();
  for (auto& ret : returns) {
    nlohmann::json r;
    r["name"] = ret.name;
    r["type"] = ret.type;
    json["returns"].push_back(r);
  }
  for (auto& opt : option) {
    json["option"][opt.first] = opt.second;
  }
  json["creation_time"] = creation_time;
  json["update_time"] = update_time;
  json["enable"] = enable;
  json["runnable"] = runnable;
  json["library"] = library;
  json["query"] = query;
  json["type"] = type;
  return json.dump();
}

void PluginMeta::setParamsFromJsonString(const std::string& json_str) {
  if (json_str.empty() || json_str == "[]" || json_str == "{}" ||
      json_str == "nu") {
    return;
  }
  auto j = nlohmann::json::parse(json_str);
  if (j.is_array()) {
    for (auto& param : j) {
      Parameter p;
      p.name = param["name"].get<std::string>();
      p.type = param["type"].get<PropertyType>();
      params.push_back(p);
    }
  } else {
    LOG(ERROR) << "Invalid params string: " << json_str;
  }
}

void PluginMeta::setReturnsFromJsonString(const std::string& json_str) {
  auto j = nlohmann::json::parse(json_str);
  for (auto& ret : j) {
    Parameter p;
    p.name = ret["name"].get<std::string>();
    p.type = ret["type"].get<PropertyType>();
    returns.push_back(p);
  }
}

void PluginMeta::setOptionFromJsonString(const std::string& json_str) {
  auto j = nlohmann::json::parse(json_str);
  for (auto& opt : j.items()) {
    option[opt.key()] = opt.value().get<std::string>();
  }
}

std::string JobMeta::ToJson(bool print_log) const {
  nlohmann::json json;
  json["id"] = id;
  json["status"] = std::to_string(status);
  json["start_time"] = start_time;
  json["end_time"] = end_time;
  if (print_log) {
    json["log"] = read_file_to_string(log_path);
  } else {
    json["log_path"] = log_path;
  }
  json["detail"]["graph_id"] = graph_id;
  json["detail"]["process_id"] = process_id;
  json["type"] = type;
  return json.dump();
}

JobMeta JobMeta::FromJson(const std::string& json_str) {
  auto j = nlohmann::json::parse(json_str);
  return JobMeta::FromJson(j);
}

JobMeta JobMeta::FromJson(const nlohmann::json& json) {
  JobMeta meta;
  if (json.contains("id")) {
    if (json["id"].is_number()) {
      meta.id = json["id"].get<int64_t>();
    } else {
      meta.id = json["id"].get<JobId>();
    }
  }
  if (json.contains("detail")) {
    auto detail = json["detail"];
    if (detail.contains("graph_id")) {
      if (detail["graph_id"].is_number()) {
        meta.graph_id = detail["graph_id"].get<int64_t>();
      } else {
        meta.graph_id = detail["graph_id"].get<GraphId>();
      }
    }
    if (detail.contains("process_id")) {
      meta.process_id = detail["process_id"].get<int32_t>();
    }
  }

  if (json.contains("start_time")) {
    meta.start_time = json["start_time"].get<int64_t>();
  }
  if (json.contains("end_time")) {
    meta.end_time = json["end_time"].get<int64_t>();
  }
  if (json.contains("status")) {
    meta.status = parseFromString(json["status"].get<std::string>());
  }
  if (json.contains("log_path")) {
    meta.log_path = json["log_path"].get<std::string>();
    VLOG(10) << "log_path: " << meta.log_path;
  }
  if (json.contains("type")) {
    meta.type = json["type"].get<std::string>();
  }
  return meta;
}

CreateGraphMetaRequest CreateGraphMetaRequest::FromJson(
    const std::string& json_str) {
  LOG(INFO) << "CreateGraphMetaRequest::FromJson: " << json_str;
  CreateGraphMetaRequest request;
  nlohmann::json json;
  try {
    json = nlohmann::json::parse(json_str);
  } catch (const std::exception& e) {
    LOG(ERROR) << "CreateGraphMetaRequest::FromJson error: " << e.what();
    return request;
  }

  if (json.contains("name")) {
    request.name = json["name"].get<std::string>();
  }
  if (json.contains("description")) {
    request.description = json["description"].get<std::string>();
  }
  if (json.contains("schema")) {
    request.schema = json["schema"].dump();
  }
  if (json.contains("data_update_time")) {
    request.data_update_time = json["data_update_time"].get<int64_t>();
  } else {
    request.data_update_time = 0;
  }
  if (json.contains("creation_time")) {
    request.creation_time = json["creation_time"].get<int64_t>();
  } else {
    request.creation_time = GetCurrentTimeStamp();
  }
  if (json.contains("stored_procedures")) {
    for (auto& plugin : json["stored_procedures"]) {
      request.plugin_metas.push_back(PluginMeta::FromJson(plugin));
    }
  }
  return request;
}

std::string CreateGraphMetaRequest::ToString() const {
  nlohmann::json json;
  json["name"] = name;
  json["description"] = description;
  json["schema"] = nlohmann::json::parse(schema);
  if (data_update_time.has_value()) {
    json["data_update_time"] = data_update_time.value();
  } else {
    json["data_update_time"] = 0;
  }
  json["creation_time"] = creation_time;
  json["stored_procedures"] = nlohmann::json::array();
  for (auto& plugin_meta : plugin_metas) {
    json["stored_procedures"].push_back(
        nlohmann::json::parse(plugin_meta.ToJson()));
  }
  return json.dump();
}

CreatePluginMetaRequest::CreatePluginMetaRequest() : enable(true) {}

std::string CreatePluginMetaRequest::paramsString() const {
  nlohmann::json json = nlohmann::json::array();
  for (auto& param : params) {
    nlohmann::json param_json;
    param_json["name"] = param.name;
    param_json["type"] = param.type;
    json.push_back(param_json);
  }
  return json.dump();
}

std::string CreatePluginMetaRequest::returnsString() const {
  nlohmann::json json;
  for (auto& ret : returns) {
    nlohmann::json ret_json;
    ret_json["name"] = ret.name;
    ret_json["type"] = ret.type;
    json.push_back(ret_json);
  }
  return json.dump();
}

std::string CreatePluginMetaRequest::optionString() const {
  nlohmann::json json;
  for (auto& opt : option) {
    json[opt.first] = opt.second;
  }
  return json.dump();
}

std::string CreatePluginMetaRequest::ToString() const {
  nlohmann::json json;
  if (id.has_value()) {
    json["id"] = id.value();
  }
  json["bound_graph"] = bound_graph;
  json["name"] = name;
  json["creation_time"] = creation_time;
  json["description"] = description;
  json["params"] = nlohmann::json::array();
  for (auto& param : params) {
    nlohmann::json param_json;
    param_json["name"] = param.name;
    param_json["type"] = param.type;
    json["params"].push_back(param_json);
  }
  json["returns"] = nlohmann::json::array();
  for (auto& ret : returns) {
    nlohmann::json ret_json;
    ret_json["name"] = ret.name;
    ret_json["type"] = ret.type;
    json["returns"].push_back(ret_json);
  }

  json["library"] = library;
  json["option"] = nlohmann::json::object();
  for (auto& opt : option) {
    json["option"][opt.first] = opt.second;
  }
  json["query"] = query;
  json["type"] = type;
  json["enable"] = enable;
  return json.dump();
}

CreatePluginMetaRequest CreatePluginMetaRequest::FromJson(
    const std::string& json) {
  auto j = nlohmann::json::parse(json);
  return CreatePluginMetaRequest::FromJson(j);
}

CreatePluginMetaRequest CreatePluginMetaRequest::FromJson(
    const nlohmann::json& j) {
  // TODO: make sure this is correct
  CreatePluginMetaRequest request;
  if (j.contains("id")) {
    if (j["id"].is_number()) {
      request.id = std::to_string(j["id"].get<int64_t>());
    } else {
      request.id = j["id"].get<PluginId>();
    }
  }
  if (j.contains("name")) {
    request.name = j["name"].get<std::string>();
  }
  if (j.contains("bound_graph")) {
    if (j["bound_graph"].is_number()) {
      request.bound_graph = j["bound_graph"].get<int64_t>();
    } else {
      request.bound_graph = j["bound_graph"].get<PluginId>();
    }
  }
  if (j.contains("creation_time")) {
    request.creation_time = j["creation_time"].get<int64_t>();
  } else {
    request.creation_time = GetCurrentTimeStamp();
  }
  if (j.contains("description")) {
    request.description = j["description"].get<std::string>();
  }
  if (j.contains("params")) {
    for (auto& param : j["params"]) {
      Parameter p;
      p.name = param["name"].get<std::string>();
      p.type = param["type"].get<PropertyType>();
      request.params.push_back(p);
    }
  }
  if (j.contains("returns")) {
    for (auto& ret : j["returns"]) {
      Parameter p;
      p.name = ret["name"].get<std::string>();
      p.type = ret["type"].get<PropertyType>();
      request.returns.push_back(p);
    }
  }
  if (j.contains("library")) {
    request.library = j["library"].get<std::string>();
  }
  if (j.contains("option")) {
    for (auto& opt : j["option"].items()) {
      request.option[opt.key()] = opt.value().get<std::string>();
    }
  }
  if (j.contains("query")) {
    request.query = j["query"].get<std::string>();
  }
  if (j.contains("type")) {
    request.type = j["type"].get<std::string>();
  }
  if (j.contains("enable")) {
    request.enable = j["enable"].get<bool>();
  }
  return request;
}

UpdatePluginMetaRequest::UpdatePluginMetaRequest() : enable(true) {}

UpdatePluginMetaRequest UpdatePluginMetaRequest::FromJson(
    const std::string& json) {
  UpdatePluginMetaRequest request;
  try {
    auto j = nlohmann::json::parse(json);
    if (j.contains("name")) {
      if (j["name"].is_number()) {
        request.name = std::to_string(j["name"].get<int64_t>());
      } else {
        request.name = j["name"].get<std::string>();
      }
    }
    if (j.contains("description")) {
      request.description = j["description"].get<std::string>();
    }
    if (j.contains("update_time")) {
      request.update_time = j["update_time"].get<int64_t>();
    } else {
      request.update_time = GetCurrentTimeStamp();
    }
    if (j.contains("params") && j["params"].is_array()) {
      request.params = std::vector<Parameter>();
      for (auto& param : j["params"]) {
        Parameter p;
        p.name = param["name"].get<std::string>();
        p.type = param["type"].get<PropertyType>();

        request.params->emplace_back(std::move(p));
      }
    }
    if (j.contains("returns") && j["returns"].is_array()) {
      request.returns = std::vector<Parameter>();
      for (auto& ret : j["returns"]) {
        Parameter p;
        p.name = ret["name"].get<std::string>();
        p.type = ret["type"].get<PropertyType>();
        request.returns->emplace_back(std::move(p));
      }
    }
    if (j.contains("library")) {
      request.library = j["library"].get<std::string>();
    }
    if (j.contains("option")) {
      request.option = std::unordered_map<std::string, std::string>();
      for (auto& opt : j["option"].items()) {
        request.option->insert({opt.key(), opt.value()});
      }
    }
    if (j.contains("enable")) {
      request.enable = j["enable"].get<bool>();
    }
  } catch (const std::exception& e) {
    LOG(ERROR) << "UpdatePluginMetaRequest::FromJson error: " << e.what();
  }
  return request;
}

std::string UpdatePluginMetaRequest::paramsString() const {
  nlohmann::json json;
  if (params.has_value()) {
    for (auto& param : params.value()) {
      nlohmann::json param_json;
      param_json["name"] = param.name;
      param_json["type"] = param.type;
      json.push_back(param_json);
    }
  }
  return json.dump();
}

std::string UpdatePluginMetaRequest::returnsString() const {
  nlohmann::json json;
  if (returns.has_value()) {
    for (auto& ret : returns.value()) {
      nlohmann::json ret_json;
      ret_json["name"] = ret.name;
      ret_json["type"] = ret.type;
      json.push_back(ret_json);
    }
  }
  return json.dump();
}

std::string UpdatePluginMetaRequest::optionString() const {
  nlohmann::json json;
  if (option.has_value()) {
    for (auto& opt : option.value()) {
      json[opt.first] = opt.second;
    }
  }
  return json.dump();
}

std::string UpdatePluginMetaRequest::ToString() const {
  nlohmann::json json;
  if (name.has_value()) {
    json["name"] = name.value();
  }
  if (bound_graph.has_value()) {
    json["bound_graph"] = bound_graph.value();
  }
  if (description.has_value()) {
    json["description"] = description.value();
  }
  if (update_time.has_value()) {
    json["update_time"] = update_time.value();
  }
  // create array of json objects
  json["params"] = nlohmann::json::array();
  if (params.has_value()) {
    for (auto& param : params.value()) {
      nlohmann::json param_json;
      param_json["name"] = param.name;
      param_json["type"] = param.type;
      json["params"].emplace_back(std::move(param_json));
    }
  }

  json["returns"] = nlohmann::json::array();
  if (returns.has_value()) {
    for (auto& ret : returns.value()) {
      nlohmann::json ret_json;
      ret_json["name"] = ret.name;
      ret_json["type"] = ret.type;
      json["returns"].emplace_back(std::move(ret_json));
    }
  }
  if (library.has_value()) {
    json["library"] = library.value();
  }
  json["option"] = nlohmann::json::object();
  if (option.has_value()) {
    for (auto& opt : option.value()) {
      json["option"][opt.first] = opt.second;
    }
  }
  if (enable.has_value()) {
    json["enable"] = enable.value();
  }
  auto dumped = json.dump();
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
  nlohmann::json json;
  json["detail"]["graph_id"] = graph_id;
  json["detail"]["process_id"] = process_id;
  json["start_time"] = start_time;
  json["status"] = std::to_string(status);
  json["log_path"] = log_path;
  json["type"] = type;
  return json.dump();
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
  nlohmann::json json;
  json["total_vertex_count"] = total_vertex_count;
  json["total_edge_count"] = total_edge_count;
  json["vertex_type_statistics"] = nlohmann::json::array();
  for (auto& type_stat : vertex_type_statistics) {
    nlohmann::json type_stat_json;
    type_stat_json["type_id"] = std::get<0>(type_stat);
    type_stat_json["type_name"] = std::get<1>(type_stat);
    type_stat_json["count"] = std::get<2>(type_stat);
    json["vertex_type_statistics"].push_back(type_stat_json);
  }
  json["edge_type_statistics"] = nlohmann::json::array();
  for (auto& type_stat : edge_type_statistics) {
    nlohmann::json type_stat_json;
    type_stat_json["type_id"] = std::get<0>(type_stat);
    type_stat_json["type_name"] = std::get<1>(type_stat);
    type_stat_json["vertex_type_pair_statistics"] = nlohmann::json::array();
    for (auto& pair_stat : std::get<2>(type_stat)) {
      nlohmann::json pair_stat_json;
      pair_stat_json["source_vertex"] = std::get<0>(pair_stat);
      pair_stat_json["destination_vertex"] = std::get<1>(pair_stat);
      pair_stat_json["count"] = std::get<2>(pair_stat);
      type_stat_json["vertex_type_pair_statistics"].push_back(pair_stat_json);
    }
    json["edge_type_statistics"].push_back(type_stat_json);
  }
  return json.dump();
}

Result<GraphStatistics> GraphStatistics::FromJson(const std::string& json_str) {
  auto j = nlohmann::json::parse(json_str);
  return GraphStatistics::FromJson(j);
}

Result<GraphStatistics> GraphStatistics::FromJson(const nlohmann::json& json) {
  GraphStatistics stat;
  stat.total_vertex_count = json["total_vertex_count"].get<int64_t>();
  stat.total_edge_count = json["total_edge_count"].get<int64_t>();
  for (auto& type_stat : json["vertex_type_statistics"]) {
    stat.vertex_type_statistics.push_back(
        {type_stat["type_id"].get<int32_t>(),
         type_stat["type_name"].get<std::string>(),
         type_stat["count"].get<int64_t>()});
  }
  for (auto& type_stat : json["edge_type_statistics"]) {
    std::vector<typename GraphStatistics::vertex_type_pair_statistic>
        vertex_type_pair_statistics;
    for (auto& pair : type_stat["vertex_type_pair_statistics"]) {
      vertex_type_pair_statistics.push_back(
          {pair["source_vertex"].get<std::string>(),
           pair["destination_vertex"].get<std::string>(),
           pair["count"].get<int64_t>()});
    }
    stat.edge_type_statistics.push_back(
        {type_stat["type_id"].get<int32_t>(),
         type_stat["type_name"].get<std::string>(),
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

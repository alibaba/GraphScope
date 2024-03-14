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

#include "flex/storages/metadata/local_file_metadata_store.h"

namespace gs {

LocalFileMetadataStore::LocalFileMetadataStore(const std::string& path)
    : root_dir_(path) {}

LocalFileMetadataStore::~LocalFileMetadataStore() { Close(); }

Result<bool> LocalFileMetadataStore::Open() {
  // First clear previous locks
  RETURN_IF_NOT_OK(clear_locks());
  VLOG(10) << "Creating directories";
  // open directories.
  RETURN_IF_NOT_OK(create_directory(root_dir_));
  RETURN_IF_NOT_OK(create_directory(get_meta_data_dir()));
  RETURN_IF_NOT_OK(create_directory(get_graph_meta_dir()));
  RETURN_IF_NOT_OK(create_directory(get_plugin_meta_dir()));
  RETURN_IF_NOT_OK(create_directory(get_job_meta_dir()));
  LOG(INFO) << "Successfully open metadata store";
  return true;
}

Result<bool> LocalFileMetadataStore::Close() {
  RETURN_IF_NOT_OK(ClearRunningGraph());
  RETURN_IF_NOT_OK(clear_locks());
  return true;
}

Result<GraphId> LocalFileMetadataStore::CreateGraphMeta(
    const CreateGraphMetaRequest& request) {
  std::unique_lock<std::recursive_mutex> lock(graph_meta_mutex_);
  GraphId graph_id;
  ASSIGN_AND_RETURN_IF_RESULT_NOT_OK(graph_id, GetNextGraphId());
  auto graph_meta_file = get_graph_meta_file(graph_id);
  auto json_str = request.ToString();
  VLOG(10) << "graph meta: " << json_str;
  nlohmann::json json;
  try {
    json = nlohmann::json::parse(json_str);
  } catch (nlohmann::json::parse_error& e) {
    return Result<GraphId>(
        Status(gs::StatusCode::InternalError, "Failed to parse json string"));
  }
  json["id"] = graph_id;
  json["creation_time"] = GetCurrentTimeStamp();
  VLOG(10) << "update id and creation_time";
  auto graph_meta = GraphMeta::FromJson(json);
  auto dump_res = dump_file(graph_meta_file, graph_meta.ToJson());
  if (!dump_res.ok()) {
    return Result<GraphId>(dump_res.status());
  }
  LOG(INFO) << "Successfully create graph meta: " << graph_id;
  return Result<GraphId>(graph_id);
}

Result<GraphMeta> LocalFileMetadataStore::GetGraphMeta(
    const GraphId& graph_id) {
  std::unique_lock<std::recursive_mutex> lock(graph_meta_mutex_);
  auto graph_meta_dir = get_graph_meta_dir(graph_id);
  if (!std::filesystem::exists(graph_meta_dir)) {
    return Result<GraphMeta>(
        Status(gs::StatusCode::NotFound, "Graph not exists"));
  }
  auto graph_meta_file = get_graph_meta_file(graph_id);
  if (!std::filesystem::exists(graph_meta_file)) {
    return Result<GraphMeta>(
        Status(gs::StatusCode::InternalError,
               "the metadata not found for graph" + graph_id));
  }
  Result<nlohmann::json> json_res = gs::read_json_from_file(graph_meta_file);
  if (!json_res.ok()) {
    return Result<GraphMeta>(json_res.status());
  }
  auto json = json_res.value();
  GraphMeta graph_meta = GraphMeta::FromJson(json);
  LOG(INFO) << "Got graph meta: " << graph_meta.ToJson();
  return Result<GraphMeta>(graph_meta);
}

Result<std::vector<GraphMeta>> LocalFileMetadataStore::GetAllGraphMeta() {
  std::unique_lock<std::recursive_mutex> lock(graph_meta_mutex_);
  std::vector<GraphMeta> graph_metas;
  auto graph_ids = get_graph_ids();
  for (auto& graph_id : graph_ids) {
    auto graph_meta_res = GetGraphMeta(graph_id);
    if (graph_meta_res.ok()) {
      graph_metas.push_back(graph_meta_res.value());
    } else {
      LOG(ERROR) << "Failed to get graph meta: " << graph_id << ", "
                 << graph_meta_res.status().error_message();
      return Result<std::vector<GraphMeta>>(graph_meta_res.status());
    }
  }
  return Result<std::vector<GraphMeta>>(graph_metas);
}

Result<bool> LocalFileMetadataStore::DeleteGraphMeta(const GraphId& graph_id) {
  std::unique_lock<std::recursive_mutex> lock(graph_meta_mutex_);
  auto graph_meta_dir = get_graph_meta_dir(graph_id);
  if (!std::filesystem::exists(graph_meta_dir)) {
    return Result<bool>(Status(gs::StatusCode::NotFound, "Graph not exists"));
  }
  if (!std::filesystem::remove_all(graph_meta_dir)) {
    return Result<bool>(
        Status(gs::StatusCode::IOError, "Failed to delete graph meta"));
  }
  return Result<bool>(true);
}

// We only update the field that present in update_request.
Result<bool> LocalFileMetadataStore::UpdateGraphMeta(
    const GraphId& graph_id, const UpdateGraphMetaRequest& update_request) {
  std::unique_lock<std::recursive_mutex> lock(graph_meta_mutex_);
  auto graph_meta_dir = get_graph_meta_dir(graph_id);
  if (!std::filesystem::exists(graph_meta_dir)) {
    return Result<bool>(Status(gs::StatusCode::NotFound, "Graph not exists"));
  }
  auto graph_meta_file = get_graph_meta_file(graph_id);
  if (!std::filesystem::exists(graph_meta_file)) {
    return Result<bool>(Status(gs::StatusCode::InternalError,
                               "the metadata not found for graph:" + graph_id));
  }
  Result<nlohmann::json> json_res = gs::read_json_from_file(graph_meta_file);
  if (!json_res.ok()) {
    return Result<bool>(json_res.status());
  }
  auto json = json_res.value();
  auto graph_meta = GraphMeta::FromJson(json);
  VLOG(10) << "before update: " << graph_meta.ToJson();
  if (update_request.graph_name.has_value()) {
    graph_meta.name = update_request.graph_name.value();
  }
  if (update_request.description.has_value()) {
    graph_meta.description = update_request.description.value();
  }
  if (update_request.data_update_time.has_value()) {
    graph_meta.data_update_time = update_request.data_update_time.value();
  }
  if (update_request.data_import_config.has_value()) {
    graph_meta.data_import_config = update_request.data_import_config.value();
  }
  auto dump_res = dump_file(graph_meta_file, graph_meta.ToJson());
  VLOG(10) << "after update: " << graph_meta.ToJson();
  if (!dump_res.ok()) {
    return Result<bool>(dump_res.status());
  }
  return Result<bool>(true);
}

/* Plugin Meta related.
 */
Result<PluginId> LocalFileMetadataStore::CreatePluginMeta(
    const CreatePluginMetaRequest& request) {
  std::unique_lock<std::recursive_mutex> lock(plugin_meta_mutex_);
  PluginId plugin_id;
  ASSIGN_AND_RETURN_IF_RESULT_NOT_OK(plugin_id, GetNextPluginId(request.name));
  auto plugin_meta_dir = get_plugin_meta_dir(plugin_id);
  auto plugin_meta_file = get_plugin_meta_file(plugin_id);
  auto json_str = request.ToString();
  VLOG(10) << "json_str: " << json_str;
  nlohmann::json json;
  try {
    json = nlohmann::json::parse(json_str);
  } catch (nlohmann::json::parse_error& e) {
    return Result<PluginId>(
        Status(gs::StatusCode::InternalError, "Failed to parse json string"));
  }
  auto ts = GetCurrentTimeStamp();
  json["id"] = plugin_id;
  json["creation_time"] = ts;
  json["update_time"] = ts;
  auto plugin_meta = PluginMeta::FromJson(json);
  auto dump_res = dump_file(plugin_meta_file, plugin_meta.ToJson());
  if (!dump_res.ok()) {
    return Result<PluginId>(dump_res.status());
  }
  LOG(INFO) << "Successfully create plugin meta: " << plugin_id;
  return Result<PluginId>(plugin_id);
}
Result<PluginMeta> LocalFileMetadataStore::GetPluginMeta(
    const GraphId& graph_id, const PluginId& plugin_id) {
  std::unique_lock<std::recursive_mutex> lock(plugin_meta_mutex_);
  auto plugin_meta_dir = get_plugin_meta_dir(plugin_id);
  if (!std::filesystem::exists(plugin_meta_dir)) {
    return Result<PluginMeta>(
        Status(gs::StatusCode::NotFound, "Plugin not exists"));
  }
  auto plugin_meta_file = get_plugin_meta_file(plugin_id);
  if (!std::filesystem::exists(plugin_meta_file)) {
    return Result<PluginMeta>(
        Status(gs::StatusCode::InternalError,
               "the metadata not found for plugin" + plugin_id));
  }
  Result<nlohmann::json> json_res = gs::read_json_from_file(plugin_meta_file);
  if (!json_res.ok()) {
    return Result<PluginMeta>(json_res.status());
  }
  auto json = json_res.value();
  PluginMeta plugin_meta = PluginMeta::FromJson(json);
  if (plugin_meta.graph_id != graph_id) {
    return Result<PluginMeta>(Status(gs::StatusCode::InternalError,
                                     "Plugin not belongs to the graph"));
  }
  return Result<PluginMeta>(plugin_meta);
}

Result<std::vector<PluginMeta>> LocalFileMetadataStore::GetAllPluginMeta(
    const GraphId& graph_id) {
  std::unique_lock<std::recursive_mutex> lock(plugin_meta_mutex_);
  std::vector<PluginMeta> plugin_metas;
  // iterator all directory in plugin_meta_dir. only select the directory that
  // has the prefix of PLUGIN_META_FILE_PREFIX. And then read the json file.
  auto plugin_meta_dir = get_plugin_meta_dir();
  for (auto& p : std::filesystem::directory_iterator(plugin_meta_dir)) {
    if (!std::filesystem::is_directory(p)) {
      continue;
    }
    auto file_name = p.path().filename().string();
    if (file_name.find(PLUGIN_META_FILE_PREFIX) != std::string::npos) {
      auto id_str = file_name.substr(strlen(PLUGIN_META_FILE_PREFIX));
      auto plugin_id = id_str;

      auto plugin_meta_res = GetPluginMeta(graph_id, plugin_id);
      if (plugin_meta_res.ok()) {
        VLOG(10) << "Got plugin meta: " << plugin_id
                 << " for graph: " << graph_id;
        plugin_metas.push_back(plugin_meta_res.value());
      }
    }
  }
  return Result<std::vector<PluginMeta>>(plugin_metas);
}
Result<bool> LocalFileMetadataStore::DeletePluginMeta(
    const GraphId& graph_id, const PluginId& plugin_id) {
  std::unique_lock<std::recursive_mutex> lock(plugin_meta_mutex_);
  auto plugin_meta_dir = get_plugin_meta_dir(plugin_id);
  if (!std::filesystem::exists(plugin_meta_dir)) {
    return Result<bool>(gs::StatusCode::NotFound, "Plugin not exists");
  }
  if (!std::filesystem::remove_all(plugin_meta_dir)) {
    return Result<bool>(
        Status(gs::StatusCode::IOError, "Failed to delete plugin meta"));
  }
  return Result<bool>(true);
}

Result<bool> LocalFileMetadataStore::DeletePluginMetaByGraphId(
    const GraphId& graph_id) {
  std::unique_lock<std::recursive_mutex> lock(plugin_meta_mutex_);
  auto plugin_meta_dir = get_plugin_meta_dir();
  for (auto& p : std::filesystem::directory_iterator(plugin_meta_dir)) {
    if (!std::filesystem::is_directory(p)) {
      continue;
    }
    auto file_name = p.path().filename().string();
    if (file_name.find(PLUGIN_META_FILE_PREFIX) != std::string::npos) {
      auto id_str = file_name.substr(strlen(PLUGIN_META_FILE_PREFIX));
      auto plugin_id = id_str;
      auto plugin_meta_res = GetPluginMeta(graph_id, plugin_id);
      if (plugin_meta_res.ok()) {
        auto plugin_meta_dir = get_plugin_meta_dir(plugin_id);
        if (!std::filesystem::exists(plugin_meta_dir)) {
          return Result<bool>(
              Status(gs::StatusCode::NotFound, "Plugin not exists"));
        }
        if (!std::filesystem::remove_all(plugin_meta_dir)) {
          return Result<bool>(
              Status(gs::StatusCode::IOError, "Failed to delete plugin meta"));
        }
      }
    }
  }
  return Result<bool>(true);
}

Result<bool> LocalFileMetadataStore::UpdatePluginMeta(
    const GraphId& graph_id, const PluginId& plugin_id,
    const UpdatePluginMetaRequest& update_request) {
  std::unique_lock<std::recursive_mutex> lock(plugin_meta_mutex_);
  auto plugin_meta_dir = get_plugin_meta_dir(plugin_id);
  if (!std::filesystem::exists(plugin_meta_dir)) {
    return Result<bool>(gs::StatusCode::NotFound, "Plugin not exists");
  }
  auto plugin_meta_file = get_plugin_meta_file(plugin_id);
  if (!std::filesystem::exists(plugin_meta_file)) {
    return Result<bool>(
        Status(gs::StatusCode::InternalError,
               "the metadata not found for plugin" + plugin_id));
  }
  Result<nlohmann::json> json_res = gs::read_json_from_file(plugin_meta_file);
  if (!json_res.ok()) {
    return Result<bool>(json_res.status());
  }
  auto json = json_res.value();
  auto plugin_meta = PluginMeta::FromJson(json);
  if (plugin_meta.graph_id != graph_id) {
    return Result<bool>(Status(gs::StatusCode::InternalError,
                               "Plugin not belongs to the graph"));
  }
  if (update_request.graph_id.has_value()) {
    if (update_request.graph_id.value() != graph_id) {
      return Result<bool>(Status(
          gs::StatusCode::IllegalOperation,
          "The plugin_id in update payload is not the same with original"));
    }
  }
  if (update_request.name.has_value()) {
    plugin_meta.name = update_request.name.value();
  }
  if (update_request.description.has_value()) {
    plugin_meta.description = update_request.description.value();
  }
  if (update_request.params.has_value()) {
    plugin_meta.params = update_request.params.value();
  }
  if (update_request.returns.has_value()) {
    plugin_meta.returns = update_request.returns.value();
  }
  if (update_request.library.has_value()) {
    plugin_meta.library = update_request.library.value();
  }
  if (update_request.option.has_value()) {
    plugin_meta.option = update_request.option.value();
  }
  if (update_request.enable.has_value()) {
    plugin_meta.enable = update_request.enable.value();
  }
  // dump
  auto new_json = plugin_meta.ToJson();
  VLOG(10) << "new json: " << new_json;
  return dump_file(plugin_meta_file, new_json);
}

/*
Job related MetaData.
*/
Result<JobId> LocalFileMetadataStore::CreateJobMeta(
    const CreateJobMetaRequest& request) {
  std::unique_lock<std::recursive_mutex> lock(job_meta_mutex_);
  JobId job_id;
  ASSIGN_AND_RETURN_IF_RESULT_NOT_OK(job_id, GetNextJobId());
  auto job_meta_dir = get_job_meta_dir(job_id);
  auto job_meta_file = get_job_meta_file(job_id);
  auto json_str = request.ToString();
  nlohmann::json json;
  try {
    json = nlohmann::json::parse(json_str);
  } catch (nlohmann::json::parse_error& e) {
    return Result<JobId>(
        Status(gs::StatusCode::InternalError, "Failed to parse json string"));
  }
  json["id"] = job_id;
  VLOG(10) << "parsing job meta from : " << json.dump();
  auto job_meta = JobMeta::FromJson(json);
  VLOG(10) << job_meta.ToJson(false);
  auto dump_res = dump_file(job_meta_file, job_meta.ToJson(false));
  if (!dump_res.ok()) {
    return Result<JobId>(dump_res.status());
  }
  LOG(INFO) << "Successfully create job meta: " << job_id;
  return Result<JobId>(job_id);
}

Result<JobMeta> LocalFileMetadataStore::GetJobMeta(const JobId& job_id) {
  std::unique_lock<std::recursive_mutex> lock(job_meta_mutex_);
  auto job_meta_dir = get_job_meta_dir(job_id);
  if (!std::filesystem::exists(job_meta_dir)) {
    return Result<JobMeta>(Status(gs::StatusCode::NotFound, "Job not exists"));
  }
  auto job_meta_file = get_job_meta_file(job_id);
  if (!std::filesystem::exists(job_meta_file)) {
    return Result<JobMeta>(Status(gs::StatusCode::InternalError,
                                  "the metadata not found for job" + job_id));
  }
  Result<nlohmann::json> json_res = gs::read_json_from_file(job_meta_file);
  if (!json_res.ok()) {
    return Result<JobMeta>(json_res.status());
  }
  auto json = json_res.value();
  JobMeta job_meta = JobMeta::FromJson(json);
  return Result<JobMeta>(job_meta);
}

Result<std::vector<JobMeta>> LocalFileMetadataStore::GetAllJobMeta() {
  std::vector<JobMeta> job_metas;
  std::unique_lock<std::recursive_mutex> lock(job_meta_mutex_);
  auto job_meta_dir = get_job_meta_dir();
  for (auto& p : std::filesystem::directory_iterator(job_meta_dir)) {
    if (!std::filesystem::is_directory(p)) {
      continue;
    }
    auto file_name = p.path().filename().string();
    if (file_name.find(JOB_META_FILE_PREFIX) != std::string::npos) {
      auto id_str = file_name.substr(strlen(JOB_META_FILE_PREFIX));
      // first check whether id_str is a number.
      if (id_str.find_first_not_of("0123456789") != std::string::npos) {
        LOG(ERROR) << "Invalid job id: " << id_str;
        continue;
      }
      auto job_id = id_str;
      auto job_meta_res = GetJobMeta(job_id);
      if (job_meta_res.ok()) {
        job_metas.push_back(job_meta_res.value());
      } else {
        LOG(ERROR) << "Failed to get job meta: " << job_id << ", "
                   << job_meta_res.status().error_message();
      }
    }
  }
  return Result<std::vector<JobMeta>>(job_metas);
}
Result<bool> LocalFileMetadataStore::DeleteJobMeta(const JobId& job_id) {
  std::unique_lock<std::recursive_mutex> lock(job_meta_mutex_);
  auto job_meta_dir = get_job_meta_dir(job_id);
  if (!std::filesystem::exists(job_meta_dir)) {
    return Result<bool>(Status(gs::StatusCode::NotFound, "Job not exists"));
  }
  if (!std::filesystem::remove_all(job_meta_dir)) {
    return Result<bool>(
        Status(gs::StatusCode::IOError, "Failed to delete job meta"));
  }
  return Result<bool>(true);
}

Result<bool> LocalFileMetadataStore::UpdateJobMeta(
    const JobId& job_id, const UpdateJobMetaRequest& update_request) {
  std::unique_lock<std::recursive_mutex> lock(job_meta_mutex_);
  auto job_meta_dir = get_job_meta_dir(job_id);
  if (!std::filesystem::exists(job_meta_dir)) {
    return Result<bool>(Status(gs::StatusCode::NotFound, "Job not exists"));
  }
  auto job_meta_file = get_job_meta_file(job_id);
  if (!std::filesystem::exists(job_meta_file)) {
    return Result<bool>(Status(gs::StatusCode::InternalError,
                               "the metadata not found for job" + job_id));
  }
  Result<nlohmann::json> json_res = gs::read_json_from_file(job_meta_file);
  if (!json_res.ok()) {
    return Result<bool>(json_res.status());
  }
  auto json = json_res.value();
  auto job_meta = JobMeta::FromJson(json);
  if (update_request.status.has_value()) {
    job_meta.status = update_request.status.value();
  }
  if (update_request.end_time.has_value()) {
    job_meta.end_time = update_request.end_time.value();
  }
  // dump
  return dump_file(job_meta_file, job_meta.ToJson(false));
}

Result<bool> LocalFileMetadataStore::LockGraphIndices(const GraphId& graph_id) {
  // First get whether the graph indices is locked.
  std::unique_lock<std::recursive_mutex> lock(graph_indices_mutex_);
  auto get_lock_res = GetGraphIndicesLocked(graph_id);
  if (!get_lock_res.ok()) {
    return Result<bool>(get_lock_res.status());
  }
  if (get_lock_res.value()) {
    return Result<bool>(
        Status(gs::StatusCode::AlreadyLocked,
               "graph " + graph_id + " indices is already locked"),
        false);
  }
  // Lock the graph indices.
  auto lock_file = get_graph_indices_lock_file(graph_id);
  std::ofstream lock_stream(lock_file, std::ios::out);
  if (!lock_stream.is_open()) {
    return Result<bool>(Status(gs::StatusCode::IOError,
                               "Failed to lock graph indices: " + lock_file),
                        false);
  }
  lock_stream.close();
  LOG(INFO) << "Lock graph indices: " << graph_id;
  return Result<bool>(true);
}

Result<bool> LocalFileMetadataStore::UnlockGraphIndices(
    const GraphId& graph_id) {
  std::unique_lock<std::recursive_mutex> lock(graph_indices_mutex_);
  // First get whether the graph indices is locked.
  auto get_lock_res = GetGraphIndicesLocked(graph_id);
  if (!get_lock_res.ok()) {
    return Result<bool>(get_lock_res.status());
  }
  if (!get_lock_res.value()) {
    return Result<bool>(
        Status(gs::StatusCode::AlreadyLocked,
               "graph " + graph_id + " indices is already unlocked"),
        false);
  }

  auto lock_file = get_graph_indices_lock_file(graph_id);
  if (!std::filesystem::exists(lock_file)) {
    return Result<bool>(gs::StatusCode::IOError,
                        "Failed to unlock graph indices", false);
  }
  if (!std::filesystem::remove(lock_file)) {
    return Result<bool>(gs::StatusCode::IOError,
                        "Failed to unlock graph indices", false);
  }
  LOG(INFO) << "Unlock graph indices: " << graph_id;
  return true;
}

Result<bool> LocalFileMetadataStore::GetGraphIndicesLocked(
    const GraphId& graph_id) {
  std::unique_lock<std::recursive_mutex> lock(graph_indices_mutex_);
  auto lock_file = get_graph_indices_lock_file(graph_id);
  if (std::filesystem::exists(lock_file)) {
    return Result<bool>(true);
  } else {
    return Result<bool>(false);
  }
}

Result<bool> LocalFileMetadataStore::LockGraphPlugins(const GraphId& graph_id) {
  std::unique_lock<std::recursive_mutex> lock(graph_plugins_mutex_);
  // First get whether the graph plugins is locked.
  auto get_lock_res = GetGraphPluginsLocked(graph_id);
  if (!get_lock_res.ok()) {
    return Result<bool>(get_lock_res.status());
  }
  if (get_lock_res.value()) {
    return Result<bool>(
        Status(gs::StatusCode::AlreadyLocked,
               "graph " + graph_id + " plugins is already locked"),
        false);
  }
  // Lock the graph plugins.
  auto lock_file = get_graph_plugins_lock_file(graph_id);
  std::ofstream lock_stream(lock_file, std::ios::out);
  if (!lock_stream.is_open()) {
    return Result<bool>(gs::StatusCode::IOError, "Failed to lock graph plugins",
                        false);
  }
  lock_stream.close();
  LOG(INFO) << "Lock graph plugins: " << graph_id;
  return Result<bool>(true);
}

Result<bool> LocalFileMetadataStore::UnlockGraphPlugins(
    const GraphId& graph_id) {
  std::unique_lock<std::recursive_mutex> lock(graph_plugins_mutex_);
  // First get whether the graph plugins is locked.
  auto get_lock_res = GetGraphPluginsLocked(graph_id);
  if (!get_lock_res.ok()) {
    return Result<bool>(get_lock_res.status());
  }
  if (!get_lock_res.value()) {
    return Result<bool>(
        Status(gs::StatusCode::AlreadyLocked,
               "graph " + graph_id + " plugins is already unlocked"),
        false);
  }
  // Unlock the graph plugins.
  auto lock_file = get_graph_plugins_lock_file(graph_id);
  if (!std::filesystem::exists(lock_file)) {
    return Result<bool>(gs::StatusCode::IOError,
                        "Failed to unlock graph plugins", false);
  }
  if (!std::filesystem::remove(lock_file)) {
    return Result<bool>(gs::StatusCode::IOError,
                        "Failed to unlock graph plugins", false);
  }
  LOG(INFO) << "Unlock graph plugins: " << graph_id;
  return Result<bool>(true);
}

Result<bool> LocalFileMetadataStore::GetGraphPluginsLocked(
    const GraphId& graph_id) {
  std::unique_lock<std::recursive_mutex> lock(graph_plugins_mutex_);
  auto lock_file = get_graph_plugins_lock_file(graph_id);
  if (std::filesystem::exists(lock_file)) {
    return Result<bool>(true);
  } else {
    return Result<bool>(false);
  }
}

Result<bool> LocalFileMetadataStore::SetRunningGraph(const GraphId& graph_id) {
  std::unique_lock<std::mutex> lock(running_graph_mutex_);
  LOG(INFO) << "Set running graph: " << graph_id;
  auto running_graph_file = get_running_graph_file();
  std::ofstream running_graph_stream(running_graph_file);
  if (!running_graph_stream.is_open()) {
    return Result<bool>(gs::StatusCode::IOError, "Failed to set running graph");
  }
  running_graph_stream << graph_id;
  running_graph_stream.close();
  return true;
}

Result<GraphId> LocalFileMetadataStore::GetRunningGraph() {
  std::unique_lock<std::mutex> lock(running_graph_mutex_);
  auto running_graph_file = get_running_graph_file();
  if (!std::filesystem::exists(running_graph_file)) {
    return Result<GraphId>(gs::StatusCode::NotFound, "No running graph");
  }
  std::ifstream running_graph_stream(running_graph_file);
  if (!running_graph_stream.is_open()) {
    return Result<GraphId>(gs::StatusCode::IOError,
                           "Failed to get running graph");
  }
  std::string graph_id;
  running_graph_stream >> graph_id;
  LOG(INFO) << "Get running graph: " << graph_id;
  running_graph_stream.close();
  return Result<GraphId>(graph_id);
}

Result<bool> LocalFileMetadataStore::ClearRunningGraph() {
  std::unique_lock<std::mutex> lock(running_graph_mutex_);
  auto running_graph_file = get_running_graph_file();
  if (!std::filesystem::exists(running_graph_file)) {
    return Result<bool>(gs::StatusCode::NotFound, "No running graph");
  }
  if (!std::filesystem::remove(running_graph_file)) {
    return Result<bool>(gs::StatusCode::IOError,
                        "Failed to clear running graph");
  }
  return true;
}

Result<GraphId> LocalFileMetadataStore::GetNextGraphId() const {
  auto graph_meta_dir = get_graph_meta_dir();
  auto new_graph_id_res = GetMaxId(graph_meta_dir, GRAPH_META_FILE_PREFIX);
  if (!new_graph_id_res.ok()) {
    return Result<GraphId>(new_graph_id_res.status());
  }
  auto new_graph_id = std::to_string(new_graph_id_res.value());
  auto dst_dir = get_graph_meta_dir(new_graph_id);
  LOG(INFO) << "Create graph meta dir: " << dst_dir;
  if (std::filesystem::create_directory(dst_dir)) {
    return gs::Result<GraphId>(new_graph_id);
  } else {
    return gs::Result<GraphId>(Status(gs::StatusCode::IOError),
                               "Fail to create directory");
  }
}

Result<PluginId> LocalFileMetadataStore::GetNextPluginId(
    const std::string& plugin_name) const {
  auto plugin_meta_dir = get_plugin_meta_dir();
  // check whether directory plugin_meta_dir + "/"  + plugin_name  exists
  auto plugin_dir = plugin_meta_dir + "/" + plugin_name;
  if (std::filesystem::exists(plugin_dir)) {
    return Result<PluginId>(Status(
        gs::StatusCode::AlreadyExists,
        "Plugin already exists: " + plugin_name + ", please use another name"));
  }
  auto dst_dir = get_plugin_meta_dir(plugin_name);
  if (std::filesystem::create_directory(dst_dir)) {
    return gs::Result<PluginId>(plugin_name);
  } else {
    return gs::Result<PluginId>(Status(gs::StatusCode::IOError,
                                       "Fail to create directory: " + dst_dir));
  }
}

Result<JobId> LocalFileMetadataStore::GetNextJobId() const {
  auto job_meta_dir = get_job_meta_dir();
  auto new_job_id_res = GetMaxId(job_meta_dir, JOB_META_FILE_PREFIX);
  if (!new_job_id_res.ok()) {
    return Result<JobId>(new_job_id_res.status());
  }
  auto new_job_id = std::to_string(new_job_id_res.value());
  auto dst_dir = get_job_meta_dir(new_job_id);
  LOG(INFO) << "Create job meta dir: " << dst_dir;
  if (std::filesystem::create_directory(dst_dir)) {
    return gs::Result<JobId>(new_job_id);
  } else {
    return gs::Result<JobId>(Status(gs::StatusCode::IOError,
                                    "Fail to create directory: " + dst_dir));
  }
}

Result<int32_t> LocalFileMetadataStore::GetMaxId(
    const std::string& dir, const std::string& prefix) const {
  // iterate all files in the directory, get the max id.
  int max_id_ = 0;
  for (auto& p : std::filesystem::directory_iterator(dir)) {
    if (!std::filesystem::is_directory(p)) {
      continue;
    }
    auto file_name = p.path().filename().string();
    if (file_name.find(prefix) != std::string::npos) {
      auto id_str = file_name.substr(prefix.size());
      // first check whether id_str is a number.
      if (id_str.find_first_not_of("0123456789") != std::string::npos) {
        continue;
      }
      int id = std::stoi(id_str);
      if (id > max_id_) {
        max_id_ = id;
      }
    }
  }
  return max_id_ + 1;
}

std::string LocalFileMetadataStore::get_meta_data_dir() const {
  auto ret = root_dir_ + "/" + METADATA_DIR;
  if (!std::filesystem::exists(ret)) {
    std::filesystem::create_directory(ret);
  }
  return ret;
}

std::string LocalFileMetadataStore::get_graph_meta_dir() const {
  auto ret = get_meta_data_dir() + "/" + +GRAPH_META_DIR;
  if (!std::filesystem::exists(ret)) {
    std::filesystem::create_directory(ret);
  }
  return ret;
}

std::string LocalFileMetadataStore::get_plugin_meta_dir() const {
  auto ret = get_meta_data_dir() + "/" + PLUGIN_META_DIR;
  if (!std::filesystem::exists(ret)) {
    std::filesystem::create_directory(ret);
  }
  return ret;
}

std::string LocalFileMetadataStore::get_job_meta_dir() const {
  auto ret = get_meta_data_dir() + "/" + JOB_META_DIR;
  if (!std::filesystem::exists(ret)) {
    std::filesystem::create_directory(ret);
  }
  return ret;
}

std::string LocalFileMetadataStore::get_graph_meta_dir(
    const GraphId& graph_id) const {
  return get_meta_data_dir() + "/" + +GRAPH_META_DIR + "/" +
         GRAPH_META_FILE_PREFIX + graph_id;
}

std::string LocalFileMetadataStore::get_plugin_meta_dir(
    const PluginId& plugin_id) const {
  return get_meta_data_dir() + "/" + PLUGIN_META_DIR + "/" +
         PLUGIN_META_FILE_PREFIX + plugin_id;
}

std::string LocalFileMetadataStore::get_job_meta_dir(
    const JobId& job_id) const {
  return get_meta_data_dir() + "/" + JOB_META_DIR + "/" + JOB_META_FILE_PREFIX +
         job_id;
}

std::string LocalFileMetadataStore::get_graph_meta_file(
    const GraphId& graph_id) const {
  return get_graph_meta_dir(graph_id) + "/" + GRAPH_META_FILE;
}

std::string LocalFileMetadataStore::get_plugin_meta_file(
    const PluginId& plugin_id) const {
  return get_plugin_meta_dir(plugin_id) + "/" + PLUGIN_META_FILE;
}

std::string LocalFileMetadataStore::get_job_meta_file(
    const JobId& job_id) const {
  return get_job_meta_dir(job_id) + "/" + JOB_META_FILE;
}

std::string LocalFileMetadataStore::get_graph_indices_lock_file(
    const GraphId& graph_id) const {
  return get_graph_meta_dir(graph_id) + "/" + GRAPH_INDICES_LOCK_FILE;
}

std::string LocalFileMetadataStore::get_graph_plugins_lock_file(
    const GraphId& graph_id) const {
  return get_graph_meta_dir(graph_id) + "/" + GRAPH_PLUGINS_LOCK_FILE;
}

std::string LocalFileMetadataStore::get_running_graph_file() const {
  return get_meta_data_dir() + "/" + RUNNING_GRAPH_FILE;
}

std::vector<GraphId> LocalFileMetadataStore::get_graph_ids() const {
  auto graph_meta_dir = get_graph_meta_dir();
  std::vector<GraphId> graph_ids;
  for (auto& p : std::filesystem::directory_iterator(graph_meta_dir)) {
    if (!std::filesystem::is_directory(p)) {
      continue;
    }
    auto file_name = p.path().filename().string();
    if (file_name.find(GRAPH_META_FILE_PREFIX) != std::string::npos) {
      auto id_str = file_name.substr(strlen(GRAPH_META_FILE_PREFIX));
      // first check whether id_str is a number.
      if (id_str.find_first_not_of("0123456789") != std::string::npos) {
        LOG(ERROR) << "Invalid graph id: " << id_str;
        continue;
      }
      graph_ids.push_back(id_str);
    }
  }
  return graph_ids;
}

Result<bool> LocalFileMetadataStore::dump_file(
    const std::string& file_path, const std::string& content) const {
  std::ofstream out_file(file_path);
  if (!out_file.is_open()) {
    return Result<bool>(gs::StatusCode::IOError, false);
  }
  out_file << content;
  out_file.close();
  return Result<bool>(true);
}

Result<bool> LocalFileMetadataStore::create_directory(
    const std::string& dir) const {
  if (!std::filesystem::exists(dir)) {
    if (!std::filesystem::create_directory(dir)) {
      return Result<bool>(gs::StatusCode::IOError,
                          "Failed to create directory");
    }
  }
  return Result<bool>(true);
}

Result<bool> LocalFileMetadataStore::clear_locks() {
  // iterate all directories in graph_meta_dir, and remove the lock file.
  auto graph_meta_dir = get_graph_meta_dir();
  if (!std::filesystem::exists(graph_meta_dir)) {
    return Result<bool>(true);
  }
  for (auto& p : std::filesystem::directory_iterator(graph_meta_dir)) {
    if (!std::filesystem::is_directory(p)) {
      continue;
    }
    auto file_name = p.path().filename().string();
    if (file_name.find(GRAPH_META_FILE_PREFIX) != std::string::npos) {
      auto id_str = file_name.substr(strlen(GRAPH_META_FILE_PREFIX));
      // first check whether id_str is a number.
      if (id_str.find_first_not_of("0123456789") != std::string::npos) {
        LOG(ERROR) << "Invalid graph id: " << id_str;
        continue;
      }
      VLOG(10) << "Clear locks for graph: " << id_str;
      auto lock_file = get_graph_indices_lock_file(id_str);
      if (std::filesystem::exists(lock_file)) {
        if (!std::filesystem::remove(lock_file)) {
          return Result<bool>(gs::StatusCode::IOError,
                              "Failed to clear graph indices lock");
        }
      }
      lock_file = get_graph_plugins_lock_file(id_str);
      if (std::filesystem::exists(lock_file)) {
        if (!std::filesystem::remove(lock_file)) {
          return Result<bool>(gs::StatusCode::IOError,
                              "Failed to clear graph plugins lock");
        }
      }
    }
  }
  return Result<bool>(true);
}

}  // namespace gs
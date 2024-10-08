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
#include "flex/storages/metadata/default_graph_meta_store.h"

namespace gs {

DefaultGraphMetaStore::DefaultGraphMetaStore(
    std::unique_ptr<IMetaStore> base_store)
    : base_store_(std::move(base_store)) {
  // Clear previous context, in case of dirty data.
  clear_locks();
}

DefaultGraphMetaStore::~DefaultGraphMetaStore() { Close(); }

Result<bool> DefaultGraphMetaStore::Open() { return base_store_->Open(); }

Result<bool> DefaultGraphMetaStore::Close() {
  RETURN_IF_NOT_OK(clear_locks());
  return base_store_->Close();
}

Result<GraphId> DefaultGraphMetaStore::CreateGraphMeta(
    const CreateGraphMetaRequest& request) {
  GraphId graph_id;
  ASSIGN_AND_RETURN_IF_RESULT_NOT_OK(
      graph_id, base_store_->CreateMeta(GRAPH_META, request.ToString()));
  return Result<GraphId>(graph_id);
}

Result<GraphMeta> DefaultGraphMetaStore::GetGraphMeta(const GraphId& graph_id) {
  auto res = base_store_->GetMeta(GRAPH_META, graph_id);
  if (!res.ok()) {
    return Result<GraphMeta>(
        Status(res.status().error_code(), "Graph not exits"));
  }
  std::string meta_str = res.move_value();
  auto meta = GraphMeta::FromJson(meta_str);
  meta.id = graph_id;
  return Result<GraphMeta>(meta);
}

Result<std::vector<GraphMeta>> DefaultGraphMetaStore::GetAllGraphMeta() {
  auto res = base_store_->GetAllMeta(GRAPH_META);
  if (!res.ok()) {
    return Result<std::vector<GraphMeta>>(res.status());
  }
  std::vector<GraphMeta> metas;
  for (auto& pair : res.move_value()) {
    auto meta = GraphMeta::FromJson(pair.second);
    meta.id = pair.first;
    metas.push_back(meta);
  }
  return Result<std::vector<GraphMeta>>(metas);
}

Result<bool> DefaultGraphMetaStore::DeleteGraphMeta(const GraphId& graph_id) {
  return base_store_->DeleteMeta(GRAPH_META, graph_id);
}

Result<bool> DefaultGraphMetaStore::UpdateGraphMeta(
    const GraphId& graph_id, const UpdateGraphMetaRequest& request) {
  return base_store_->UpdateMeta(
      GRAPH_META, graph_id, [graph_id, &request](const std::string& old_meta) {
        rapidjson::Document json;
        if (json.Parse(old_meta.c_str()).HasParseError()) {
          LOG(ERROR) << "Fail to parse old graph meta:" << json.GetParseError();
          return Result<std::string>(
              Status(StatusCode::INTERNAL_ERROR,
                     std::string("Fail to parse old graph meta: ") +
                         std::to_string(json.GetParseError())));
        }
        auto graph_meta = GraphMeta::FromJson(json);
        if (request.graph_name.has_value()) {
          graph_meta.name = request.graph_name.value();
        }
        if (request.description.has_value()) {
          graph_meta.description = request.description.value();
        }
        if (request.data_update_time.has_value()) {
          graph_meta.data_update_time = request.data_update_time.value();
        }
        if (request.data_import_config.has_value()) {
          graph_meta.data_import_config = request.data_import_config.value();
        }
        return Result<std::string>(graph_meta.ToJson());
      });
}

Result<PluginId> DefaultGraphMetaStore::CreatePluginMeta(
    const CreatePluginMetaRequest& request) {
  if (request.id.has_value()) {
    auto real_meta_key =
        generate_real_plugin_meta_key(request.bound_graph, request.id.value());
    RETURN_IF_NOT_OK(base_store_->CreateMeta(PLUGIN_META, real_meta_key,
                                             request.ToString()));
    return Result<PluginId>(request.id.value());
  } else {
    LOG(ERROR) << "Can not create plugin meta without id";
    return Result<PluginId>(Status(StatusCode::INVALID_ARGUMENT,
                                   "Can not create plugin meta without id"));
  }
}

Result<PluginMeta> DefaultGraphMetaStore::GetPluginMeta(
    const GraphId& graph_id, const PluginId& plugin_id) {
  auto real_meta_key = generate_real_plugin_meta_key(graph_id, plugin_id);
  auto res = base_store_->GetMeta(PLUGIN_META, real_meta_key);
  if (!res.ok()) {
    return Result<PluginMeta>(res.status());
  }
  std::string meta_str = res.move_value();
  auto meta = PluginMeta::FromJson(meta_str);
  if (meta.bound_graph != graph_id) {
    return Result<PluginMeta>(Status(StatusCode::INVALID_ARGUMENT,
                                     "Plugin not belongs to the graph"));
  }
  if (meta.id != plugin_id) {
    return Result<PluginMeta>(
        Status(StatusCode::INVALID_ARGUMENT,
               "Plugin id not match: " + plugin_id + " vs " + meta.id));
  }
  return Result<PluginMeta>(meta);
}

Result<std::vector<PluginMeta>> DefaultGraphMetaStore::GetAllPluginMeta(
    const GraphId& graph_id) {
  auto res = base_store_->GetAllMeta(PLUGIN_META);
  if (!res.ok()) {
    return Result<std::vector<PluginMeta>>(res.status());
  }
  std::vector<PluginMeta> metas;
  for (auto& pair : res.move_value()) {
    auto plugin_meta = PluginMeta::FromJson(pair.second);
    if (plugin_meta.bound_graph == graph_id) {
      metas.push_back(plugin_meta);
    }
  }
  // Sort the plugin metas by create time.
  std::sort(metas.begin(), metas.end(),
            [](const PluginMeta& a, const PluginMeta& b) {
              return a.creation_time < b.creation_time;
            });
  return Result<std::vector<PluginMeta>>(metas);
}

Result<bool> DefaultGraphMetaStore::DeletePluginMeta(
    const GraphId& graph_id, const PluginId& plugin_id) {
  auto real_meta_key = generate_real_plugin_meta_key(graph_id, plugin_id);
  return base_store_->DeleteMeta(PLUGIN_META, real_meta_key);
}

Result<bool> DefaultGraphMetaStore::DeletePluginMetaByGraphId(
    const GraphId& graph_id) {
  // get all plugin meta, and get the plugin_ids which belong to graph graph_id
  auto res = base_store_->GetAllMeta(PLUGIN_META);
  if (!res.ok()) {
    return Result<bool>(res.status());
  }
  std::vector<PluginId> plugin_ids;
  for (auto& meta_str : res.value()) {
    auto plugin_meta = PluginMeta::FromJson(meta_str.second);
    if (plugin_meta.bound_graph == graph_id) {
      plugin_ids.push_back(plugin_meta.id);
    }
  }
  VLOG(10) << "Found plugin_ids: " << plugin_ids.size();
  for (auto& plugin_id : plugin_ids) {
    RETURN_IF_NOT_OK(DeletePluginMeta(graph_id, plugin_id));
  }
  return Result<bool>(true);
}

Result<bool> DefaultGraphMetaStore::UpdatePluginMeta(
    const GraphId& graph_id, const PluginId& plugin_id,
    const UpdatePluginMetaRequest& update_request) {
  auto real_meta_key = generate_real_plugin_meta_key(graph_id, plugin_id);
  return base_store_->UpdateMeta(
      PLUGIN_META, real_meta_key,
      [graph_id, plugin_id, &update_request](const std::string& old_meta) {
        rapidjson::Document json;
        if (json.Parse(old_meta.c_str()).HasParseError()) {
          LOG(ERROR) << "Fail to parse old plugin meta:"
                     << json.GetParseError();
          return Result<std::string>(
              Status(StatusCode::INTERNAL_ERROR,
                     std::string("Fail to parse old plugin meta: ") +
                         std::to_string(json.GetParseError())));
        }
        auto plugin_meta = PluginMeta::FromJson(json);
        if (plugin_meta.bound_graph != graph_id) {
          return Result<std::string>(Status(gs::StatusCode::INTERNAL_ERROR,
                                            "Plugin not belongs to the graph"));
        }
        if (update_request.bound_graph.has_value()) {
          if (update_request.bound_graph.value() != graph_id) {
            return Result<std::string>(
                Status(gs::StatusCode::ILLEGAL_OPERATION,
                       "The plugin_id in update payload is not "
                       "the same with original"));
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
        if (update_request.update_time.has_value()) {
          plugin_meta.update_time = update_request.update_time.value();
        }
        return Result<std::string>(plugin_meta.ToJson());
      });
}

Result<JobId> DefaultGraphMetaStore::CreateJobMeta(
    const CreateJobMetaRequest& request) {
  JobId job_id;
  ASSIGN_AND_RETURN_IF_RESULT_NOT_OK(
      job_id, base_store_->CreateMeta(JOB_META, request.ToString()));
  return Result<JobId>(job_id);
}

Result<JobMeta> DefaultGraphMetaStore::GetJobMeta(const JobId& job_id) {
  auto res = base_store_->GetMeta(JOB_META, job_id);
  if (!res.ok()) {
    return Result<JobMeta>(res.status());
  }
  std::string meta_str = res.move_value();
  auto job = JobMeta::FromJson(meta_str);
  job.id = job_id;
  return Result<JobMeta>(job);
}

Result<std::vector<JobMeta>> DefaultGraphMetaStore::GetAllJobMeta() {
  auto res = base_store_->GetAllMeta(JOB_META);
  if (!res.ok()) {
    return Result<std::vector<JobMeta>>(res.status());
  }
  std::vector<JobMeta> metas;
  for (auto& pair : res.move_value()) {
    auto meta = JobMeta::FromJson(pair.second);
    meta.id = pair.first;
    metas.push_back(meta);
  }
  return Result<std::vector<JobMeta>>(metas);
}

Result<bool> DefaultGraphMetaStore::DeleteJobMeta(const JobId& job_id) {
  return base_store_->DeleteMeta(JOB_META, job_id);
}

Result<bool> DefaultGraphMetaStore::UpdateJobMeta(
    const JobId& job_id, const UpdateJobMetaRequest& update_request) {
  return base_store_->UpdateMeta(
      JOB_META, job_id, [&update_request](const std::string& old_meta) {
        rapidjson::Document json;
        if (json.Parse(old_meta.c_str()).HasParseError()) {
          LOG(ERROR) << "Fail to parse old job meta:" << json.GetParseError();
          return Result<std::string>(
              Status(StatusCode::INTERNAL_ERROR,
                     std::string("Fail to parse old job meta: ") +
                         std::to_string(json.GetParseError())));
        }
        auto job_meta = JobMeta::FromJson(json);
        if (update_request.status.has_value()) {
          job_meta.status = update_request.status.value();
        }
        if (update_request.end_time.has_value()) {
          job_meta.end_time = update_request.end_time.value();
        }
        return Result<std::string>(job_meta.ToJson(false));
      });
}

Result<bool> DefaultGraphMetaStore::LockGraphIndices(const GraphId& graph_id) {
  // First try to get lock
  auto get_lock_res = GetGraphIndicesLocked(graph_id);
  if (!get_lock_res.ok()) {
    return get_lock_res.status();
  }
  if (get_lock_res.value()) {
    LOG(WARNING) << "graph " << graph_id << "'s indices is already locked";
    return Result<bool>(false);
  }
  auto lock_res = base_store_->CreateMeta(INDICES_LOCK, graph_id, LOCKED);
  if (!lock_res.ok()) {
    // If the key already exists, update it.
    return base_store_->UpdateMeta(
        INDICES_LOCK, graph_id, [graph_id](const std::string& old_value) {
          if (old_value == LOCKED) {
            return old_value;
          } else if (old_value == UNLOCKED) {
            return std::string(LOCKED);
          } else {
            LOG(ERROR) << "Unknow value: " << old_value;
            return old_value;
          }
        });
  }
  return Result<bool>(true);
}

Result<bool> DefaultGraphMetaStore::UnlockGraphIndices(
    const GraphId& graph_id) {
  // First try to get lock
  auto get_lock_res = GetGraphIndicesLocked(graph_id);
  if (!get_lock_res.ok()) {
    return get_lock_res.status();
  }
  if (!get_lock_res.value()) {
    LOG(WARNING) << "graph " << graph_id << "'s indices is already unlocked";
    return Result<bool>(false);
  }
  return base_store_->UpdateMeta(
      INDICES_LOCK, graph_id, [graph_id](const std::string& old_value) {
        if (old_value == LOCKED) {
          return std::string(UNLOCKED);
        } else if (old_value == UNLOCKED) {
          LOG(WARNING) << "graph " << graph_id
                       << "'s indices is already unlocked";
          return std::string(UNLOCKED);
        } else {
          LOG(ERROR) << "Unknow value: " << old_value;
          return old_value;
        }
      });
}

Result<bool> DefaultGraphMetaStore::GetGraphIndicesLocked(
    const GraphId& graph_id) {
  auto res = base_store_->GetMeta(INDICES_LOCK, graph_id);
  if (!res.ok()) {
    return false;  // If the key not exists, return true.
  }
  return Result<bool>(res.value() == LOCKED);
}

Result<bool> DefaultGraphMetaStore::LockGraphPlugins(const GraphId& graph_id) {
  // First try to get lock
  auto get_lock_res = GetGraphPluginsLocked(graph_id);
  if (!get_lock_res.ok()) {
    return get_lock_res.status();
  }
  if (get_lock_res.value()) {
    LOG(WARNING) << "graph " << graph_id << "'s plugins is already locked";
    return Result<bool>(false);
  }
  auto res = base_store_->CreateMeta(PLUGINS_LOCK, graph_id, LOCKED);
  if (!res.ok()) {
    // If the key already exists, update it.
    return base_store_->UpdateMeta(
        PLUGINS_LOCK, graph_id, [graph_id](const std::string& old_value) {
          if (old_value == LOCKED) {
            return old_value;
          } else if (old_value == UNLOCKED) {
            return std::string(LOCKED);
          } else {
            LOG(ERROR) << "Unknow value: " << old_value;
            return old_value;
          }
        });
  }
  return Result<bool>(true);
}

Result<bool> DefaultGraphMetaStore::UnlockGraphPlugins(
    const GraphId& graph_id) {
  // First try to get lock
  auto get_lock_res = GetGraphPluginsLocked(graph_id);
  if (!get_lock_res.ok()) {
    return get_lock_res.status();
  }
  if (!get_lock_res.value()) {
    LOG(WARNING) << "graph " << graph_id << "'s plugins is already unlocked";
    return Result<bool>(false);
  }

  return base_store_->UpdateMeta(
      PLUGINS_LOCK, graph_id, [graph_id](const std::string& old_value) {
        if (old_value == LOCKED) {
          return std::string(UNLOCKED);
        } else if (old_value == UNLOCKED) {
          LOG(WARNING) << "graph " << graph_id
                       << "'s plugins is already unlocked";
          return std::string(UNLOCKED);
        } else {
          LOG(ERROR) << "Unknow value: " << old_value;
          return old_value;
        }
      });
}

Result<bool> DefaultGraphMetaStore::GetGraphPluginsLocked(
    const GraphId& graph_id) {
  auto res = base_store_->GetMeta(PLUGINS_LOCK, graph_id);
  if (!res.ok()) {
    return false;  // If the key not exists, return true.
  }
  return Result<bool>(res.value() == LOCKED);
}

Result<bool> DefaultGraphMetaStore::SetRunningGraph(const GraphId& graph_id) {
  auto create_res =
      base_store_->CreateMeta(RUNNING_GRAPH, RUNNING_GRAPH, graph_id);
  if (!create_res.ok()) {
    // If the key already exists, update it.
    return base_store_->UpdateMeta(
        RUNNING_GRAPH, RUNNING_GRAPH,
        [graph_id](const std::string& old_value) { return graph_id; });
  }
  return Result<bool>(true);
}

Result<GraphId> DefaultGraphMetaStore::GetRunningGraph() {
  auto res = base_store_->GetMeta(RUNNING_GRAPH, RUNNING_GRAPH);
  if (!res.ok()) {
    return Result<GraphId>(res.status());
  }
  return Result<GraphId>(res.value());
}

Result<bool> DefaultGraphMetaStore::ClearRunningGraph() {
  return base_store_->DeleteMeta(RUNNING_GRAPH, RUNNING_GRAPH);
}

Result<bool> DefaultGraphMetaStore::clear_locks() {
  RETURN_IF_NOT_OK(base_store_->DeleteAllMeta(INDICES_LOCK));
  RETURN_IF_NOT_OK(base_store_->DeleteAllMeta(PLUGINS_LOCK));
  return Result<bool>(true);
}

std::string DefaultGraphMetaStore::generate_real_plugin_meta_key(
    const GraphId& graph_id, const PluginId& plugin_id) {
  return graph_id + "_" + plugin_id;
}

}  // namespace gs
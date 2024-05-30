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

#ifndef FLEX_STORAGES_METADATA_DEFAULT_GRAPH_META_STORE_H_
#define FLEX_STORAGES_METADATA_DEFAULT_GRAPH_META_STORE_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "flex/storages/metadata/graph_meta_store.h"
#include "flex/storages/metadata/i_meta_store.h"
#include "flex/utils/property/types.h"
#include "flex/utils/result.h"
#include "flex/utils/service_utils.h"

namespace gs {
/*
 *The default implementation of IGraphMetaStore.
 *Which hold a base meta store to store the metadata.
 *The base meta store can be based on sqlite/file system or other storage.
 */
class DefaultGraphMetaStore : public IGraphMetaStore {
 public:
  static constexpr const char* GRAPH_META = "GRAPH_META";
  static constexpr const char* PLUGIN_META = "PLUGIN_META";
  static constexpr const char* JOB_META = "JOB_META";
  static constexpr const char* RUNNING_GRAPH = "RUNNING_GRAPH";
  static constexpr const char* INDICES_LOCK = "INDICES_LOCK";
  static constexpr const char* PLUGINS_LOCK = "PLUGINS_LOCK";
  static constexpr const char* LOCKED = "LOCKED";
  static constexpr const char* UNLOCKED = "UNLOCKED";

  DefaultGraphMetaStore(std::unique_ptr<IMetaStore> base_store);
  ~DefaultGraphMetaStore();

  Result<bool> Open() override;
  Result<bool> Close() override;

  /* Graph Meta related.
   */
  Result<GraphId> CreateGraphMeta(
      const CreateGraphMetaRequest& request) override;
  Result<GraphMeta> GetGraphMeta(const GraphId& graph_id) override;
  Result<std::vector<GraphMeta>> GetAllGraphMeta() override;
  // Will also delete the plugin meta related to the graph.
  Result<bool> DeleteGraphMeta(const GraphId& graph_id) override;
  Result<bool> UpdateGraphMeta(
      const GraphId& graph_id,
      const UpdateGraphMetaRequest& update_request) override;

  /* Plugin Meta related.
   */
  Result<PluginId> CreatePluginMeta(
      const CreatePluginMetaRequest& request) override;
  Result<PluginMeta> GetPluginMeta(const GraphId& graph_id,
                                   const PluginId& plugin_id) override;
  Result<std::vector<PluginMeta>> GetAllPluginMeta(
      const GraphId& graph_id) override;
  Result<bool> DeletePluginMeta(const GraphId& graph_id,
                                const PluginId& plugin_id) override;
  Result<bool> DeletePluginMetaByGraphId(const GraphId& graph_id) override;
  Result<bool> UpdatePluginMeta(
      const GraphId& graph_id, const PluginId& plugin_id,
      const UpdatePluginMetaRequest& update_request) override;

  /*
  Job related MetaData.
  */
  Result<JobId> CreateJobMeta(const CreateJobMetaRequest& request) override;
  Result<JobMeta> GetJobMeta(const JobId& job_id) override;
  Result<std::vector<JobMeta>> GetAllJobMeta() override;
  Result<bool> DeleteJobMeta(const JobId& job_id) override;
  Result<bool> UpdateJobMeta(
      const JobId& job_id, const UpdateJobMetaRequest& update_request) override;

  /*
  Use a field to represent the status of the graph.
  */
  Result<bool> LockGraphIndices(const GraphId& graph_id) override;
  Result<bool> UnlockGraphIndices(const GraphId& graph_id) override;
  Result<bool> GetGraphIndicesLocked(const GraphId& graph_id) override;

  // Lock the plugin directory to avoid concurrent access.
  Result<bool> LockGraphPlugins(const GraphId& graph_id) override;
  Result<bool> UnlockGraphPlugins(const GraphId& graph_id) override;
  Result<bool> GetGraphPluginsLocked(const GraphId& graph_id) override;

  Result<bool> SetRunningGraph(const GraphId& graph_id) override;
  Result<GraphId> GetRunningGraph() override;
  Result<bool> ClearRunningGraph() override;

 private:
  Result<bool> clear_locks();
  // We assume the graph_id
  std::string generate_real_plugin_meta_key(const GraphId& graph_id,
                                            const PluginId& plugin_id);

  std::unique_ptr<IMetaStore> base_store_;
};
}  // namespace gs

#endif  // FLEX_STORAGES_METADATA_DEFAULT_GRAPH_META_STORE_H_
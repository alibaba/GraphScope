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

#ifndef FLEX_STORAGES_METADATA_LOCAL_FILE_METADATA_STORE_H_
#define FLEX_STORAGES_METADATA_LOCAL_FILE_METADATA_STORE_H_

#include <mutex>
#include <string>
#include <vector>

#include "flex/storages/metadata/metadata_store.h"
#include "flex/utils/service_utils.h"

#include <boost/format.hpp>

namespace gs {

/**
 * @brief LocalFileMetadataStore is a concrete implementation of MetadataStore,
 * which stores metadata via local files.
 *
 * We store the graph meta and procedure meta in to files under workspace.
 * ├── graph
   │   ├── ldbc
   │   └── modern
   ├── job
   │   ├── job_1
   │   └── job_2
   └── plugin
        ├── plugin_1
        └── plugin_2
 */
class LocalFileMetadataStore : public IMetaDataStore {
 public:
  static constexpr const char* METADATA_DIR = "METADATA";
  static constexpr const char* GRAPH_META_DIR = "graph";
  static constexpr const char* PLUGIN_META_DIR = "plugin";
  static constexpr const char* JOB_META_DIR = "job";
  static constexpr const char* GRAPH_META_FILE_PREFIX = "GRAPH_";
  static constexpr const char* PLUGIN_META_FILE_PREFIX = "PLUGIN_";
  static constexpr const char* JOB_META_FILE_PREFIX = "JOB_";
  static constexpr const char* GRAPH_META_FILE = "GRAPH_META";
  static constexpr const char* PLUGIN_META_FILE = "PLUGIN_META";
  static constexpr const char* JOB_META_FILE = "JOB_META";
  static constexpr const char* GRAPH_INDICES_LOCK_FILE = "GRAPH_INDICES_LOCK";
  static constexpr const char* GRAPH_PLUGINS_LOCK_FILE = "GRAPH_PLUGINS_LOCK";

  static constexpr const char* RUNNING_GRAPH_FILE = "RUNNING_GRAPH";

  LocalFileMetadataStore(const std::string& path);

  ~LocalFileMetadataStore();

  Result<bool> Open() override;

  Result<bool> Close() override;
  /* Graph Meta related.
   */
  Result<GraphId> CreateGraphMeta(
      const CreateGraphMetaRequest& request) override;
  Result<GraphMeta> GetGraphMeta(const GraphId& graph_id) override;
  Result<std::vector<GraphMeta>> GetAllGraphMeta() override;
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

  /*Lock graph and unlock graph
   */
  Result<bool> LockGraphIndices(const GraphId& graph_id) override;
  Result<bool> UnlockGraphIndices(const GraphId& graph_id) override;
  Result<bool> GetGraphIndicesLocked(const GraphId& graph_id) override;

  Result<bool> LockGraphPlugins(const GraphId& graph_id) override;
  Result<bool> UnlockGraphPlugins(const GraphId& graph_id) override;
  Result<bool> GetGraphPluginsLocked(const GraphId& graph_id) override;

  Result<bool> SetRunningGraph(const GraphId& graph_id) override;
  Result<GraphId> GetRunningGraph() override;
  Result<bool> ClearRunningGraph() override;

 private:
  // Get the next available graph id
  Result<GraphId> GetNextGraphId() const;
  Result<PluginId> GetNextPluginId(const std::string& plugin_name) const;
  Result<JobId> GetNextJobId() const;
  Result<int32_t> GetMaxId(const std::string& dir,
                           const std::string& prefix) const;

  std::string get_meta_data_dir() const;

  std::string get_graph_meta_dir() const;
  std::string get_graph_meta_dir(const GraphId& graph_id) const;
  std::string get_graph_meta_file(const GraphId& graph_id) const;

  std::string get_plugin_meta_dir() const;
  std::string get_plugin_meta_dir(const PluginId& plugin_id) const;
  std::string get_plugin_meta_file(const PluginId& plugin_id) const;

  std::string get_job_meta_dir() const;
  std::string get_job_meta_dir(const JobId& job_id) const;
  std::string get_job_meta_file(const JobId& job_id) const;

  std::string get_graph_indices_lock_file(const GraphId& graph_id) const;
  std::string get_graph_plugins_lock_file(const GraphId& graph_id) const;

  std::string get_running_graph_file() const;

  std::vector<GraphId> get_graph_ids() const;
  Result<bool> dump_file(const std::string& file_path,
                         const std::string& content) const;

  Result<bool> create_directory(const std::string& dir) const;
  Result<bool> clear_locks();

  std::recursive_mutex graph_meta_mutex_;
  std::recursive_mutex plugin_meta_mutex_;
  std::recursive_mutex job_meta_mutex_;

  std::recursive_mutex graph_indices_mutex_;
  std::recursive_mutex graph_plugins_mutex_;

  std::mutex running_graph_mutex_;

  std::string root_dir_;
};
}  // namespace gs

#endif  // FLEX_STORAGES_METADATA_LOCAL_FILE_METADATA_STORE_H_
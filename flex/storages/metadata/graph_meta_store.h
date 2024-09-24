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

#ifndef FLEX_STORAGES_METADATA_GRAPH_META_STORE_H_
#define FLEX_STORAGES_METADATA_GRAPH_META_STORE_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "flex/storages/rt_mutable_graph/schema.h"
#include "flex/utils/property/types.h"
#include "flex/utils/result.h"
#include "flex/utils/service_utils.h"

#include <rapidjson/document.h>
#include <yaml-cpp/yaml.h>

namespace gs {

using GraphId = std::string;
using PluginId = std::string;
using JobId = std::string;

// Describe the input and output of the plugin.
struct Parameter {
  std::string name;
  PropertyType type;

  std::string ToJson() const;
};

enum class JobStatus {
  kRunning,
  kSuccess,
  kFailed,
  kCancelled,
  kUnknown,
};

JobStatus parseFromString(const std::string& status_string);

////////////////// MetaData ///////////////////////
struct PluginMeta;
const std::vector<PluginMeta>& get_builtin_plugin_metas();

struct GraphMeta {
  GraphId id;
  std::string version;
  std::string name;
  std::string description;
  uint64_t creation_time;
  uint64_t data_update_time;
  std::string data_import_config;
  std::string schema;
  std::string store_type{"mutable_csr"};

  std::vector<PluginMeta> plugin_metas;

  std::string ToJson() const;
  void ToJson(rapidjson::Value& json,
              rapidjson::Document::AllocatorType& allocator) const;
  static GraphMeta FromJson(const std::string& json_str);
  static GraphMeta FromJson(const rapidjson::Value& json);
};

struct PluginMeta {
  PluginId id;
  std::string name;
  GraphId bound_graph;
  std::string description;
  std::vector<Parameter> params;
  std::vector<Parameter> returns;
  std::string library;
  std::unordered_map<std::string, std::string>
      option;  // other optional configuration
  std::string query;
  std::string type;

  bool enable;    // whether the plugin is enabled.
  bool runnable;  // whether the plugin is runnable.
  uint64_t creation_time;
  uint64_t update_time;

  void setParamsFromJsonString(const rapidjson::Value& json);

  void setReturnsFromJsonString(const rapidjson::Value& json);

  void setOptionFromJsonString(const std::string& json_str);

  std::string ToJson() const;
  void ToJson(rapidjson::Value& json,
              rapidjson::Document::AllocatorType& allocator) const;

  static PluginMeta FromJson(const std::string& json_str);
  static PluginMeta FromJson(const rapidjson::Value& json);
};

struct JobMeta {
  JobId id;
  GraphId graph_id;
  int32_t process_id;
  uint64_t start_time;
  uint64_t end_time;
  JobStatus status;
  std::string log_path;  // The path to log file.
  std::string type;

  /*
   * Convert the JobMeta to a json string.
   * @param print_log: whether to print the real log or just the path.
   * @return: the json string.
   */
  std::string ToJson(bool print_log = true) const;
  static JobMeta FromJson(const std::string& json_str);
  static JobMeta FromJson(const rapidjson::Value& json_str);
};

////////////////// CreateMetaRequest ///////////////////////
struct CreateGraphMetaRequest {
  std::string version;
  std::string name;
  std::string description;
  std::string schema;  // all in one string.
  std::optional<uint64_t> data_update_time;
  int64_t creation_time;

  std::vector<PluginMeta> plugin_metas;

  static Result<CreateGraphMetaRequest> FromJson(const std::string& json_str);

  std::string ToString() const;
};

struct CreatePluginMetaRequest {
  std::optional<PluginId> id;
  std::string name;
  GraphId bound_graph;
  int64_t creation_time;
  std::string description;
  std::vector<Parameter> params;
  std::vector<Parameter> returns;
  std::string library;
  std::unordered_map<std::string, std::string> option;
  std::string query;
  std::string type;
  bool enable;  // default true

  CreatePluginMetaRequest();

  std::string ToString() const;

  std::string paramsString() const;

  std::string returnsString() const;

  std::string optionString() const;

  static CreatePluginMetaRequest FromJson(const std::string& json_str);

  static CreatePluginMetaRequest FromJson(const rapidjson::Value& json_obj);
};

////////////////// UpdateMetaRequest ///////////////////////
struct UpdateGraphMetaRequest {
  std::optional<std::string> graph_name;
  std::optional<std::string> description;
  std::optional<uint64_t> data_update_time;
  std::optional<std::string> data_import_config;

  UpdateGraphMetaRequest(int64_t data_update_time,
                         const std::string& data_import_config);
};

// Used internally, can update params, returns, library and option.
struct UpdatePluginMetaRequest {
  std::optional<std::string> name;
  std::optional<GraphId> bound_graph;
  std::optional<std::string> description;
  std::optional<int64_t> update_time;
  std::optional<std::vector<Parameter>> params;
  std::optional<std::vector<Parameter>> returns;
  std::optional<std::string> library;
  std::optional<std::unordered_map<std::string, std::string>> option;
  std::optional<bool> enable;

  UpdatePluginMetaRequest();

  std::string paramsString() const;

  std::string returnsString() const;

  std::string optionString() const;

  std::string ToString() const;

  static UpdatePluginMetaRequest FromJson(const std::string& json_str);
};

struct CreateJobMetaRequest {
  GraphId graph_id;
  int32_t process_id;
  uint64_t start_time;
  JobStatus status;
  std::string log_path;
  std::string type;
  std::string ToString() const;

  static CreateJobMetaRequest NewRunning(const GraphId& graph_id,
                                         int32_t process_id,
                                         const std::string& log_path,
                                         const std::string& type);
};

struct UpdateJobMetaRequest {
  std::optional<JobStatus> status;
  std::optional<uint64_t> end_time;

  static UpdateJobMetaRequest NewCancel();
  static UpdateJobMetaRequest NewFinished(int rc);
};

struct GraphStatistics {
  // type_id, type_name, count
  using vertex_type_statistic = std::tuple<int32_t, std::string, int32_t>;
  // src_vertex_type_name, dst_vertex_type_name, count
  using vertex_type_pair_statistic =
      std::tuple<std::string, std::string, int32_t>;
  // edge_type_id, edge_type_name, Vec<vertex_type_pair_statistics>
  using edge_type_statistic =
      std::tuple<int32_t, std::string, std::vector<vertex_type_pair_statistic>>;

  GraphStatistics() : total_vertex_count(0), total_edge_count(0) {}

  uint64_t total_vertex_count;
  uint64_t total_edge_count;
  std::vector<vertex_type_statistic> vertex_type_statistics;
  std::vector<edge_type_statistic> edge_type_statistics;

  std::string ToJson() const;
  static Result<GraphStatistics> FromJson(const std::string& json_str);
  static Result<GraphStatistics> FromJson(const rapidjson::Value& json);
};

/*
 * The metadata store is responsible for storing metadata of the graph, plugins
 * and other information.
 *
 * MetadataStore should be thread safe inside.
 */
class IGraphMetaStore {
 public:
  virtual ~IGraphMetaStore() = default;

  virtual Result<bool> Open() = 0;
  virtual Result<bool> Close() = 0;

  /* Graph Meta related.
   */
  virtual Result<GraphId> CreateGraphMeta(
      const CreateGraphMetaRequest& request) = 0;
  virtual Result<GraphMeta> GetGraphMeta(const GraphId& graph_id) = 0;
  virtual Result<std::vector<GraphMeta>> GetAllGraphMeta() = 0;
  // Will also delete the plugin meta related to the graph.
  virtual Result<bool> DeleteGraphMeta(const GraphId& graph_id) = 0;
  virtual Result<bool> UpdateGraphMeta(
      const GraphId& graph_id,
      const UpdateGraphMetaRequest& update_request) = 0;

  /* Plugin Meta related.
   */
  virtual Result<PluginId> CreatePluginMeta(
      const CreatePluginMetaRequest& request) = 0;
  virtual Result<PluginMeta> GetPluginMeta(const GraphId& graph_id,
                                           const PluginId& plugin_id) = 0;
  virtual Result<std::vector<PluginMeta>> GetAllPluginMeta(
      const GraphId& graph_id) = 0;
  virtual Result<bool> DeletePluginMeta(const GraphId& graph_id,
                                        const PluginId& plugin_id) = 0;
  virtual Result<bool> DeletePluginMetaByGraphId(const GraphId& graph_id) = 0;
  virtual Result<bool> UpdatePluginMeta(
      const GraphId& graph_id, const PluginId& plugin_id,
      const UpdatePluginMetaRequest& update_request) = 0;

  /*
  Job related MetaData.
  */
  virtual Result<JobId> CreateJobMeta(const CreateJobMetaRequest& request) = 0;
  virtual Result<JobMeta> GetJobMeta(const JobId& job_id) = 0;
  virtual Result<std::vector<JobMeta>> GetAllJobMeta() = 0;
  virtual Result<bool> DeleteJobMeta(const JobId& job_id) = 0;
  virtual Result<bool> UpdateJobMeta(
      const JobId& job_id, const UpdateJobMetaRequest& update_request) = 0;

  /*
  Use a field to represent the status of the graph.
  */
  virtual Result<bool> LockGraphIndices(const GraphId& graph_id) = 0;
  virtual Result<bool> UnlockGraphIndices(const GraphId& graph_id) = 0;
  virtual Result<bool> GetGraphIndicesLocked(const GraphId& graph_id) = 0;
  // Lock the plugin directory to avoid concurrent access.
  virtual Result<bool> LockGraphPlugins(const GraphId& graph_id) = 0;
  virtual Result<bool> UnlockGraphPlugins(const GraphId& graph_id) = 0;
  virtual Result<bool> GetGraphPluginsLocked(const GraphId& graph_id) = 0;

  virtual Result<bool> SetRunningGraph(const GraphId& graph_id) = 0;
  virtual Result<GraphId> GetRunningGraph() = 0;
  virtual Result<bool> ClearRunningGraph() = 0;
};

};  // namespace gs

namespace std {
std::string to_string(const gs::JobStatus& status);

std::ostream& operator<<(std::ostream& os, const gs::JobStatus& status);
}  // namespace std

namespace YAML {
template <>
struct convert<gs::Parameter> {
  static bool decode(const Node& config, gs::Parameter& parameter) {
    if (!config.IsMap()) {
      return false;
    }
    if (!config["name"] || !config["type"]) {
      return false;
    }
    parameter.name = config["name"].as<std::string>();
    parameter.type = config["type"].as<gs::PropertyType>();
    return true;
  }

  static Node encode(const gs::Parameter& parameter) {
    YAML::Node node;
    node["name"] = parameter.name;
    node["type"] = YAML::convert<gs::PropertyType>::encode(parameter.type);
    return node;
  }
};
}  // namespace YAML

#endif  // FLEX_STORAGES_METADATA_GRAPH_META_STORE_H_
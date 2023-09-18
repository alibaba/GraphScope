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

#ifndef ENGINES_HTTP_SERVER_WORKSPACE_MANAGER_H_
#define ENGINES_HTTP_SERVER_WORKSPACE_MANAGER_H_

#include <yaml-cpp/yaml.h>
#include <boost/property_tree/json_parser.hpp>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <string>
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/http_server/types.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"
#include "flex/storages/rt_mutable_graph/schema.h"
#include "flex/utils/result.h"
#include "flex/utils/service_utils.h"
#include "flex/utils/yaml_utils.h"

#include <filesystem>
#include "flex/third_party/nlohmann/json.hpp"

namespace server {
class WorkspaceManager {
 public:
  static WorkspaceManager& Get();
  static const std::string LOCK_FILE;
  static const std::string DATA_DIR_NAME;
  static const std::string GRAPH_SCHEMA_FILE_NAME;
  static const std::string GRAPH_INDICES_FILE_NAME;
  static const std::string GRAPH_INDICES_DIR_NAME;
  static const std::string GRAPH_PLUGIN_DIR_NAME;
  static const std::string CONF_ENGINE_CONFIG_FILE_NAME;
  static const std::string RUNNING_GRAPH_FILE_NAME;

 private:
  WorkspaceManager();

 public:
  ~WorkspaceManager();
  WorkspaceManager(const WorkspaceManager&) = delete;
  void Init(const std::string& workspace, const std::string& codegen_bin,
            const std::string& running_graph);

  void SetRunningGraph(const std::string& graph_name);

  std::string GetRunningGraph() const;

  /**
   * @brief Create a graph with a given name and config.
   * @param boost_ptree The config of the graph.
   */
  gs::Result<seastar::sstring> CreateGraph(const YAML::Node& yaml_node);

  /**
   * @brief Get a graph with a given name.
   * @param graph_name The name of the graph.
   */
  gs::Result<seastar::sstring> GetGraphSchemaString(
      const std::string& graph_name) const;

  gs::Result<gs::Schema> GetGraphSchema(const std::string& graph_name) const;

  gs::Result<seastar::sstring> GetDataDirectory(
      const std::string& graph_name) const;

  /**
   * @brief List all graphs.
   * @return A vector of graph configs.
   */
  gs::Result<seastar::sstring> ListGraphs() const;

  /**
   * @brief Delete a graph with a given name.
   * @param graph_name The name of the graph.
   */
  gs::Result<seastar::sstring> DeleteGraph(const std::string& graph_name);

  /**
   * @brief Load a graph with a given name and config.
   * @param graph_name The name of the graph.
   * @param boost_ptree The config of the graph.
   */
  gs::Result<seastar::sstring> LoadGraph(const std::string& graph_name,
                                         const YAML::Node& yaml_node);

  /**
   * @brief Get all procedures bound to the graph.
   * @param graph_name The name of the graph.
   * @param boost_ptree The config of the graph.
   */
  gs::Result<seastar::sstring> GetProceduresByGraphName(
      const std::string& graph_name) const;

  /**
   * @brief Get a procedure with a given name.
   * @param graph_name The name of the graph.
   * @param procedure_name The name of the procedure.
   */
  gs::Result<seastar::sstring> GetProcedureByGraphAndProcedureName(
      const std::string& graph_name, const std::string& procedure_name) const;

  seastar::future<seastar::sstring> CreateProcedure(
      const std::string& graph_name, const std::string& parameter);

  gs::Result<seastar::sstring> DeleteProcedure(
      const std::string& graph_name, const std::string& procedure_name);

  gs::Result<seastar::sstring> UpdateProcedure(
      const std::string& graph_name, const std::string& procedure_name,
      const std::string& parameter);

  gs::Result<seastar::sstring> GetProcedureLibPath(
      const std::string& graph_name, const std::string& procedure_name) const;

 private:
  gs::Result<seastar::sstring> create_procedure_sanity_check(
      const nlohmann::json& json) const;

  std::string get_graph_schema_path(const std::string& graph_name) const;

  std::string get_graph_lock_file(const std::string& graph_name) const;

  std::string get_graph_indices_file(const std::string& graph_name) const;

  std::string get_graph_indices_dir(const std::string& graph_name) const;

  std::string get_graph_plugin_dir(const std::string& graph_name) const;

  std::string get_engine_config_path() const;

  bool is_graph_exist(const std::string& graph_name) const;

  bool is_graph_loaded(const std::string& graph_name) const;

  bool is_graph_running(const std::string& graph_name) const;

  bool ensure_graph_dir_exists(const std::string& graph_name) const;

  gs::Result<std::string> dump_graph_schema(
      const YAML::Node& yaml_config, const std::string& graph_name) const;

  gs::Result<std::string> load_graph(const YAML::Node& yaml_config,
                                     const std::string& graph_name) const;

  // Generate the procedure, return the generated yaml config.
  seastar::future<seastar::sstring> generate_procedure(
      const nlohmann::json& json);

  seastar::future<seastar::sstring> add_procedure_to_graph(
      const nlohmann::json& json, const std::string& proc_yaml_config);

  // Get all the procedure yaml configs in plugins directory, add additional
  // enabled:false to each config.
  gs::Result<seastar::sstring> get_all_procedure_yamls(
      const std::string& graph_name) const;

  // Get all the procedure yaml configs in plugins directory, add additional
  // enabled:true to enabled_list, add additional enabled:false to others.
  gs::Result<seastar::sstring> get_all_procedure_yamls(
      const std::string& graph_name,
      const std::vector<std::string>& enabled_list) const;

  gs::Result<seastar::sstring> get_procedure_yaml(
      const std::string& graph_name, const std::string& procedure_names) const;

  gs::Result<seastar::sstring> enable_procedure_on_graph(
      const std::string& graph_name, const std::string& procedure_name);

  gs::Result<seastar::sstring> disable_procedure_on_graph(
      const std::string& graph_name, const std::string& procedure_name);

  gs::Result<seastar::sstring> dump_yaml_to_file(
      const YAML::Node& node, const std::string& file_path) const;

  std::string workspace_;
  std::string codegen_bin_;
  std::string data_workspace_;

  static std::mutex mtx_;
};
}  // namespace server

#endif  // ENGINES_HTTP_SERVER_WORKSPACE_MANAGER_H_
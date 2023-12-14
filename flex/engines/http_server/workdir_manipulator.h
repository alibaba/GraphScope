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

#ifndef ENGINES_HTTP_SERVER_WORKDIR_MANIPULATOR_H_
#define ENGINES_HTTP_SERVER_WORKDIR_MANIPULATOR_H_

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
#include "nlohmann/json.hpp"

namespace server {

/**
 * @brief The class to manipulate the workspace. All methods are static.
 */
class WorkDirManipulator {
 private:
  // A static variable to store the workspace path, and shall not be changed
  static std::string workspace;

 public:
  static const std::string LOCK_FILE;
  static const std::string DATA_DIR_NAME;
  static const std::string GRAPH_SCHEMA_FILE_NAME;
  static const std::string GRAPH_INDICES_FILE_NAME;
  static const std::string GRAPH_INDICES_DIR_NAME;
  static const std::string GRAPH_PLUGIN_DIR_NAME;
  static const std::string CONF_ENGINE_CONFIG_FILE_NAME;
  static const std::string RUNNING_GRAPH_FILE_NAME;
  static const std::string TMP_DIR;
  static const std::string GRAPH_LOADER_BIN;

  static void SetWorkspace(const std::string& workspace_path);

  static void SetRunningGraph(const std::string& graph_name);

  static std::string GetRunningGraph();

  /**
   * @brief Create a graph with a given name and config.
   * @param boost_ptree The config of the graph.
   */
  static gs::Result<seastar::sstring> CreateGraph(const YAML::Node& yaml_node);

  /**
   * @brief Get a graph with a given name.
   * @param graph_name The name of the graph.
   */
  static gs::Result<seastar::sstring> GetGraphSchemaString(
      const std::string& graph_name);

  static gs::Result<gs::Schema> GetGraphSchema(const std::string& graph_name);

  static gs::Result<seastar::sstring> GetDataDirectory(
      const std::string& graph_name);

  /**
   * @brief List all graphs.
   * @return A vector of graph configs.
   */
  static gs::Result<seastar::sstring> ListGraphs();

  /**
   * @brief Delete a graph with a given name.
   * @param graph_name The name of the graph.
   */
  static gs::Result<seastar::sstring> DeleteGraph(
      const std::string& graph_name);

  /**
   * @brief Load a graph with a given name and config.
   * @param graph_name The name of the graph.
   * @param yaml_node The config of the graph.
   * @param loading_thread_num The number of threads to load the graph.
   */
  static gs::Result<seastar::sstring> LoadGraph(const std::string& graph_name,
                                                const YAML::Node& yaml_node,
                                                int32_t loading_thread_num);

  /**
   * @brief Load a graph with a given name and config.
   * @param yaml_config_file The config file of the graph.
   * @param yaml_node The config of the graph.
   * @param loading_thread_num The number of threads to load the graph.
   */
  static gs::Result<std::string> LoadGraph(const std::string& yaml_config_file,
                                           const std::string& graph_name,
                                           int32_t thread_num);

  /**
   * @brief Get all procedures bound to the graph.
   * @param graph_name The name of the graph.
   * @param boost_ptree The config of the graph.
   */
  static gs::Result<seastar::sstring> GetProceduresByGraphName(
      const std::string& graph_name);

  /**
   * @brief Get a procedure with a given name.
   * @param graph_name The name of the graph.
   * @param procedure_name The name of the procedure.
   */
  static gs::Result<seastar::sstring> GetProcedureByGraphAndProcedureName(
      const std::string& graph_name, const std::string& procedure_name);

  static seastar::future<seastar::sstring> CreateProcedure(
      const std::string& graph_name, const std::string& parameter);

  static gs::Result<seastar::sstring> DeleteProcedure(
      const std::string& graph_name, const std::string& procedure_name);

  static gs::Result<seastar::sstring> UpdateProcedure(
      const std::string& graph_name, const std::string& procedure_name,
      const std::string& parameter);

  static gs::Result<seastar::sstring> GetProcedureLibPath(
      const std::string& graph_name, const std::string& procedure_name);

  static std::string GetGraphSchemaPath(const std::string& graph_name);

  static std::string GetGraphIndicesDir(const std::string& graph_name);

 private:
  static gs::Result<seastar::sstring> create_procedure_sanity_check(
      const nlohmann::json& json);

  static std::string get_graph_indices_file(const std::string& graph_name);

  static std::string get_graph_lock_file(const std::string& graph_name);

  static std::string get_graph_plugin_dir(const std::string& graph_name);

  static std::string get_graph_dir(const std::string& graph_name);

  static std::string get_engine_config_path();

  static bool is_graph_exist(const std::string& graph_name);

  static bool is_graph_loaded(const std::string& graph_name);

  static bool is_graph_running(const std::string& graph_name);

  static bool is_graph_locked(const std::string& graph_name);

  static bool try_lock_graph(const std::string& graph_name);

  static void unlock_graph(const std::string& graph_name);

  static bool ensure_graph_dir_exists(const std::string& graph_name);

  static gs::Result<std::string> dump_graph_schema(
      const YAML::Node& yaml_config, const std::string& graph_name);

  // Generate the procedure, return the generated yaml config.
  static seastar::future<seastar::sstring> generate_procedure(
      const nlohmann::json& json);

  static seastar::future<seastar::sstring> add_procedure_to_graph(
      const nlohmann::json& json, const std::string& proc_yaml_config);

  // Get all the procedure yaml configs in plugins directory, add additional
  // enabled:false to each config.
  static gs::Result<seastar::sstring> get_all_procedure_yamls(
      const std::string& graph_name);

  // Get all the procedure yaml configs in plugins directory, add additional
  // enabled:true to enabled_list, add additional enabled:false to others.
  static gs::Result<seastar::sstring> get_all_procedure_yamls(
      const std::string& graph_name,
      const std::vector<std::string>& enabled_list);

  static gs::Result<seastar::sstring> get_procedure_yaml(
      const std::string& graph_name, const std::string& procedure_names);

  static gs::Result<seastar::sstring> enable_procedure_on_graph(
      const std::string& graph_name, const std::string& procedure_name);

  static gs::Result<seastar::sstring> disable_procedure_on_graph(
      const std::string& graph_name, const std::string& procedure_name);

  static gs::Result<seastar::sstring> dump_yaml_to_file(
      const YAML::Node& node, const std::string& file_path);
};
}  // namespace server

#endif  // ENGINES_HTTP_SERVER_WORKDIR_MANIPULATOR_H_

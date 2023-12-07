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

// A struct which represents a lock file.
// remove the lock file when the struct is destructed.
struct LockFile {
  std::string graph_name;
  std::string lock_path;

  LockFile() = default;
  ~LockFile();
  LockFile(const std::string& graph_name, const std::string& lock_path);
  LockFile(const LockFile&) = delete;
  LockFile& operator=(const LockFile&) = delete;
  LockFile(LockFile&& other);
};

// Ensure the atomic int got descreased when the struct is destructed.
struct AtomicIntDecrementer {
  std::atomic<int32_t>* count_;

  AtomicIntDecrementer(std::atomic<int32_t>& count);
  ~AtomicIntDecrementer();
  AtomicIntDecrementer(const AtomicIntDecrementer&) = delete;
  AtomicIntDecrementer& operator=(const AtomicIntDecrementer&) = delete;
  AtomicIntDecrementer(AtomicIntDecrementer&& other);
};

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
  static const std::string EXIT_CODE_FILE_NAME;
  static const std::string GRAPH_NAME_FILE_NAME;
  static const std::string JOB_STATUS_FILE_NAME;
  static const std::string JOB_LOG_FILE_NAME;
  static const std::string JOB_TMP_LOG_FILE_NAME;
  static const std::string START_TIME_FILE_NAME;
  static const std::string END_TIME_FILE_NAME;

  static void SetWorkspace(const std::string& workspace_path);

  static void SetRunningGraph(const std::string& graph_name);

  static void ClearRunningGraph();

  static void ClearLockFile();

  static std::string GetRunningGraph();

  /**
   * @brief Create a graph with a given name and config.
   * @param boost_ptree The config of the graph.
   */
  static gs::Result<seastar::sstring> CreateGraph(YAML::Node& yaml_node);

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
  static gs::Result<seastar::sstring> LoadGraph(
      const std::string& graph_name, const YAML::Node& yaml_node,
      int32_t loading_thread_num, AtomicIntDecrementer&& decrementer);

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
      const std::string& graph_name, const std::string& parameter,
      const std::string& engine_config_path);

  static gs::Result<seastar::sstring> DeleteProcedure(
      const std::string& graph_name, const std::string& procedure_name);

  static gs::Result<seastar::sstring> UpdateProcedure(
      const std::string& graph_name, const std::string& procedure_name,
      const std::string& parameter);

  static gs::Result<seastar::sstring> GetProcedureLibPath(
      const std::string& graph_name, const std::string& procedure_name);

  static std::string GetGraphSchemaPath(const std::string& graph_name);

  static std::string GetGraphIndicesDir(const std::string& graph_name);

  static std::string GetLogDir();

  static std::string GetCompilerLogFile();
  static std::string trim_string(const std::string& graph_name);

  // job related
  static gs::Result<seastar::sstring> GetJob(const seastar::sstring& job_id);

  // get all jobs, including running and finished
  static gs::Result<seastar::sstring> ListJobs();

  static gs::Result<seastar::sstring> CancelJob(const seastar::sstring& job_id);

 private:
  // the job_id is a string, in format job_{timestamp}_{graph_name}_{pid}
  // the timestamp is the time when the job is created,the graph_name is the
  // name of the graph,the pid is the process id of the job
  // So the jobId must be unique.
  // Before call this method, make sure the job_meta is initialized.
  static gs::Result<seastar::sstring> create_job(
      const std::string& graph_name, int64_t time_stamp, int32_t pid,
      const std::string& tmp_log_path);
  static std::string get_job_dir(const std::string& job_id);

  static std::string get_log_dir();

  static std::string get_job_meta(const std::string& job_id,
                                  const std::string& file_name,
                                  const std::string& default_value);

  static void update_job_meta(const std::string& job_id,
                              const std::string& tmp_job_log,
                              int32_t exit_code);

  static gs::Result<int32_t> get_pid_from_job_id(const std::string& job_id);

  static int64_t get_start_time_from_job_id(const std::string& job_id);

  static std::string get_file_content(const std::string& file_name,
                                      int32_t last_lines_limit);

  // When a job is canceled, update the job meta
  static void update_cancelled_job_meta(const std::string& pid);

  static std::string get_tmp_bulk_loading_job_log_path(
      const std::string& graph_name);

  // Get the runnable procedures, i.e. the procedures are in current running
  // graph's schema, and is already loaded.
  static std::vector<std::string> get_runnable_procedures();
  /**
   * @brief Load a graph with a given name and config.
   * @param yaml_config_file The config file of the graph.
   * @param yaml_node The config of the graph.
   * @param loading_thread_num The number of threads to load the graph.
   */
  static gs::Result<seastar::sstring> load_graph_impl(
      const std::string& yaml_config_file, const std::string& graph_name,
      int32_t thread_num, bool overwrite, AtomicIntDecrementer&& decrementer,
      LockFile&& lock_file);

  static gs::Result<seastar::sstring> create_procedure_sanity_check(
      const nlohmann::json& json);

  static std::string get_graph_indices_file(const std::string& graph_name);

  static std::string get_graph_lock_file(const std::string& graph_name);

  static std::string get_graph_plugin_dir(const std::string& graph_name);

  static std::string get_graph_dir(const std::string& graph_name);

  static bool is_graph_exist(const std::string& graph_name);

  static bool is_graph_loaded(const std::string& graph_name);

  static bool is_graph_running(const std::string& graph_name);

  static bool is_graph_locked(const std::string& graph_name);

  static gs::Result<LockFile> try_lock_graph(const std::string& graph_name);

  static bool ensure_graph_dir_exists(const std::string& graph_name);

  static gs::Result<seastar::sstring> dump_graph_schema(
      const YAML::Node& yaml_config, const std::string& graph_name);

  // Generate the procedure, return the generated yaml config.
  static seastar::future<seastar::sstring> generate_procedure(
      const nlohmann::json& json, const std::string& engine_config_path);

  static seastar::future<seastar::sstring> add_procedure_to_graph(
      const nlohmann::json& json, const std::string& proc_yaml_config);

  // Get all the procedure yaml configs in plugins directory, add additional
  // enabled:false to each config.
  static gs::Result<seastar::sstring> get_all_procedure_yamls(
      const std::string& graph_name,
      const std::vector<std::string>& runnable_procedures);

  // Get all the procedure yaml configs in plugins directory, add additional
  // enabled:true to enabled_list, add additional enabled:false to others.
  static gs::Result<seastar::sstring> get_all_procedure_yamls(
      const std::string& graph_name,
      const std::vector<std::string>& enabled_list,
      const std::vector<std::string>& runnable_procedures);

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

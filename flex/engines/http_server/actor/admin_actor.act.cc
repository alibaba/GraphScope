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

#include "flex/engines/http_server/actor/admin_actor.act.h"

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/http_server/codegen_proxy.h"
#include "flex/engines/http_server/workdir_manipulator.h"
#include "flex/utils/service_utils.h"
#include "nlohmann/json.hpp"

#include <seastar/core/print.hh>

namespace server {

void add_runnable_info(gs::PluginMeta& plugin_meta) {
  const auto& graph_db = gs::GraphDB::get();
  const auto& schema = graph_db.schema();
  const auto& plugins_map = schema.GetPlugins();
  auto plugin_iter = plugins_map.find(plugin_meta.id);
  if (plugin_iter != plugins_map.end()) {
    plugin_meta.runnable = true;
  } else {
    plugin_meta.runnable = false;
  }
}

/*
  Loading data to a graph means we need change both the GraphData and
  GraphMetaData. We use a transaction to ensure the atomicity of the operation.
*/
class GraphLoadingTransaction {
 public:
  using result_t = gs::Result<bool>;
  GraphLoadingTransaction(std::shared_ptr<gs::IMetaDataStore> metadata_store,
                          const std::string& graph_id,
                          const YAML::Node& loading_config,
                          int32_t loading_thread_num,
                          std::unique_ptr<gs::FlexLockGuard> lock_guard)
      : metadata_store_(metadata_store),
        graph_id_(graph_id),
        loading_config_(loading_config),
        loading_thread_num_(loading_thread_num),
        lock_guard_(std::move(lock_guard)) {}

  ~GraphLoadingTransaction() {}

  // At this step, we will
  // - Try to load graph to a tmp directory.
  // - If success, we will update the graph meta data.
  // - If fail, we will rollback the transaction.
  gs::Result<gs::JobId> CommitOrRollBack() {
    // First try to load to a tmp directory.
    // If the loading process itself is atomic(The tmp data dir will be cleaned
    // if process terminated unexpectedly, and the original data dir is
    // recovered)
    auto tmp_indices_dir = WorkDirManipulator::GetTempIndicesDir(graph_id_);
    WorkDirManipulator::CleanTempIndicesDir(graph_id_);
    auto loading_res = server::WorkDirManipulator::LoadGraph(
        graph_id_, loading_config_, loading_thread_num_, tmp_indices_dir,
        std::move(lock_guard_), metadata_store_);
    if (!loading_res.ok()) {
      WorkDirManipulator::CleanTempIndicesDir(graph_id_);
      return loading_res.status();
    }
    auto job_id = loading_res.value();
    return gs::Result<gs::JobId>(job_id);
  }

 private:
  std::shared_ptr<gs::IMetaDataStore> metadata_store_;
  std::string graph_id_;
  YAML::Node loading_config_;
  int32_t loading_thread_num_;
  std::unique_ptr<gs::FlexLockGuard> lock_guard_;
};

/**
 * Create Plugin Transaction will Do these things in a transactional manner
 * - Create the plugin libxx.so and xxx.yaml on disk
 * - Insert the plugin meta.
 * - Update the Graph's meta about the plugin.(enable_lists)
 */
class CreatePluginTransaction {
 public:
  using result_t = gs::Result<bool>;
  CreatePluginTransaction(std::shared_ptr<gs::IMetaDataStore> metadata_store,
                          const std::string& graph_id,
                          const std::string& plugin_creation_parameter)
      : metadata_store_(metadata_store),
        graph_id_(graph_id),
        plugin_creation_parameter_(plugin_creation_parameter) {}

  ~CreatePluginTransaction() {}

  // This interface returns future since the codegen process utilize the hiactor
  // thread_pool to run the codegen process, which provides async interface.
  seastar::future<seastar::sstring> Run() {
    auto& hqps_service = HQPSService::get();
    // First create a plugin meta to get the plugin id, then do the real
    // creation.
    nlohmann::json json;
    try {
      LOG(INFO) << "parsing: " << plugin_creation_parameter_;
      json = nlohmann::json::parse(plugin_creation_parameter_);
    } catch (const std::exception& e) {
      return seastar::make_exception_future<seastar::sstring>(
          "Fail to parse parameter as json: " + plugin_creation_parameter_);
    }
    json["graph_id"] = graph_id_;
    auto procedure_meta_request = gs::CreatePluginMetaRequest::FromJson(json);

    LOG(INFO) << "parse create plugin meta:"
              << procedure_meta_request.ToString();
    auto insert_res = metadata_store_->CreatePluginMeta(procedure_meta_request);
    if (!insert_res.ok()) {
      return seastar::make_exception_future<seastar::sstring>(
          std::runtime_error(insert_res.status().error_message()));
    }
    auto plugin_id = insert_res.value();

    return server::WorkDirManipulator::CreateProcedure(
               graph_id_, plugin_id, json,
               hqps_service.get_service_config().engine_config_path)
        .then_wrapped([graph_id = graph_id_, old_plugin_id = plugin_id,
                       json = json,
                       metadata_store = metadata_store_](auto&& f) {
          std::string proc_id;
          try {
            proc_id = f.get0();
            // proc_yaml path should already checked to exists.
            if (proc_id.empty()) {
              metadata_store->DeletePluginMeta(graph_id, old_plugin_id);
              return seastar::make_exception_future<seastar::sstring>(
                  std::runtime_error("Fail to create plugin: " + proc_id));
            }
            if (proc_id != old_plugin_id) {
              metadata_store->DeletePluginMeta(graph_id, old_plugin_id);
              return seastar::make_exception_future<
                  seastar::sstring>(std::runtime_error(
                  std::string(
                      "the generated plugin id is not same as the old one:") +
                  proc_id + " " + old_plugin_id));
            }
            VLOG(10) << "Successfully create plugin and meta: " << proc_id
                     << ", now update "
                        "the plugin meta and update the graph meta: "
                     << graph_id;

            //  Then insert the plugin meta.
            auto procedure_meta_from_file =
                WorkDirManipulator::GetProcedureByGraphAndProcedureName(
                    graph_id, proc_id);
            if (!procedure_meta_from_file.ok()) {
              VLOG(10) << "Fail to insert plugin meta: "
                       << procedure_meta_from_file.status().error_message();
              metadata_store->DeletePluginMeta(graph_id, old_plugin_id);
              WorkDirManipulator::DeleteProcedure(graph_id, proc_id);
              return seastar::make_exception_future<seastar::sstring>(
                  std::runtime_error(
                      procedure_meta_from_file.status().error_message() + ", " +
                      proc_id.c_str()));
            }
            seastar::sstring procedure_meta_str =
                procedure_meta_from_file.value();
            VLOG(10) << "got procedure meta: " << procedure_meta_str;
            auto internal_plugin_update =
                gs::UpdatePluginMetaRequest::FromJson(procedure_meta_str);
            // the field enable should be parsed from json
            if (json.contains("enable")) {
              internal_plugin_update.enable = json["enable"].get<bool>();
            }
            auto str = internal_plugin_update.toString();
            VLOG(10) << "internal plugin update: " << str;
            auto update_res = metadata_store->UpdatePluginMeta(
                graph_id, proc_id, internal_plugin_update);
            VLOG(10) << "update_res: " << update_res.status().ok();
            if (!update_res.ok()) {
              metadata_store->DeletePluginMeta(graph_id, old_plugin_id);
              WorkDirManipulator::DeleteProcedure(graph_id, proc_id);
              return seastar::make_exception_future<seastar::sstring>(
                  std::runtime_error(update_res.status().error_message()));
            }
            VLOG(10) << "Successfully created procedure: " << proc_id;
            std::string response = "{\"procedure_id\":\"" + proc_id + "\"}";
            return seastar::make_ready_future<seastar::sstring>(response);
          } catch (std::exception& e) {
            LOG(ERROR) << "Fail to create plugin: " << e.what();
            metadata_store->DeletePluginMeta(graph_id, old_plugin_id);
            WorkDirManipulator::DeleteProcedure(graph_id, old_plugin_id);
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("Fail to create plugin: "));
          }
        });
  }

  // If commit failed, we need to delete the plugin libxx.so and xxx.yaml.
  gs::Status Commit() { return gs::Status::OK(); }

 private:
  std::shared_ptr<gs::IMetaDataStore> metadata_store_;
  std::string graph_id_;
  std::string plugin_creation_parameter_;
};

class DeletePluginTransaction {
 public:
  DeletePluginTransaction(std::shared_ptr<gs::IMetaDataStore> metadata_store,
                          const std::string& graph_id,
                          const std::string& procedure_id)
      : metadata_store_(metadata_store),
        graph_id_(graph_id),
        procedure_id_(procedure_id) {}

  gs::Status Run() {
    // First delete the plugin meta.
    auto delete_meta_res =
        metadata_store_->DeletePluginMeta(graph_id_, procedure_id_);
    if (!delete_meta_res.ok()) {
      return delete_meta_res.status();
    }
    // Then delete the plugin libxx.so and xxx.yaml on disk
    auto delete_res =
        server::WorkDirManipulator::DeleteProcedure(graph_id_, procedure_id_);
    if (!delete_res.ok()) {
      return delete_res.status();
    }
    return gs::Status::OK();
  }

  // If commit failed, we need to delete the plugin libxx.so and xxx.yaml.
  gs::Status Commit() { return gs::Status::OK(); }

 private:
  std::shared_ptr<gs::IMetaDataStore> metadata_store_;
  std::string graph_id_;
  std::string procedure_id_;
};

// util functions

std::string to_json_str(const std::vector<gs::PluginMeta>& plugin_metas) {
  nlohmann::json res;
  for (auto& plugin_meta : plugin_metas) {
    res.push_back(nlohmann::json::parse(plugin_meta.ToJson()));
  }
  return res.empty() ? "{}" : res.dump();
}

std::string to_json_str(const std::vector<gs::JobMeta>& job_metas) {
  nlohmann::json res;
  for (auto& job_meta : job_metas) {
    res.push_back(nlohmann::json::parse(job_meta.ToJson(true)));
  }
  return res.empty() ? "{}" : res.dump();
}

admin_actor::~admin_actor() {
  // finalization
  // ...
}

admin_actor::admin_actor(hiactor::actor_base* exec_ctx,
                         const hiactor::byte_t* addr)
    : hiactor::actor(exec_ctx, addr) {
  set_max_concurrency(1);  // set max concurrency for task reentrancy (stateful)
  // initialization
  // ...
  auto& hqps_service = HQPSService::get();
  // meta_data_ should be thread safe.
  metadata_store_ = hqps_service.get_metadata_store();
}

// Create a new Graph with the passed graph config.
seastar::future<admin_query_result> admin_actor::run_create_graph(
    query_param&& query_param) {
  LOG(INFO) << "Creating Graph: " << query_param.content;

  YAML::Node yaml;

  try {
    nlohmann::json json = nlohmann::json::parse(query_param.content);
    std::stringstream json_ss;
    json_ss << query_param.content;
    yaml = YAML::Load(json_ss);
  } catch (std::exception& e) {
    LOG(ERROR) << "Fail to parse json: " << e.what();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::StatusCode::InvalidSchema,
            "Fail to parse json: " + std::string(e.what())));
  } catch (...) {
    LOG(ERROR) << "Fail to parse json: " << query_param.content;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(gs::StatusCode::InvalidSchema,
                                     "Fail to parse json: "));
  }

  auto parse_schema_res = gs::Schema::LoadFromYamlNode(yaml);
  if (!parse_schema_res.ok()) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(parse_schema_res.status()));
  }

  auto result = metadata_store_->CreateGraphMeta(
      gs::CreateGraphMetaRequest::FromJson(query_param.content));
  // we also need to store a graph.yaml on disk, for other services to read.
  if (result.ok()) {
    auto dump_res = WorkDirManipulator::DumpGraphSchema(result.value(), yaml);
    if (!dump_res.ok()) {
      LOG(ERROR) << "Fail to dump graph schema: "
                 << dump_res.status().error_message();
      // If dump schema fails, we should delete the graph meta.
      metadata_store_->DeleteGraphMeta(result.value());
      return seastar::make_ready_future<admin_query_result>(
          gs::Result<seastar::sstring>(dump_res.status()));
    } else {
      VLOG(10) << "Successfully created graph";
      std::string response = "{\"graph_id\":\"" + result.value() + "\"}";
      return seastar::make_ready_future<admin_query_result>(
          gs::Result<seastar::sstring>(std::move(response)));
    }
  } else {
    LOG(ERROR) << "Fail to create graph: " << result.status().error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(result.status()));
  }
}

// get graph schema
// query_param is the graph name
seastar::future<admin_query_result> admin_actor::run_get_graph_schema(
    query_param&& query_param) {
  LOG(INFO) << "Get Graph schema for graph_id: " << query_param.content;
  auto schema_res = metadata_store_->GetGraphMeta(query_param.content);

  if (schema_res.ok()) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(std::move(schema_res.value().schema)));
  } else {
    LOG(ERROR) << "Fail to get graph schema: "
               << schema_res.status().error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(schema_res.status()));
  }
}

// list all graphs
seastar::future<admin_query_result> admin_actor::run_list_graphs(
    query_param&& query_param) {
  LOG(INFO) << "List all graphs.";
  // auto list_result = server::WorkDirManipulator::ListGraphs();
  auto all_graph_meta_res = metadata_store_->GetAllGraphMeta();
  if (!all_graph_meta_res.ok()) {
    LOG(ERROR) << "Fail to list graphs: "
               << all_graph_meta_res.status().error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(all_graph_meta_res.status()));
  } else {
    VLOG(10) << "Successfully list graphs";
    // collect all 'schema' field into a json stirng
    nlohmann::json res;
    for (auto& graph_meta : all_graph_meta_res.value()) {
      res.push_back(nlohmann::json::parse(graph_meta.ToJson()));
    }
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(res.dump()));
  }
}

// delete one graph
seastar::future<admin_query_result> admin_actor::run_delete_graph(
    query_param&& query_param) {
  LOG(INFO) << "Delete graph: " << query_param.content;

  auto get_res = metadata_store_->GetGraphMeta(query_param.content);
  if (!get_res.ok()) {
    LOG(ERROR) << "Graph not exists: " << query_param.content;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(get_res.status()));
  }
  // can not delete a builtin graph
  if (get_res.value().is_builtin) {
    LOG(ERROR) << "Can not delete a builtin graph: " << query_param.content;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::StatusCode::IllegalOperation,
            "Can not delete a builtin graph: " + query_param.content));
  }

  auto delete_res = metadata_store_->DeleteGraphMeta(query_param.content);

  if (delete_res.ok()) {
    // delete the disk data
    auto delete_plugins_res =
        metadata_store_->DeletePluginMetaByGraphId(query_param.content);
    if (!delete_plugins_res.ok()) {
      LOG(ERROR) << "Fail to delete graph's plugins: "
                 << delete_plugins_res.status().error_message();
      return seastar::make_ready_future<admin_query_result>(
          gs::Result<seastar::sstring>(delete_plugins_res.status()));
    }
    WorkDirManipulator::DeleteGraph(query_param.content);
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>("Successfully delete graph: " +
                                     query_param.content));
  } else {
    LOG(ERROR) << "Fail to delete graph: "
               << delete_res.status().error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(delete_res.status()));
  }
}

// load the graph.
seastar::future<admin_query_result> admin_actor::run_graph_loading(
    graph_management_param&& query_param) {
  // query_param contains two parameter, first for graph name, second for
  // graph config
  auto content = query_param.content;
  auto& graph_id = content.first;
  VLOG(1) << "Parse json payload for graph: " << graph_id;
  auto& loading_config = content.second;

  YAML::Node yaml;
  try {
    // parse json from query_param.content
    nlohmann::json json = nlohmann::json::parse(loading_config);
    std::stringstream json_ss;
    json_ss << loading_config;
    yaml = YAML::Load(json_ss);
  } catch (std::exception& e) {
    LOG(ERROR) << "Fail to parse json: " << e.what();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::StatusCode::InvalidImportFile,
            "Fail to parse json: " + std::string(e.what())));
  } catch (...) {
    LOG(ERROR) << "Fail to parse json: " << loading_config;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(gs::StatusCode::InvalidImportFile,
                                     "Fail to parse json: "));
  }

  int32_t loading_thread_num = 1;
  if (yaml["loading_thread_num"]) {
    loading_thread_num = yaml["loading_thread_num"].as<int32_t>();
  }
  // First check graph exists
  auto graph_meta_res = metadata_store_->GetGraphMeta(graph_id);
  if (!graph_meta_res.ok()) {
    LOG(ERROR) << "Graph not exists: " << graph_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(graph_meta_res.status()));
  }

  // Use a lock guard, to represent the graph's data directory is locked.
  // Other operations should return failure if the lock is already required.
  // Will be release after loading is done.

  std::unique_ptr<gs::FlexLockGuard> lock_guard(new gs::FlexLockGuard(
      [metadata_store = metadata_store_, graph_id]() {  // lock func
        return metadata_store->LockGraphIndices(graph_id);
      },
      [metadata_store = metadata_store_, graph_id]() {  // unlock func
        return metadata_store->UnlockGraphIndices(graph_id);
      }));
  auto lock_res = lock_guard->TryLock();
  if (!lock_res.ok() || !lock_res.value()) {
    LOG(ERROR) << "Fail to lock graph: " << graph_id << " :"
               << lock_res.status().error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(lock_res.status()));
  }

  std::string graph_id_str = graph_id.c_str();
  GraphLoadingTransaction transaction(metadata_store_, graph_id_str, yaml,
                                      loading_thread_num,
                                      std::move(lock_guard));
  auto job_id_res = transaction.CommitOrRollBack();
  if (!job_id_res.ok()) {
    LOG(ERROR) << "Fail to run graph loading transaction: "
               << job_id_res.status().error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(job_id_res.status()));
  }
  seastar::sstring res = "{\"job_id\":\"" + job_id_res.value() + "\"}";
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(std::move(res)));
}

// Get all procedure with graph_name and procedure_name
seastar::future<admin_query_result>
admin_actor::get_procedure_by_procedure_name(
    procedure_query_param&& query_param) {
  auto& graph_id = query_param.content.first;
  auto& procedure_id = query_param.content.second;

  auto get_graph_res = metadata_store_->GetGraphMeta(graph_id);
  if (!get_graph_res.ok()) {
    LOG(ERROR) << "Graph not exists: " << graph_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(get_graph_res.status()));
  }

  LOG(INFO) << "Get procedure: " << procedure_id << " for graph: " << graph_id;
  auto get_procedure_res =
      metadata_store_->GetPluginMeta(graph_id, procedure_id);

  if (get_procedure_res.ok()) {
    VLOG(10) << "Successfully get procedure procedures";
    auto& proc_meta = get_procedure_res.value();
    add_runnable_info(proc_meta);
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(proc_meta.ToJson()));
  } else {
    LOG(ERROR) << "Fail to get procedure for graph: " << graph_id
               << " and procedure: " << procedure_id << ", error message: "
               << get_procedure_res.status().error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(get_procedure_res.status()));
  }
}

// Get all procedures of one graph.
seastar::future<admin_query_result> admin_actor::get_procedures_by_graph_name(
    query_param&& query_param) {
  auto& graph_id = query_param.content;
  // first check graph exists
  auto graph_meta_res = metadata_store_->GetGraphMeta(graph_id);
  if (!graph_meta_res.ok()) {
    LOG(ERROR) << "Graph not exists: " << graph_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(graph_meta_res.status()));
  }

  auto get_all_procedure_res = metadata_store_->GetAllPluginMeta(graph_id);
  if (get_all_procedure_res.ok()) {
    VLOG(10) << "Successfully get all procedures: "
             << get_all_procedure_res.value().size();
    auto& all_plugin_metas = get_all_procedure_res.value();
    for (auto& plugin_meta : all_plugin_metas) {
      add_runnable_info(plugin_meta);
    }
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(to_json_str(all_plugin_metas)));
  } else {
    LOG(ERROR) << "Fail to get all procedures: "
               << get_all_procedure_res.status().error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(get_all_procedure_res.status()));
  }
}

seastar::future<admin_query_result> admin_actor::create_procedure(
    create_procedure_query_param&& query_param) {
  auto& graph_id = query_param.content.first;
  auto& parameter = query_param.content.second;

  auto graph_meta_res = metadata_store_->GetGraphMeta(graph_id);
  if (!graph_meta_res.ok()) {
    LOG(ERROR) << "Graph not exists: " << graph_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(graph_meta_res.status()));
  }

  gs::FlexLockGuard lock_guard(
      [metadata_store = metadata_store_, graph_id]() {  // lock func
        return metadata_store->LockGraphPlugins(graph_id);
      },
      [metadata_store = metadata_store_, graph_id]() {  // unlock func
        return metadata_store->UnlockGraphPlugins(graph_id);
      });
  auto lock_res = lock_guard.TryLock();
  if (!lock_res.ok() || !lock_res.value()) {
    LOG(ERROR) << "Fail to lock graph plugin dir: " << graph_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::StatusCode::AlreadyLocked,
            "Fail to acquire lock for graph plugin dir: " + graph_id +
                ", try again later"));
  }

  // Use a transaction to ensure the transactional
  CreatePluginTransaction transaction(metadata_store_, graph_id, parameter);

  return transaction.Run().then_wrapped([](auto&& f) {
    try {
      auto res = f.get();
      return seastar::make_ready_future<admin_query_result>(
          gs::Result<seastar::sstring>(std::move(res)));
    } catch (std::exception& e) {
      LOG(ERROR) << "Fail to create procedure: " << e.what();
      return seastar::make_ready_future<admin_query_result>(
          gs::Result<seastar::sstring>(
              gs::StatusCode::InternalError,
              "Fail to create procedure: " + std::string(e.what())));
    }
  });
}

// Delete a procedure by graph name and procedure name
seastar::future<admin_query_result> admin_actor::delete_procedure(
    create_procedure_query_param&& query_param) {
  auto& graph_id = query_param.content.first;
  auto& procedure_id = query_param.content.second;

  auto graph_meta_res = metadata_store_->GetGraphMeta(graph_id);
  if (!graph_meta_res.ok()) {
    LOG(ERROR) << "Graph not exists: " << graph_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(graph_meta_res.status()));
  }

  auto get_procedure_res =
      metadata_store_->GetPluginMeta(graph_id, procedure_id);

  if (!get_procedure_res.ok()) {
    LOG(ERROR) << "Procedure " << procedure_id
               << " not exists on graph: " << graph_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(get_procedure_res.status()));
  }

  // To delete a procedure, we need to lock the plugin directory first.
  gs::FlexLockGuard lock_guard(
      [metadata_store = metadata_store_, graph_id]() {  // lock func
        return metadata_store->LockGraphPlugins(graph_id);
      },
      [metadata_store = metadata_store_, graph_id]() {  // unlock func
        return metadata_store->UnlockGraphPlugins(graph_id);
      });
  auto lock_res = lock_guard.TryLock();
  if (!lock_res.ok() || !lock_res.value()) {
    LOG(ERROR) << "Fail to lock graph plugin dir: " << graph_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::StatusCode::AlreadyLocked,
            "Fail to acquire lock for graph plugin dir: " + graph_id +
                ", try again later"));
  }

  DeletePluginTransaction transaction(metadata_store_, graph_id, procedure_id);
  auto run_res = transaction.Run();
  if (!run_res.ok()) {
    LOG(ERROR) << "Fail to run delete procedure transaction: "
               << run_res.error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(run_res));
  }
  auto commit_res = transaction.Commit();
  if (!commit_res.ok()) {
    LOG(ERROR) << "Fail to commit delete procedure transaction: "
               << commit_res.error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(commit_res));
  }

  VLOG(10) << "Successfully get all procedures";
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>("Successfully delete procedure: " +
                                   procedure_id));
}

// update a procedure by graph name and procedure name
seastar::future<admin_query_result> admin_actor::update_procedure(
    update_procedure_query_param&& query_param) {
  auto& graph_id = std::get<0>(query_param.content);
  auto& procedure_id = std::get<1>(query_param.content);
  auto& update_request_json = std::get<2>(query_param.content);

  auto graph_meta_res = metadata_store_->GetGraphMeta(graph_id);
  if (!graph_meta_res.ok()) {
    LOG(ERROR) << "Graph not exists: " << graph_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(graph_meta_res.status()));
  }

  auto get_procedure_res =
      metadata_store_->GetPluginMeta(graph_id, procedure_id);

  if (!get_procedure_res.ok()) {
    LOG(ERROR) << "Procedure not exists: " << procedure_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(get_procedure_res.status()));
  }

  auto req = gs::UpdatePluginMetaRequest::FromJson(update_request_json);
  // If updatePluginMetaRequest contains field params, returns, library, and
  // option, we warning and return.
  if (req.params.has_value() || req.returns.has_value() ||
      req.library.has_value() || req.option.has_value()) {
    LOG(ERROR) << "UpdatePluginMetaRequest contains field params, returns, "
                  "library, or option, which should not be updated.";
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::StatusCode::IllegalOperation,
            "UpdatePluginMetaRequest contains field params, returns, library, "
            "and option, which should not be updated."));
  }

  if (req.name.has_value()) {
    LOG(ERROR) << "UpdatePluginMetaRequest contains field 'name', which should "
                  "not be updated.";
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(gs::StatusCode::IllegalOperation,
                                     "UpdatePluginMetaRequest contains field "
                                     "'name', which should not be updated."));
  }

  auto update_res =
      metadata_store_->UpdatePluginMeta(graph_id, procedure_id, req);

  if (update_res.ok()) {
    VLOG(10) << "Successfully update procedure: " << procedure_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>("Successfully update procedure: " +
                                     procedure_id));
  } else {
    LOG(ERROR) << "Fail to create procedure: "
               << update_res.status().error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(update_res.status()));
  }
}

// Start service on a graph first means stop all current running actors, then
// switch graph and create new actors with a unused scope_id.
seastar::future<admin_query_result> admin_actor::start_service(
    query_param&& query_param) {
  // parse query_param.content as json and get graph_name
  auto& content = query_param.content;
  std::string graph_name;

  auto cur_running_graph_res = metadata_store_->GetRunningGraph();
  if (!cur_running_graph_res.ok()) {
    LOG(ERROR) << "Fail to get running graph: "
               << cur_running_graph_res.status().error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(cur_running_graph_res.status()));
  }
  auto cur_running_graph = cur_running_graph_res.value();
  LOG(INFO) << "Current running graph: " << cur_running_graph;
  try {
    if (!content.empty()) {
      nlohmann::json json = nlohmann::json::parse(content);
      if (json.contains("graph_id")) {
        graph_name = json["graph_id"].get<std::string>();
      }
    } else {
      graph_name = cur_running_graph;
      LOG(WARNING)
          << "Request payload is empty, will restart on current graph: "
          << graph_name;
    }
    LOG(WARNING) << "Starting service with graph: " << graph_name;
  } catch (std::exception& e) {
    LOG(ERROR) << "Fail to Start service: ";
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::StatusCode::InvalidSchema,
            "Fail to parse json: " + std::string(e.what())));
  }

  auto get_graph_res = metadata_store_->GetGraphMeta(graph_name);
  if (!get_graph_res.ok()) {
    LOG(ERROR) << "Fail to get graph meta: "
               << get_graph_res.status().error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(get_graph_res.status()));
  }

  auto get_lock_res = metadata_store_->GetGraphIndicesLocked(graph_name);
  if (!get_lock_res.ok()) {
    LOG(ERROR) << "Failed to get lock for graph: " << graph_name;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(get_lock_res.status()));
  }
  auto prev_lock = get_lock_res.value();
  if (prev_lock) {
    if (cur_running_graph == graph_name) {
      LOG(INFO) << "Service already running on graph: " << graph_name;
    } else {
      LOG(ERROR) << "The graph is locked but not running: " << graph_name
                 << ", maybe a data loading job is running on this graph";
      return seastar::make_ready_future<admin_query_result>(
          gs::Result<seastar::sstring>(
              gs::StatusCode::AlreadyLocked,
              "The graph is locked but not running: " + graph_name +
                  ", maybe a data loading job is running on this graph"));
    }
  } else {
    LOG(INFO) << "The graph is not locked: " << graph_name;
    auto acquire_lock_res = metadata_store_->LockGraphIndices(graph_name);
    if (!acquire_lock_res.ok() || !acquire_lock_res.value()) {
      LOG(ERROR) << "Fail to lock graph: " << graph_name;
      return seastar::make_ready_future<admin_query_result>(
          gs::Result<seastar::sstring>(gs::StatusCode::AlreadyLocked,
                                       "Fail to acquire lock for graph: " +
                                           graph_name + ", try again later"));
    }
    LOG(INFO) << "Successfully locked graph: " << graph_name;
  }

  // Dump the latest schema to file, which include all enabled plugins.
  auto plugins_res = metadata_store_->GetAllPluginMeta(graph_name);
  if (!plugins_res.ok()) {
    LOG(ERROR) << "Fail to get all plugins: "
               << plugins_res.status().error_message();
    if (!prev_lock) {
      // If the graph is not locked before, and we fail at some
      // steps after locking, we should unlock it.
      metadata_store_->UnlockGraphIndices(graph_name);
    }
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(plugins_res.status()));
  }
  // With all enabled plugins and graph schema, dump to a new schema file.
  auto dump_res = WorkDirManipulator::DumpGraphSchema(get_graph_res.value(),
                                                      plugins_res.value());
  if (!dump_res.ok()) {
    LOG(ERROR) << "Fail to dump graph schema: "
               << dump_res.status().error_message();
    if (!prev_lock) {
      metadata_store_->UnlockGraphIndices(graph_name);
    }
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(dump_res.status()));
  }

  auto schema_res = server::WorkDirManipulator::GetGraphSchema(graph_name);
  if (!schema_res.ok()) {
    LOG(ERROR) << "Fail to get graph schema: "
               << schema_res.status().error_message() << ", " << graph_name;
    if (!prev_lock) {
      metadata_store_->UnlockGraphIndices(graph_name);
    }
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(schema_res.status()));
  }
  auto& schema_value = schema_res.value();
  auto data_dir = server::WorkDirManipulator::GetDataDirectory(graph_name);
  if (!data_dir.ok()) {
    LOG(ERROR) << "Fail to get data directory: "
               << data_dir.status().error_message();
    if (!prev_lock) {  // If the graph is not locked before, and we fail at some
                       // steps after locking, we should unlock it.
      metadata_store_->UnlockGraphIndices(graph_name);
    }
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(data_dir.status()));
  }
  auto data_dir_value = data_dir.value();

  // First Stop query_handler's actors.

  auto& hqps_service = HQPSService::get();
  return hqps_service.stop_query_actors().then([this, prev_lock, graph_name,
                                                schema_value, cur_running_graph,
                                                data_dir_value, &hqps_service] {
    LOG(INFO) << "Successfully stopped query handler";

    {
      std::lock_guard<std::mutex> lock(mtx_);
      auto& db = gs::GraphDB::get();
      LOG(INFO) << "Update service running on graph:" << graph_name;

      // use the previous thread num
      auto thread_num = db.SessionNum();
      db.Close();
      if (!db.Open(schema_value, data_dir_value, thread_num).ok()) {
        LOG(ERROR) << "Fail to load graph from data directory: "
                   << data_dir_value;
        if (!prev_lock) {  // If the graph is not locked before, and we fail at
                           // some
                           // steps after locking, we should unlock it.
          metadata_store_->UnlockGraphIndices(graph_name);
        }
        return seastar::make_ready_future<admin_query_result>(
            gs::Result<seastar::sstring>(
                gs::StatusCode::InternalError,
                "Fail to load graph from data directory: " + data_dir_value));
      }
      // unlock the previous graph
      if (graph_name != cur_running_graph) {
        auto unlock_res =
            metadata_store_->UnlockGraphIndices(cur_running_graph);
        if (!unlock_res.ok()) {
          LOG(ERROR) << "Fail to unlock graph: " << cur_running_graph;
          if (!prev_lock) {
            metadata_store_->UnlockGraphIndices(graph_name);
          }
          return seastar::make_ready_future<admin_query_result>(
              gs::Result<seastar::sstring>(unlock_res.status()));
        }
      }
      auto set_res = metadata_store_->SetRunningGraph(graph_name);
      if (!set_res.ok()) {
        LOG(ERROR) << "Fail to set running graph: " << graph_name;
        if (!prev_lock) {
          metadata_store_->UnlockGraphIndices(graph_name);
        }
        return seastar::make_ready_future<admin_query_result>(
            gs::Result<seastar::sstring>(set_res.status()));
      }
    }
    hqps_service.start_query_actors();  // start on a new scope.
    LOG(INFO) << "Successfully restart query actors";
    // now start the compiler
    auto schema_path =
        server::WorkDirManipulator::GetGraphSchemaPath(graph_name);
    if (!hqps_service.start_compiler_subprocess(schema_path)) {
      LOG(ERROR) << "Fail to start compiler";
      return seastar::make_ready_future<admin_query_result>(
          gs::Result<seastar::sstring>(gs::StatusCode::InternalError,
                                       "Fail to start compiler"));
    }
    LOG(INFO) << "Successfully started service with graph: " << graph_name;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>("Successfully start service"));
  });
}

// Stop service.
// Actually stop the query_handler's actors.
// The port is still connectable.
seastar::future<admin_query_result> admin_actor::stop_service(
    query_param&& query_param) {
  auto& hqps_service = HQPSService::get();
  return hqps_service.stop_query_actors().then([&hqps_service] {
    LOG(INFO) << "Successfully stopped query handler";
    if (hqps_service.stop_compiler_subprocess()) {
      LOG(INFO) << "Successfully stop compiler";
      return seastar::make_ready_future<admin_query_result>(
          gs::Result<seastar::sstring>("Successfully stop service"));
    } else {
      LOG(ERROR) << "Fail to stop compiler";
      return seastar::make_ready_future<admin_query_result>(
          gs::Result<seastar::sstring>(gs::StatusCode::InternalError,
                                       "Fail to stop compiler"));
    }
  });
}

// get service status
seastar::future<admin_query_result> admin_actor::service_status(
    query_param&& query_param) {
  auto& hqps_service = HQPSService::get();
  auto query_port = hqps_service.get_query_port();
  auto running_graph_res = metadata_store_->GetRunningGraph();
  nlohmann::json res;
  if (query_port != 0) {
    res["status"] = hqps_service.is_actors_running() ? "Running" : "Stopped";
    res["query_port"] = query_port;
    if (running_graph_res.ok()) {
      res["graph_id"] = running_graph_res.value();
    } else {
      res["graph_id"] = "UNKNOWN";
    }
  } else {
    LOG(INFO) << "Query service has not been inited!";
    res["status"] = "Query service has not been inited!";
  }
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(res.dump()));
}

// get node status.
seastar::future<admin_query_result> admin_actor::node_status(
    query_param&& query_param) {
  // get current host' cpu usage and memory usage
  auto cpu_usage = gs::get_current_cpu_usage();
  auto mem_usage = gs::get_total_physical_memory_usage();
  // construct the result json string
  nlohmann::json json;
  {
    std::stringstream ss;
    if (cpu_usage.first < 0 || cpu_usage.second <= 0) {
      ss << "cpu_usage is not available";
    } else {
      ss << "cpu_usage is " << cpu_usage.first << " / " << cpu_usage.second;
    }
    json["cpu_usage"] = ss.str();
  }
  {
    std::stringstream ss;
    ss << "memory_usage is " << gs::memory_to_mb_str(mem_usage.first) << " / "
       << gs::memory_to_mb_str(mem_usage.second);
    json["memory_usage"] = ss.str();
  }
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(json.dump()));
}

///////////////////////// Job related /////////////////////////
seastar::future<admin_query_result> admin_actor::get_job(
    query_param&& query_param) {
  auto& job_id = query_param.content;
  auto job_meta_res = metadata_store_->GetJobMeta(job_id);
  if (job_meta_res.ok()) {
    VLOG(10) << "Successfully get job: " << job_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(job_meta_res.value().ToJson()));
  } else {
    LOG(ERROR) << "Fail to get job: " << job_id
               << ", error message: " << job_meta_res.status().error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(job_meta_res.status()));
  }
}

seastar::future<admin_query_result> admin_actor::list_jobs(
    query_param&& query_param) {
  auto list_res = metadata_store_->GetAllJobMeta();
  if (list_res.ok()) {
    VLOG(10) << "Successfully list jobs";
    auto list_job_metas_str = to_json_str(list_res.value());
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(std::move(list_job_metas_str)));
  } else {
    LOG(ERROR) << "Fail to list jobs: " << list_res.status().error_message();
    return seastar::make_ready_future<admin_query_result>(list_res.status());
  }
}

// cancel job
seastar::future<admin_query_result> admin_actor::cancel_job(
    query_param&& query_param) {
  auto& job_id = query_param.content;
  auto get_job_meta_res = metadata_store_->GetJobMeta(job_id);
  if (!get_job_meta_res.ok()) {
    LOG(ERROR) << "Job not exists: " << job_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(get_job_meta_res.status()));
  }
  auto& job_meta = get_job_meta_res.value();
  if (job_meta.process_id <= 0) {
    LOG(ERROR) << "Invalid process id: " << job_meta.process_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::StatusCode::InternalError,
            "Invalid process id: " + std::to_string(job_meta.process_id)));
  }
  // if job is already cancelled, return directly.
  if (job_meta.status == gs::JobStatus::kCancelled ||
      job_meta.status == gs::JobStatus::kFailed ||
      job_meta.status == gs::JobStatus::kSuccess) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>("Job already " +
                                     std::to_string(job_meta.status) + ": " +
                                     job_id.c_str()));
  }
  if (job_meta.status == gs::JobStatus::kUnknown) {
    VLOG(10) << "Job status is unknown, try cancelling";
  }

  boost::process::child::child_handle child(job_meta.process_id);
  std::error_code ec;
  boost::process::detail::api::terminate(child, ec);

  VLOG(10) << "Killing process: " << job_meta.process_id
           << ", res: " << ec.message();
  if (ec.value() != 0) {
    LOG(ERROR) << "Fail to kill process: " << job_meta.process_id
               << ", error message: " << ec.message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::StatusCode::InternalError,
            "Fail to kill process: " + std::to_string(job_meta.process_id) +
                ", error message: " + ec.message()));
  }
  // Now update job meta to cancelled.
  auto update_job_meta_request = gs::UpdateJobMetaRequest::NewCancel();
  auto cancel_meta_res =
      metadata_store_->UpdateJobMeta(job_id, update_job_meta_request);

  if (cancel_meta_res.ok()) {
    VLOG(10) << "Successfully cancel job: " << job_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>("Successfully cancel job: " + job_id));
  } else {
    LOG(ERROR) << "Fail to cancel job: " << job_id << ", error message: "
               << cancel_meta_res.status().error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(cancel_meta_res.status()));
  }
}

}  // namespace server
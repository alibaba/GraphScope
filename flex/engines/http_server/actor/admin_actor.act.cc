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
#include "flex/engines/http_server/service/hqps_service.h"
#include "flex/engines/http_server/workdir_manipulator.h"
#include "flex/utils/service_utils.h"
#include "nlohmann/json.hpp"

#include <seastar/core/print.hh>

namespace server {

gs::GraphStatistics get_graph_statistics(const gs::GraphDBSession& sess) {
  gs::GraphStatistics stat;
  const auto& graph = sess.graph();
  const auto& schema = sess.graph().schema();
  auto vertex_label_num = graph.schema().vertex_label_num();
  auto edge_label_num = graph.schema().edge_label_num();
  for (auto i = 0; i < vertex_label_num; ++i) {
    stat.total_vertex_count += graph.vertex_num(i);
    stat.vertex_type_statistics.emplace_back(
        std::tuple{i, schema.get_vertex_label_name(i), graph.vertex_num(i)});
  }
  for (auto edge_label_id = 0; edge_label_id < edge_label_num;
       ++edge_label_id) {
    auto edge_label_name = schema.get_edge_label_name(edge_label_id);
    std::vector<std::tuple<std::string, std::string, int32_t>>
        vertex_type_pair_statistics;
    for (auto src_label_id = 0; src_label_id < vertex_label_num;
         ++src_label_id) {
      auto src_label_name = schema.get_vertex_label_name(src_label_id);
      for (auto dst_label_id = 0; dst_label_id < vertex_label_num;
           ++dst_label_id) {
        auto dst_label_name = schema.get_vertex_label_name(dst_label_id);
        if (schema.exist(src_label_id, dst_label_id, edge_label_id)) {
          auto oe_csr =
              graph.get_oe_csr(src_label_id, dst_label_id, edge_label_id);
          auto ie_csr =
              graph.get_ie_csr(dst_label_id, src_label_id, edge_label_id);
          size_t cur_edge_cnt = 0;
          if (oe_csr) {
            cur_edge_cnt += oe_csr->size();
          } else if (ie_csr) {
            cur_edge_cnt += ie_csr->size();
          }
          stat.total_edge_count += cur_edge_cnt;
          vertex_type_pair_statistics.emplace_back(
              std::tuple{src_label_name, dst_label_name, cur_edge_cnt});
        }
      }
    }
    if (!vertex_type_pair_statistics.empty()) {
      stat.edge_type_statistics.emplace_back(std::tuple{
          edge_label_id, edge_label_name, vertex_type_pair_statistics});
    }
  }
  return stat;
}

std::string merge_graph_and_plugin_meta(
    std::shared_ptr<gs::IGraphMetaStore> metadata_store,
    const std::vector<gs::GraphMeta>& graph_metas) {
  std::vector<gs::GraphMeta> res_graph_metas;
  for (auto& graph_meta : graph_metas) {
    res_graph_metas.push_back(graph_meta);
  }
  for (auto& graph_meta : res_graph_metas) {
    auto all_plugin_meta = metadata_store->GetAllPluginMeta(graph_meta.id);
    graph_meta.plugin_metas.insert(graph_meta.plugin_metas.end(),
                                   all_plugin_meta.value().begin(),
                                   all_plugin_meta.value().end());
  }

  nlohmann::json res;
  for (auto& graph_meta : res_graph_metas) {
    res.push_back(nlohmann::json::parse(graph_meta.ToJson()));
  }
  return res.empty() ? "{}" : res.dump();
}

gs::Result<YAML::Node> preprocess_vertex_schema(YAML::Node root,
                                                const std::string& type_name) {
  // 1. To support open a empty graph, we should check if the x_csr_params is
  // set for each vertex type, if not set, we set it to a rather small max_vnum,
  // to avoid to much memory usage.
  auto types = root[type_name];
  for (auto type : types) {
    if (!type["x_csr_params"]) {
      type["x_csr_params"]["max_vertex_num"] = 8192;
    }
  }
  return types;
}

gs::Result<YAML::Node> preprocess_vertex_edge_types(
    YAML::Node root, const std::string& type_name) {
  auto types = root[type_name];
  int32_t cur_type_id = 0;
  for (auto type : types) {
    if (type["type_id"]) {
      auto type_id = type["type_id"].as<int32_t>();
      if (type_id != cur_type_id) {
        return gs::Status(gs::StatusCode::InvalidSchema,
                          "Invalid " + type_name +
                              " type_id: " + std::to_string(type_id) +
                              ", expect: " + std::to_string(cur_type_id));
      }
    } else {
      type["type_id"] = cur_type_id;
    }
    cur_type_id++;
    int32_t cur_prop_id = 0;
    if (type["properties"]) {
      for (auto prop : type["properties"]) {
        if (prop["property_id"]) {
          auto prop_id = prop["property_id"].as<int32_t>();
          if (prop_id != cur_prop_id) {
            return gs::Status(gs::StatusCode::InvalidSchema,
                              "Invalid " + type_name + " property_id: " +
                                  type["type_name"].as<std::string>() + " : " +
                                  std::to_string(prop_id) +
                                  ", expect: " + std::to_string(cur_prop_id));
          }
        } else {
          prop["property_id"] = cur_prop_id;
        }
        cur_prop_id++;
      }
    }
  }
  return types;
}

// Preprocess the schema to be compatible with the current storage.
// 1. check if any property_id or type_id is set for each type, If set, then all
// vertex/edge types should all set.
// 2. If property_id or type_id is not set, then set them according to the order
gs::Result<YAML::Node> preprocess_graph_schema(YAML::Node&& node) {
  if (node["schema"] && node["schema"]["vertex_types"]) {
    // First check whether property_id or type_id is set in the schema
    RETURN_IF_NOT_OK(
        preprocess_vertex_edge_types(node["schema"], "vertex_types"));
    RETURN_IF_NOT_OK(preprocess_vertex_schema(node["schema"], "vertex_types"));
    if (node["schema"]["edge_types"]) {
      // edge_type could be optional.
      RETURN_IF_NOT_OK(
          preprocess_vertex_edge_types(node["schema"], "edge_types"));
    }
    return node;
  } else {
    return gs::Status(gs::StatusCode::InvalidSchema, "Invalid graph schema: ");
  }
}

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

gs::Result<gs::JobId> invoke_loading_graph(
    std::shared_ptr<gs::IGraphMetaStore> metadata_store,
    const std::string& graph_id, const YAML::Node& loading_config,
    int32_t loading_thread_num) {
  // First try to load to a tmp directory.
  // If the loading process itself is atomic(The tmp data dir will be cleaned
  // if process terminated unexpectedly, and the original data dir is
  // recovered)
  auto tmp_indices_dir = WorkDirManipulator::GetTempIndicesDir(graph_id);
  WorkDirManipulator::CleanTempIndicesDir(graph_id);
  auto loading_res = server::WorkDirManipulator::LoadGraph(
      graph_id, loading_config, loading_thread_num, tmp_indices_dir,
      metadata_store);
  if (!loading_res.ok()) {
    WorkDirManipulator::CleanTempIndicesDir(graph_id);
    return loading_res.status();
  }
  auto job_id = loading_res.value();
  return gs::Result<gs::JobId>(job_id);
}

seastar::future<seastar::sstring> invoke_creating_procedure(
    std::shared_ptr<gs::IGraphMetaStore> metadata_store,
    const std::string& graph_id, const std::string& plugin_creation_parameter) {
  auto& hqps_service = HQPSService::get();
  // First create a plugin meta to get the plugin id, then do the real
  // creation.
  nlohmann::json json;
  try {
    LOG(INFO) << "parsing: " << plugin_creation_parameter;
    json = nlohmann::json::parse(plugin_creation_parameter);
  } catch (const std::exception& e) {
    return seastar::make_exception_future<seastar::sstring>(
        "Fail to parse parameter as json: " + plugin_creation_parameter);
  }
  if (json.contains("name")) {
    // Currently we need id== name
    json["id"] = json["name"];
  }
  json["bound_graph"] = graph_id;
  json["creation_time"] = gs::GetCurrentTimeStamp();
  json["update_time"] = json["creation_time"];
  if (!json.contains("enable")) {
    json["enable"] = true;
  }
  auto procedure_meta_request = gs::CreatePluginMetaRequest::FromJson(json);

  LOG(INFO) << "parse create plugin meta:" << procedure_meta_request.ToString();
  auto insert_res = metadata_store->CreatePluginMeta(procedure_meta_request);
  if (!insert_res.ok()) {
    return seastar::make_exception_future<seastar::sstring>(
        std::runtime_error(insert_res.status().error_message()));
  }
  auto plugin_id = insert_res.value();

  return server::WorkDirManipulator::CreateProcedure(
             graph_id, plugin_id, json,
             hqps_service.get_service_config().engine_config_path)
      .then_wrapped([graph_id = graph_id, old_plugin_id = plugin_id,
                     json = json, metadata_store = metadata_store](auto&& f) {
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
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error(
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
              WorkDirManipulator::GetProcedureByGraphAndProcedureName(graph_id,
                                                                      proc_id);
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
          // When updating procedure meta, we should not change the name. since
          // neo4j use name as the key.
          auto internal_plugin_update =
              gs::UpdatePluginMetaRequest::FromJson(procedure_meta_str);
          // the field enable should be parsed from json
          if (json.contains("enable")) {
            internal_plugin_update.enable = json["enable"].get<bool>();
          } else {
            internal_plugin_update.enable = true;
          }
          // update the library path to the full path
          if (internal_plugin_update.library.has_value()) {
            internal_plugin_update.library =
                WorkDirManipulator::GetGraphPluginDir(graph_id) + "/" +
                internal_plugin_update.library.value();
          }

          auto str = internal_plugin_update.ToString();
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
              std::runtime_error("Fail to create plugin: " +
                                 std::string(e.what())));
        }
      });
}

gs::Status invoke_delete_plugin_meta(
    std::shared_ptr<gs::IGraphMetaStore> metadata_store,
    const std::string& graph_id, const std::string& procedure_id) {
  // First delete the plugin meta.
  auto delete_meta_res =
      metadata_store->DeletePluginMeta(graph_id, procedure_id);
  if (!delete_meta_res.ok()) {
    return delete_meta_res.status();
  }
  // Then delete the plugin libxx.so and xxx.yaml on disk
  auto delete_res =
      server::WorkDirManipulator::DeleteProcedure(graph_id, procedure_id);
  if (!delete_res.ok()) {
    return delete_res.status();
  }
  return gs::Status::OK();
}

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
            gs::Status(gs::StatusCode::InvalidSchema,
                       "Fail to parse json: " + std::string(e.what()))));
  } catch (...) {
    LOG(ERROR) << "Fail to parse json: " << query_param.content;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::Status(gs::StatusCode::InvalidSchema, "Fail to parse json: ")));
  }
  // preprocess the schema yaml,
  auto res_yaml = preprocess_graph_schema(std::move(yaml));
  if (!res_yaml.ok()) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(res_yaml.status()));
  }
  auto& yaml_value = res_yaml.value();
  // set default value
  if (!yaml_value["store_type"]) {
    yaml_value["store_type"] = "mutable_csr";
  }

  auto parse_schema_res = gs::Schema::LoadFromYamlNode(yaml_value);
  if (!parse_schema_res.ok()) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(parse_schema_res.status()));
  }

  auto real_schema_json = gs::get_json_string_from_yaml(yaml_value);
  if (!real_schema_json.ok()) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(real_schema_json.status()));
  }

  auto result = metadata_store_->CreateGraphMeta(
      gs::CreateGraphMetaRequest::FromJson(real_schema_json.value()));
  // we also need to store a graph.yaml on disk, for other services to read.
  if (result.ok()) {
    auto dump_res =
        WorkDirManipulator::DumpGraphSchema(result.value(), res_yaml.value());
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

// Get the metadata of a graph.
seastar::future<admin_query_result> admin_actor::run_get_graph_meta(
    query_param&& query_param) {
  LOG(INFO) << "Get Graph meta for graph_id: " << query_param.content;
  auto meta_res = metadata_store_->GetGraphMeta(query_param.content);

  if (meta_res.ok()) {
    auto get_all_procedure_res =
        metadata_store_->GetAllPluginMeta(query_param.content);
    if (get_all_procedure_res.ok()) {
      VLOG(10) << "Successfully get all procedures: "
               << get_all_procedure_res.value().size();
      auto& all_plugin_metas = get_all_procedure_res.value();
      for (auto& plugin_meta : all_plugin_metas) {
        add_runnable_info(plugin_meta);
      }
      auto& graph_meta = meta_res.value();
      // There can also be procedures that builtin in the graph meta.
      for (auto& plugin_meta : graph_meta.plugin_metas) {
        add_runnable_info(plugin_meta);
      }
      graph_meta.plugin_metas.insert(graph_meta.plugin_metas.end(),
                                     all_plugin_metas.begin(),
                                     all_plugin_metas.end());
      return seastar::make_ready_future<admin_query_result>(
          gs::Result<seastar::sstring>(std::move(graph_meta.ToJson())));
    } else {
      LOG(ERROR) << "Fail to get all procedures: "
                 << get_all_procedure_res.status().error_message() << " for "
                 << query_param.content;
      return seastar::make_ready_future<admin_query_result>(
          gs::Result<seastar::sstring>(get_all_procedure_res.status()));
    }
  } else {
    LOG(ERROR) << "Fail to get graph schema: "
               << meta_res.status().error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(meta_res.status()));
  }
}

// list all graphs
seastar::future<admin_query_result> admin_actor::run_list_graphs(
    query_param&& query_param) {
  LOG(INFO) << "List all graphs.";
  auto all_graph_meta_res = metadata_store_->GetAllGraphMeta();
  // The plugin meta are stored separately, so we need to merge them.
  if (!all_graph_meta_res.ok()) {
    LOG(ERROR) << "Fail to list graphs: "
               << all_graph_meta_res.status().error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(all_graph_meta_res.status()));
  } else {
    VLOG(10) << "Successfully list graphs";
    // collect all 'schema' field into a json stirng
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(merge_graph_and_plugin_meta(
            metadata_store_, all_graph_meta_res.value())));
  }
}

// delete one graph
seastar::future<admin_query_result> admin_actor::run_delete_graph(
    query_param&& query_param) {
  LOG(INFO) << "Delete graph: " << query_param.content;

  auto lock_info = metadata_store_->GetGraphIndicesLocked(query_param.content);
  if (!lock_info.ok()) {
    LOG(ERROR) << "Fail to get lock info for graph: " << query_param.content;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(lock_info.status()));
  }
  if (lock_info.value()) {
    LOG(ERROR) << "Graph is running, cannot delete: " << query_param.content;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(gs::Status(
            gs::StatusCode::AlreadyLocked,
            "Graph is running, cannot delete: " + query_param.content)));
  }

  auto get_res = metadata_store_->GetGraphMeta(query_param.content);
  if (!get_res.ok()) {
    LOG(ERROR) << "Graph not exists: " << query_param.content;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(get_res.status()));
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
            gs::Status(gs::StatusCode::InvalidImportFile,
                       "Fail to parse json: " + std::string(e.what()))));
  } catch (...) {
    LOG(ERROR) << "Fail to parse json: " << loading_config;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(gs::Status(
            gs::StatusCode::InvalidImportFile, "Fail to parse json: ")));
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

  // try to lock the graph indices dir
  auto lock_res = metadata_store_->LockGraphIndices(graph_id);
  if (!lock_res.ok() || !lock_res.value()) {
    LOG(ERROR) << "Fail to lock graph indices dir: " << graph_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(gs::Status(
            gs::StatusCode::AlreadyLocked,
            "Fail to acquire lock for graph indices dir: " + graph_id +
                ", maybe the graph is already running")));
  }

  std::string graph_id_str = graph_id.c_str();
  auto job_id_res = invoke_loading_graph(metadata_store_, graph_id_str, yaml,
                                         loading_thread_num);
  if (!job_id_res.ok()) {
    LOG(ERROR) << "Fail to run graph loading : "
               << job_id_res.status().error_message();
    metadata_store_->UnlockGraphIndices(graph_id);
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
    for (auto& plugin_meta : graph_meta_res.value().plugin_metas) {
      add_runnable_info(plugin_meta);
    }
    all_plugin_metas.insert(all_plugin_metas.end(),
                            graph_meta_res.value().plugin_metas.begin(),
                            graph_meta_res.value().plugin_metas.end());
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

  auto lock_res = metadata_store_->LockGraphPlugins(graph_id);
  if (!lock_res.ok() || !lock_res.value()) {
    LOG(ERROR) << "Fail to lock graph plugin dir: " << graph_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(gs::Status(
            gs::StatusCode::AlreadyLocked,
            "Fail to acquire lock for graph plugin dir: " + graph_id +
                ", try again later")));
  }

  return invoke_creating_procedure(metadata_store_, graph_id, parameter)
      .then_wrapped([this, graph_id = graph_id](auto&& f) {
        auto unlock_res = metadata_store_->UnlockGraphPlugins(graph_id);
        if (!unlock_res.ok()) {
          LOG(ERROR) << "Fail to unlock graph plugin dir: " << graph_id;
        }
        try {
          auto res = f.get();
          return seastar::make_ready_future<admin_query_result>(
              gs::Result<seastar::sstring>(std::move(res)));
        } catch (std::exception& e) {
          LOG(ERROR) << "Fail to create procedure: " << e.what();
          return seastar::make_ready_future<admin_query_result>(
              gs::Result<seastar::sstring>(gs::Status(
                  gs::StatusCode::InternalError,
                  "Fail to create procedure: " + std::string(e.what()))));
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
        gs::Result<seastar::sstring>(
            gs::Status(gs::StatusCode::NotFound,
                       "Procedure " + procedure_id +
                           " not exists on graph: " + graph_id)));
  }

  auto lock_res = metadata_store_->LockGraphPlugins(graph_id);
  if (!lock_res.ok() || !lock_res.value()) {
    LOG(ERROR) << "Fail to lock graph plugin dir: " << graph_id;
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(gs::Status(
            gs::StatusCode::AlreadyLocked,
            "Fail to acquire lock for graph plugin dir: " + graph_id +
                ", try again later")));
  }

  auto delete_res =
      invoke_delete_plugin_meta(metadata_store_, graph_id, procedure_id);
  auto unlock_res = metadata_store_->UnlockGraphPlugins(graph_id);
  if (!unlock_res.ok()) {
    LOG(ERROR) << "Fail to unlock graph plugin dir: " << graph_id;
  }
  if (!delete_res.ok()) {
    LOG(ERROR) << "Fail to run delete procedure: "
               << delete_res.error_message();
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(delete_res));
  }

  VLOG(10) << "Successfully delete procedure: " << procedure_id;
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
  VLOG(10) << "update request json: " << update_request_json;
  auto req = gs::UpdatePluginMetaRequest::FromJson(update_request_json);
  VLOG(10) << "Update plugin req: " << req.ToString();
  // If updatePluginMetaRequest contains field params, returns, library, and
  // option, we warning and return.
  if (req.params.has_value() || req.returns.has_value() ||
      req.library.has_value() || req.option.has_value()) {
    LOG(ERROR) << "UpdatePluginMetaRequest contains field params, returns, "
                  "library, or option, which should not be updated.";
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(gs::Status(
            gs::StatusCode::IllegalOperation,
            "UpdatePluginMetaRequest contains field params, returns, library, "
            "and option, which should not be updated.")));
  }

  if (req.name.has_value()) {
    LOG(ERROR) << "UpdatePluginMetaRequest contains field 'name', which should "
                  "not be updated.";
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::Status(gs::StatusCode::IllegalOperation,
                       "UpdatePluginMetaRequest contains field "
                       "'name', which should not be updated.")));
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
    LOG(INFO) << "No running graph, will start on the graph in request";
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
            gs::Status(gs::StatusCode::InvalidSchema,
                       "Fail to parse json: " + std::string(e.what()))));
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
          gs::Result<seastar::sstring>(gs::Status(
              gs::StatusCode::AlreadyLocked,
              "The graph is locked but not running: " + graph_name +
                  ", maybe a data loading job is running on this graph")));
    }
  } else {
    LOG(INFO) << "The graph is not locked: " << graph_name;
    auto acquire_lock_res = metadata_store_->LockGraphIndices(graph_name);
    if (!acquire_lock_res.ok() || !acquire_lock_res.value()) {
      LOG(ERROR) << "Fail to lock graph: " << graph_name;
      return seastar::make_ready_future<admin_query_result>(
          gs::Result<seastar::sstring>(
              gs::Status(gs::StatusCode::AlreadyLocked,
                         "Fail to acquire lock for graph: " + graph_name +
                             ", try again later")));
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
    if (!prev_lock) {  // If the graph is not locked before, and we fail at
                       // some steps after locking, we should unlock it.
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
      VLOG(10) << "Closed the previous graph db";
      if (!db.Open(schema_value, data_dir_value, thread_num).ok()) {
        LOG(ERROR) << "Fail to load graph from data directory: "
                   << data_dir_value;
        if (!prev_lock) {  // If the graph is not locked before, and we
                           // fail at some steps after locking, we should
                           // unlock it.
          metadata_store_->UnlockGraphIndices(graph_name);
        }
        return seastar::make_ready_future<admin_query_result>(
            gs::Result<seastar::sstring>(gs::Status(
                gs::StatusCode::InternalError,
                "Fail to load graph from data directory: " + data_dir_value)));
      }
      LOG(INFO) << "Successfully load graph from data directory: "
                << data_dir_value;
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
      LOG(INFO) << "Update running graph to: " << graph_name;
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
          gs::Result<seastar::sstring>(gs::Status(gs::StatusCode::InternalError,
                                                  "Fail to start compiler")));
    }
    LOG(INFO) << "Successfully started service with graph: " << graph_name;
    hqps_service.reset_start_time();
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
  return hqps_service.stop_query_actors().then([this, &hqps_service] {
    LOG(INFO) << "Successfully stopped query handler";
    // Add also remove current running graph
    {
      std::lock_guard<std::mutex> lock(mtx_);
      // unlock the graph
      auto cur_running_graph_res = metadata_store_->GetRunningGraph();
      if (cur_running_graph_res.ok()) {
        auto unlock_res =
            metadata_store_->UnlockGraphIndices(cur_running_graph_res.value());
        if (!unlock_res.ok()) {
          LOG(ERROR) << "Fail to unlock graph: "
                     << cur_running_graph_res.value();
          return seastar::make_ready_future<admin_query_result>(
              gs::Result<seastar::sstring>(unlock_res.status()));
        }
        if (!metadata_store_->ClearRunningGraph().ok()) {
          LOG(ERROR) << "Fail to clear running graph";
          return seastar::make_ready_future<admin_query_result>(
              gs::Result<seastar::sstring>(
                  gs::Status(gs::StatusCode::InternalError,
                             "Fail to clear running graph")));
        }
      }

      if (hqps_service.stop_compiler_subprocess()) {
        LOG(INFO) << "Successfully stop compiler";
        return seastar::make_ready_future<admin_query_result>(
            gs::Result<seastar::sstring>("Successfully stop service"));
      } else {
        LOG(ERROR) << "Fail to stop compiler";
        return seastar::make_ready_future<admin_query_result>(
            gs::Result<seastar::sstring>(gs::Status(
                gs::StatusCode::InternalError, "Fail to stop compiler")));
      }
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
    res["hqps_port"] = query_port;
    res["bolt_port"] = hqps_service.get_service_config().bolt_port;
    res["gremlin_port"] = hqps_service.get_service_config().gremlin_port;
    if (running_graph_res.ok()) {
      auto graph_meta_res =
          metadata_store_->GetGraphMeta(running_graph_res.value());
      if (graph_meta_res.ok()) {
        auto& graph_meta = graph_meta_res.value();
        // Add the plugin meta.
        auto get_all_procedure_res =
            metadata_store_->GetAllPluginMeta(running_graph_res.value());
        if (get_all_procedure_res.ok()) {
          VLOG(10) << "Successfully get all procedures: "
                   << get_all_procedure_res.value().size();
          auto& all_plugin_metas = get_all_procedure_res.value();
          VLOG(10) << "original all plugins : " << all_plugin_metas.size();
          for (auto& plugin_meta : all_plugin_metas) {
            add_runnable_info(plugin_meta);
          }
          for (auto& plugin_meta : graph_meta.plugin_metas) {
            add_runnable_info(plugin_meta);
          }

          VLOG(10) << "original graph meta: " << graph_meta.plugin_metas.size();
          for (auto& plugin_meta : all_plugin_metas) {
            if (plugin_meta.runnable) {
              graph_meta.plugin_metas.emplace_back(plugin_meta);
            }
          }
          VLOG(10) << "got graph meta: " << graph_meta.ToJson();
          res["graph"] = nlohmann::json::parse(graph_meta.ToJson());
        } else {
          LOG(ERROR) << "Fail to get all procedures: "
                     << get_all_procedure_res.status().error_message();
          return seastar::make_exception_future<admin_query_result>(
              get_all_procedure_res.status());
        }
      } else {
        LOG(ERROR) << "Fail to get graph meta: "
                   << graph_meta_res.status().error_message();
        res["graph"] = {};
        return seastar::make_exception_future<admin_query_result>(
            graph_meta_res.status());
      }
    } else {
      res["graph"] = {};
      LOG(ERROR) << "Fail to get running graph: "
                 << running_graph_res.status().error_message();
      return seastar::make_exception_future<admin_query_result>(
          running_graph_res.status());
    }
    res["start_time"] = hqps_service.get_start_time();
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
        gs::Result<seastar::sstring>(
            gs::Status(gs::StatusCode::IllegalOperation,
                       "Job already " + std::to_string(job_meta.status) + ": " +
                           job_id.c_str())));
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
        gs::Result<seastar::sstring>(gs::Status(
            gs::StatusCode::InternalError,
            "Fail to kill process: " + std::to_string(job_meta.process_id) +
                ", error message: " + ec.message())));
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

// Get the statistics of the current running graph, if no graph is running,
// return empty.
seastar::future<admin_query_result> admin_actor::run_get_graph_statistic(
    query_param&& query_param) {
  std::string queried_graph = query_param.content.c_str();
  auto cur_running_graph_res = metadata_store_->GetRunningGraph();
  if (!cur_running_graph_res.ok()) {
    // no graph is running
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(gs::Status(
            gs::StatusCode::NotFound, "No graph is running currently")));
  }
  auto& graph_id = cur_running_graph_res.value();
  if (graph_id != queried_graph) {
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(
            gs::Status(gs::StatusCode::NotFound,
                       "The queried graph is not running: " + graph_id +
                           ", current running graph is: " + queried_graph)));
  }
  auto statistics = get_graph_statistics(
      gs::GraphDB::get().GetSession(hiactor::local_shard_id()));
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(statistics.ToJson()));
}

}  // namespace server
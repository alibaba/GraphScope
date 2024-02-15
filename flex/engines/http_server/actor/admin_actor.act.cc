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

#include "flex/engines/http_server/actor/admin_actor.act.h"

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/http_server/codegen_proxy.h"
#include "flex/engines/http_server/workdir_manipulator.h"
#include "flex/utils/service_utils.h"
#include "nlohmann/json.hpp"

#include <seastar/core/print.hh>

namespace server {

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
}

// Create a new Graph with the passed graph config.
seastar::future<query_result> admin_actor::run_create_graph(
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
    return seastar::make_exception_future<query_result>(
        std::runtime_error("Fail to parse json: " + std::string(e.what())));
  } catch (...) {
    LOG(ERROR) << "Fail to parse json: " << query_param.content;
    return seastar::make_exception_future<query_result>(
        std::runtime_error("Fail to parse json: " + query_param.content));
  }

  auto result = server::WorkDirManipulator::CreateGraph(yaml);

  if (result.ok()) {
    VLOG(10) << "Successfully created graph";
    return seastar::make_ready_future<query_result>(std::move(result.value()));
  } else {
    LOG(ERROR) << "Fail to create graph: " << result.status().error_message();
    return seastar::make_exception_future<query_result>(std::runtime_error(
        "Fail to create graph: " + result.status().error_message()));
  }
}

// get graph schema
// query_param is the graph name
seastar::future<query_result> admin_actor::run_get_graph_schema(
    query_param&& query_param) {
  LOG(INFO) << "Get Graph schema for graph: " << query_param.content;

  auto schema_result =
      server::WorkDirManipulator::GetGraphSchemaString(query_param.content);
  if (schema_result.ok()) {
    return seastar::make_ready_future<query_result>(
        std::move(schema_result.value()));
  } else {
    LOG(ERROR) << "Fail to get graph schema: "
               << schema_result.status().error_message();
    return seastar::make_exception_future<query_result>(std::runtime_error(
        "Fail to get graph schema: " + schema_result.status().error_message()));
  }
}

// list all graphs
seastar::future<query_result> admin_actor::run_list_graphs(
    query_param&& query_param) {
  LOG(INFO) << "List all graphs.";
  auto list_result = server::WorkDirManipulator::ListGraphs();
  if (!list_result.ok()) {
    LOG(ERROR) << "Fail to list graphs: "
               << list_result.status().error_message();
    return seastar::make_exception_future<query_result>(std::runtime_error(
        "Fail to list graphs: " + list_result.status().error_message()));
  } else {
    VLOG(10) << "Successfully list graphs";
    return seastar::make_ready_future<query_result>(
        std::move(list_result.value()));
  }
}

// delete one graph
seastar::future<query_result> admin_actor::run_delete_graph(
    query_param&& query_param) {
  LOG(INFO) << "Delete graph: " << query_param.content;

  auto delete_res =
      server::WorkDirManipulator::DeleteGraph(query_param.content);
  if (delete_res.ok()) {
    return seastar::make_ready_future<query_result>(
        std::move(delete_res.value()));
  } else {
    LOG(ERROR) << "Fail to delete graph: "
               << delete_res.status().error_message();
    return seastar::make_exception_future<query_result>(std::runtime_error(
        "Fail to delete graph: " + delete_res.status().error_message()));
  }
}

// load the graph.
seastar::future<query_result> admin_actor::run_graph_loading(
    graph_management_param&& query_param) {
  // query_param contains two parameter, first for graph name, second for graph
  // config
  auto content = query_param.content;
  auto& graph_name = content.first;
  VLOG(1) << "Parse json payload for graph: " << graph_name;
  auto& graph_config = content.second;

  YAML::Node yaml;
  try {
    // parse json from query_param.content
    nlohmann::json json = nlohmann::json::parse(graph_config);
    std::stringstream json_ss;
    json_ss << graph_config;
    yaml = YAML::Load(json_ss);
  } catch (std::exception& e) {
    LOG(ERROR) << "Fail to parse json: " << e.what();
    return seastar::make_exception_future<query_result>(
        std::runtime_error("Fail to parse json: " + std::string(e.what())));
  } catch (...) {
    LOG(ERROR) << "Fail to parse json: " << graph_config;
    return seastar::make_exception_future<query_result>(std::runtime_error(
        "Fail to parse json when running dataloading for : " + graph_name));
  }
  int32_t loading_thread_num = 1;
  if (yaml["loading_thread_num"]) {
    loading_thread_num = yaml["loading_thread_num"].as<int32_t>();
  }

  auto graph_loading_res = server::WorkDirManipulator::LoadGraph(
      graph_name, yaml, loading_thread_num);

  if (graph_loading_res.ok()) {
    VLOG(10) << "Successfully loaded graph";
    return seastar::make_ready_future<query_result>(
        std::move(graph_loading_res.value()));
  } else {
    LOG(ERROR) << "Fail to load graph: "
               << graph_loading_res.status().error_message();
    return seastar::make_exception_future<query_result>(std::runtime_error(
        "Fail to load graph: " + graph_loading_res.status().error_message()));
  }
}

// Get all procedure with graph_name and procedure_name
seastar::future<query_result> admin_actor::get_procedure_by_procedure_name(
    procedure_query_param&& query_param) {
  auto& graph_name = query_param.content.first;
  auto& procedure_name = query_param.content.second;
  LOG(INFO) << "Get procedure: " << procedure_name
            << " for graph: " << graph_name;
  auto get_procedure_res =
      server::WorkDirManipulator::GetProcedureByGraphAndProcedureName(
          graph_name, procedure_name);
  if (get_procedure_res.ok()) {
    VLOG(10) << "Successfully get procedure procedures";
    return seastar::make_ready_future<query_result>(
        std::move(get_procedure_res.value()));
  } else {
    LOG(ERROR) << "Fail to get procedure for graph: " << graph_name
               << " and procedure: " << procedure_name << ", error message: "
               << get_procedure_res.status().error_message();
    return seastar::make_exception_future<query_result>(
        std::runtime_error("Fail to get procedure: " +
                           get_procedure_res.status().error_message()));
  }
}

// Get all procedures of one graph.
seastar::future<query_result> admin_actor::get_procedures_by_graph_name(
    query_param&& query_param) {
  auto& graph_name = query_param.content;
  auto get_all_procedure_res =
      server::WorkDirManipulator::GetProceduresByGraphName(graph_name);
  if (get_all_procedure_res.ok()) {
    VLOG(10) << "Successfully get all procedures: "
             << get_all_procedure_res.value();
    return seastar::make_ready_future<query_result>(
        std::move(get_all_procedure_res.value()));
  } else {
    LOG(ERROR) << "Fail to get all procedures: "
               << get_all_procedure_res.status().error_message();
    return seastar::make_exception_future<query_result>(
        std::runtime_error("Fail to get all procedures: " +
                           get_all_procedure_res.status().error_message()));
  }
}

seastar::future<query_result> admin_actor::create_procedure(
    create_procedure_query_param&& query_param) {
  auto& graph_name = query_param.content.first;
  auto& parameter = query_param.content.second;
  return server::WorkDirManipulator::CreateProcedure(graph_name, parameter)
      .then_wrapped([](auto&& f) {
        try {
          auto res = f.get();
          return seastar::make_ready_future<query_result>(
              query_result{std::move(res)});
        } catch (std::exception& e) {
          LOG(ERROR) << "Fail to create procedure: " << e.what();
          return seastar::make_exception_future<query_result>(
              std::runtime_error("Fail to create procedure: " +
                                 std::string(e.what())));
        }
      });
}

// Delete a procedure by graph name and procedure name
seastar::future<query_result> admin_actor::delete_procedure(
    create_procedure_query_param&& query_param) {
  auto& graph_name = query_param.content.first;
  auto& procedure_name = query_param.content.second;
  auto delete_procedure_res =
      server::WorkDirManipulator::DeleteProcedure(graph_name, procedure_name);
  if (delete_procedure_res.ok()) {
    VLOG(10) << "Successfully get all procedures";
    return seastar::make_ready_future<query_result>(
        std::move(delete_procedure_res.value()));
  } else {
    LOG(ERROR) << "Fail to create procedure: "
               << delete_procedure_res.status().error_message();
    return seastar::make_exception_future<query_result>(
        std::runtime_error("Fail to create procedures: " +
                           delete_procedure_res.status().error_message()));
  }
}

// update a procedure by graph name and procedure name
seastar::future<query_result> admin_actor::update_procedure(
    update_procedure_query_param&& query_param) {
  auto& graph_name = std::get<0>(query_param.content);
  auto& procedure_name = std::get<1>(query_param.content);
  auto& parameter = std::get<2>(query_param.content);
  auto update_procedure_res = server::WorkDirManipulator::UpdateProcedure(
      graph_name, procedure_name, parameter);
  if (update_procedure_res.ok()) {
    VLOG(10) << "Successfully update procedure: " << procedure_name;
    return seastar::make_ready_future<query_result>(
        std::move(update_procedure_res.value()));
  } else {
    LOG(ERROR) << "Fail to create procedure: "
               << update_procedure_res.status().error_message();
    return seastar::make_exception_future<query_result>(
        std::runtime_error("Fail to create procedures: " +
                           update_procedure_res.status().error_message()));
  }
}

// Start service on a graph first means stop all current running actors, then
// switch graph and create new actors with a unused scope_id.
seastar::future<query_result> admin_actor::start_service(
    query_param&& query_param) {
  // parse query_param.content as json and get graph_name
  auto& content = query_param.content;
  std::string graph_name;
  try {
    if (!content.empty()) {
      nlohmann::json json = nlohmann::json::parse(content);
      if (json.contains("graph_name")) {
        graph_name = json["graph_name"].get<std::string>();
      }
    } else {
      graph_name = server::WorkDirManipulator::GetRunningGraph();
      LOG(WARNING)
          << "Request payload is empty, will restart on current graph: "
          << server::WorkDirManipulator::GetRunningGraph();
    }
    LOG(WARNING) << "Starting service with graph: " << graph_name;
  } catch (std::exception& e) {
    LOG(ERROR) << "Fail to Start service: ";
    return seastar::make_exception_future<query_result>(
        std::runtime_error(e.what()));
  }

  auto schema_result = server::WorkDirManipulator::GetGraphSchema(graph_name);
  if (!schema_result.ok()) {
    LOG(ERROR) << "Fail to get graph schema: "
               << schema_result.status().error_message() << ", " << graph_name;
    return seastar::make_exception_future<query_result>(std::runtime_error(
        "Fail to get graph schema: " + schema_result.status().error_message() +
        ", " + graph_name));
  }
  auto& schema_value = schema_result.value();
  auto data_dir = server::WorkDirManipulator::GetDataDirectory(graph_name);
  if (!data_dir.ok()) {
    LOG(ERROR) << "Fail to get data directory: "
               << data_dir.status().error_message();
    return seastar::make_exception_future<query_result>(std::runtime_error(
        "Fail to get data directory: " + data_dir.status().error_message()));
  }
  auto data_dir_value = data_dir.value();

  // First Stop query_handler's actors.

  auto& hqps_service = HQPSService::get();
  return hqps_service.stop_query_actors().then([this, graph_name, schema_value,
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
        return seastar::make_exception_future<query_result>(std::runtime_error(
            "Fail to load graph from data directory: " + data_dir_value));
      }
      server::WorkDirManipulator::SetRunningGraph(graph_name);
    }
    hqps_service.start_query_actors();  // start on a new scope.
    LOG(INFO) << "Successfully restart query actors";
    LOG(INFO) << "Successfully started service with graph: " << graph_name;
    return seastar::make_ready_future<query_result>(
        "Successfully start service");
  });
}

// get service status
seastar::future<query_result> admin_actor::service_status(
    query_param&& query_param) {
  auto& hqps_service = HQPSService::get();
  auto query_port = hqps_service.get_query_port();
  nlohmann::json res;
  if (query_port != 0) {
    res["status"] = "running";
    res["query_port"] = query_port;
    res["graph_name"] = server::WorkDirManipulator::GetRunningGraph();
  } else {
    LOG(INFO) << "Query service has not been inited!";
    res["status"] = "Query service has not been inited!";
  }
  return seastar::make_ready_future<query_result>(res.dump());
}

// get node status.
seastar::future<query_result> admin_actor::node_status(
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
  return seastar::make_ready_future<query_result>(json.dump());
}

}  // namespace server
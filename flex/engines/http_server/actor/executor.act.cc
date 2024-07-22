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

#include "flex/engines/http_server/actor/executor.act.h"

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/database/manager.h"
#include "flex/engines/http_server/service/hqps_service.h"
#include "flex/utils/service_utils.h"
#include "nlohmann/json.hpp"

#include <seastar/core/print.hh>

namespace server {

executor::~executor() {
  // finalization
  // ...
}

executor::executor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr)
    : hiactor::actor(exec_ctx, addr) {
  set_max_concurrency(1);  // set max concurrency for task reentrancy (stateful)
  // initialization
  // ...
  auto& hqps_service = HQPSService::get();
  // meta_data_ should be thread safe.
  metadata_store_ = hqps_service.get_metadata_store();
}

seastar::future<query_result> executor::run_graph_db_query(
    query_param&& param) {
  auto ret = gs::GraphDB::get()
                 .GetSession(hiactor::local_shard_id())
                 .Eval(param.content);
  if (!ret.ok()) {
    LOG(ERROR) << "Eval failed: " << ret.status().error_message();
    return seastar::make_exception_future<query_result>(
      "Query failed: " + ret.status().error_message());
  }
  auto result = ret.value();
  seastar::sstring content(result.data(), result.size());
  return seastar::make_ready_future<query_result>(std::move(content));
}

inline seastar::future<admin_query_result> errorResponse(
    gs::StatusCode error_code, const std::string& error_message) {
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(gs::Status(error_code, error_message)));
}

nlohmann::json getSchemaData(
    std::shared_ptr<gs::IGraphMetaStore> metadata_store_,
    std::string graph_id) {
  auto running_graph_res = metadata_store_->GetRunningGraph();
  if (!running_graph_res.ok() || running_graph_res.value() != graph_id) {
    throw std::runtime_error("The queried graph is not running: " + graph_id);
  }
  auto graph_meta_res = metadata_store_->GetGraphMeta(graph_id);
  if (!graph_meta_res.ok()) {
    throw std::runtime_error("Graph not exists: " + graph_id);
  }
  return nlohmann::json::parse(graph_meta_res.value().schema);
}

seastar::future<admin_query_result> executor::create_vertex(
    graph_management_param&& param) {
  nlohmann::json input_json, schema_json;
  std::vector<gs::VertexData> vertex_data;
  std::vector<gs::EdgeData> edge_data;
  // Parse the input json
  try {
    input_json = nlohmann::json::parse(param.content.second);
  } catch (const std::exception& e) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         "Bad input json : " + std::string(e.what()));
  }
  // Check if the input json contains vertex_request and edge_request
  if (input_json.contains("vertex_request") == false ||
      input_json["vertex_request"].is_array() == false ||
      input_json["vertex_request"].size() == 0 ||
      (input_json.contains("edge_request") == true &&
       input_json["edge_request"].is_array() == false)) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         "Invalid input json, vertex_request and edge_request "
                         "should be array and not empty");
  }
  //  Check if the currently running graph is graph_id
  try {
    schema_json = getSchemaData(metadata_store_, param.content.first);
  } catch (const std::exception& e) {
    return errorResponse(gs::StatusCode::NotFound,
                         "Graph not exists : " + std::string(e.what()));
  }
  // input vertex data and edge data
  try {
    // vertex data
    for (auto& vertex_insert : input_json["vertex_request"]) {
      vertex_data.push_back(gs::inputVertex(vertex_insert, schema_json,
                                            hiactor::local_shard_id()));
    }
    // edge data
    for (auto& edge_insert : input_json["edge_request"]) {
      edge_data.push_back(
          gs::inputEdge(edge_insert, schema_json, hiactor::local_shard_id()));
    }
  } catch (std::exception& e) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         " Bad input parameter : " + std::string(e.what()));
  }
  try {
    gs::VertexEdgeManager::insertVertex(vertex_data, edge_data,
                                        hiactor::local_shard_id());
  } catch (std::exception& e) {
    return errorResponse(
        gs::StatusCode::InvalidSchema,
        "fail to insert vertex/edge : " + std::string(e.what()));
  }
  nlohmann::json result;
  result["message"] = "Vertex data is successfully inserted";
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(result.dump()));
}

seastar::future<admin_query_result> executor::create_edge(
    graph_management_param&& param) {
  nlohmann::json input_json, schema_json;
  std::vector<gs::VertexData> vertex_data;
  std::vector<gs::EdgeData> edge_data;
  // Parse the input json
  try {
    input_json = nlohmann::json::parse(param.content.second);
  } catch (const std::exception& e) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         "Bad input json : " + std::string(e.what()));
  }
  // Check if the input json contains edge_request
  if (input_json.is_array() == false || input_json.size() == 0) {
    return errorResponse(
        gs::StatusCode::InvalidSchema,
        "Invalid input json, edge_request should be array and not empty");
  }
  //  Check if the currently running graph is graph_id
  try {
    schema_json = getSchemaData(metadata_store_, param.content.first);
  } catch (const std::exception& e) {
    return errorResponse(gs::StatusCode::NotFound,
                         "Graph not exists : " + std::string(e.what()));
  }
  // input edge data
  try {
    for (auto& edge_insert : input_json) {
      edge_data.push_back(
          gs::inputEdge(edge_insert, schema_json, hiactor::local_shard_id()));
    }
  } catch (std::exception& e) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         " Bad input parameter : " + std::string(e.what()));
  }
  try {
    gs::VertexEdgeManager::insertEdge(edge_data, hiactor::local_shard_id());
  } catch (std::exception& e) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         "fail to insert edge : " + std::string(e.what()));
  }
  nlohmann::json result;
  result["message"] = "Edge is successfully inserted";
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(result.dump()));
}

seastar::future<admin_query_result> executor::update_vertex(
    graph_management_param&& param) {
  nlohmann::json input_json, schema_json;
  std::vector<gs::VertexData> vertex_data;
  std::vector<gs::EdgeData> edge_data;
  // Parse the input json
  try {
    input_json = nlohmann::json::parse(param.content.second);
  } catch (const std::exception& e) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         " Bad input json : " + std::string(e.what()));
  }
  //  Check if the currently running graph is graph_id
  try {
    schema_json = getSchemaData(metadata_store_, param.content.first);
  } catch (const std::exception& e) {
    return errorResponse(gs::StatusCode::NotFound,
                         "Graph not exists : " + std::string(e.what()));
  }
  // input vertex data
  try {
    vertex_data.push_back(
        gs::inputVertex(input_json, schema_json, hiactor::local_shard_id()));
  } catch (std::exception& e) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         " Bad input parameter : " + std::string(e.what()));
  }
  try {
    gs::VertexEdgeManager::updateVertex(vertex_data, hiactor::local_shard_id());
  } catch (std::exception& e) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         "fail to update vertex : " + std::string(e.what()));
  }
  nlohmann::json result;
  result["message"] = "Successfully update Vertex";
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(result.dump()));
}
seastar::future<admin_query_result> executor::update_edge(
    graph_management_param&& param) {
  nlohmann::json input_json, schema_json;
  std::vector<gs::VertexData> vertex_data;
  std::vector<gs::EdgeData> edge_data;
  // Parse the input json
  try {
    input_json = nlohmann::json::parse(param.content.second);
  } catch (const std::exception& e) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         " Bad input json : " + std::string(e.what()));
  }
  //  Check if the currently running graph is graph_id
  try {
    schema_json = getSchemaData(metadata_store_, param.content.first);
  } catch (const std::exception& e) {
    return errorResponse(gs::StatusCode::NotFound,
                         "Graph not exists : " + std::string(e.what()));
  }
  // input edge data
  try {
    edge_data.push_back(
        gs::inputEdge(input_json, schema_json, hiactor::local_shard_id()));
  } catch (std::exception& e) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         " Bad input parameter : " + std::string(e.what()));
  }
  try {
    gs::VertexEdgeManager::updateEdge(edge_data, hiactor::local_shard_id());
  } catch (std::exception& e) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         "fail to update edge : " + std::string(e.what()));
  }
  nlohmann::json result;
  result["message"] = "Successfully update Edge";
  return seastar::make_ready_future<admin_query_result>(
      gs::Result<seastar::sstring>(result.dump()));
}

seastar::future<admin_query_result> executor::get_vertex(
    graph_management_query_param&& param) {
  nlohmann::json schema_json, result;
  std::vector<gs::VertexData> vertex_data;
  std::vector<gs::EdgeData> edge_data;
  // Parse the input
  auto query_params = std::move(param.content.second);
  //  Check if the currently running graph is graph_id
  try {
    schema_json = getSchemaData(metadata_store_, param.content.first);
  } catch (const std::exception& e) {
    return errorResponse(gs::StatusCode::NotFound,
                         "Graph not exists : " + std::string(e.what()));
  }
  // input vertex data
  try {
    gs::VertexData vertex;
    std::string label = query_params["label"];
    result["label"] = label;
    vertex.pk_value = gs::Any(std::string(query_params["primary_key_value"]));
    gs::VertexEdgeManager::checkVertexSchema(schema_json, vertex, label);
    gs::VertexEdgeManager::getVertexLabelId(vertex, label,
                                            hiactor::local_shard_id());
    vertex_data.push_back(vertex);
  } catch (std::exception& e) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         " Bad input parameter : " + std::string(e.what()));
  }
  try {
    result["values"] = gs::VertexEdgeManager::getVertex(
        vertex_data, hiactor::local_shard_id());
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(result.dump()));
  } catch (std::exception& e) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         "fail to get vertex : " + std::string(e.what()));
  }
}

seastar::future<admin_query_result> executor::get_edge(
    graph_management_query_param&& param) {
  nlohmann::json schema_json, result;
  std::vector<gs::VertexData> vertex_data;
  std::vector<gs::EdgeData> edge_data;
  // Parse the input json
  auto query_params = std::move(param.content.second);
  //  Check if the currently running graph is graph_id
  try {
    schema_json = getSchemaData(metadata_store_, param.content.first);
  } catch (const std::exception& e) {
    return errorResponse(gs::StatusCode::NotFound,
                         "Graph not exists : " + std::string(e.what()));
  }
  // input edge data
  try {
    gs::EdgeData edge;
    std::string src_label = query_params["src_label"];
    std::string dst_label = query_params["dst_label"];
    std::string edge_label = query_params["edge_label"];
    std::string src_pk_value = query_params["src_primary_key_value"];
    std::string dst_pk_value = query_params["dst_primary_key_value"];
    edge.src_pk_value = gs::Any(src_pk_value);
    edge.dst_pk_value = gs::Any(dst_pk_value);
    gs::VertexEdgeManager::checkEdgeSchema(schema_json, edge, src_label,
                                           dst_label, edge_label);
    gs::VertexEdgeManager::getEdgeLabelId(
        edge, src_label, dst_label, edge_label, hiactor::local_shard_id());
    edge_data.push_back(edge);
    result["src_label"] = src_label;
    result["dst_label"] = dst_label;
    result["edge_label"] = edge_label;
    result["src_primary_key_value"] = src_pk_value;
    result["dst_primary_key_value"] = dst_pk_value;
  } catch (std::exception& e) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         " Bad input parameter : " + std::string(e.what()));
  }
  try {
    result["properties"] =
        gs::VertexEdgeManager::getEdge(edge_data, hiactor::local_shard_id());
    return seastar::make_ready_future<admin_query_result>(
        gs::Result<seastar::sstring>(result.dump()));
  } catch (std::exception& e) {
    return errorResponse(gs::StatusCode::InvalidSchema,
                         "fail to get edge : " + std::string(e.what()));
  }
}

seastar::future<admin_query_result> executor::delete_vertex(
    graph_management_param&& param) {
  return errorResponse(gs::StatusCode::NotFound,
                       "delete_vertex is not implemented");
}

seastar::future<admin_query_result> executor::delete_edge(
    graph_management_param&& param) {
  return errorResponse(gs::StatusCode::NotFound,
                       "delete_vertex is not implemented");
}

}  // namespace server

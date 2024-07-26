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

#include "flex/engines/graph_db/database/graph_db_operations.h"
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <vector>
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/utils/service_utils.h"

namespace gs {

Result<std::string> GraphDBOperations::CreateVertex(
    GraphDBSession& session, nlohmann::json&& input_json) {
  std::vector<VertexData> vertex_data;
  std::vector<EdgeData> edge_data;
  // Check if the input json contains vertex_request and edge_request
  if (input_json.contains("vertex_request") == false ||
      input_json["vertex_request"].is_array() == false ||
      input_json["vertex_request"].size() == 0 ||
      (input_json.contains("edge_request") == true &&
       input_json["edge_request"].is_array() == false)) {
    return response(StatusCode::InvalidSchema,
                    "Invalid input json, vertex_request and edge_request "
                    "should be array and not empty");
  }
  const Schema& schema = session.schema();
  // input vertex data and edge data
  try {
    // vertex data
    for (auto& vertex_insert : input_json["vertex_request"]) {
      vertex_data.push_back(inputVertex(vertex_insert, schema, session));
    }
    // edge data
    for (auto& edge_insert : input_json["edge_request"]) {
      edge_data.push_back(inputEdge(edge_insert, schema, session));
    }
  } catch (std::exception& e) {
    return response(StatusCode::InvalidSchema,
                    " Bad input parameter : " + std::string(e.what()));
  }
  try {
    insertVertex(std::move(vertex_data), std::move(edge_data), session);
  } catch (std::exception& e) {
    return response(StatusCode::InvalidSchema,
                    "fail to insert vertex/edge : " + std::string(e.what()));
  }
  nlohmann::json result;
  result["message"] = "Vertex data is successfully inserted";
  return response(StatusCode::OK, result.dump());
}
Result<std::string> GraphDBOperations::CreateEdge(GraphDBSession& session,
                                                  nlohmann::json&& input_json) {
  std::vector<VertexData> vertex_data;
  std::vector<EdgeData> edge_data;
  // Check if the input json contains edge_request
  if (input_json.is_array() == false || input_json.size() == 0) {
    return response(
        StatusCode::InvalidSchema,
        "Invalid input json, edge_request should be array and not empty");
  }
  const Schema& schema = session.schema();
  // input edge data
  try {
    for (auto& edge_insert : input_json) {
      edge_data.push_back(inputEdge(edge_insert, schema, session));
    }
  } catch (std::exception& e) {
    return response(StatusCode::InvalidSchema,
                    " Bad input parameter : " + std::string(e.what()));
  }
  try {
    insertEdge(std::move(edge_data), session);
  } catch (std::exception& e) {
    return response(StatusCode::InvalidSchema,
                    "fail to insert edge : " + std::string(e.what()));
  }
  nlohmann::json result;
  result["message"] = "Edge is successfully inserted";
  return response(StatusCode::OK, result.dump());
}
Result<std::string> GraphDBOperations::UpdateVertex(
    GraphDBSession& session, nlohmann::json&& input_json) {
  std::vector<VertexData> vertex_data;
  std::vector<EdgeData> edge_data;
  const Schema& schema = session.schema();
  // input vertex data
  try {
    vertex_data.push_back(inputVertex(input_json, schema, session));
  } catch (std::exception& e) {
    return response(StatusCode::InvalidSchema,
                    " Bad input parameter : " + std::string(e.what()));
  }
  try {
    updateVertex(std::move(vertex_data), session);
  } catch (std::exception& e) {
    return response(StatusCode::InvalidSchema,
                    "fail to update vertex : " + std::string(e.what()));
  }
  nlohmann::json result;
  result["message"] = "Successfully update Vertex";
  return response(StatusCode::OK, result.dump());
}
Result<std::string> GraphDBOperations::UpdateEdge(GraphDBSession& session,
                                                  nlohmann::json&& input_json) {
  std::vector<VertexData> vertex_data;
  std::vector<EdgeData> edge_data;
  const Schema& schema = session.schema();
  // input edge data
  try {
    edge_data.push_back(inputEdge(input_json, schema, session));
  } catch (std::exception& e) {
    return response(StatusCode::InvalidSchema,
                    " Bad input parameter : " + std::string(e.what()));
  }
  try {
    updateEdge(std::move(edge_data), session);
  } catch (std::exception& e) {
    return response(StatusCode::InvalidSchema,
                    "fail to update edge : " + std::string(e.what()));
  }
  nlohmann::json result;
  result["message"] = "Successfully update Edge";
  return response(StatusCode::OK, result.dump());
}
Result<std::string> GraphDBOperations::GetVertex(
    GraphDBSession& session,
    std::unordered_map<std::string, std::string>&& params) {
  nlohmann::json result;
  std::vector<VertexData> vertex_data;
  std::vector<EdgeData> edge_data;
  const Schema& schema = session.schema();
  // input vertex data
  try {
    VertexData vertex;
    std::string label = params["label"];
    result["label"] = label;
    vertex.pk_value = Any(std::string(params["primary_key_value"]));
    checkVertexSchema(schema, vertex, label);
    vertex_data.push_back(vertex);
  } catch (std::exception& e) {
    return response(StatusCode::InvalidSchema,
                    " Bad input parameter : " + std::string(e.what()));
  }
  try {
    result["values"] = getVertex(std::move(vertex_data), session);
    return response(StatusCode::OK, result.dump());
  } catch (std::exception& e) {
    return response(StatusCode::InvalidSchema,
                    "fail to get vertex : " + std::string(e.what()));
  }
}
Result<std::string> GraphDBOperations::GetEdge(
    GraphDBSession& session,
    std::unordered_map<std::string, std::string>&& params) {
  nlohmann::json result;
  std::vector<VertexData> vertex_data;
  std::vector<EdgeData> edge_data;
  const Schema& schema = session.schema();
  // input edge data
  try {
    EdgeData edge;
    std::string src_label = params["src_label"];
    std::string dst_label = params["dst_label"];
    std::string edge_label = params["edge_label"];
    std::string src_pk_value = params["src_primary_key_value"];
    std::string dst_pk_value = params["dst_primary_key_value"];
    edge.src_pk_value = Any(src_pk_value);
    edge.dst_pk_value = Any(dst_pk_value);
    checkEdgeSchema(schema, edge, src_label, dst_label, edge_label);
    edge_data.push_back(edge);
    result["src_label"] = src_label;
    result["dst_label"] = dst_label;
    result["edge_label"] = edge_label;
    result["src_primary_key_value"] = src_pk_value;
    result["dst_primary_key_value"] = dst_pk_value;
  } catch (std::exception& e) {
    return response(StatusCode::InvalidSchema,
                    " Bad input parameter : " + std::string(e.what()));
  }
  try {
    result["properties"] = getEdge(std::move(edge_data), session);
    return response(StatusCode::OK, result.dump());
  } catch (std::exception& e) {
    return response(StatusCode::InvalidSchema,
                    "fail to get edge : " + std::string(e.what()));
  }
}
Result<std::string> GraphDBOperations::DeleteVertex(
    GraphDBSession& session, nlohmann::json&& input_json) {
  // not implemented
  return response(StatusCode::Unimplemented,
                  "delete_vertex is not implemented");
}
Result<std::string> GraphDBOperations::DeleteEdge(GraphDBSession& session,
                                                  nlohmann::json&& input_json) {
  // not implemented
  return response(StatusCode::Unimplemented, "delete_edge is not implemented");
}

VertexData GraphDBOperations::inputVertex(const nlohmann::json& vertex_json,
                                          const Schema& schema,
                                          GraphDBSession& session) {
  VertexData vertex;
  std::string label = jsonToString(vertex_json["label"]);
  vertex.pk_value = Any(jsonToString(vertex_json["primary_key_value"]));
  std::unordered_set<std::string> property_names;
  for (auto& property : vertex_json["properties"]) {
    auto name_string = jsonToString(property["name"]);
    auto value_string = jsonToString(property["value"]);
    if (property_names.find(name_string) != property_names.end()) {
      throw std::runtime_error("property already exists in input properties: " +
                               name_string);
    } else {
      property_names.insert(name_string);
    }
    vertex.properties.emplace_back(name_string, Any(value_string));
  }
  checkVertexSchema(schema, vertex, label);
  return vertex;
}
EdgeData GraphDBOperations::inputEdge(const nlohmann::json& edge_json,
                                      const Schema& schema,
                                      GraphDBSession& session) {
  EdgeData edge;
  std::string src_label = jsonToString(edge_json["src_label"]);
  std::string dst_label = jsonToString(edge_json["dst_label"]);
  std::string edge_label = jsonToString(edge_json["edge_label"]);
  edge.src_pk_value = Any(jsonToString(edge_json["src_primary_key_value"]));
  edge.dst_pk_value = Any(jsonToString(edge_json["dst_primary_key_value"]));
  // Check that all parameters in the parameter
  if (edge_json["properties"].size() != 1) {
    throw std::runtime_error(
        "size should be 1(only support single property edge)");
  }
  edge.property_name = jsonToString(edge_json["properties"][0]["name"]);
  edge.property_value = Any(jsonToString(edge_json["properties"][0]["value"]));
  checkEdgeSchema(schema, edge, src_label, dst_label, edge_label);
  return edge;
}

void GraphDBOperations::checkVertexSchema(const Schema& schema,
                                          VertexData& vertex,
                                          const std::string& label) {
  vertex.label_id = schema.get_vertex_label_id(label);
  PropertyType colType =
      std::get<0>(schema.get_vertex_primary_key(vertex.label_id)[0]);
  vertex.pk_value = ConvertStringToAny(vertex.pk_value.to_string(), colType);
  auto properties_type = schema.get_vertex_properties(vertex.label_id);
  auto properties_name = schema.get_vertex_property_names(vertex.label_id);
  bool get_flag = (vertex.properties.size() == 0);
  if (vertex.properties.size() != properties_name.size() && get_flag == false) {
    throw std::runtime_error("properties size not match");
  }
  for (int col_index = 0; col_index < int(properties_name.size());
       col_index++) {
    auto property_name = properties_name[col_index];
    PropertyType colType = properties_type[col_index];
    if (get_flag) {
      vertex.properties.push_back(std::make_pair(property_name, Any()));
      continue;
    }
    if (vertex.properties[col_index].first != property_name) {
      throw std::runtime_error(
          "properties name not match, pleace check the order and name");
    }
    vertex.properties[col_index].second = ConvertStringToAny(
        vertex.properties[col_index].second.to_string(), colType);
  }
}
void GraphDBOperations::checkEdgeSchema(const Schema& schema, EdgeData& edge,
                                        const std::string& src_label,
                                        const std::string& dst_label,
                                        const std::string& edge_label) {
  edge.src_label_id = schema.get_vertex_label_id(src_label);
  edge.dst_label_id = schema.get_vertex_label_id(dst_label);
  edge.edge_label_id = schema.get_edge_label_id(edge_label);
  // get edge
  if (edge.property_name.empty()) {
    edge.property_name = schema.get_edge_property_names(
        edge.src_label_id, edge.dst_label_id, edge.edge_label_id)[0];
  } else {
    // update or add
    PropertyType colType = schema.get_edge_property(
        edge.src_label_id, edge.dst_label_id, edge.edge_label_id);
    edge.property_value =
        ConvertStringToAny(edge.property_value.to_string(), colType);
  }
  edge.src_pk_value = ConvertStringToAny(
      edge.src_pk_value.to_string(),
      std::get<0>(schema.get_vertex_primary_key(edge.src_label_id)[0]));
  edge.dst_pk_value = ConvertStringToAny(
      edge.dst_pk_value.to_string(),
      std::get<0>(schema.get_vertex_primary_key(edge.dst_label_id)[0]));
}

void GraphDBOperations::checkEdgeExistsWithInsert(
    const std::vector<EdgeData>& edge_data, GraphDBSession& session) {
  auto txn = session.GetReadTransaction();
  for (auto& edge : edge_data) {
    vid_t src_vid, dst_vid;
    if (txn.GetVertexIndex(edge.src_label_id, edge.src_pk_value, src_vid) ==
            false ||
        txn.GetVertexIndex(edge.dst_label_id, edge.dst_pk_value, dst_vid) ==
            false) {
      // It could be that this point is about to be inserted
      continue;
    }
    // If the edge already exists, just report the error
    for (auto edgeIt = txn.GetOutEdgeIterator(
             edge.src_label_id, src_vid, edge.dst_label_id, edge.edge_label_id);
         edgeIt.IsValid(); edgeIt.Next()) {
      if (edgeIt.GetNeighbor() == dst_vid) {
        txn.Abort();
        throw std::runtime_error("Edge already exists");
      }
    }
  }
}

void GraphDBOperations::checkEdgeExists(const std::vector<EdgeData>& edge_data,
                                        GraphDBSession& session) {
  auto txn = session.GetReadTransaction();
  for (auto& edge : edge_data) {
    vid_t src_vid, dst_vid;
    if (txn.GetVertexIndex(edge.src_label_id, edge.src_pk_value, src_vid) ==
            false ||
        txn.GetVertexIndex(edge.dst_label_id, edge.dst_pk_value, dst_vid) ==
            false) {
      txn.Abort();
      throw std::runtime_error("Vertex not exists");
    }
    // If the edge already exists, just report the error
    for (auto edgeIt = txn.GetOutEdgeIterator(
             edge.src_label_id, src_vid, edge.dst_label_id, edge.edge_label_id);
         edgeIt.IsValid(); edgeIt.Next()) {
      if (edgeIt.GetNeighbor() == dst_vid) {
        txn.Abort();
        throw std::runtime_error("Edge already exists");
      }
    }
  }
}

void GraphDBOperations::checkVertexExists(
    const std::vector<VertexData>& vertex_data, GraphDBSession& session) {
  auto txn = session.GetReadTransaction();
  for (auto& vertex : vertex_data) {
    vid_t vid;
    if (txn.GetVertexIndex(vertex.label_id, vertex.pk_value, vid)) {
      txn.Abort();
      throw std::runtime_error("Vertex already exists");
    }
  }
  txn.Commit();
}
void GraphDBOperations::singleInsertVertex(
    std::vector<VertexData>&& vertex_data, std::vector<EdgeData>&& edge_data,
    GraphDBSession& session) {
  auto txnWrite = session.GetSingleVertexInsertTransaction();
  for (auto& vertex : vertex_data) {
    std::vector<Any> insert_arr;
    for (auto& prop : vertex.properties) {
      insert_arr.push_back(prop.second);
    }
    if (txnWrite.AddVertex(vertex.label_id, vertex.pk_value, insert_arr) ==
        false) {
      txnWrite.Abort();
      throw std::runtime_error(
          "Fail to create vertex: " + vertex.pk_value.to_string() +
          "; All inserts are rollbacked");
    }
  }
  for (auto& edge : edge_data) {
    if (txnWrite.AddEdge(edge.src_label_id, edge.src_pk_value,
                         edge.dst_label_id, edge.dst_pk_value,
                         edge.edge_label_id, edge.property_value) == false) {
      txnWrite.Abort();
      throw std::runtime_error(
          "Fail to create edge; All inserts are rollbacked");
    }
  }
  txnWrite.Commit();
}

void GraphDBOperations::multiInsert(std::vector<VertexData>&& vertex_data,
                                    std::vector<EdgeData>&& edge_data,
                                    GraphDBSession& session) {
  auto txnWrite = session.GetInsertTransaction();
  for (auto& vertex : vertex_data) {
    std::vector<Any> insert_arr;
    for (auto& prop : vertex.properties) {
      insert_arr.push_back(prop.second);
    }
    if (txnWrite.AddVertex(vertex.label_id, vertex.pk_value, insert_arr) ==
        false) {
      txnWrite.Abort();
      throw std::runtime_error(
          "Fail to create vertex: " + vertex.pk_value.to_string() +
          "; All inserts are rollbacked");
    }
  }
  for (auto& edge : edge_data) {
    if (txnWrite.AddEdge(edge.src_label_id, edge.src_pk_value,
                         edge.dst_label_id, edge.dst_pk_value,
                         edge.edge_label_id, edge.property_value) == false) {
      txnWrite.Abort();
      throw std::runtime_error(
          "Fail to create edge; All inserts are rollbacked");
    }
  }
  txnWrite.Commit();
}
void GraphDBOperations::insertVertex(std::vector<VertexData>&& vertex_data,
                                     std::vector<EdgeData>&& edge_data,
                                     GraphDBSession& session) {
  checkVertexExists(vertex_data, session);
  checkEdgeExistsWithInsert(edge_data, session);
  if (vertex_data.size() == 1) {
    singleInsertVertex(std::move(vertex_data), std::move(edge_data), session);
  } else {
    multiInsert(std::move(vertex_data), std::move(edge_data), session);
  }
}

void GraphDBOperations::singleInsertEdge(std::vector<EdgeData>&& edge_data,
                                         GraphDBSession& session) {
  auto txnWrite = session.GetSingleEdgeInsertTransaction();
  for (auto& edge : edge_data) {
    if (txnWrite.AddEdge(edge.src_label_id, edge.src_pk_value,
                         edge.dst_label_id, edge.dst_pk_value,
                         edge.edge_label_id, edge.property_value) == false) {
      txnWrite.Abort();
      throw std::runtime_error(
          "Fail to create edge; All inserts are rollbacked");
    }
  }
  txnWrite.Commit();
}

void GraphDBOperations::insertEdge(std::vector<EdgeData>&& edge_data,
                                   GraphDBSession& session) {
  checkEdgeExists(edge_data, session);
  if (edge_data.size() == 1) {
    singleInsertEdge(std::move(edge_data), session);
  } else {
    multiInsert(std::vector<VertexData>(), std::move(edge_data), session);
  }
}

void GraphDBOperations::updateVertex(std::vector<VertexData>&& vertex_data,
                                     GraphDBSession& session) {
  const auto& vertex = vertex_data[0];
  auto txnRead = session.GetReadTransaction();
  vid_t vertex_lid;
  if (txnRead.GetVertexIndex(vertex.label_id, vertex.pk_value, vertex_lid) ==
      false) {
    txnRead.Abort();
    throw std::runtime_error("Vertex not exists");
  }
  txnRead.Commit();
  auto txnWrite = session.GetUpdateTransaction();
  for (int i = 0; i < int(vertex.properties.size()); i++) {
    if (txnWrite.SetVertexField(vertex.label_id, vertex_lid, i,
                                vertex.properties[i].second) == false) {
      txnWrite.Abort();
      throw std::runtime_error("Fail to update vertex");
    }
  }
  txnWrite.Commit();
}

void GraphDBOperations::updateEdge(std::vector<EdgeData>&& edge_data,
                                   GraphDBSession& session) {
  const auto& edge = edge_data[0];
  auto txn = session.GetReadTransaction();
  vid_t src_vid, dst_vid;
  if (txn.GetVertexIndex(edge.src_label_id, edge.src_pk_value, src_vid) ==
          false ||
      txn.GetVertexIndex(edge.dst_label_id, edge.dst_pk_value, dst_vid) ==
          false) {
    txn.Abort();
    throw std::runtime_error("Vertex not found");
  }
  bool edge_exists = false;
  for (auto edgeIt = txn.GetOutEdgeIterator(
           edge.src_label_id, src_vid, edge.dst_label_id, edge.edge_label_id);
       edgeIt.IsValid(); edgeIt.Next()) {
    if (edgeIt.GetNeighbor() == dst_vid) {
      edge_exists = true;
      break;
    }
  }
  if (!edge_exists) {
    txn.Abort();
    throw std::runtime_error("Edge not found");
  }
  txn.Commit();
  auto txn2 = session.GetUpdateTransaction();
  txn2.SetEdgeData(true, edge.src_label_id, src_vid, edge.dst_label_id, dst_vid,
                   edge.edge_label_id, edge.property_value);
  txn2.Commit();
}

nlohmann::json GraphDBOperations::getVertex(
    std::vector<VertexData>&& vertex_data, GraphDBSession& session) {
  auto& vertex = vertex_data[0];
  nlohmann::json result = nlohmann::json::array();
  auto txn = session.GetReadTransaction();
  auto vertex_db = txn.FindVertex(vertex.label_id, vertex.pk_value);
  if (vertex_db.IsValid() == false) {
    txn.Abort();
    throw std::runtime_error("Vertex not found");
  }
  for (int i = 0; i < vertex_db.FieldNum(); i++) {
    nlohmann::json values;
    values["name"] = vertex.properties[i].first;
    values["value"] = vertex_db.GetField(i).to_string();
    result.push_back(values);
  }
  txn.Commit();
  return result;
}

nlohmann::json GraphDBOperations::getEdge(std::vector<EdgeData>&& edge_data,
                                          GraphDBSession& session) {
  const auto& edge = edge_data[0];
  nlohmann::json result = nlohmann::json::array();
  auto txn = session.GetReadTransaction();
  vid_t src_vid, dst_vid;
  if (txn.GetVertexIndex(edge.src_label_id, edge.src_pk_value, src_vid) ==
          false ||
      txn.GetVertexIndex(edge.dst_label_id, edge.dst_pk_value, dst_vid) ==
          false) {
    txn.Abort();
    throw std::runtime_error("Vertex not found");
  }
  for (auto edgeIt = txn.GetOutEdgeIterator(
           edge.src_label_id, src_vid, edge.dst_label_id, edge.edge_label_id);
       edgeIt.IsValid(); edgeIt.Next()) {
    if (edgeIt.GetNeighbor() != dst_vid)
      continue;
    nlohmann::json push_json;
    push_json["name"] = edge.property_name;
    push_json["value"] = edgeIt.GetData().to_string();
    result.push_back(push_json);
    break;
  }
  if (result.empty()) {
    txn.Abort();
    throw std::runtime_error("Edge not found");
  }
  txn.Commit();
  return result;
}

}  // namespace gs
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

#include <string>
#include <unordered_map>
#include <vector>

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_operations.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/utils/service_utils.h"
#include "utils/result.h"
#include "utils/service_utils.h"

namespace gs {

Result<std::string> GraphDBOperations::CreateVertex(
    GraphDBSession& session, rapidjson::Document&& input_json) {
  std::vector<VertexData> vertex_data;
  std::vector<EdgeData> edge_data;
  // Check if the input json contains vertex_request and edge_request
  if (input_json.HasMember("vertex_request") == false ||
      input_json["vertex_request"].IsArray() == false ||
      input_json["vertex_request"].Size() == 0 ||
      (input_json.HasMember("edge_request") &&
       input_json["edge_request"].IsArray() == false)) {
    return Result<std::string>(gs::Status(
        StatusCode::INVALID_SCHEMA,
        "Invalid input json, vertex_request and edge_request should be array "
        "and not empty"));
  }
  const Schema& schema = session.schema();
  // input vertex data and edge data
  try {
    // vertex data
    for (auto& vertex_insert : input_json["vertex_request"].GetArray()) {
      vertex_data.push_back(inputVertex(vertex_insert, schema, session));
    }
    // edge data
    for (auto& edge_insert : input_json["edge_request"].GetArray()) {
      edge_data.push_back(inputEdge(edge_insert, schema, session));
    }
    LOG(INFO) << "CreateVertex edge_data: " << edge_data.size();
  } catch (std::exception& e) {
    return Result<std::string>(
        gs::Status(StatusCode::INVALID_SCHEMA,
                   " Bad input parameter : " + std::string(e.what())));
  }
  auto insert_result =
      insertVertex(std::move(vertex_data), std::move(edge_data), session);
  if (insert_result.ok()) {
    rapidjson::Document result(rapidjson::kObjectType);
    result.AddMember("message", "Vertex data is successfully inserted",
                     result.GetAllocator());
    return Result<std::string>(rapidjson_stringify(result));
  }
  return Result<std::string>(insert_result);
}
Result<std::string> GraphDBOperations::CreateEdge(
    GraphDBSession& session, rapidjson::Document&& input_json) {
  std::vector<VertexData> vertex_data;
  std::vector<EdgeData> edge_data;
  // Check if the input json contains edge_request
  if (input_json.IsArray() == false || input_json.Size() == 0) {
    return Result<std::string>(gs::Status(
        StatusCode::INVALID_SCHEMA,
        "Invalid input json, edge_request should be array and not empty"));
  }
  const Schema& schema = session.schema();
  // input edge data
  try {
    for (auto& edge_insert : input_json.GetArray()) {
      edge_data.push_back(inputEdge(edge_insert, schema, session));
    }
  } catch (std::exception& e) {
    return Result<std::string>(
        gs::Status(StatusCode::INVALID_SCHEMA,
                   " Bad input parameter : " + std::string(e.what())));
  }
  auto insert_result = insertEdge(std::move(edge_data), session);
  if (insert_result.ok()) {
    rapidjson::Document result(rapidjson::kObjectType);
    result.AddMember("message", "Edge data is successfully inserted",
                     result.GetAllocator());
    return Result<std::string>(rapidjson_stringify(result));
  }
  return Result<std::string>(insert_result);
}
Result<std::string> GraphDBOperations::UpdateVertex(
    GraphDBSession& session, rapidjson::Document&& input_json) {
  std::vector<VertexData> vertex_data;
  std::vector<EdgeData> edge_data;
  const Schema& schema = session.schema();
  // input vertex data
  try {
    vertex_data.push_back(inputVertex(input_json, schema, session));
  } catch (std::exception& e) {
    return Result<std::string>(
        gs::Status(StatusCode::INVALID_SCHEMA,
                   " Bad input parameter : " + std::string(e.what())));
  }
  auto update_result = updateVertex(std::move(vertex_data), session);
  if (update_result.ok()) {
    rapidjson::Document result(rapidjson::kObjectType);
    result.AddMember("message", "Successfully update Vertex",
                     result.GetAllocator());
    return Result<std::string>(rapidjson_stringify(result));
  }
  return Result<std::string>(update_result);
}
Result<std::string> GraphDBOperations::UpdateEdge(
    GraphDBSession& session, rapidjson::Document&& input_json) {
  std::vector<VertexData> vertex_data;
  std::vector<EdgeData> edge_data;
  const Schema& schema = session.schema();
  // input edge data
  try {
    edge_data.push_back(inputEdge(input_json, schema, session));
  } catch (std::exception& e) {
    return Result<std::string>(
        gs::Status(StatusCode::INVALID_SCHEMA,
                   " Bad input parameter : " + std::string(e.what())));
  }
  auto update_result = updateEdge(std::move(edge_data), session);
  if (update_result.ok()) {
    rapidjson::Document result(rapidjson::kObjectType);
    result.AddMember("message", "Successfully update Edge",
                     result.GetAllocator());
    return Result<std::string>(rapidjson_stringify(result));
  }
  return Result<std::string>(update_result);
}
Result<std::string> GraphDBOperations::GetVertex(
    GraphDBSession& session,
    std::unordered_map<std::string, std::string>&& params) {
  rapidjson::Document result(rapidjson::kObjectType);
  std::vector<VertexData> vertex_data;
  std::vector<EdgeData> edge_data;
  std::vector<std::string> property_names;
  const Schema& schema = session.schema();
  // input vertex data
  VertexData vertex;
  std::string label = params["label"];
  result.AddMember("label", label, result.GetAllocator());
  vertex.pk_value = Any(std::string(params["primary_key_value"]));
  auto check_result =
      checkVertexSchema(schema, vertex, label, property_names, true);
  if (check_result.ok() == false) {
    return Result<std::string>(check_result);
  }
  vertex_data.push_back(vertex);
  auto get_result = getVertex(std::move(vertex_data), property_names, session,
                              result.GetAllocator());
  if (get_result.ok()) {
    result.AddMember("values", get_result.value(), result.GetAllocator());
    return Result<std::string>(rapidjson_stringify(result));
  }
  return Result<std::string>(get_result.status());
}
Result<std::string> GraphDBOperations::GetEdge(
    GraphDBSession& session,
    std::unordered_map<std::string, std::string>&& params) {
  rapidjson::Document result(rapidjson::kObjectType);
  std::vector<VertexData> vertex_data;
  std::vector<EdgeData> edge_data;
  const Schema& schema = session.schema();
  std::string property_name;
  // input edge data
  EdgeData edge;
  std::string src_label = params["src_label"];
  std::string dst_label = params["dst_label"];
  std::string edge_label = params["edge_label"];
  std::string src_pk_value = params["src_primary_key_value"];
  std::string dst_pk_value = params["dst_primary_key_value"];
  edge.src_pk_value = Any(src_pk_value);
  edge.dst_pk_value = Any(dst_pk_value);
  auto check_result = checkEdgeSchema(schema, edge, src_label, dst_label,
                                      edge_label, property_name, true);
  if (check_result.ok() == false) {
    return Result<std::string>(check_result);
  }
  edge_data.push_back(edge);
  result.AddMember("src_label", src_label, result.GetAllocator());
  result.AddMember("dst_label", dst_label, result.GetAllocator());
  result.AddMember("edge_label", edge_label, result.GetAllocator());
  result.AddMember("src_primary_key_value", src_pk_value,
                   result.GetAllocator());
  result.AddMember("dst_primary_key_value", dst_pk_value,
                   result.GetAllocator());
  if (property_name.empty()) {
    rapidjson::Value properties(rapidjson::kObjectType);
    result.AddMember("properties", properties, result.GetAllocator());
    return Result<std::string>(rapidjson_stringify(result));
  }
  auto get_result = getEdge(std::move(edge_data), property_name, session,
                            result.GetAllocator());
  if (get_result.ok()) {
    result.AddMember("properties", get_result.value(), result.GetAllocator());
    return Result<std::string>(rapidjson_stringify(result));
  }
  return Result<std::string>(get_result.status());
}
Result<std::string> GraphDBOperations::DeleteVertex(
    GraphDBSession& session, rapidjson::Document&& input_json) {
  // not implemented
  return Result<std::string>(StatusCode::UNIMPLEMENTED,
                             "delete_vertex is not implemented");
}
Result<std::string> GraphDBOperations::DeleteEdge(
    GraphDBSession& session, rapidjson::Document&& input_json) {
  // not implemented
  return Result<std::string>(StatusCode::UNIMPLEMENTED,
                             "delete_edge is not implemented");
}

VertexData GraphDBOperations::inputVertex(const rapidjson::Value& vertex_json,
                                          const Schema& schema,
                                          GraphDBSession& session) {
  VertexData vertex;
  std::string label = jsonToString(vertex_json["label"]);
  vertex.pk_value = Any(jsonToString(vertex_json["primary_key_value"]));
  std::unordered_set<std::string> property_names;
  std::vector<std::string> property_names_arr;
  for (auto& property : vertex_json["properties"].GetArray()) {
    auto name_string = jsonToString(property["name"]);
    auto value_string = jsonToString(property["value"]);
    if (property_names.find(name_string) != property_names.end()) {
      throw std::runtime_error("property already exists in input properties: " +
                               name_string);
    } else {
      property_names.insert(name_string);
      property_names_arr.push_back(name_string);
    }
    vertex.properties.emplace_back(value_string);
  }
  auto check_result =
      checkVertexSchema(schema, vertex, label, property_names_arr);
  if (check_result.ok() == false) {
    throw std::runtime_error(check_result.error_message());
  }
  return vertex;
}
EdgeData GraphDBOperations::inputEdge(const rapidjson::Value& edge_json,
                                      const Schema& schema,
                                      GraphDBSession& session) {
  EdgeData edge;
  std::string src_label = jsonToString(edge_json["src_label"]);
  std::string dst_label = jsonToString(edge_json["dst_label"]);
  std::string edge_label = jsonToString(edge_json["edge_label"]);
  edge.src_pk_value = Any(jsonToString(edge_json["src_primary_key_value"]));
  edge.dst_pk_value = Any(jsonToString(edge_json["dst_primary_key_value"]));
  // Check that all parameters in the parameter
  if (edge_json["properties"].Size() > 1) {
    throw std::runtime_error(
        "size should be 1(only support single property edge)");
  }
  std::string property_name = "";
  if (edge_json["properties"].Size() == 1) {
    edge.property_value =
        Any(jsonToString(edge_json["properties"][0]["value"]));
    property_name = edge_json["properties"][0]["name"].GetString();
  }
  auto check_result = checkEdgeSchema(schema, edge, src_label, dst_label,
                                      edge_label, property_name);
  if (check_result.ok() == false) {
    throw std::runtime_error(check_result.error_message());
  }
  return edge;
}

Status GraphDBOperations::checkVertexSchema(
    const Schema& schema, VertexData& vertex, const std::string& label,
    std::vector<std::string>& input_property_names, bool is_get) {
  try {
    vertex.label_id = schema.get_vertex_label_id(label);
    PropertyType colType =
        std::get<0>(schema.get_vertex_primary_key(vertex.label_id)[0]);
    vertex.pk_value = ConvertStringToAny(vertex.pk_value.to_string(), colType);
    auto properties_type = schema.get_vertex_properties(vertex.label_id);
    auto properties_name = schema.get_vertex_property_names(vertex.label_id);
    if (vertex.properties.size() != properties_name.size() && is_get == false) {
      throw std::runtime_error("properties size not match");
    }
    if (is_get) {
      input_property_names = properties_name;
      return Status::OK();
    }
    for (int col_index = 0; col_index < int(properties_name.size());
         col_index++) {
      if (input_property_names[col_index] != properties_name[col_index]) {
        throw std::runtime_error(
            "properties name not match, pleace check the order and name");
      }
      vertex.properties[col_index] = ConvertStringToAny(
          vertex.properties[col_index].to_string(), properties_type[col_index]);
    }
    return Status::OK();
  } catch (std::exception& e) {
    return Status(StatusCode::INVALID_SCHEMA,
                  " Bad input parameter : " + std::string(e.what()));
  }
}
Status GraphDBOperations::checkEdgeSchema(const Schema& schema, EdgeData& edge,
                                          const std::string& src_label,
                                          const std::string& dst_label,
                                          const std::string& edge_label,
                                          std::string& property_name,
                                          bool is_get) {
  try {
    edge.src_label_id = schema.get_vertex_label_id(src_label);
    edge.dst_label_id = schema.get_vertex_label_id(dst_label);
    edge.edge_label_id = schema.get_edge_label_id(edge_label);
    auto& result = schema.get_edge_property_names(
        edge.src_label_id, edge.dst_label_id, edge.edge_label_id);
    if (is_get) {
      if (result.size() >= 1) {
        property_name = result[0];
      } else {
        property_name = "";
      }
    } else {
      // update or add
      if (property_name != (result.size() >= 1 ? result[0] : "")) {
        throw std::runtime_error("property name not match");
      }
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
    return Status::OK();
  } catch (std::exception& e) {
    return Status(StatusCode::INVALID_SCHEMA,
                  " Bad input parameter : " + std::string(e.what()));
  }
}

Status GraphDBOperations::checkEdgeExistsWithInsert(
    const std::vector<EdgeData>& edge_data, GraphDBSession& session) {
  try {
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
      for (auto edgeIt =
               txn.GetOutEdgeIterator(edge.src_label_id, src_vid,
                                      edge.dst_label_id, edge.edge_label_id);
           edgeIt.IsValid(); edgeIt.Next()) {
        if (edgeIt.GetNeighbor() == dst_vid) {
          txn.Abort();
          throw std::runtime_error("Fail to create edge: Edge already exists");
        }
      }
    }
  } catch (std::exception& e) {
    return Status(StatusCode::INVALID_SCHEMA, e.what());
  }
  return Status::OK();
}

Status GraphDBOperations::checkEdgeExists(
    const std::vector<EdgeData>& edge_data, GraphDBSession& session) {
  try {
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
      for (auto edgeIt =
               txn.GetOutEdgeIterator(edge.src_label_id, src_vid,
                                      edge.dst_label_id, edge.edge_label_id);
           edgeIt.IsValid(); edgeIt.Next()) {
        if (edgeIt.GetNeighbor() == dst_vid) {
          txn.Abort();
          throw std::runtime_error("Fail to create edge: Edge already exists");
        }
      }
    }
  } catch (std::exception& e) {
    return Status(StatusCode::INVALID_SCHEMA, e.what());
  }
  return Status::OK();
}

Status GraphDBOperations::checkVertexExists(
    const std::vector<VertexData>& vertex_data, GraphDBSession& session) {
  try {
    auto txn = session.GetReadTransaction();
    for (auto& vertex : vertex_data) {
      vid_t vid;
      if (txn.GetVertexIndex(vertex.label_id, vertex.pk_value, vid)) {
        txn.Abort();
        throw std::runtime_error(
            "Fail to create vertex: Vertex already exists");
      }
    }
    txn.Commit();
  } catch (std::exception& e) {
    return Status(StatusCode::INVALID_SCHEMA, e.what());
  }
  return Status::OK();
}
Status GraphDBOperations::singleInsertVertex(
    std::vector<VertexData>&& vertex_data, std::vector<EdgeData>&& edge_data,
    GraphDBSession& session) {
  try {
    auto txnWrite = session.GetSingleVertexInsertTransaction();
    for (auto& vertex : vertex_data) {
      if (txnWrite.AddVertex(vertex.label_id, vertex.pk_value,
                             vertex.properties) == false) {
        txnWrite.Abort();
        throw std::runtime_error(
            "Fail to create vertex; All inserts are rollbacked");
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
  } catch (std::exception& e) {
    return Status(StatusCode::INVALID_SCHEMA, e.what());
  }
  return Status::OK();
}

Status GraphDBOperations::multiInsert(std::vector<VertexData>&& vertex_data,
                                      std::vector<EdgeData>&& edge_data,
                                      GraphDBSession& session) {
  try {
    auto txnWrite = session.GetInsertTransaction();
    for (auto& vertex : vertex_data) {
      if (txnWrite.AddVertex(vertex.label_id, vertex.pk_value,
                             vertex.properties) == false) {
        txnWrite.Abort();
        throw std::runtime_error(
            "Fail to create vertex; All inserts are rollbacked");
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
  } catch (std::exception& e) {
    return Status(StatusCode::INVALID_SCHEMA, e.what());
  }
  return Status::OK();
}
Status GraphDBOperations::insertVertex(std::vector<VertexData>&& vertex_data,
                                       std::vector<EdgeData>&& edge_data,
                                       GraphDBSession& session) {
  auto check_result = checkVertexExists(vertex_data, session);
  if (check_result.ok() == false) {
    return check_result;
  }
  check_result = checkEdgeExistsWithInsert(edge_data, session);
  if (check_result.ok() == false) {
    return check_result;
  }
  if (vertex_data.size() == 1) {
    return singleInsertVertex(std::move(vertex_data), std::move(edge_data),
                              session);
  } else {
    return multiInsert(std::move(vertex_data), std::move(edge_data), session);
  }
}

Status GraphDBOperations::singleInsertEdge(std::vector<EdgeData>&& edge_data,
                                           GraphDBSession& session) {
  try {
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
  } catch (std::exception& e) {
    return Status(StatusCode::INVALID_SCHEMA, e.what());
  }
  return Status::OK();
}

Status GraphDBOperations::insertEdge(std::vector<EdgeData>&& edge_data,
                                     GraphDBSession& session) {
  auto check_result = checkEdgeExists(edge_data, session);
  if (check_result.ok() == false) {
    return check_result;
  }
  if (edge_data.size() == 1) {
    return singleInsertEdge(std::move(edge_data), session);
  } else {
    return multiInsert(std::vector<VertexData>(), std::move(edge_data),
                       session);
  }
}

Status GraphDBOperations::updateVertex(std::vector<VertexData>&& vertex_data,
                                       GraphDBSession& session) {
  try {
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
                                  vertex.properties[i]) == false) {
        txnWrite.Abort();
        throw std::runtime_error("Fail to update vertex");
      }
    }
    txnWrite.Commit();
  } catch (std::exception& e) {
    return Status(StatusCode::INVALID_SCHEMA, e.what());
  }
  return Status::OK();
}

Status GraphDBOperations::updateEdge(std::vector<EdgeData>&& edge_data,
                                     GraphDBSession& session) {
  try {
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
    txn2.SetEdgeData(true, edge.src_label_id, src_vid, edge.dst_label_id,
                     dst_vid, edge.edge_label_id, edge.property_value);
    txn2.Commit();
  } catch (std::exception& e) {
    return Status(StatusCode::INVALID_SCHEMA, e.what());
  }
  return Status::OK();
}

Result<rapidjson::Value> GraphDBOperations::getVertex(
    std::vector<VertexData>&& vertex_data,
    const std::vector<std::string>& property_names, GraphDBSession& session,
    rapidjson::Document::AllocatorType& allocator) {
  try {
    auto& vertex = vertex_data[0];
    rapidjson::Document result(rapidjson::kArrayType, &allocator);
    auto txn = session.GetReadTransaction();
    auto vertex_db = txn.FindVertex(vertex.label_id, vertex.pk_value);
    if (vertex_db.IsValid() == false) {
      txn.Abort();
      throw std::runtime_error("Vertex not found");
    }
    for (int i = 0; i < vertex_db.FieldNum(); i++) {
      rapidjson::Document values(rapidjson::kObjectType, &allocator);
      values.AddMember("name", property_names[i], allocator);
      values.AddMember("value", vertex_db.GetField(i).to_string(), allocator);
      result.PushBack(values, allocator);
    }
    txn.Commit();
    return Result<rapidjson::Value>(std::move(result));
  } catch (std::exception& e) {
    return Result<rapidjson::Value>(
        Status(StatusCode::INVALID_SCHEMA, e.what()));
  }
}

Result<rapidjson::Value> GraphDBOperations::getEdge(
    std::vector<EdgeData>&& edge_data, const std::string& property_name,
    GraphDBSession& session, rapidjson::Document::AllocatorType& allocator) {
  try {
    const auto& edge = edge_data[0];
    rapidjson::Document result(rapidjson::kArrayType);
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
      rapidjson::Document push_json(rapidjson::kObjectType, &allocator);
      push_json.AddMember("name", property_name, allocator);
      push_json.AddMember("value", edgeIt.GetData().to_string(), allocator);
      result.PushBack(push_json, allocator);
      break;
    }
    if (result.Empty()) {
      txn.Abort();
      throw std::runtime_error("Edge not found");
    }
    txn.Commit();
    return Result<rapidjson::Value>(std::move(result));
  } catch (std::exception& e) {
    return Result<rapidjson::Value>(
        Status(StatusCode::INVALID_SCHEMA, e.what()));
  }
}

}  // namespace gs
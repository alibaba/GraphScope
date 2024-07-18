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

#include "flex/engines/graph_db/database/manager.h"
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <vector>
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/utils/service_utils.h"

namespace gs {

gs::VertexData inputVertex(const nlohmann::json& vertex_json,
                           const nlohmann::json& schema_json, int shard_id) {
  gs::VertexData vertex;
  std::string label = gs::jsonToString(vertex_json["label"]);
  vertex.pk_value = gs::Any(gs::jsonToString(vertex_json["primary_key_value"]));
  std::unordered_set<std::string> property_names;
  for (auto& property : vertex_json["properties"]["properties"]) {
    auto name_string = gs::jsonToString(property["name"]);
    auto value_string = gs::jsonToString(property["value"]);
    if (property_names.find(name_string) != property_names.end()) {
      throw std::runtime_error("property already exists in input properties: " +
                               name_string);
    } else {
      property_names.insert(name_string);
    }
    vertex.properties.emplace_back(name_string, gs::Any(value_string));
  }
  gs::VertexEdgeManager::checkVertexSchema(schema_json, vertex, label);
  gs::VertexEdgeManager::getVertexLabelId(vertex, label, shard_id);
  return vertex;
}

gs::EdgeData inputEdge(const nlohmann::json& edge_json,
                       const nlohmann::json& schema_json, int shard_id) {
  gs::EdgeData edge;
  std::string src_label = gs::jsonToString(edge_json["src_label"]);
  std::string dst_label = gs::jsonToString(edge_json["dst_label"]);
  std::string edge_label = gs::jsonToString(edge_json["edge_label"]);
  edge.src_pk_value =
      gs::Any(gs::jsonToString(edge_json["src_primary_key_value"]));
  edge.dst_pk_value =
      gs::Any(gs::jsonToString(edge_json["dst_primary_key_value"]));
  // Check that all parameters in the parameter
  if (edge_json["properties"].size() != 1) {
    throw std::runtime_error(
        "size should be 1(only support single property edge)");
  }
  edge.property_name = gs::jsonToString(edge_json["properties"][0]["name"]);
  edge.property_value =
      gs::Any(gs::jsonToString(edge_json["properties"][0]["value"]));
  gs::VertexEdgeManager::checkEdgeSchema(schema_json, edge, src_label,
                                         dst_label, edge_label);
  gs::VertexEdgeManager::getEdgeLabelId(edge, src_label, dst_label, edge_label,
                                        shard_id);
  return edge;
}

void VertexEdgeManager::checkVertexSchema(const nlohmann::json& schema_json,
                                          VertexData& vertex,
                                          std::string& label) {
  bool vertex_exists = false;
  for (auto& vertex_types : schema_json["vertex_types"]) {
    if (vertex_types["type_name"] != label)
      continue;
    vertex_exists = true;
    auto pk_name = vertex_types["primary_keys"][0];
    bool get_flag = (vertex.properties.size() == 0);
    if (vertex.properties.size() + 1 != vertex_types["properties"].size() &&
        get_flag == false) {
      throw std::runtime_error("properties size not match");
    }
    // compute colNames
    int col_index = 0;
    for (auto& property : vertex_types["properties"]) {
      auto property_name = property["property_name"];
      gs::PropertyType colType;
      if (property_name == pk_name) {
        gs::from_json(property["property_type"], colType);
        vertex.pk_value =
            gs::ConvertStringToAny(vertex.pk_value.to_string(), colType);
        continue;
      }
      if (get_flag) {
        vertex.properties.push_back(std::make_pair(property_name, gs::Any()));
        continue;
      }
      if (vertex.properties[col_index].first != property_name) {
        throw std::runtime_error(
            "properties name not match, pleace check the order and name");
      }
      gs::from_json(property["property_type"], colType);
      vertex.properties[col_index].second = gs::ConvertStringToAny(
          vertex.properties[col_index].second.to_string(), colType);
      col_index++;
    }
    break;
  }
  if (!vertex_exists) {
    throw std::runtime_error("Vertex Label not exists in schema");
  }
}
void VertexEdgeManager::checkEdgeSchema(const nlohmann::json& schema_json,
                                        EdgeData& edge, std::string& src_label,
                                        std::string& dst_label,
                                        std::string& edge_label) {
  bool edge_exists = false;
  // Check if the edge exists
  for (auto& edge_types : schema_json["edge_types"]) {
    if (jsonToString(edge_types["type_name"]) != edge_label)
      continue;
    edge_exists = true;
    // get edge
    if (edge.property_name.empty()) {
      edge.property_name =
          jsonToString(edge_types["properties"][0]["property_name"]);
    } else {
      // update or add
      gs::PropertyType colType;
      gs::from_json(edge_types["properties"][0]["property_type"], colType);
      edge.property_value =
          gs::ConvertStringToAny(edge.property_value.to_string(), colType);
    }
    break;
  }
  if (!edge_exists) {
    throw std::runtime_error("Edge Label not exists in schema");
  }
  int vertex_label_exist = 0;
  enum src_or_dst { src, dst };
  src_or_dst sod;
  // Check for the exist of src_label and dst_label.
  for (auto& vertex_types : schema_json["vertex_types"]) {
    if (vertex_label_exist == 2)
      break;
    bool src_dst_same = false;
    if (vertex_types["type_name"] == src_label) {
      sod = src_or_dst::src;
      if (src_label == dst_label) {
        vertex_label_exist++;
        src_dst_same = true;
      }
    } else if (vertex_types["type_name"] == dst_label) {
      sod = src_or_dst::dst;
    } else {
      continue;
    }
    vertex_label_exist++;
    gs::PropertyType primary_key_type;
    auto& pk_value =
        (sod == src_or_dst::src ? edge.src_pk_value : edge.dst_pk_value);
    std::string primary_key_name = vertex_types["primary_keys"][0];
    for (auto& property : vertex_types["properties"]) {
      if (property["property_name"] == primary_key_name) {
        gs::from_json(property["property_type"], primary_key_type);
        pk_value =
            gs::ConvertStringToAny(pk_value.to_string(), primary_key_type);
        break;
      }
    }
    if (src_dst_same) {
      edge.dst_pk_value = gs::ConvertStringToAny(edge.dst_pk_value.to_string(),
                                                 primary_key_type);
    }
  }
  if (vertex_label_exist != 2) {
    throw std::runtime_error("src_label or dst_label not exists in schema");
  }
}

void VertexEdgeManager::getVertexLabelId(VertexData& vertex, std::string& label,
                                         int shard_id) {
  auto& db = gs::GraphDB::get().GetSession(shard_id);
  vertex.label_id = db.schema().get_vertex_label_id(label);
}

void VertexEdgeManager::getEdgeLabelId(EdgeData& edge, std::string& src_label,
                                       std::string& dst_label,
                                       std::string& edge_label, int shard_id) {
  auto& db = gs::GraphDB::get().GetSession(shard_id);
  edge.src_label_id = db.schema().get_vertex_label_id(src_label);
  edge.dst_label_id = db.schema().get_vertex_label_id(dst_label);
  edge.edge_label_id = db.schema().get_edge_label_id(edge_label);
}

void VertexEdgeManager::checkEdgeExistsWithInsert(
    const std::vector<EdgeData>& edge_data, int shard_id) {
  auto& db = gs::GraphDB::get().GetSession(shard_id);
  auto txn = db.GetReadTransaction();
  for (auto& edge : edge_data) {
    gs::vid_t src_vid, dst_vid;
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

void VertexEdgeManager::checkEdgeExists(const std::vector<EdgeData>& edge_data,
                                        int shard_id) {
  auto& db = gs::GraphDB::get().GetSession(shard_id);
  auto txn = db.GetReadTransaction();
  for (auto& edge : edge_data) {
    gs::vid_t src_vid, dst_vid;
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

void VertexEdgeManager::checkVertexExists(
    const std::vector<VertexData>& vertex_data, int shard_id) {
  auto& db = gs::GraphDB::get().GetSession(shard_id);
  auto txn = db.GetReadTransaction();
  for (auto& vertex : vertex_data) {
    gs::vid_t vid;
    if (txn.GetVertexIndex(vertex.label_id, vertex.pk_value, vid)) {
      txn.Abort();
      throw std::runtime_error("Vertex already exists");
    }
  }
  txn.Commit();
}
void VertexEdgeManager::singleInsertVertex(std::vector<VertexData>& vertex_data,
                                           std::vector<EdgeData>& edge_data,
                                           int shard_id) {
  auto& db = gs::GraphDB::get().GetSession(shard_id);
  auto txnWrite = db.GetSingleVertexInsertTransaction();
  for (auto& vertex : vertex_data) {
    std::vector<gs::Any> insert_arr;
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

void VertexEdgeManager::multiInsertVertex(std::vector<VertexData>& vertex_data,
                                          std::vector<EdgeData>& edge_data,
                                          int shard_id) {
  auto& db = gs::GraphDB::get().GetSession(shard_id);
  auto txnWrite = db.GetInsertTransaction();
  for (auto& vertex : vertex_data) {
    std::vector<gs::Any> insert_arr;
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
void VertexEdgeManager::insertVertex(std::vector<VertexData>& vertex_data,
                                     std::vector<EdgeData>& edge_data,
                                     int shard_id) {
  checkVertexExists(vertex_data, shard_id);
  checkEdgeExistsWithInsert(edge_data, shard_id);
  if (vertex_data.size() == 1) {
    singleInsertVertex(vertex_data, edge_data, shard_id);
  } else {
    multiInsertVertex(vertex_data, edge_data, shard_id);
  }
}

void VertexEdgeManager::singleInsertEdge(std::vector<EdgeData>& edge_data,
                                         int shard_id) {
  auto& db = gs::GraphDB::get().GetSession(shard_id);
  auto txnWrite = db.GetSingleEdgeInsertTransaction();
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

void VertexEdgeManager::multiInsertEdge(std::vector<EdgeData>& edge_data,
                                        int shard_id) {
  auto& db = gs::GraphDB::get().GetSession(shard_id);
  auto txnWrite = db.GetInsertTransaction();
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

void VertexEdgeManager::insertEdge(std::vector<EdgeData>& edge_data,
                                   int shard_id) {
  checkEdgeExists(edge_data, shard_id);
  if (edge_data.size() == 1) {
    singleInsertEdge(edge_data, shard_id);
  } else {
    multiInsertEdge(edge_data, shard_id);
  }
}

void VertexEdgeManager::updateVertex(std::vector<VertexData>& vertex_data,
                                     int shard_id) {
  auto& db = gs::GraphDB::get().GetSession(shard_id);
  auto& vertex = vertex_data[0];
  auto txnRead = db.GetReadTransaction();
  gs::vid_t vertex_lid;
  if (txnRead.GetVertexIndex(vertex.label_id, vertex.pk_value, vertex_lid) ==
      false) {
    txnRead.Abort();
    throw std::runtime_error("Vertex not exists");
  }
  txnRead.Commit();
  auto txnWrite = db.GetUpdateTransaction();
  for (int i = 0; i < int(vertex.properties.size()); i++) {
    if (txnWrite.SetVertexField(vertex.label_id, vertex_lid, i,
                                vertex.properties[i].second) == false) {
      txnWrite.Abort();
      throw std::runtime_error("Fail to update vertex");
    }
  }
  txnWrite.Commit();
}

void VertexEdgeManager::updateEdge(std::vector<EdgeData>& edge_data,
                                   int shard_id) {
  auto& db = gs::GraphDB::get().GetSession(shard_id);
  auto& edge = edge_data[0];
  auto txn = db.GetReadTransaction();
  gs::vid_t src_vid, dst_vid;
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
  auto txn2 = db.GetUpdateTransaction();
  txn2.SetEdgeData(true, edge.src_label_id, src_vid, edge.dst_label_id, dst_vid,
                   edge.edge_label_id, edge.property_value);
  txn2.Commit();
}

nlohmann::json VertexEdgeManager::getVertex(
    std::vector<VertexData>& vertex_data, int shard_id) {
  auto& db = gs::GraphDB::get().GetSession(shard_id);
  auto& vertex = vertex_data[0];
  nlohmann::json result = nlohmann::json::array();
  auto txn = db.GetReadTransaction();
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

nlohmann::json VertexEdgeManager::getEdge(std::vector<EdgeData>& edge_data,
                                          int shard_id) {
  auto& db = gs::GraphDB::get().GetSession(shard_id);
  auto& edge = edge_data[0];
  nlohmann::json result = nlohmann::json::array();
  auto txn = db.GetReadTransaction();
  gs::vid_t src_vid, dst_vid;
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
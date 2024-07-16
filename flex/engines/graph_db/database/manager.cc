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
VertexEdgeManager::VertexEdgeManager(std::vector<VertexData>&& vertex_data,
                                     std::vector<EdgeData>&& edge_data,
                                     nlohmann::json&& schema_json, int shard_id)
    : vertex_data(std::move(vertex_data)),
      edge_data(std::move(edge_data)),
      schema_json(std::move(schema_json)),
      db(gs::GraphDB::get().GetSession(shard_id)) {}

void VertexEdgeManager::checkVertexSchema() {
  for (auto& vertex : vertex_data) {
    bool vertex_exists = false;
    for (auto& vertex_types : schema_json["vertex_types"]) {
      if (vertex_types["type_name"] != vertex.label)
        continue;
      vertex_exists = true;
      vertex.pk_name = vertex_types["primary_keys"][0];
      bool get_flag = (vertex.properties.size() == 0);
      if (vertex.properties.size() + 1 != vertex_types["properties"].size() &&
          get_flag == false) {
        throw std::runtime_error("properties size not match");
      }
      // compute colNames
      for (auto& property : vertex_types["properties"]) {
        auto property_name = property["property_name"];
        gs::PropertyType colType;
        if (property_name == vertex.pk_name) {
          gs::from_json(property["property_type"], colType);
          vertex.pk_value =
              gs::ConvertStringToAny(vertex.pk_value.to_string(), colType);
          continue;
        }
        vertex.col_names.push_back(property_name);
        if (get_flag)
          continue;
        auto properties_iter = vertex.properties.find(property_name);
        if (properties_iter == vertex.properties.end()) {
          throw std::runtime_error("property not exists in input properties: " +
                                   std::string(property_name));
        }
        gs::from_json(property["property_type"], colType);
        properties_iter->second = gs::ConvertStringToAny(
            properties_iter->second.to_string(), colType);
      }
      break;
    }
    if (!vertex_exists) {
      throw std::runtime_error("Vertex Label not exists in schema");
    }
  }
}

void VertexEdgeManager::checkEdgeSchema() {
  for (auto& edge : edge_data) {
    bool edge_exists = false;
    // Check if the edge exists
    for (auto& edge_types : schema_json["edge_types"]) {
      if (jsonToString(edge_types["type_name"]) != edge.edge_label)
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
      if (vertex_types["type_name"] == edge.src_label) {
        sod = src_or_dst::src;
        if (edge.src_label == edge.dst_label) {
          vertex_label_exist++;
          src_dst_same = true;
        }
      } else if (vertex_types["type_name"] == edge.dst_label) {
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
        edge.dst_pk_value = gs::ConvertStringToAny(
            edge.dst_pk_value.to_string(), primary_key_type);
      }
    }
    if (vertex_label_exist != 2) {
      throw std::runtime_error("src_label or dst_label not exists in schema");
    }
  }
}
void VertexEdgeManager::getLabelId() {
  auto txn = db.GetReadTransaction();
  // solve edge insert
  for (auto& edge : edge_data) {
    edge.src_label_id = db.schema().get_vertex_label_id(edge.src_label);
    edge.dst_label_id = db.schema().get_vertex_label_id(edge.dst_label);
    edge.edge_label_id = db.schema().get_edge_label_id(edge.edge_label);
  }
  for (auto& vertex : vertex_data) {
    vertex.label_id = db.schema().get_vertex_label_id(vertex.label);
  }
  txn.Commit();
}

void VertexEdgeManager::checkEdgeExistsWithInsert() {
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

void VertexEdgeManager::checkEdgeExists() {
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

void VertexEdgeManager::checkVertexExists() {
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
void VertexEdgeManager::singleInsertVertex() {
  auto txnWrite = db.GetSingleVertexInsertTransaction();
  for (auto& vertex : vertex_data) {
    auto label_id = db.schema().get_vertex_label_id(vertex.label);
    std::vector<gs::Any> insert_arr;
    for (auto& col : vertex.col_names) {
      insert_arr.push_back(vertex.properties[col]);
    }
    if (txnWrite.AddVertex(label_id, vertex.pk_value, insert_arr) == false) {
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

void VertexEdgeManager::multiInsertVertex() {
  auto txnWrite = db.GetInsertTransaction();
  for (auto& vertex : vertex_data) {
    auto label_id = db.schema().get_vertex_label_id(vertex.label);
    std::vector<gs::Any> insert_arr;
    for (auto& col : vertex.col_names) {
      insert_arr.push_back(vertex.properties[col]);
    }
    if (txnWrite.AddVertex(label_id, vertex.pk_value, insert_arr) == false) {
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
void VertexEdgeManager::insertVertex() {
  if (vertex_data.size() == 1) {
    singleInsertVertex();
  } else {
    multiInsertVertex();
  }
}

void VertexEdgeManager::singleInsertEdge() {
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

void VertexEdgeManager::multiInsertEdge() {
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

void VertexEdgeManager::insertEdge() {
  if (edge_data.size() == 1) {
    singleInsertEdge();
  } else {
    multiInsertEdge();
  }
}

void VertexEdgeManager::updateVertex() {
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
  for (int i = 0; i < int(vertex.col_names.size()); i++) {
    if (txnWrite.SetVertexField(vertex.label_id, vertex_lid, i,
                                vertex.properties[vertex.col_names[i]]) ==
        false) {
      txnWrite.Abort();
      throw std::runtime_error("Fail to update vertex");
    }
  }
  txnWrite.Commit();
}

void VertexEdgeManager::updateEdge() {
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

std::string VertexEdgeManager::getVertex() {
  auto& vertex = vertex_data[0];
  nlohmann::json result;
  result["label"] = vertex.label;
  auto txn = db.GetReadTransaction();
  auto vertex_db = txn.FindVertex(vertex.label_id, vertex.pk_value);
  if (vertex_db.IsValid() == false) {
    txn.Abort();
    throw std::runtime_error("Vertex not found");
  }
  nlohmann::json primary_key;
  primary_key["name"] = vertex.pk_name;
  primary_key["value"] = vertex.pk_value.to_string();
  result["values"].push_back(primary_key);
  for (int i = 0; i < vertex_db.FieldNum(); i++) {
    nlohmann::json values;
    values["name"] = vertex.col_names[i];
    values["value"] = vertex_db.GetField(i).to_string();
    result["values"].push_back(values);
  }
  txn.Commit();
  return result.dump();
}

std::string VertexEdgeManager::getEdge() {
  auto& edge = edge_data[0];
  nlohmann::json result;
  result["src_label"] = edge.src_label;
  result["dst_label"] = edge.dst_label;
  result["edge_label"] = edge.edge_label;
  result["src_primary_key_value"] = edge.src_pk_value.to_string();
  result["dst_primary_key_value"] = edge.dst_pk_value.to_string();
  result["properties"] = nlohmann::json::array();
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
    result["properties"].push_back(push_json);
    break;
  }
  if (result["properties"].empty()) {
    txn.Abort();
    throw std::runtime_error("Edge not found");
  }
  txn.Commit();
  return result.dump();
}

}  // namespace gs
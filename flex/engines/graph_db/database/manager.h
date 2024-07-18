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
#ifndef ENGINES_GRAPH_DB_APP_MANAGER_H_
#define ENGINES_GRAPH_DB_APP_MANAGER_H_

#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <vector>
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/database/manager.h"
#include "flex/utils/service_utils.h"

namespace gs {

struct VertexData {
  gs::Any pk_value;
  gs::label_t label_id;
  std::vector<std::pair<std::string, gs::Any>> properties;
  VertexData() {}
  ~VertexData() {}
};

struct EdgeData {
  gs::label_t src_label_id, dst_label_id, edge_label_id; 
  gs::Any src_pk_value, dst_pk_value;
  gs::Any property_value;
  std::string property_name;
  EdgeData() {}
  ~EdgeData() {}
};

class VertexEdgeManager {
 public:
  // check schema
  static void checkVertexSchema(const nlohmann::json& schema_json, VertexData& vertex,
                         std::string& label);
  static void checkEdgeSchema(const nlohmann::json& schema_json, EdgeData& edge,
                       std::string& src_label, std::string& dst_label,
                       std::string& edge_label);

  // check labelId
  static void getVertexLabelId(VertexData& vertex, std::string& label, int shard_id);
  static void getEdgeLabelId(EdgeData& edge, std::string& src_label,
                      std::string& dst_label, std::string& edge_label,
                      int shard_id);

  // db check
  static void checkEdgeExistsWithInsert(const std::vector<EdgeData>& edge_data,
                                 int shard_id);
  static void checkEdgeExists(const std::vector<EdgeData>& edge_data, int shard_id);
  static void checkVertexExists(const std::vector<VertexData>& vertex_data,
                         int shard_id);

  // db operations
  static void singleInsertVertex(std::vector<VertexData>& vertex_data,
                          std::vector<EdgeData>& edge_data, int shard_id);
  static void multiInsertVertex(std::vector<VertexData>& vertex_data,
                         std::vector<EdgeData>& edge_data, int shard_id);
  static void insertVertex(std::vector<VertexData>& vertex_data,
                    std::vector<EdgeData>& edge_data, int shard_id);
  static void singleInsertEdge(std::vector<EdgeData>& edge_data, int shard_id);
  static void multiInsertEdge(std::vector<EdgeData>& edge_data, int shard_id);
  static void insertEdge(std::vector<EdgeData>& edge_data, int shard_id);
  static void updateVertex(std::vector<VertexData>& vertex_data, int shard_id);
  static void updateEdge(std::vector<EdgeData>& edge_data, int shard_id);
  static nlohmann::json getVertex(std::vector<VertexData>& vertex_data, int shard_id);
  static nlohmann::json getEdge(std::vector<EdgeData>& edge_data, int shard_id);
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_APP_MANAGER_H_
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

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/manager.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/utils/service_utils.h"
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <vector>

namespace gs {

struct VertexData{
  std::string label;
  gs::Any pk_value;
  std::unordered_map<std::string, gs::Any> properties;
  std::string pk_name;
  std::vector<std::string> col_names;
  gs::label_t label_id;
  VertexData(){}
  ~VertexData(){}
};

struct EdgeData{
  std::string src_label, dst_label, edge_label;
  gs::Any src_pk_value, dst_pk_value;
  std::string property_name;
  gs::Any property_value;

  gs::label_t src_label_id, dst_label_id, edge_label_id;
  EdgeData(){}
  ~EdgeData(){}
};

// base class for vertex and edge manager
class VertexEdgeManager {
 public:
  VertexEdgeManager(std::vector<VertexData>&& vertex_data,
                    std::vector<EdgeData>&& edge_data,
                    nlohmann::json&& schema_json, int shard_id);
  ~VertexEdgeManager() {}
  void checkVertexSchema();
  void checkEdgeSchema();
  void getLabelId();
  void checkEdgeExistsWithInsert();
  void checkEdgeExists();
  // check vertex exists
  void checkVertexExists();
  void singleInsertVertex();
  void multiInsertVertex();
  void insertVertex();
  void singleInsertEdge();
  void multiInsertEdge();
  void insertEdge();
  void updateVertex();
  void updateEdge();
  std::string getVertex();
  std::string getEdge();
 protected:
  std::vector<VertexData> vertex_data;
  std::vector<EdgeData> edge_data;
  const nlohmann::json &schema_json;
  gs::GraphDBSession& db;
};

}  // namespace gs

#endif // ENGINES_GRAPH_DB_APP_MANAGER_H_
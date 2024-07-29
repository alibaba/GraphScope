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
#ifndef ENGINES_GRAPH_DB_OPERATIONS_H_
#define ENGINES_GRAPH_DB_OPERATIONS_H_

#include <string>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "utils/result.h"

namespace gs {

struct VertexData {
  Any pk_value;
  label_t label_id;
  std::vector<Any> properties;
  VertexData() {}
  ~VertexData() {}
};

struct EdgeData {
  label_t src_label_id, dst_label_id, edge_label_id;
  Any src_pk_value, dst_pk_value;
  Any property_value;
  EdgeData() {}
  ~EdgeData() {}
};

class GraphDBOperations {
 public:
  static Result<std::string> CreateVertex(GraphDBSession& session,
                                          nlohmann::json&& input_json);
  static Result<std::string> CreateEdge(GraphDBSession& session,
                                        nlohmann::json&& input_json);
  static Result<std::string> UpdateVertex(GraphDBSession& session,
                                          nlohmann::json&& input_json);
  static Result<std::string> UpdateEdge(GraphDBSession& session,
                                        nlohmann::json&& input_json);
  static Result<std::string> GetVertex(
      GraphDBSession& session,
      std::unordered_map<std::string, std::string>&& params);
  static Result<std::string> GetEdge(
      GraphDBSession& session,
      std::unordered_map<std::string, std::string>&& params);
  static Result<std::string> DeleteVertex(GraphDBSession& session,
                                          nlohmann::json&& input_json);
  static Result<std::string> DeleteEdge(GraphDBSession& session,
                                        nlohmann::json&& input_json);

 private:
  static Result<std::string> response(const StatusCode& code,
                                      const std::string& message) {
    return Result<std::string>(code, message);
  }
  // The following interfaces are called before the Transaction is constructed
  static VertexData inputVertex(const nlohmann::json& vertex_json,
                                const Schema& schema, GraphDBSession& session);
  static EdgeData inputEdge(const nlohmann::json& edge_json,
                            const Schema& schema, GraphDBSession& session);
  // check schema
  static void checkVertexSchema(const Schema& schema, VertexData& vertex,
                                const std::string& label,
                                std::vector<std::string>& input_property_names,
                                bool is_get = false);

  static std::string checkEdgeSchema(const Schema& schema, EdgeData& edge,
                                     const std::string& src_label,
                                     const std::string& dst_label,
                                     const std::string& edge_label,
                                     bool is_get = false);

  // The following interfaces are called after the Transaction is constructed
  // db check
  static void checkEdgeExistsWithInsert(const std::vector<EdgeData>& edge_data,
                                        GraphDBSession& session);
  static void checkEdgeExists(const std::vector<EdgeData>& edge_data,
                              GraphDBSession& session);
  static void checkVertexExists(const std::vector<VertexData>& vertex_data,
                                GraphDBSession& session);
  // db operations
  static void multiInsert(std::vector<VertexData>&& vertex_data,
                          std::vector<EdgeData>&& edge_data,
                          GraphDBSession& session);
  static void singleInsertVertex(std::vector<VertexData>&& vertex_data,
                                 std::vector<EdgeData>&& edge_data,
                                 GraphDBSession& session);
  static void insertVertex(std::vector<VertexData>&& vertex_data,
                           std::vector<EdgeData>&& edge_data,
                           GraphDBSession& session);
  static void singleInsertEdge(std::vector<EdgeData>&& edge_data,
                               GraphDBSession& session);
  static void insertEdge(std::vector<EdgeData>&& edge_data,
                         GraphDBSession& session);
  static void updateVertex(std::vector<VertexData>&& vertex_data,
                           GraphDBSession& session);
  static void updateEdge(std::vector<EdgeData>&& edge_data,
                         GraphDBSession& session);
  static nlohmann::json getVertex(std::vector<VertexData>&& vertex_data,
                                  std::vector<std::string> property_names,
                                  GraphDBSession& session);
  static nlohmann::json getEdge(std::vector<EdgeData>&& edge_data,
                                std::string property_name,
                                GraphDBSession& session);
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_OPERATIONS_H_
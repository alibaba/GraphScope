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
#ifndef ENGINES_GRAPH_DB_DATABASE_OPERATIONS_H_
#define ENGINES_GRAPH_DB_DATABASE_OPERATIONS_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "utils/result.h"

#include <rapidjson/document.h>

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
                                          rapidjson::Document&& input_json);
  static Result<std::string> CreateEdge(GraphDBSession& session,
                                        rapidjson::Document&& input_json);
  static Result<std::string> UpdateVertex(GraphDBSession& session,
                                          rapidjson::Document&& input_json);
  static Result<std::string> UpdateEdge(GraphDBSession& session,
                                        rapidjson::Document&& input_json);
  static Result<std::string> GetVertex(
      GraphDBSession& session,
      std::unordered_map<std::string, std::string>&& params);
  static Result<std::string> GetEdge(
      GraphDBSession& session,
      std::unordered_map<std::string, std::string>&& params);
  static Result<std::string> DeleteVertex(GraphDBSession& session,
                                          rapidjson::Document&& input_json);
  static Result<std::string> DeleteEdge(GraphDBSession& session,
                                        rapidjson::Document&& input_json);

 private:
  // The following interfaces are called before the Transaction is constructed
  static VertexData inputVertex(const rapidjson::Value& vertex_json,
                                const Schema& schema, GraphDBSession& session);
  static EdgeData inputEdge(const rapidjson::Value& edge_json,
                            const Schema& schema, GraphDBSession& session);
  // check schema
  static Status checkVertexSchema(
      const Schema& schema, VertexData& vertex, const std::string& label,
      std::vector<std::string>& input_property_names, bool is_get = false);

  static Status checkEdgeSchema(const Schema& schema, EdgeData& edge,
                                const std::string& src_label,
                                const std::string& dst_label,
                                const std::string& edge_label,
                                std::string& property_name,
                                bool is_get = false);

  // The following interfaces are called after the Transaction is constructed
  // db check
  static Status checkEdgeExistsWithInsert(
      const std::vector<EdgeData>& edge_data, GraphDBSession& session);
  static Status checkEdgeExists(const std::vector<EdgeData>& edge_data,
                                GraphDBSession& session);
  static Status checkVertexExists(const std::vector<VertexData>& vertex_data,
                                  GraphDBSession& session);
  // db operations
  static Status multiInsert(std::vector<VertexData>&& vertex_data,
                            std::vector<EdgeData>&& edge_data,
                            GraphDBSession& session);
  static Status singleInsertVertex(std::vector<VertexData>&& vertex_data,
                                   std::vector<EdgeData>&& edge_data,
                                   GraphDBSession& session);
  static Status insertVertex(std::vector<VertexData>&& vertex_data,
                             std::vector<EdgeData>&& edge_data,
                             GraphDBSession& session);
  static Status singleInsertEdge(std::vector<EdgeData>&& edge_data,
                                 GraphDBSession& session);
  static Status insertEdge(std::vector<EdgeData>&& edge_data,
                           GraphDBSession& session);
  static Status updateVertex(std::vector<VertexData>&& vertex_data,
                             GraphDBSession& session);
  static Status updateEdge(std::vector<EdgeData>&& edge_data,
                           GraphDBSession& session);
  static Result<rapidjson::Value> getEdge(
      std::vector<EdgeData>&& edge_data, const std::string& property_name,
      GraphDBSession& session, rapidjson::Document::AllocatorType& allocator);
  static Result<rapidjson::Value> getVertex(
      std::vector<VertexData>&& vertex_data,
      const std::vector<std::string>& property_names, GraphDBSession& session,
      rapidjson::Document::AllocatorType& allocator);
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_DATABASE_OPERATIONS_H_
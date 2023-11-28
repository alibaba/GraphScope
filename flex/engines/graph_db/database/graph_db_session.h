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

#ifndef GRAPHSCOPE_DATABASE_GRAPH_DB_SESSION_H_
#define GRAPHSCOPE_DATABASE_GRAPH_DB_SESSION_H_

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/insert_transaction.h"
#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/database/single_edge_insert_transaction.h"
#include "flex/engines/graph_db/database/single_vertex_insert_transaction.h"
#include "flex/engines/graph_db/database/update_transaction.h"
#include "flex/proto_generated_gie/stored_procedure.pb.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/utils/property/column.h"
#include "flex/utils/result.h"

namespace gs {

class GraphDB;
class WalWriter;
class ArenaAllocator;

void put_argment(gs::Encoder& encoder, const query::Argument& argment);

class GraphDBSession {
 public:
  static constexpr int32_t MAX_RETRY = 4;
  GraphDBSession(GraphDB& db, ArenaAllocator& alloc, WalWriter& logger,
                 int thread_id)
      : db_(db), alloc_(alloc), logger_(logger), thread_id_(thread_id) {
    for (auto& app : apps_) {
      app = nullptr;
    }
  }
  ~GraphDBSession() {}

  ReadTransaction GetReadTransaction();

  InsertTransaction GetInsertTransaction();

  SingleVertexInsertTransaction GetSingleVertexInsertTransaction();

  SingleEdgeInsertTransaction GetSingleEdgeInsertTransaction();

  UpdateTransaction GetUpdateTransaction();

  const MutablePropertyFragment& graph() const;
  MutablePropertyFragment& graph();

  const Schema& schema() const;

  std::shared_ptr<ColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& col_name) const;

  // Get vertex id column.
  std::shared_ptr<RefColumnBase> get_vertex_id_column(uint8_t label) const;

  Result<std::vector<char>> Eval(const std::string& input);

  // Evaluate a temporary stored procedure. close the handle of the dynamic lib
  // immediately.
  Result<std::vector<char>> EvalAdhoc(const std::string& input_lib_path);

  // Evaluate a stored procedure with input parameters given.
  Result<std::vector<char>> EvalHqpsProcedure(const query::Query& query_pb);

  void GetAppInfo(Encoder& result);

  int SessionId() const;

 private:
  GraphDB& db_;
  ArenaAllocator& alloc_;
  WalWriter& logger_;
  int thread_id_;

  std::array<AppWrapper, 256> app_wrappers_;
  std::array<AppBase*, 256> apps_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_GRAPH_DB_SESSION_H_

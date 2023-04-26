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

#ifndef GRAPHSCOPE_DATABASE_GRAPH_DB_H_
#define GRAPHSCOPE_DATABASE_GRAPH_DB_H_

#include <dlfcn.h>

#include <map>
#include <mutex>
#include <thread>
#include <vector>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/insert_transaction.h"
#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/database/single_edge_insert_transaction.h"
#include "flex/engines/graph_db/database/single_vertex_insert_transaction.h"
#include "flex/engines/graph_db/database/update_transaction.h"
#include "flex/engines/graph_db/database/version_manager.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

namespace gs {

class GraphDB;
class GraphDBSession;
class SessionLocalContext;

class GraphDB {
 public:
  GraphDB();
  ~GraphDB();

  static GraphDB& get();

  void Init(
      const Schema& schema,
      const std::vector<std::pair<std::string, std::string>>& vertex_files,
      const std::vector<std::tuple<std::string, std::string, std::string,
                                   std::string>>& edge_files,
      const std::vector<std::string>& plugins, const std::string& data_dir,
      int thread_num = 1);

  /** @brief Create a transaction to read vertices and edges.
   *
   * @return graph_dir The directory of graph data.
   */
  ReadTransaction GetReadTransaction();

  /** @brief Create a transaction to insert vertices and edges with a default
   * allocator.
   *
   * @return InsertTransaction
   */
  InsertTransaction GetInsertTransaction(int thread_id = 0);

  /** @brief Create a transaction to insert a single vertex.
   *
   * @param alloc Allocator to allocate memory for graph.
   * @return SingleVertexInsertTransaction
   */
  SingleVertexInsertTransaction GetSingleVertexInsertTransaction(
      int thread_id = 0);

  /** @brief Create a transaction to insert a single edge.
   *
   * @param alloc Allocator to allocate memory for graph.
   * @return
   */
  SingleEdgeInsertTransaction GetSingleEdgeInsertTransaction(int thread_id = 0);

  /** @brief Create a transaction to update vertices and edges.
   *
   * @param alloc Allocator to allocate memory for graph.
   * @return UpdateTransaction
   */
  UpdateTransaction GetUpdateTransaction(int thread_id = 0);

  const MutablePropertyFragment& graph() const;
  MutablePropertyFragment& graph();

  const Schema& schema() const;

  std::shared_ptr<ColumnBase> get_vertex_property_column(
      uint8_t label, const std::string& col_name) const;

  AppWrapper CreateApp(uint8_t app_type, int thread_id);

  void GetAppInfo(Encoder& result);

  GraphDBSession& GetSession(int thread_id);

  int SessionNum() const;

 private:
  void registerApp(const std::string& path, uint8_t index = 0);

  void ingestWals(const std::vector<std::string>& wals, int thread_num);

  void initApps(const std::vector<std::string>& plugins);

  friend class GraphDBSession;

  SessionLocalContext* contexts_;

  int thread_num_;

  MutablePropertyFragment graph_;
  VersionManager version_manager_;

  std::array<std::string, 256> app_paths_;
  std::array<std::shared_ptr<AppFactoryBase>, 256> app_factories_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_GRAPH_DB_H_

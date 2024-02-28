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
#include "flex/storages/rt_mutable_graph/loader/loader_factory.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

namespace gs {

class GraphDB;
class GraphDBSession;
struct SessionLocalContext;

struct GraphDBConfig {
  GraphDBConfig(const Schema& schema_, const std::string& data_dir_,
                int thread_num_ = 1)
      : schema(schema_),
        data_dir(data_dir_),
        thread_num(thread_num_),
        warmup(false),
        enable_monitoring(false),
        enable_auto_compaction(false),
        memory_level(1) {}

  Schema schema;
  std::string data_dir;
  int thread_num;
  bool warmup;
  bool enable_monitoring;
  bool enable_auto_compaction;

  /*
    0 - sync with disk;
    1 - mmap virtual memory;
    2 - preferring hugepages;
    3 - force hugepages;
  */
  int memory_level;
};

class GraphDB {
 public:
  GraphDB();
  ~GraphDB();

  static GraphDB& get();

  /**
   * @brief Load the graph from data directory.
   * @param schema The schema of graph. It should be the same as the schema,
   * except that the procedure enable_lists changes.
   * @param data_dir The directory of graph data.
   * @param thread_num The number of threads for graph db concurrency
   * @param warmup Whether to warmup the graph db.
   */
  Result<bool> Open(const Schema& schema, const std::string& data_dir,
                    int32_t thread_num = 1, bool warmup = false,
                    bool memory_only = true,
                    bool enable_auto_compaction = false);

  Result<bool> Open(const GraphDBConfig& config);

  /**
   * @brief Close the current opened graph.
   */
  void Close();

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
  const GraphDBSession& GetSession(int thread_id) const;

  int SessionNum() const;

  void UpdateCompactionTimestamp(timestamp_t ts);
  timestamp_t GetLastCompactionTimestamp() const;

 private:
  bool registerApp(const std::string& path, uint8_t index = 0);

  void ingestWals(const std::vector<std::string>& wals,
                  const std::string& work_dir, int thread_num);

  void initApps(
      const std::unordered_map<std::string, std::pair<std::string, uint8_t>>&
          plugins);

  void openWalAndCreateContexts(const std::string& data_dir_path,
                                MemoryStrategy allocator_strategy);

  void showAppMetrics() const;

  size_t getExecutedQueryNum() const;

  friend class GraphDBSession;

  std::string work_dir_;
  SessionLocalContext* contexts_;

  int thread_num_;

  MutablePropertyFragment graph_;
  VersionManager version_manager_;

  std::array<std::string, 256> app_paths_;
  std::array<std::shared_ptr<AppFactoryBase>, 256> app_factories_;

  std::thread monitor_thread_;
  bool monitor_thread_running_;

  timestamp_t last_compaction_ts_;
  bool compact_thread_running_ = false;
  std::thread compact_thread_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_GRAPH_DB_H_

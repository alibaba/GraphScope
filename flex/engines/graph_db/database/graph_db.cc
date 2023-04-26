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

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"

#include "flex/engines/graph_db/app/server_app.h"
#include "flex/engines/graph_db/database/wal.h"

namespace gs {

struct SessionLocalContext {
  SessionLocalContext(GraphDB& db, int thread_id)
      : session(db, allocator, logger, thread_id) {}
  ~SessionLocalContext() { logger.close(); }

  ArenaAllocator allocator;
  char _padding0[128 - sizeof(ArenaAllocator) % 128];
  WalWriter logger;
  char _padding1[4096 - sizeof(WalWriter) - sizeof(ArenaAllocator) -
                 sizeof(_padding0)];
  GraphDBSession session;
  char _padding2[4096 - sizeof(GraphDBSession) % 4096];
};

GraphDB::GraphDB() = default;
GraphDB::~GraphDB() {
  for (int i = 0; i < thread_num_; ++i) {
    contexts_[i].~SessionLocalContext();
  }
  free(contexts_);
}

GraphDB& GraphDB::get() {
  static GraphDB db;
  return db;
}

void GraphDB::Init(
    const Schema& schema,
    const std::vector<std::pair<std::string, std::string>>& vertex_files,
    const std::vector<std::tuple<std::string, std::string, std::string,
                                 std::string>>& edge_files,
    const std::vector<std::string>& plugins, const std::string& data_dir,
    int thread_num) {
  std::filesystem::path data_dir_path(data_dir);
  if (!std::filesystem::exists(data_dir_path)) {
    std::filesystem::create_directory(data_dir_path);
  }

  std::filesystem::path serial_path = data_dir_path / "init_snapshot.bin";
  if (!std::filesystem::exists(serial_path)) {
    if (!vertex_files.empty() || !edge_files.empty()) {
      LOG(INFO) << "Initializing graph db through bulk loading";
    } else {
      LOG(INFO) << "Initializing empty graph db";
    }
    graph_.Init(schema, vertex_files, edge_files, thread_num);
    graph_.Serialize(data_dir_path.string());
  } else {
    LOG(INFO) << "Initializing graph db from data files of work directory";
    if (!vertex_files.empty() || !edge_files.empty()) {
      LOG(WARNING) << "Bulk loading is ignored because data files of work "
                      "directory exist";
    }
    graph_.Deserialize(data_dir_path.string());
    if (!graph_.schema().Equals(schema)) {
      LOG(FATAL)
          << "Schema of work directory is not compatible with the given schema";
    }
  }

  std::filesystem::path wal_dir = data_dir_path / "wal";
  if (!std::filesystem::exists(wal_dir)) {
    std::filesystem::create_directory(wal_dir);
  }
  std::vector<std::string> wal_files;
  for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
    wal_files.push_back(entry.path().string());
  }

  thread_num_ = thread_num;
  contexts_ = static_cast<SessionLocalContext*>(
      aligned_alloc(4096, sizeof(SessionLocalContext) * thread_num));
  for (int i = 0; i < thread_num_; ++i) {
    new (&contexts_[i]) SessionLocalContext(*this, i);
  }
  ingestWals(wal_files, thread_num_);

  for (int i = 0; i < thread_num_; ++i) {
    contexts_[i].logger.open(wal_dir.string(), i);
  }

  initApps(plugins);
}

ReadTransaction GraphDB::GetReadTransaction() {
  uint32_t ts = version_manager_.acquire_read_timestamp();
  return {graph_, version_manager_, ts};
}

InsertTransaction GraphDB::GetInsertTransaction(int thread_id) {
  return contexts_[thread_id].session.GetInsertTransaction();
}

SingleVertexInsertTransaction GraphDB::GetSingleVertexInsertTransaction(
    int thread_id) {
  return contexts_[thread_id].session.GetSingleVertexInsertTransaction();
}

SingleEdgeInsertTransaction GraphDB::GetSingleEdgeInsertTransaction(
    int thread_id) {
  return contexts_[thread_id].session.GetSingleEdgeInsertTransaction();
}

UpdateTransaction GraphDB::GetUpdateTransaction(int thread_id) {
  return contexts_[thread_id].session.GetUpdateTransaction();
}

GraphDBSession& GraphDB::GetSession(int thread_id) {
  return contexts_[thread_id].session;
}

int GraphDB::SessionNum() const { return thread_num_; }

const MutablePropertyFragment& GraphDB::graph() const { return graph_; }
MutablePropertyFragment& GraphDB::graph() { return graph_; }

const Schema& GraphDB::schema() const { return graph_.schema(); }

std::shared_ptr<ColumnBase> GraphDB::get_vertex_property_column(
    uint8_t label, const std::string& col_name) const {
  return graph_.get_vertex_table(label).get_column(col_name);
}

AppWrapper GraphDB::CreateApp(uint8_t app_type, int thread_id) {
  if (app_factories_[app_type] == nullptr) {
    LOG(ERROR) << "Stored procedure " << static_cast<int>(app_type)
               << " is not registered.";
    return AppWrapper(NULL, NULL);
  } else {
    return app_factories_[app_type]->CreateApp(contexts_[thread_id].session);
  }
}

void GraphDB::registerApp(const std::string& path, uint8_t index) {
  // this function will only be called when initializing the graph db
  if (index == 0) {
    for (size_t i = 1; i != 256; ++i) {
      if (app_factories_[i] == nullptr) {
        index = static_cast<uint8_t>(i);
        break;
      }
    }
  }
  if (index == 0) {
    LOG(ERROR) << "too many stored procedures...";
  }
  app_paths_[index] = path;
  app_factories_[index] = std::make_shared<SharedLibraryAppFactory>(path);
}

void GraphDB::GetAppInfo(Encoder& output) {
  std::string ret;
  for (size_t i = 1; i != 256; ++i) {
    output.put_string(app_paths_[i]);
  }
}

static void IngestWalRange(SessionLocalContext* contexts,
                           MutablePropertyFragment& graph,
                           const WalsParser& parser, uint32_t from, uint32_t to,
                           int thread_num) {
  std::atomic<uint32_t> cur_ts(from);
  std::vector<std::thread> threads(thread_num);
  for (int i = 0; i < thread_num; ++i) {
    threads[i] = std::thread(
        [&](int tid) {
          auto& alloc = contexts[tid].allocator;
          while (true) {
            uint32_t got_ts = cur_ts.fetch_add(1);
            if (got_ts >= to) {
              break;
            }
            const auto& unit = parser.get_insert_wal(got_ts);
            InsertTransaction::IngestWal(graph, got_ts, unit.ptr, unit.size,
                                         alloc);
            if (got_ts % 1000000 == 0) {
              LOG(INFO) << "Ingested " << got_ts << " WALs";
            }
          }
        },
        i);
  }
  for (auto& thrd : threads) {
    thrd.join();
  }
}

void GraphDB::ingestWals(const std::vector<std::string>& wals, int thread_num) {
  WalsParser parser(wals);
  uint32_t from_ts = 1;
  for (auto& update_wal : parser.update_wals()) {
    uint32_t to_ts = update_wal.timestamp;
    if (from_ts < to_ts) {
      IngestWalRange(contexts_, graph_, parser, from_ts, to_ts, thread_num);
    }
    UpdateTransaction::IngestWal(graph_, to_ts, update_wal.ptr, update_wal.size,
                                 contexts_[0].allocator);
    from_ts = to_ts + 1;
  }
  if (from_ts <= parser.last_ts()) {
    IngestWalRange(contexts_, graph_, parser, from_ts, parser.last_ts() + 1,
                   thread_num);
  }
  version_manager_.init_ts(parser.last_ts());
}

void GraphDB::initApps(const std::vector<std::string>& plugins) {
  for (size_t i = 0; i < 256; ++i) {
    app_factories_[i] = nullptr;
  }
  app_factories_[0] = std::make_shared<ServerAppFactory>();

  uint8_t sp_index = 1;
  for (auto path : plugins) {
    registerApp(path, sp_index++);
  }
}

}  // namespace gs

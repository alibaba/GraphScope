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
#include "flex/utils/yaml_utils.h"

#include "flex/third_party/httplib.h"

namespace gs {

struct SessionLocalContext {
  SessionLocalContext(GraphDB& db, const std::string& work_dir, int thread_id,
                      bool memory_only)
      : allocator(memory_only
                      ? ""
                      : thread_local_allocator_prefix(work_dir, thread_id)),
        session(db, allocator, logger, work_dir, thread_id) {}
  ~SessionLocalContext() { logger.close(); }
  Allocator allocator;
  char _padding0[128 - sizeof(Allocator) % 128];
  WalWriter logger;
  char _padding1[4096 - sizeof(WalWriter) - sizeof(Allocator) -
                 sizeof(_padding0)];
  GraphDBSession session;
  char _padding2[4096 - sizeof(GraphDBSession) % 4096];
};

GraphDB::GraphDB() = default;
GraphDB::~GraphDB() {
  if (compact_thread_running_) {
    compact_thread_running_ = false;
    compact_thread_.join();
  }
  for (int i = 0; i < thread_num_; ++i) {
    contexts_[i].~SessionLocalContext();
  }
  free(contexts_);
}

GraphDB& GraphDB::get() {
  static GraphDB db;
  return db;
}

Result<bool> GraphDB::Open(const Schema& schema, const std::string& data_dir,
                           int32_t thread_num, bool warmup, bool memory_only,
                           bool enable_auto_compaction, int port) {
  if (!std::filesystem::exists(data_dir)) {
    std::filesystem::create_directories(data_dir);
  }

  std::string schema_file = schema_path(data_dir);
  bool create_empty_graph = false;
  if (!std::filesystem::exists(schema_file)) {
    create_empty_graph = true;
    graph_.mutable_schema() = schema;
  }
  work_dir_ = data_dir;
  thread_num_ = thread_num;
  try {
    graph_.Open(data_dir, memory_only);
  } catch (std::exception& e) {
    LOG(ERROR) << "Exception: " << e.what();
    return Result<bool>(StatusCode::InternalError,
                        "Exception: " + std::string(e.what()), false);
  }

  if ((!create_empty_graph) && (!graph_.schema().Equals(schema))) {
    LOG(ERROR) << "Schema inconsistent..\n";
    return Result<bool>(StatusCode::InternalError,
                        "Schema of work directory is not compatible with the "
                        "graph schema",
                        false);
  }
  // Set the plugin info from schema to graph_.schema(), since the plugin info
  // is not serialized and deserialized.
  auto& mutable_schema = graph_.mutable_schema();
  mutable_schema.SetPluginDir(schema.GetPluginDir());
  std::vector<std::string> plugin_paths;
  const auto& plugins = schema.GetPlugins();
  for (auto plugin_pair : plugins) {
    plugin_paths.emplace_back(plugin_pair.first);
  }

  std::sort(plugin_paths.begin(), plugin_paths.end(),
            [&](const std::string& a, const std::string& b) {
              return plugins.at(a).second < plugins.at(b).second;
            });
  mutable_schema.EmplacePlugins(plugin_paths);

  last_compaction_ts_ = 0;
  openWalAndCreateContexts(data_dir, memory_only);

  if ((!create_empty_graph) && warmup) {
    graph_.Warmup(thread_num_);
  }

  if (enable_auto_compaction && (port != -1)) {
    if (compact_thread_running_) {
      compact_thread_running_ = false;
      compact_thread_.join();
    }
    compact_thread_running_ = true;
    compact_thread_ = std::thread([&](int http_port) {
      size_t last_compaction_at = 0;
      while (compact_thread_running_) {
        size_t query_num_before = getExecutedQueryNum();
        sleep(30);
        if (!compact_thread_running_) {
          break;
        }
        size_t query_num_after = getExecutedQueryNum();
        if (query_num_before == query_num_after &&
            (query_num_after > (last_compaction_at + 100000))) {
          VLOG(10) << "Trigger auto compaction";
          last_compaction_at = query_num_after;
          std::string url = "127.0.0.1";
          httplib::Client cli(url, http_port);
          cli.set_connection_timeout(0, 300000);
          cli.set_read_timeout(300, 0);
          cli.set_write_timeout(300, 0);

          std::vector<char> buf;
          Encoder encoder(buf);
          encoder.put_string("COMPACTION");
          encoder.put_byte(0);
          std::string content(buf.data(), buf.size());
          auto res = cli.Post("/interactive/query", content, "text/plain");
          std::string ret = res->body;
          Decoder decoder(ret.data(), ret.size());
          std::string_view info = decoder.get_string();

          VLOG(10) << "Finish compaction, info: " << info;
        }
      }
    }, port);
  }

  return Result<bool>(true);
}

void GraphDB::Close() {
#ifdef MONITOR_SESSIONS
  monitor_thread_running_ = false;
  monitor_thread_.join();
#endif
  if (compact_thread_running_) {
    compact_thread_running_ = false;
    compact_thread_.join();
  }
  //-----------Clear graph_db----------------
  graph_.Clear();
  version_manager_.clear();
  if (contexts_ != nullptr) {
    for (int i = 0; i < thread_num_; ++i) {
      contexts_[i].~SessionLocalContext();
    }
    free(contexts_);
  }
  std::fill(app_paths_.begin(), app_paths_.end(), "");
  std::fill(app_factories_.begin(), app_factories_.end(), nullptr);
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

void GraphDB::UpdateCompactionTimestamp(timestamp_t ts) {
  last_compaction_ts_ = ts;
}
timestamp_t GraphDB::GetLastCompactionTimestamp() const {
  return last_compaction_ts_;
}

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

bool GraphDB::registerApp(const std::string& plugin_path, uint8_t index) {
  // this function will only be called when initializing the graph db
  VLOG(10) << "Registering stored procedure at:" << std::to_string(index)
           << ", path:" << plugin_path;
  if (!app_factories_[index] && app_paths_[index].empty()) {
    app_paths_[index] = plugin_path;
    app_factories_[index] =
        std::make_shared<SharedLibraryAppFactory>(plugin_path);
    return true;
  } else {
    LOG(ERROR) << "Stored procedure has already been registered at:"
               << std::to_string(index) << ", path:" << app_paths_[index];
    return false;
  }
}

void GraphDB::GetAppInfo(Encoder& output) {
  std::string ret;
  for (size_t i = 1; i != 256; ++i) {
    if (!app_paths_.empty()) {
      output.put_string(app_paths_[i]);
    }
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

void GraphDB::ingestWals(const std::vector<std::string>& wals,
                         const std::string& work_dir, int thread_num) {
  WalsParser parser(wals);
  uint32_t from_ts = 1;
  for (auto& update_wal : parser.update_wals()) {
    uint32_t to_ts = update_wal.timestamp;
    if (from_ts < to_ts) {
      IngestWalRange(contexts_, graph_, parser, from_ts, to_ts, thread_num);
    }
    if (update_wal.size == 0) {
      graph_.Compact(update_wal.timestamp);
      last_compaction_ts_ = update_wal.timestamp;
    } else {
      UpdateTransaction::IngestWal(graph_, work_dir, to_ts, update_wal.ptr,
                                   update_wal.size, contexts_[0].allocator);
    }
    from_ts = to_ts + 1;
  }
  if (from_ts <= parser.last_ts()) {
    IngestWalRange(contexts_, graph_, parser, from_ts, parser.last_ts() + 1,
                   thread_num);
  }
  version_manager_.init_ts(parser.last_ts(), thread_num);
}

void GraphDB::initApps(
    const std::unordered_map<std::string, std::pair<std::string, uint8_t>>&
        plugins) {
  VLOG(1) << "Initializing stored procedures, size: " << plugins.size()
          << " ...";
  for (size_t i = 0; i < 256; ++i) {
    app_factories_[i] = nullptr;
  }
  app_factories_[0] = std::make_shared<ServerAppFactory>();
  size_t valid_plugins = 0;
  for (auto& path_and_index : plugins) {
    auto path = path_and_index.second.first;
    auto index = path_and_index.second.second;
    if (registerApp(path, index)) {
      ++valid_plugins;
    }
  }
  LOG(INFO) << "Successfully registered stored procedures : " << valid_plugins
            << ", from " << plugins.size();
}

void GraphDB::openWalAndCreateContexts(const std::string& data_dir,
                                       bool memory_only) {
  std::string wal_dir_path = wal_dir(data_dir);
  if (!std::filesystem::exists(wal_dir_path)) {
    std::filesystem::create_directory(wal_dir_path);
  }

  std::vector<std::string> wal_files;
  for (const auto& entry : std::filesystem::directory_iterator(wal_dir_path)) {
    wal_files.push_back(entry.path().string());
  }

  contexts_ = static_cast<SessionLocalContext*>(
      aligned_alloc(4096, sizeof(SessionLocalContext) * thread_num_));
  std::filesystem::create_directories(allocator_dir(data_dir));
  for (int i = 0; i < thread_num_; ++i) {
    new (&contexts_[i]) SessionLocalContext(*this, data_dir, i, memory_only);
  }
  ingestWals(wal_files, data_dir, thread_num_);

  for (int i = 0; i < thread_num_; ++i) {
    contexts_[i].logger.open(wal_dir_path, i);
  }

  initApps(graph_.schema().GetPlugins());
  VLOG(1) << "Successfully restore load plugins";

#ifdef MONITOR_SESSIONS
  monitor_thread_running_ = true;
  monitor_thread_ = std::thread([&]() {
    size_t last_allocated_size = 0;
    std::vector<double> last_eval_durations(thread_num_, 0);
    std::vector<int64_t> last_query_nums(thread_num_, 0);
    while (monitor_thread_running_) {
      sleep(10);
      size_t curr_allocated_size = 0;
      double total_eval_durations = 0;
      double min_eval_duration = std::numeric_limits<double>::max();
      double max_eval_duration = 0;
      int64_t total_query_num = 0;
      int64_t min_query_num = std::numeric_limits<int64_t>::max();
      int64_t max_query_num = 0;

      for (int i = 0; i < thread_num_; ++i) {
        curr_allocated_size += contexts_[i].allocator.allocated_memory();
        if (last_eval_durations[i] == 0) {
          last_eval_durations[i] = contexts_[i].session.eval_duration();
        } else {
          double curr = contexts_[i].session.eval_duration();
          double eval_duration = curr;
          total_eval_durations += eval_duration;
          min_eval_duration = std::min(min_eval_duration, eval_duration);
          max_eval_duration = std::max(max_eval_duration, eval_duration);

          last_eval_durations[i] = curr;
        }
        if (last_query_nums[i] == 0) {
          last_query_nums[i] = contexts_[i].session.query_num();
        } else {
          int64_t curr = contexts_[i].session.query_num();
          total_query_num += curr;
          min_query_num = std::min(min_query_num, curr);
          max_query_num = std::max(max_query_num, curr);

          last_query_nums[i] = curr;
        }
      }
      last_allocated_size = curr_allocated_size;
      if (max_query_num != 0) {
        double avg_eval_durations =
            total_eval_durations / static_cast<double>(thread_num_);
        double avg_query_num = static_cast<double>(total_query_num) /
                               static_cast<double>(thread_num_);
        double allocated_size_in_gb =
            static_cast<double>(curr_allocated_size) / 1024.0 / 1024.0 / 1024.0;
        LOG(INFO) << "allocated: " << allocated_size_in_gb << " GB, eval: ["
                  << min_eval_duration << ", " << avg_eval_durations << ", "
                  << max_eval_duration << "] s, query num: [" << min_query_num
                  << ", " << avg_query_num << ", " << max_query_num << "]";
      }
    }
  });
#endif
}

size_t GraphDB::getExecutedQueryNum() const {
  size_t ret = 0;
  for (int i = 0; i < thread_num_; ++i) {
    ret += contexts_[i].session.query_num();
  }
  return ret;
}

}  // namespace gs

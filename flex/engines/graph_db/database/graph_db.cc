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
#include "flex/engines/graph_db/app/builtin/count_vertices.h"
#include "flex/engines/graph_db/app/builtin/k_hop_neighbors.h"
#include "flex/engines/graph_db/app/builtin/pagerank.h"
#include "flex/engines/graph_db/app/builtin/shortest_path_among_three.h"
#include "flex/engines/graph_db/app/cypher_read_app.h"
#include "flex/engines/graph_db/app/cypher_write_app.h"
#include "flex/engines/graph_db/app/hqps_app.h"
#include "flex/engines/graph_db/app/server_app.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/database/wal/wal.h"
#include "flex/engines/graph_db/runtime/execute/plan_parser.h"
#include "flex/engines/graph_db/runtime/utils/cypher_runner_impl.h"
#include "flex/utils/yaml_utils.h"

#include "flex/third_party/httplib.h"

namespace gs {

struct SessionLocalContext {
  SessionLocalContext(GraphDB& db, const std::string& work_dir, int thread_id,
                      MemoryStrategy allocator_strategy,
                      std::unique_ptr<IWalWriter> in_logger)
      : allocator(allocator_strategy,
                  (allocator_strategy != MemoryStrategy::kSyncToFile
                       ? ""
                       : thread_local_allocator_prefix(work_dir, thread_id))),
        logger(std::move(in_logger)),
        session(db, allocator, *logger, work_dir, thread_id) {}
  ~SessionLocalContext() { logger->close(); }
  Allocator allocator;
  char _padding0[128 - sizeof(Allocator) % 128];
  std::unique_ptr<IWalWriter> logger;
  char _padding1[4096 - sizeof(std::unique_ptr<IWalWriter>) -
                 sizeof(Allocator) - sizeof(_padding0)];
  GraphDBSession session;
  char _padding2[4096 - sizeof(GraphDBSession) % 4096];
};

GraphDB::GraphDB() = default;
GraphDB::~GraphDB() {
  if (compact_thread_running_) {
    compact_thread_running_ = false;
    compact_thread_.join();
  }
  if (contexts_ != nullptr) {
    // showAppMetrics();
    for (int i = 0; i < thread_num_; ++i) {
      contexts_[i].~SessionLocalContext();
    }

    free(contexts_);
  }
  WalWriterFactory::Finalize();
  WalParserFactory::Finalize();
}

GraphDB& GraphDB::get() {
  static GraphDB db;
  return db;
}

Result<bool> GraphDB::Open(const Schema& schema, const std::string& data_dir,
                           int32_t thread_num, bool warmup, bool memory_only,
                           bool enable_auto_compaction) {
  GraphDBConfig config(schema, data_dir, "", thread_num);
  config.warmup = warmup;
  if (memory_only) {
    config.memory_level = 1;
  } else {
    config.memory_level = 0;
  }
  config.enable_auto_compaction = enable_auto_compaction;
  return Open(config);
}

Result<bool> GraphDB::Open(const GraphDBConfig& config) {
  config_ = config;
  const std::string& data_dir = config.data_dir;
  const Schema& schema = config.schema;
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
  thread_num_ = config.thread_num;
  try {
    graph_.Open(data_dir, config.memory_level);
  } catch (std::exception& e) {
    LOG(ERROR) << "Exception: " << e.what();
    return Result<bool>(StatusCode::INTERNAL_ERROR,
                        "Exception: " + std::string(e.what()), false);
  }

  if ((!create_empty_graph) && (!graph_.schema().Equals(schema))) {
    LOG(ERROR) << "Schema inconsistent..\n";
    return Result<bool>(StatusCode::INTERNAL_ERROR,
                        "Schema of work directory is not compatible with the "
                        "graph schema",
                        false);
  }
  // Set the plugin info from schema to graph_.schema(), since the plugin info
  // is not serialized and deserialized.
  auto& mutable_schema = graph_.mutable_schema();
  mutable_schema.SetPluginDir(schema.GetPluginDir());
  mutable_schema.set_compiler_path(config.compiler_path);
  std::vector<std::pair<std::string, std::string>> plugin_name_paths;
  const auto& plugins = schema.GetPlugins();
  for (auto plugin_pair : plugins) {
    plugin_name_paths.emplace_back(
        std::make_pair(plugin_pair.first, plugin_pair.second.first));
  }

  std::sort(plugin_name_paths.begin(), plugin_name_paths.end(),
            [&](const std::pair<std::string, std::string>& a,
                const std::pair<std::string, std::string>& b) {
              return plugins.at(a.first).second < plugins.at(b.first).second;
            });
  mutable_schema.EmplacePlugins(plugin_name_paths);

  last_compaction_ts_ = 0;
  MemoryStrategy allocator_strategy = MemoryStrategy::kMemoryOnly;
  if (config.memory_level == 0) {
    allocator_strategy = MemoryStrategy::kSyncToFile;
  } else if (config.memory_level >= 2) {
    allocator_strategy = MemoryStrategy::kHugepagePrefered;
  }

  openWalAndCreateContexts(config, data_dir, allocator_strategy);

  if ((!create_empty_graph) && config.warmup) {
    graph_.Warmup(thread_num_);
  }

  if (config.enable_monitoring) {
    if (monitor_thread_running_) {
      monitor_thread_running_ = false;
      monitor_thread_.join();
    }
    monitor_thread_running_ = true;
    monitor_thread_ = std::thread([&]() {
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
        if (max_query_num != 0) {
          double avg_eval_durations =
              total_eval_durations / static_cast<double>(thread_num_);
          double avg_query_num = static_cast<double>(total_query_num) /
                                 static_cast<double>(thread_num_);
          double allocated_size_in_gb =
              static_cast<double>(curr_allocated_size) / 1024.0 / 1024.0 /
              1024.0;
          LOG(INFO) << "allocated: " << allocated_size_in_gb << " GB, eval: ["
                    << min_eval_duration << ", " << avg_eval_durations << ", "
                    << max_eval_duration << "] s, query num: [" << min_query_num
                    << ", " << avg_query_num << ", " << max_query_num << "]";
        }
      }
    });
  }

  if (config.enable_auto_compaction) {
    if (compact_thread_running_) {
      compact_thread_running_ = false;
      compact_thread_.join();
    }
    compact_thread_running_ = true;
    compact_thread_ = std::thread([&]() {
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
          timestamp_t ts = this->version_manager_.acquire_update_timestamp();
          auto txn =
              CompactTransaction(this->graph_, *this->contexts_[0].logger,
                                 this->version_manager_, ts);
          OutputCypherProfiles("./" + std::to_string(ts) + "_");
          txn.Commit();
          VLOG(10) << "Finish compaction";
        }
      }
    });
  }

  unlink((work_dir_ + "/statistics.json").c_str());
  graph_.generateStatistics(work_dir_);
  runtime::CypherRunnerImpl::get().clear_cache();

  return Result<bool>(true);
}

void GraphDB::Close() {
  if (monitor_thread_running_) {
    monitor_thread_running_ = false;
    monitor_thread_.join();
  }
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
    contexts_ = nullptr;
  }
  std::fill(app_paths_.begin(), app_paths_.end(), "");
  std::fill(app_factories_.begin(), app_factories_.end(), nullptr);
}

ReadTransaction GraphDB::GetReadTransaction(int thread_id) {
  return contexts_[thread_id].session.GetReadTransaction();
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

const GraphDBSession& GraphDB::GetSession(int thread_id) const {
  return contexts_[thread_id].session;
}

int GraphDB::SessionNum() const { return thread_num_; }

void GraphDB::UpdateCompactionTimestamp(timestamp_t ts) {
  last_compaction_ts_ = ts;
}
timestamp_t GraphDB::GetLastCompactionTimestamp() const {
  return last_compaction_ts_;
}

AppWrapper GraphDB::CreateApp(uint8_t app_type, int thread_id) {
  if (app_factories_[app_type] == nullptr) {
    LOG(ERROR) << "Stored procedure " << static_cast<int>(app_type)
               << " is not registered.";
    return AppWrapper(NULL, NULL);
  } else {
    return app_factories_[app_type]->CreateApp(*this);
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
                           const IWalParser& parser, uint32_t from, uint32_t to,
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

void GraphDB::ingestWals(IWalParser& parser, const std::string& work_dir,
                         int thread_num) {
  uint32_t from_ts = 1;
  for (auto& update_wal : parser.get_update_wals()) {
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
  // Builtin apps
  app_factories_[0] = std::make_shared<ServerAppFactory>();
  app_factories_[Schema::BUILTIN_COUNT_VERTICES_PLUGIN_ID] =
      std::make_shared<CountVerticesFactory>();
  app_factories_[Schema::BUILTIN_PAGERANK_PLUGIN_ID] =
      std::make_shared<PageRankFactory>();
  app_factories_[Schema::BUILTIN_K_DEGREE_NEIGHBORS_PLUGIN_ID] =
      std::make_shared<KNeighborsFactory>();
  app_factories_[Schema::BUILTIN_TVSP_PLUGIN_ID] =
      std::make_shared<ShortestPathAmongThreeFactory>();

  app_factories_[Schema::HQPS_ADHOC_READ_PLUGIN_ID] =
      std::make_shared<HQPSAdhocReadAppFactory>();
  app_factories_[Schema::HQPS_ADHOC_WRITE_PLUGIN_ID] =
      std::make_shared<HQPSAdhocWriteAppFactory>();
  app_factories_[Schema::ADHOC_READ_PLUGIN_ID] =
      std::make_shared<CypherReadAppFactory>();
  app_factories_[Schema::CYPHER_READ_DEBUG_PLUGIN_ID] =
      std::make_shared<CypherReadAppFactory>();

  auto& parser = gs::runtime::PlanParser::get();
  parser.init();
  app_factories_[Schema::ADHOC_READ_PLUGIN_ID] =
      std::make_shared<CypherReadAppFactory>();

  app_factories_[Schema::CYPHER_READ_PLUGIN_ID] =
      std::make_shared<CypherReadAppFactory>();
  app_factories_[Schema::CYPHER_WRITE_PLUGIN_ID] =
      std::make_shared<CypherWriteAppFactory>();

  size_t valid_plugins = 0;
  for (auto& path_and_index : plugins) {
    auto path = path_and_index.second.first;
    auto name = path_and_index.first;
    auto index = path_and_index.second.second;
    if (!Schema::IsBuiltinPlugin(name)) {
      if (registerApp(path, index)) {
        ++valid_plugins;
      }
    } else {
      valid_plugins++;
    }
  }
  LOG(INFO) << "Successfully registered stored procedures : " << valid_plugins
            << ", from " << plugins.size();
}

void GraphDB::openWalAndCreateContexts(const GraphDBConfig& config,
                                       const std::string& data_dir,
                                       MemoryStrategy allocator_strategy) {
  WalWriterFactory::Init();
  WalParserFactory::Init();
  contexts_ = static_cast<SessionLocalContext*>(
      aligned_alloc(4096, sizeof(SessionLocalContext) * thread_num_));
  std::filesystem::create_directories(allocator_dir(data_dir));

  // Open the wal writer.
  std::string wal_uri = config.wal_uri;
  if (wal_uri.empty()) {
    LOG(ERROR) << "wal_uri is not set, use default wal_uri";
    wal_uri = wal_dir(data_dir);
  } else if (wal_uri.find("{GRAPH_DATA_DIR}") != std::string::npos) {
    LOG(INFO) << "Template {GRAPH_DATA_DIR} found in wal_uri, replace it with "
                 "data_dir";
    wal_uri = std::regex_replace(wal_uri, std::regex("\\{GRAPH_DATA_DIR\\}"),
                                 data_dir);
  }
  VLOG(1) << "Using wal uri: " << wal_uri;

  auto wal_parser = WalParserFactory::CreateWalParser(wal_uri);
  ingestWals(*wal_parser, data_dir, thread_num_);
  for (int i = 0; i < thread_num_; ++i) {
    new (&contexts_[i])
        SessionLocalContext(*this, data_dir, i, allocator_strategy,
                            WalWriterFactory::CreateWalWriter(wal_uri));
    contexts_[i].logger->open(wal_uri, i);
  }

  initApps(graph_.schema().GetPlugins());
  VLOG(1) << "Successfully restore load plugins";
}

void GraphDB::showAppMetrics() const {
  int session_num = SessionNum();
  for (int i = 0; i < 256; ++i) {
    AppMetric summary;
    for (int k = 0; k < session_num; ++k) {
      summary += GetSession(k).GetAppMetric(i);
    }
    if (!summary.empty()) {
      std::string query_name = "UNKNOWN";
      if (i == 0) {
        query_name = "ServerApp";
      } else {
        query_name = "Query-" + std::to_string(i);
      }
      summary.output(query_name);
    }
  }
}

size_t GraphDB::getExecutedQueryNum() const {
  size_t ret = 0;
  for (int i = 0; i < thread_num_; ++i) {
    ret += contexts_[i].session.query_num();
  }
  return ret;
}

void GraphDB::OutputCypherProfiles(const std::string& prefix) {
  runtime::OprTimer read_timer, write_timer;
  int session_num = SessionNum();
  for (int i = 0; i < session_num; ++i) {
    auto read_app_ptr = GetSession(i).GetApp(Schema::CYPHER_READ_PLUGIN_ID);
    auto casted_read_app = dynamic_cast<CypherReadApp*>(read_app_ptr);
    if (casted_read_app) {
      read_timer += casted_read_app->timer();
    }

    auto write_app_ptr = GetSession(i).GetApp(Schema::CYPHER_WRITE_PLUGIN_ID);
    auto casted_write_app = dynamic_cast<CypherWriteApp*>(write_app_ptr);
    if (casted_write_app) {
      write_timer += casted_write_app->timer();
    }
  }

  read_timer.output(prefix + "read_profile.log");
  write_timer.output(prefix + "write_profile.log");
}

}  // namespace gs

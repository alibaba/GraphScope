#include "flex/engines/graph_db/app/cypher_read_app.h"
#include "flex/engines/graph_db/app/cypher_app_utils.h"

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/sink.h"
#include "flex/engines/graph_db/runtime/execute/plan_parser.h"

namespace gs {

bool CypherReadApp::Query(const GraphDBSession& graph, Decoder& input,
                          Encoder& output) {
  std::string_view r_bytes = input.get_bytes();
  uint8_t type = static_cast<uint8_t>(r_bytes.back());
  std::string_view bytes = std::string_view(r_bytes.data(), r_bytes.size() - 1);
  if (type == Schema::ADHOC_READ_PLUGIN_ID) {
    physical::PhysicalPlan plan;
    if (!plan.ParseFromString(std::string(bytes))) {
      LOG(ERROR) << "Parse plan failed...";
      return false;
    }

    LOG(INFO) << "plan: " << plan.DebugString();
    auto txn = graph.GetReadTransaction();

    gs::runtime::GraphReadInterface gri(txn);
    auto ctx = runtime::PlanParser::get()
                   .parse_read_pipeline(graph.schema(),
                                        gs::runtime::ContextMeta(), plan)
                   .Execute(gri, runtime::Context(), {}, timer_);

    runtime::Sink::sink(ctx, txn, output);
  } else {
    size_t sep = bytes.find_first_of("&?");
    auto query_str = bytes.substr(0, sep);
    auto params_str = bytes.substr(sep + 2);
    std::map<std::string, std::string> params;
    parse_params(params_str, params);
    auto query = std::string(query_str.data(), query_str.size());
    if (!pipeline_cache_.count(query)) {
      if (plan_cache_.count(query)) {
        // LOG(INFO) << "Hit cache for query ";
      } else {
        auto& query_cache = db_.getQueryCache();
        std::string_view plan_str;
        if (query_cache.get(query, plan_str)) {
          physical::PhysicalPlan plan;
          if (!plan.ParseFromString(std::string(plan_str))) {
            return false;
          }
          plan_cache_[query] = plan;
        } else {
          const std::string statistics = db_.work_dir() + "/statistics.json";
          const std::string& compiler_yaml = db_.work_dir() + "/graph.yaml";
          const std::string& tmp_dir = db_.work_dir() + "/runtime/tmp/";
          for (int i = 0; i < 3; ++i) {
            if (!generate_plan(query, statistics, compiler_yaml, tmp_dir,
                               plan_cache_)) {
              LOG(ERROR) << "Generate plan failed for query: " << query;
            } else {
              query_cache.put(query, plan_cache_[query].SerializeAsString());
              break;
            }
          }
        }
      }
      const auto& plan = plan_cache_[query];
      pipeline_cache_.emplace(
          query, runtime::PlanParser::get().parse_read_pipeline(
                     db_.schema(), gs::runtime::ContextMeta(), plan));
    }
    auto txn = graph.GetReadTransaction();

    gs::runtime::GraphReadInterface gri(txn);
    auto ctx = pipeline_cache_.at(query).Execute(gri, runtime::Context(),
                                                 params, timer_);

    runtime::Sink::sink_encoder(ctx, gri, output);
  }
  return true;
}
AppWrapper CypherReadAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new CypherReadApp(db), NULL);
}
}  // namespace gs
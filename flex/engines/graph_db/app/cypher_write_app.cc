#include "flex/engines/graph_db/app/cypher_write_app.h"
#include "flex/engines/graph_db/app/cypher_app_utils.h"
#include "flex/engines/graph_db/database/graph_db.h"

#include "flex/engines/graph_db/runtime/execute/plan_parser.h"

namespace gs {

bool CypherWriteApp::Query(GraphDBSession& graph, Decoder& input,
                           Encoder& output) {
  auto txn = graph.GetInsertTransaction();
  std::string_view bytes = input.get_bytes();

  size_t sep = bytes.find_first_of("&?");
  auto query_str = bytes.substr(0, sep);
  auto params_str = bytes.substr(sep + 2);
  std::map<std::string, std::string> params;
  parse_params(params_str, params);
  auto query = std::string(query_str.data(), query_str.size());
  if (!pipeline_cache_.count(query)) {
    if (plan_cache_.count(query)) {
    } else {
      const std::string statistics = db_.work_dir() + "/statistics.json";
      const std::string& compiler_yaml = db_.work_dir() + "/graph.yaml";
      const std::string& tmp_dir = db_.work_dir() + "/runtime/tmp/";

      auto& query_cache = db_.getQueryCache();
      std::string_view plan_str;
      if (query_cache.get(query, plan_str)) {
        physical::PhysicalPlan plan;
        if (!plan.ParseFromString(std::string(plan_str))) {
          return false;
        }
        plan_cache_[query] = plan;
      } else {
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
        query,
        runtime::PlanParser::get().parse_write_pipeline(db_.schema(), plan));
  } else {
  }

  gs::runtime::GraphInsertInterface gri(txn);
  auto ctx = pipeline_cache_.at(query).Execute(gri, runtime::WriteContext(),
                                               params, timer_);

  return true;
}
AppWrapper CypherWriteAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new CypherWriteApp(db), NULL);
}
}  // namespace gs
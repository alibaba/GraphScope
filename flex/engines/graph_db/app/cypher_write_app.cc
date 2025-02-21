#include "flex/engines/graph_db/app/cypher_write_app.h"
#include "flex/engines/graph_db/app/cypher_app_utils.h"
#include "flex/engines/graph_db/database/graph_db.h"

#include "flex/engines/graph_db/runtime/execute/plan_parser.h"
#include "flex/engines/graph_db/runtime/utils/cypher_runner_impl.h"

namespace gs {

bool CypherWriteApp::Query(GraphDBSession& graph, Decoder& input,
                           Encoder& output) {
  auto txn = graph.GetInsertTransaction();
  std::string_view r_bytes = input.get_bytes();
  std::string_view bytes = std::string_view(r_bytes.data(), r_bytes.size() - 1);

  size_t sep = bytes.find_first_of("&?");
  auto query_str = bytes.substr(0, sep);
  auto params_str = bytes.substr(sep + 2);
  std::map<std::string, std::string> params;
  parse_params(params_str, params);
  auto query = std::string(query_str.data(), query_str.size());
  if (!pipeline_cache_.count(query)) {
    if (plan_cache_.count(query)) {
    } else {
      physical::PhysicalPlan plan;
      std::string plan_str;

      if (!gs::runtime::CypherRunnerImpl::get().gen_plan(db_, query,
                                                         plan_str)) {
        return false;
      } else {
        if (!plan.ParseFromString(plan_str)) {
          LOG(ERROR) << "Parse plan failed for query: " << query;
          return false;
        }
        plan_cache_[query] = std::move(plan);
      }
    }
    const auto& plan = plan_cache_[query];
    pipeline_cache_.emplace(query, runtime::PlanParser::get()
                                       .parse_write_pipeline(db_.schema(), plan)
                                       .value());
  }

  gs::runtime::GraphInsertInterface gri(txn);
  auto ctx = pipeline_cache_.at(query).Execute(gri, runtime::WriteContext(),
                                               params, timer_);
  txn.Commit();
  return true;
}
AppWrapper CypherWriteAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new CypherWriteApp(db), NULL);
}
}  // namespace gs
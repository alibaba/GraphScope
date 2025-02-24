#include "flex/engines/graph_db/app/cypher_read_app.h"
#include "flex/engines/graph_db/app/cypher_app_utils.h"

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/sink.h"
#include "flex/engines/graph_db/runtime/execute/plan_parser.h"
#include "flex/engines/graph_db/runtime/utils/cypher_runner_impl.h"

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

    gs::runtime::Context ctx;
    gs::Status status = gs::Status::OK();
    {
      ctx = bl::try_handle_all(
          [this, &gri, &plan]() -> bl::result<runtime::Context> {
            return runtime::PlanParser::get()
                .parse_read_pipeline(gri.schema(), gs::runtime::ContextMeta(),
                                     plan)
                .value()
                .Execute(gri, runtime::Context(), {}, timer_);
          },
          [&status](const gs::Status& err) {
            status = err;
            return runtime::Context();
          },
          [&](const bl::error_info& err) {
            status =
                gs::Status(gs::StatusCode::INTERNAL_ERROR,
                           "Error: " + std::to_string(err.error().value()) +
                               ", Exception: " + err.exception()->what());
            return runtime::Context();
          },
          [&]() {
            status = gs::Status(gs::StatusCode::UNKNOWN, "Unknown error");
            return runtime::Context();
          });
    }

    if (!status.ok()) {
      LOG(ERROR) << "Error: " << status.ToString();
      // We encode the error message to the output, so that the client can
      // get the error message.
      output.put_string(status.ToString());
      return false;
    }
    runtime::Sink::sink(ctx, txn, output);
    return true;
  } else {
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
          LOG(ERROR) << "Generate plan failed for query: " << query;
          std::string error =
              "    Compiler failed to generate physical plan: " + query;
          output.put_bytes(error.data(), error.size());

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
      pipeline_cache_.emplace(
          query, runtime::PlanParser::get()
                     .parse_read_pipeline(db_.schema(),
                                          gs::runtime::ContextMeta(), plan)
                     .value());
    }
    auto txn = graph.GetReadTransaction();

    gs::runtime::GraphReadInterface gri(txn);
    auto ctx = pipeline_cache_.at(query).Execute(gri, runtime::Context(),
                                                 params, timer_);
    if (type == Schema::CYPHER_READ_PLUGIN_ID) {
      runtime::Sink::sink_encoder(ctx.value(), gri, output);
    } else {
      runtime::Sink::sink_beta(ctx.value(), gri, output);
    }
  }
  return true;
}
AppWrapper CypherReadAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new CypherReadApp(db), NULL);
}
}  // namespace gs
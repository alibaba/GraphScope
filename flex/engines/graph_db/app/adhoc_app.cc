#include "flex/engines/graph_db/app/adhoc_app.h"

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/runtime.h"

#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {

bool AdhocReadApp::Query(const GraphDBSession& graph, Decoder& input,
                         Encoder& output) {
  auto txn = graph.GetReadTransaction();

  std::string_view plan_str = input.get_bytes();
  physical::PhysicalPlan plan;
  if (!plan.ParseFromString(std::string(plan_str))) {
    LOG(ERROR) << "Parse plan failed...";
    return false;
  }

  LOG(INFO) << "plan: " << plan.DebugString();

  auto ctx = runtime::runtime_eval(plan, txn, {});

  runtime::eval_sink(ctx, txn, output);

  return true;
}
AppWrapper AdhocReadAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new AdhocReadApp(), NULL);
}
}  // namespace gs
#include "flex/engines/graph_db/runtime/adhoc/runtime.h"
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"

#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {

class Runtime : public ReadAppBase {
 public:
  Runtime(GraphDB& graph) {}
  ~Runtime() {}

  bool Query(const GraphDBSession& graph_, Decoder& input,
             Encoder& output) override {
    auto txn = graph_.GetReadTransaction();

    std::string_view plan_str = input.get_string();
    physical::PhysicalPlan plan;
    if (!plan.ParseFromString(std::string(plan_str))) {
      LOG(ERROR) << "Parse plan failed...";
      return false;
    }

    std::map<std::string, std::string> params;
    while (!input.empty()) {
      std::string_view key = input.get_string();
      std::string_view value = input.get_string();
      params.emplace(key, value);
    }

    LOG(INFO) << "plan: " << plan.DebugString();

    auto ctx = runtime::runtime_eval(plan, txn, params);

    runtime::eval_sink(ctx, output);

    return true;
  }

 private:
};

}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDB& db) {
  gs::Runtime* app = new gs::Runtime(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::Runtime* casted = static_cast<gs::Runtime*>(app);
  delete casted;
}
}

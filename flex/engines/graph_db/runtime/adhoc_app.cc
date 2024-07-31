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

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/runtime.h"

#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {

class AdhocApp : public ReadAppBase {
 public:
  AdhocApp(GraphDB& graph) {}
  ~AdhocApp() {}

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
  gs::AdhocApp* app = new gs::AdhocApp(db);
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::AdhocApp* casted = static_cast<gs::AdhocApp*>(app);
  delete casted;
}
}

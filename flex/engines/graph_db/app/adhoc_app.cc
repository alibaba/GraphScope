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
#include "flex/engines/graph_db/app/adhoc_app.h"

#include "flex/engines/graph_db/runtime/common/operators/retrieve/sink.h"

#include "flex/engines/graph_db/runtime/execute/pipeline.h"
#include "flex/engines/graph_db/runtime/execute/plan_parser.h"
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
  gs::runtime::GraphReadInterface gri(txn);
  auto ctx =
      runtime::PlanParser::get()
          .parse_read_pipeline(graph.schema(), gs::runtime::ContextMeta(), plan)
          .Execute(gri, runtime::Context(), {}, timer_);

  runtime::Sink::sink(ctx, txn, output);

  return true;
}
AppWrapper AdhocReadAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new AdhocReadApp(), NULL);
}
}  // namespace gs
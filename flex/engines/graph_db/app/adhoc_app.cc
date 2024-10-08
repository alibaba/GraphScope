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
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/runtime.h"
#include "flex/proto_generated_gie/physical.pb.h"

#include <string>

namespace bl = boost::leaf;

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

  gs::runtime::Context ctx;
  gs::Status status = gs::Status::OK();
  {
    ctx = bl::try_handle_all(
        [&plan, &txn]() { return runtime::runtime_eval(plan, txn, {}); },
        [&ctx, &status](const gs::Status& err) {
          status = err;
          return ctx;
        },
        [&](const bl::error_info& err) {
          status = gs::Status(
              gs::StatusCode::INTERNAL_ERROR,
              "BOOST LEAF Error: " + std::to_string(err.error().value()) +
                  ", Exception: " + err.exception()->what());
          return ctx;
        },
        [&]() {
          status = gs::Status(gs::StatusCode::UNKNOWN, "Unknown error");
          return ctx;
        });
  }

  if (!status.ok()) {
    LOG(ERROR) << "Error: " << status.ToString();
    // We encode the error message to the output, so that the client can
    // get the error message.
    output.put_string(status.ToString());
    return false;
  }

  runtime::eval_sink(ctx, txn, output);

  return true;
}
AppWrapper AdhocReadAppFactory::CreateApp(const GraphDB& db) {
  return AppWrapper(new AdhocReadApp(), NULL);
}
}  // namespace gs
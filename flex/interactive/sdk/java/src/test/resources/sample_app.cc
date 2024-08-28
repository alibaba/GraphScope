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
#include "flex/engines/hqps_db/app/interactive_app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/utils/app_utils.h"

namespace gs {
class ExampleQuery : public CypherReadAppBase<int32_t> {
 public:
  using Engine = SyncEngine<gs::MutableCSRInterface>;
  using label_id_t = typename gs::MutableCSRInterface::label_id_t;
  using vertex_id_t = typename gs::MutableCSRInterface::vertex_id_t;
  ExampleQuery() {}
  // Query function for query class
  results::CollectiveResults Query(const gs::GraphDBSession& sess,
                                   int32_t param1) override {
    LOG(INFO) << "param1: " << param1;
    gs::MutableCSRInterface graph(sess);
    auto ctx0 = Engine::template ScanVertex<gs::AppendOpt::Persist>(
        graph, 0, Filter<TruePredicate>());

    auto ctx1 = Engine::Project<PROJ_TO_NEW>(
        graph, std::move(ctx0),
        std::tuple{gs::make_mapper_with_variable<INPUT_COL_ID(0)>(
            gs::PropertySelector<int64_t>("id"))});
    auto ctx2 = Engine::Limit(std::move(ctx1), 0, 5);
    auto res = Engine::Sink(graph, ctx2, std::array<int32_t, 1>{0});
    LOG(INFO) << "res: " << res.DebugString();
    return res;
  }
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::ExampleQuery* app = new gs::ExampleQuery();
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::ExampleQuery* casted = static_cast<gs::ExampleQuery*>(app);
  delete casted;
}
}
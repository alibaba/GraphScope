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

#include "flex/engines/graph_db/runtime/execute/pipeline.h"

namespace gs {
namespace runtime {

Context ReadPipeline::Execute(const GraphReadInterface& graph, Context&& ctx,
                              const std::map<std::string, std::string>& params,
                              OprTimer& timer) {
  for (auto& opr : operators_) {
    ctx = opr->Eval(graph, params, std::move(ctx), timer);
  }
  return ctx;
}

WriteContext InsertPipeline::Execute(
    GraphInsertInterface& graph, WriteContext&& ctx,
    const std::map<std::string, std::string>& params, OprTimer& timer) {
  for (auto& opr : operators_) {
    ctx = opr->Eval(graph, params, std::move(ctx), timer);
  }
  return ctx;
}

}  // namespace runtime

}  // namespace gs

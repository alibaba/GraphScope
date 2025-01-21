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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/sink.h"

namespace gs {
namespace runtime {
namespace ops {

class SinkOpr : public IReadOperator {
 public:
  SinkOpr(const std::vector<int>& tag_ids) : tag_ids_(tag_ids) {}

  bl::result<Context> Eval(const GraphReadInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer& timer) override {
    ctx.tag_ids = tag_ids_;
    return ctx;
  }

 private:
  std::vector<int> tag_ids_;
};

std::pair<std::unique_ptr<IReadOperator>, ContextMeta> SinkOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  auto& opr = plan.plan(op_idx).opr().sink();
  std::vector<int> tag_ids;
  for (auto& tag : opr.tags()) {
    tag_ids.push_back(tag.tag().value());
  }
  if (tag_ids.empty() && op_idx) {
    while (op_idx - 1 && (!plan.plan(op_idx - 1).opr().has_project()) &&
           (!plan.plan(op_idx - 1).opr().has_group_by())) {
      op_idx--;
    }
    auto prev_opr = plan.plan(op_idx - 1).opr();
    if (prev_opr.has_project()) {
      int mapping_size = prev_opr.project().mappings_size();
      for (int i = 0; i < mapping_size; ++i) {
        tag_ids.emplace_back(prev_opr.project().mappings(i).alias().value());
      }
    } else if (prev_opr.has_group_by()) {
      int mapping_size = prev_opr.group_by().mappings_size();
      for (int i = 0; i < mapping_size; ++i) {
        tag_ids.emplace_back(prev_opr.group_by().mappings(i).alias().value());
      }
      int function_size = prev_opr.group_by().functions_size();
      for (int i = 0; i < function_size; ++i) {
        tag_ids.emplace_back(prev_opr.group_by().functions(i).alias().value());
      }
    }
  }
  return std::make_pair(std::make_unique<SinkOpr>(tag_ids), ctx_meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
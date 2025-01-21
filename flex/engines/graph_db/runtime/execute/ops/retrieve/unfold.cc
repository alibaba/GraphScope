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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/unfold.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/unfold.h"

namespace gs {
namespace runtime {
namespace ops {
class UnfoldOpr : public IReadOperator {
 public:
  UnfoldOpr(const physical::Unfold& opr)
      : opr_(opr), tag_(opr.tag().value()), alias_(opr.alias().value()) {}

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    return Unfold::unfold(std::move(ctx), tag_, alias_);
  }

 private:
  physical::Unfold opr_;
  int tag_;
  int alias_;
};

std::pair<std::unique_ptr<IReadOperator>, ContextMeta> UnfoldOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  int alias = plan.plan(op_idx).opr().unfold().alias().value();
  ret_meta.set(alias);
  return std::make_pair(
      std::make_unique<UnfoldOpr>(plan.plan(op_idx).opr().unfold()), ret_meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
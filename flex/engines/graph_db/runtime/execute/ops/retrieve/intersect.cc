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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/intersect.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/intersect.h"
#include "flex/engines/graph_db/runtime/execute/pipeline.h"

#include "flex/engines/graph_db/runtime/execute/plan_parser.h"
namespace gs {
namespace runtime {
namespace ops {
class IntersectOpr : public IReadOperator {
 public:
  IntersectOpr(const physical::Intersect& intersect_opr,
               std::vector<ReadPipeline>&& sub_plans)
      : key_(intersect_opr.key()), sub_plans_(std::move(sub_plans)) {}

  std::string get_operator_name() const override { return "IntersectOpr"; }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    std::vector<gs::runtime::Context> ctxs;
    for (auto& plan : sub_plans_) {
      Context n_ctx(ctx);
      n_ctx.gen_offset();
      auto n_ctx_res = plan.Execute(graph, std::move(n_ctx), params, timer);
      if (!n_ctx_res) {
        return n_ctx_res;
      }
      ctxs.push_back(std::move(n_ctx_res.value()));
    }
    return Intersect::intersect(std::move(ctx), std::move(ctxs), key_);
  }

 private:
  int key_;
  std::vector<ReadPipeline> sub_plans_;
};

bl::result<ReadOpBuildResultT> IntersectOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  std::vector<ReadPipeline> sub_plans;
  for (int i = 0; i < plan.plan(op_idx).opr().intersect().sub_plans_size();
       ++i) {
    auto& sub_plan = plan.plan(op_idx).opr().intersect().sub_plans(i);
    auto sub_plan_res = PlanParser::get().parse_read_pipeline_with_meta(
        schema, ctx_meta, sub_plan);
    if (!sub_plan_res) {
      return std::make_pair(nullptr, ContextMeta());
    }
    sub_plans.push_back(std::move(sub_plan_res.value().first));
  }
  ContextMeta meta = ctx_meta;
  meta.set(plan.plan(op_idx).opr().intersect().key());
  return std::make_pair(
      std::make_unique<IntersectOpr>(plan.plan(op_idx).opr().intersect(),
                                     std::move(sub_plans)),
      meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
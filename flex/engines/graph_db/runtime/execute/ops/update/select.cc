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
#include "flex/engines/graph_db/runtime/execute/ops/update/select.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/select.h"
#include "flex/engines/graph_db/runtime/utils/expr.h"
namespace gs {
namespace runtime {
namespace ops {
class USelectOpr : public IUpdateOperator {
 public:
  USelectOpr(const common::Expression& predicate) : predicate_(predicate) {}
  bl::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer& timer) override {
    Expr expr(graph, ctx, params, predicate_, VarType::kPathVar);
    Arena arena;
    if (expr.is_optional()) {
      return Select::select(std::move(ctx), [&](size_t idx) {
        return expr.eval_path(idx, arena, 0).as_bool();
      });
    } else {
      return Select::select(std::move(ctx), [&](size_t idx) {
        return expr.eval_path(idx, arena).as_bool();
      });
    }
  }

  std::string get_operator_name() const override { return "USelectOpr"; }

 private:
  common::Expression predicate_;
};

std::unique_ptr<IUpdateOperator> USelectOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  auto opr = plan.plan(op_idx).opr().select();
  return std::make_unique<USelectOpr>(opr.predicate());
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs

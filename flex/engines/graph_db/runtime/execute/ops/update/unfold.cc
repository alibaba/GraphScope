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
#include "flex/engines/graph_db/runtime/execute/ops/update/unfold.h"
#include "flex/engines/graph_db/runtime/common/operators/update/unfold.h"

namespace gs {

namespace runtime {

namespace ops {

class UnfoldInsertOpr : public IInsertOperator {
 public:
  UnfoldInsertOpr(int tag, int alias) : tag_(tag), alias_(alias) {}

  std::string get_operator_name() const override { return "UnfoldInsertOpr"; }

  bl::result<gs::runtime::WriteContext> Eval(
      gs::runtime::GraphInsertInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer& timer) override {
    return Unfold::unfold(std::move(ctx), tag_, alias_);
  }

 private:
  int tag_;
  int alias_;
};

std::unique_ptr<IInsertOperator> UnfoldInsertOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_id) {
  const auto& opr = plan.plan(op_id).opr().unfold();
  int key = opr.tag().value();
  int value = opr.alias().value();
  return std::make_unique<UnfoldInsertOpr>(key, value);
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs

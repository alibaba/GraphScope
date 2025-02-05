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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/limit.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/limit.h"

namespace gs {
namespace runtime {
namespace ops {
class LimitOpr : public IReadOperator {
 public:
  LimitOpr(const algebra::Limit& opr) {
    lower_ = 0;
    upper_ = std::numeric_limits<size_t>::max();
    if (opr.has_range()) {
      lower_ = std::max(lower_, static_cast<size_t>(opr.range().lower()));
      upper_ = std::min(upper_, static_cast<size_t>(opr.range().upper()));
    }
  }

  std::string get_operator_name() const override { return "LimitOpr"; }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    return Limit::limit(std::move(ctx), lower_, upper_);
  }

 private:
  size_t lower_;
  size_t upper_;
};

bl::result<ReadOpBuildResultT> LimitOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  return std::make_pair(
      std::make_unique<LimitOpr>(plan.plan(op_idx).opr().limit()), ctx_meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
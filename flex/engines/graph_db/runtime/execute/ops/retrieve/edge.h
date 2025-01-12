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

#ifndef RUNTIME_EXECUTE_RETRIEVE_OPS_EDGE_H_
#define RUNTIME_EXECUTE_RETRIEVE_OPS_EDGE_H_

#include "flex/engines/graph_db/runtime/execute/operator.h"

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/predicates.h"
#include "flex/engines/graph_db/runtime/adhoc/utils.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/edge_expand.h"

namespace gs {

namespace runtime {

namespace ops {

class EdgeExpandOprBuilder : public IReadOperatorBuilder {
 public:
  EdgeExpandOprBuilder() = default;
  ~EdgeExpandOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override;

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kEdge};
  }
};

class EdgeExpandGetVOprBuilder : public IReadOperatorBuilder {
 public:
  EdgeExpandGetVOprBuilder() = default;
  ~EdgeExpandGetVOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override;

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {
        physical::PhysicalOpr_Operator::OpKindCase::kEdge,
        physical::PhysicalOpr_Operator::OpKindCase::kVertex,
    };
  }
};

class TCOpr : public IReadOperator {
 public:
  TCOpr(const physical::EdgeExpand& ee_opr0,
        const physical::GroupBy& group_by_opr,
        const physical::EdgeExpand& ee_opr1, const physical::GetV& v_opr1,
        const physical::EdgeExpand& ee_opr2, const algebra::Select& select_opr,
        const physical::PhysicalOpr_MetaData& meta0,
        const physical::PhysicalOpr_MetaData& meta1,
        const physical::PhysicalOpr_MetaData& meta2)
      : ee_opr0_(ee_opr0),
        group_by_opr_(group_by_opr),
        ee_opr1_(ee_opr1),
        v_opr1_(v_opr1),
        ee_opr2_(ee_opr2),
        select_opr_(select_opr),
        meta0_(meta0),
        meta1_(meta1),
        meta2_(meta2) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    return gs::runtime::eval_tc(ee_opr0_, group_by_opr_, ee_opr1_, v_opr1_,
                                ee_opr2_, select_opr_, graph, std::move(ctx),
                                params, meta0_, meta1_, meta2_);
  }

 private:
  physical::EdgeExpand ee_opr0_;
  physical::GroupBy group_by_opr_;
  physical::EdgeExpand ee_opr1_;
  physical::GetV v_opr1_;
  physical::EdgeExpand ee_opr2_;
  algebra::Select select_opr_;

  physical::PhysicalOpr_MetaData meta0_;
  physical::PhysicalOpr_MetaData meta1_;
  physical::PhysicalOpr_MetaData meta2_;
};

class TCOprBuilder : public IReadOperatorBuilder {
 public:
  TCOprBuilder() = default;
  ~TCOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override {
    if (tc_fusable(plan.plan(op_idx).opr().edge(),
                   plan.plan(op_idx + 1).opr().group_by(),
                   plan.plan(op_idx + 2).opr().edge(),
                   plan.plan(op_idx + 3).opr().vertex(),
                   plan.plan(op_idx + 4).opr().edge(),
                   plan.plan(op_idx + 5).opr().select())) {
      int alias1 = -1;
      if (plan.plan(op_idx + 2).opr().edge().has_alias()) {
        alias1 = plan.plan(op_idx + 2).opr().edge().alias().value();
      }
      if (plan.plan(op_idx + 3).opr().vertex().has_alias()) {
        alias1 = plan.plan(op_idx + 3).opr().vertex().alias().value();
      }
      int alias2 = -1;
      if (plan.plan(op_idx + 4).opr().edge().has_alias()) {
        alias2 = plan.plan(op_idx + 4).opr().edge().alias().value();
      }
      ContextMeta meta = ctx_meta;
      meta.set(alias1);
      meta.set(alias2);
      return std::make_pair(
          std::make_unique<TCOpr>(plan.plan(op_idx).opr().edge(),
                                  plan.plan(op_idx + 1).opr().group_by(),
                                  plan.plan(op_idx + 2).opr().edge(),
                                  plan.plan(op_idx + 3).opr().vertex(),
                                  plan.plan(op_idx + 4).opr().edge(),
                                  plan.plan(op_idx + 5).opr().select(),
                                  plan.plan(op_idx).meta_data(0),
                                  plan.plan(op_idx + 2).meta_data(0),
                                  plan.plan(op_idx + 4).meta_data(0)),
          meta);
    } else {
      return std::make_pair(nullptr, ContextMeta());
    }
  }

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kEdge,
            physical::PhysicalOpr_Operator::OpKindCase::kGroupBy,
            physical::PhysicalOpr_Operator::OpKindCase::kEdge,
            physical::PhysicalOpr_Operator::OpKindCase::kVertex,
            physical::PhysicalOpr_Operator::OpKindCase::kEdge,
            physical::PhysicalOpr_Operator::OpKindCase::kSelect};
  }
};

}  // namespace ops

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_RETRIEVE_OPS_EDGE_H_
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

#include "flex/engines/graph_db/runtime/execute/ops/update/edge.h"
#include "flex/engines/graph_db/runtime/common/operators/update/edge_expand.h"
#include "flex/engines/graph_db/runtime/utils/params.h"
#include "flex/engines/graph_db/runtime/utils/utils.h"

namespace gs {
namespace runtime {
namespace ops {

class UEdgeExpandVWithoutPredOpr : public IUpdateOperator {
 public:
  UEdgeExpandVWithoutPredOpr(EdgeExpandParams params) : params_(params) {}
  ~UEdgeExpandVWithoutPredOpr() = default;

  std::string get_operator_name() const override { return "UEdgeExpandVOpr"; }

  bl::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer& timer) override {
    return UEdgeExpand::edge_expand_v_without_pred(graph, std::move(ctx),
                                                   params_);
  }

 private:
  EdgeExpandParams params_;
};

class UEdgeExpandEWithoutPredOpr : public IUpdateOperator {
 public:
  UEdgeExpandEWithoutPredOpr(EdgeExpandParams params) : params_(params) {}
  ~UEdgeExpandEWithoutPredOpr() = default;

  std::string get_operator_name() const override { return "UEdgeExpandEOpr"; }

  bl::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer& timer) override {
    return UEdgeExpand::edge_expand_e_without_pred(graph, std::move(ctx),
                                                   params_);
  }

 private:
  EdgeExpandParams params_;
};

std::unique_ptr<IUpdateOperator> UEdgeExpandBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  auto& edge = plan.plan(op_idx).opr().edge();
  auto& meta = plan.plan(op_idx).meta_data(0);
  int alias = edge.has_alias() ? edge.alias().value() : -1;
  int v_tag = edge.has_v_tag() ? edge.v_tag().value() : -1;
  Direction dir = parse_direction(edge.direction());
  bool is_optional = edge.is_optional();
  if (is_optional) {
    LOG(ERROR) << "Optional edge expand is not supported yet";
    return nullptr;
  }
  const auto& query_params = edge.params();
  EdgeExpandParams eep;
  eep.v_tag = v_tag;
  eep.alias = alias;
  eep.dir = dir;
  eep.labels = parse_label_triplets(meta);
  eep.is_optional = is_optional;
  if (edge.expand_opt() == physical::EdgeExpand_ExpandOpt_VERTEX) {
    if (query_params.has_predicate()) {
      LOG(ERROR) << "Edge expand with predicate is not supported yet";
      return nullptr;
    } else {
      return std::make_unique<UEdgeExpandVWithoutPredOpr>(eep);
    }
  } else {
    if (query_params.has_predicate()) {
      LOG(ERROR) << "Edge expand with predicate is not supported yet";
      return nullptr;
    } else {
      return std::make_unique<UEdgeExpandEWithoutPredOpr>(eep);
    }
  }
  return nullptr;
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs

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

#include "flex/engines/graph_db/runtime/execute/ops/update/sink.h"
namespace gs {
namespace runtime {
namespace ops {

class SinkInsertOpr : public IInsertOperator {
 public:
  SinkInsertOpr() {}

  std::string get_operator_name() const override { return "SinkInsertOpr"; }

  template <typename GraphInterface>
  bl::result<gs::runtime::WriteContext> eval_impl(
      GraphInterface& graph, const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer& timer) {
    return ctx;
  }

  bl::result<gs::runtime::WriteContext> Eval(
      gs::runtime::GraphInsertInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer& timer) override {
    return eval_impl(graph, params, std::move(ctx), timer);
  }

  bl::result<gs::runtime::WriteContext> Eval(
      gs::runtime::GraphUpdateInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer& timer) override {
    return eval_impl(graph, params, std::move(ctx), timer);
  }
};

std::unique_ptr<IInsertOperator> SinkInsertOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  return std::make_unique<SinkInsertOpr>();
}

class USinkOpr : public IUpdateOperator {
 public:
  USinkOpr(const std::vector<int>& tag_ids) : tag_ids(tag_ids) {}

  std::string get_operator_name() const override { return "USinkOpr"; }

  bl::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer& timer) override {
    ctx.tag_ids = tag_ids;
    return ctx;
  }
  std::vector<int> tag_ids;
};

std::unique_ptr<IUpdateOperator> USinkOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  auto& opr = plan.plan(op_idx).opr().sink();
  std::vector<int> tag_ids;
  for (auto& tag : opr.tags()) {
    tag_ids.push_back(tag.tag().value());
  }
  return std::make_unique<USinkOpr>(tag_ids);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
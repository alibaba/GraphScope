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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/order_by.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/order_by.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/order_by_utils.h"
#include "flex/engines/graph_db/runtime/utils/utils.h"

namespace gs {
namespace runtime {
namespace ops {

class OrderByOprBeta : public IReadOperator {
 public:
  OrderByOprBeta(
      std::vector<std::pair<common::Variable, bool>> keys, int lower, int upper,
      const std::function<
          std::optional<std::function<std::optional<std::vector<size_t>>(
              const GraphReadInterface&, const Context&)>>(const Context&)>&
          func)
      : keys_(std::move(keys)), lower_(lower), upper_(upper), func_(func) {}

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    int keys_num = keys_.size();
    GeneralComparer cmp;
    for (int i = 0; i < keys_num; ++i) {
      Var v(graph, ctx, keys_[i].first, VarType::kPathVar);
      cmp.add_keys(std::move(v), keys_[i].second);
    }
    auto func = func_(ctx);
    if (func.has_value()) {
      return OrderBy::order_by_with_limit_with_indices<GeneralComparer>(
          graph, std::move(ctx), func.value(), cmp, lower_, upper_);
    }

    return OrderBy::order_by_with_limit<GeneralComparer>(graph, std::move(ctx),
                                                         cmp, lower_, upper_);
  }

 private:
  std::vector<std::pair<common::Variable, bool>> keys_;

  int lower_;
  int upper_;
  std::function<std::optional<std::function<std::optional<std::vector<size_t>>(
      const GraphReadInterface&, const Context&)>>(const Context&)>
      func_;
};

std::pair<std::unique_ptr<IReadOperator>, ContextMeta> OrderByOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto opr = plan.plan(op_idx).opr().order_by();
  int lower = 0;
  int upper = std::numeric_limits<int>::max();
  if (opr.has_limit()) {
    lower = std::max(lower, static_cast<int>(opr.limit().lower()));
    upper = std::min(upper, static_cast<int>(opr.limit().upper()));
  }
  int keys_num = opr.pairs_size();
  CHECK_GE(keys_num, 1);
  std::vector<std::pair<common::Variable, bool>> keys;

  for (int i = 0; i < keys_num; ++i) {
    const auto& pair = opr.pairs(i);
    CHECK(pair.order() == algebra::OrderBy_OrderingPair_Order::
                              OrderBy_OrderingPair_Order_ASC ||
          pair.order() == algebra::OrderBy_OrderingPair_Order::
                              OrderBy_OrderingPair_Order_DESC);
    bool asc =
        pair.order() ==
        algebra::OrderBy_OrderingPair_Order::OrderBy_OrderingPair_Order_ASC;
    keys.emplace_back(pair.key(), asc);
  }

  const auto key = keys[0].first;
  const auto order = keys[0].second;

  auto func = [key, order, upper](const Context& ctx)
      -> std::optional<std::function<std::optional<std::vector<size_t>>(
          const GraphReadInterface& graph, const Context& ctx)>> {
    if (key.has_tag() &&
        key.tag().item_case() == common::NameOrId::ItemCase::kId) {
      int tag = key.tag().id();
      auto col = ctx.get(tag);
      CHECK(col != nullptr);
      if (!key.has_property()) {
        if (col->column_type() == ContextColumnType::kValue) {
          return [=](const GraphReadInterface& graph,
                     const Context& ctx) -> std::optional<std::vector<size_t>> {
            std::vector<size_t> indices;
            if (col->order_by_limit(order, upper, indices)) {
              return indices;
            } else {
              return std::nullopt;
            }
          };
        }
      } else if (col->column_type() == ContextColumnType::kVertex) {
        std::string prop_name = key.property().key().name();
        auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
        int label_num = vertex_col->get_labels_set().size();
        if (prop_name == "id" && label_num == 1) {
          return [=](const GraphReadInterface& graph,
                     const Context& ctx) -> std::optional<std::vector<size_t>> {
            std::vector<size_t> indices;
            if (vertex_id_topN(order, upper, vertex_col, graph, indices)) {
              return indices;
            } else {
              return std::nullopt;
            }
          };
        } else {
          return [=](const GraphReadInterface& graph,
                     const Context& ctx) -> std::optional<std::vector<size_t>> {
            std::vector<size_t> indices;
            if (vertex_property_topN(order, upper, vertex_col, graph, prop_name,
                                     indices)) {
              return indices;
            } else {
              return std::nullopt;
            }
          };
        }
      }
    }
    return std::nullopt;
  };
  return std::make_pair(std::make_unique<OrderByOprBeta>(
                            std::move(keys), lower, upper, std::move(func)),
                        ctx_meta);
  // return std::make_pair(
  //   std::make_unique<OrderByOpr>(plan.plan(op_idx).opr().order_by()),
  // ctx_meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
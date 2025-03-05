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
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/select.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/select.h"
#include "flex/engines/graph_db/runtime/utils/expr.h"
#include "flex/engines/graph_db/runtime/utils/special_predicates.h"

namespace gs {
namespace runtime {
namespace ops {

struct ExprWrapper {
  ExprWrapper(Expr&& expr) : expr_(std::move(expr)) {}

  bool operator()(size_t idx, Arena& arena) const {
    return expr_.eval_path(idx, arena).as_bool();
  }

  Expr expr_;
};

struct OptionalExprWrapper {
  OptionalExprWrapper(Expr&& expr) : expr_(std::move(expr)) {}

  bool operator()(size_t idx, Arena& arena) const {
    auto val = expr_.eval_path(idx, arena, 0);
    return (!val.is_null()) && val.as_bool();
  }

  Expr expr_;
};

class SelectIdNeOpr : public IReadOperator {
 public:
  SelectIdNeOpr(const common::Expression& expr) : expr_(expr) {}

  std::string get_operator_name() const override { return "SelectIdNeOpr"; }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    auto tag = expr_.operators(0).var().tag().id();
    auto col = ctx.get(tag);
    const auto& name = expr_.operators(0).var().property().key().name();
    if ((!col->is_optional()) &&
        col->column_type() == ContextColumnType::kVertex) {
      auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
      auto labels = vertex_col->get_labels_set();
      if (labels.size() == 1 &&
          name == graph.schema().get_vertex_primary_key_name(*labels.begin())) {
        auto label = *labels.begin();
        int64_t oid = std::stoll(params.at(expr_.operators(2).param().name()));
        vid_t vid;
        if (graph.GetVertexIndex(label, oid, vid)) {
          if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
            const SLVertexColumn& sl_vertex_col =
                *(dynamic_cast<const SLVertexColumn*>(vertex_col.get()));

            return Select::select(
                std::move(ctx), [&sl_vertex_col, vid](size_t i) {
                  return sl_vertex_col.get_vertex(i).vid_ != vid;
                });
          } else {
            return Select::select(std::move(ctx), [&vertex_col, vid](size_t i) {
              return vertex_col->get_vertex(i).vid_ != vid;
            });
          }
        }
      }
    }
    Expr expr(graph, ctx, params, expr_, VarType::kPathVar);
    Arena arena;

    if (!expr.is_optional()) {
      ExprWrapper wrapper(std::move(expr));
      return Select::select(std::move(ctx), [&wrapper, &arena](size_t i) {
        return wrapper(i, arena);
      });
    } else {
      OptionalExprWrapper wrapper(std::move(expr));
      return Select::select(std::move(ctx), [&wrapper, &arena](size_t i) {
        return wrapper(i, arena);
      });
    }
  }
  common::Expression expr_;
};

class SelectOpr : public IReadOperator {
 public:
  SelectOpr(const common::Expression& expr) : expr_(expr) {}

  std::string get_operator_name() const override { return "SelectOpr"; }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    Expr expr(graph, ctx, params, expr_, VarType::kPathVar);
    Arena arena;
    if (!expr.is_optional()) {
      ExprWrapper wrapper(std::move(expr));
      return Select::select(std::move(ctx), [&wrapper, &arena](size_t i) {
        return wrapper(i, arena);
      });
    } else {
      OptionalExprWrapper wrapper(std::move(expr));
      return Select::select(std::move(ctx), [&wrapper, &arena](size_t i) {
        return wrapper(i, arena);
      });
    }
  }

 private:
  common::Expression expr_;
};

bl::result<ReadOpBuildResultT> SelectOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  auto opr = plan.plan(op_idx).opr().select();
  auto type = parse_sp_pred(opr.predicate());
  if (type == SPPredicateType::kPropertyNE) {
    auto var = opr.predicate().operators(0).var();
    if (var.has_property()) {
      auto name = var.property().key().name();
      auto type = parse_from_ir_data_type(
          opr.predicate().operators(2).param().data_type());
      if (name == "id" && type == RTAnyType::kI64Value) {
        return std::make_pair(std::make_unique<SelectIdNeOpr>(opr.predicate()),
                              ctx_meta);
      }
    }
  }
  return std::make_pair(std::make_unique<SelectOpr>(opr.predicate()), ctx_meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
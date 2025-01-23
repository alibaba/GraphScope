
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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/project.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/order_by.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/project.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/order_by_utils.h"
#include "flex/engines/graph_db/runtime/utils/expr.h"
#include "flex/engines/graph_db/runtime/utils/special_predicates.h"

namespace gs {
namespace runtime {
namespace ops {

template <typename T>
struct ValueCollector {
  struct ExprWrapper {
    using V = T;
    ExprWrapper(Expr&& expr) : expr(std::move(expr)) {}
    T operator()(size_t idx) const {
      auto val = expr.eval_path(idx);
      return TypedConverter<T>::to_typed(val);
    }
    Expr expr;
  };
  using EXPR = ExprWrapper;
  void collect(const EXPR& expr, size_t idx) {
    auto val = expr.expr.eval_path(idx);
    builder.push_back_opt(TypedConverter<T>::to_typed(val));
  }
  auto get(const EXPR&) { return builder.finish(); }
  ValueColumnBuilder<T> builder;
};

template <typename VertexColoumn, typename T>
struct SLPropertyExpr {
  using V = T;
  SLPropertyExpr(const GraphReadInterface& graph, const VertexColoumn& column,
                 const std::string& property_name)
      : column(column) {
    auto labels = column.get_labels_set();
    auto& label = *labels.begin();
    property = graph.GetVertexColumn<T>(label, property_name);
    is_optional_ = property.is_null();
  }
  inline T operator()(size_t idx) const {
    auto v = column.get_vertex(idx);
    return property.get_view(v.vid_);
  }
  bool is_optional() const { return is_optional_; }
  bool is_optional_;
  const VertexColoumn& column;
  GraphReadInterface::vertex_column_t<T> property;
};

template <typename VertexColoumn, typename T>
struct MLPropertyExpr {
  using V = T;
  MLPropertyExpr(const GraphReadInterface& graph, const VertexColoumn& vertex,
                 const std::string& property_name)
      : vertex(vertex) {
    auto labels = vertex.get_labels_set();
    int label_num = graph.schema().vertex_label_num();
    property.resize(label_num);
    is_optional_ = false;
    for (auto label : labels) {
      property[label] = graph.GetVertexColumn<T>(label, property_name);
      if (property[label].is_null()) {
        is_optional_ = true;
      }
    }
  }
  bool is_optional() const { return is_optional_; }
  inline T operator()(size_t idx) const {
    auto v = vertex.get_vertex(idx);
    return property[v.label_].get_view(v.vid_);
  }
  const VertexColoumn& vertex;
  std::vector<GraphReadInterface::vertex_column_t<T>> property;

  bool is_optional_;
};

template <typename EXPR>
struct PropertyValueCollector {
  PropertyValueCollector(const Context& ctx) { builder.reserve(ctx.row_num()); }
  void collect(const EXPR& expr, size_t idx) {
    builder.push_back_opt(expr(idx));
  }
  auto get(const EXPR&) { return builder.finish(); }

  ValueColumnBuilder<typename EXPR::V> builder;
};

template <typename VertexColumn>
std::unique_ptr<ProjectExprBase> create_sl_property_expr(
    const Context& ctx, const GraphReadInterface& graph,
    const VertexColumn& column, const std::string& property_name,
    RTAnyType type, int alias) {
  switch (type) {
  case RTAnyType::kI32Value: {
    auto expr =
        SLPropertyExpr<VertexColumn, int32_t>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<ProjectExpr<SLPropertyExpr<VertexColumn, int32_t>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kI64Value: {
    auto expr =
        SLPropertyExpr<VertexColumn, int64_t>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<ProjectExpr<SLPropertyExpr<VertexColumn, int64_t>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kF64Value: {
    auto expr =
        SLPropertyExpr<VertexColumn, double>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<
        ProjectExpr<SLPropertyExpr<VertexColumn, double>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case RTAnyType::kStringValue: {
    auto expr = SLPropertyExpr<VertexColumn, std::string_view>(graph, column,
                                                               property_name);
    PropertyValueCollector<decltype(expr)> collector(ctx);
    if (expr.is_optional()) {
      return nullptr;
    }

    return std::make_unique<ProjectExpr<
        SLPropertyExpr<VertexColumn, std::string_view>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case RTAnyType::kDate32: {
    auto expr = SLPropertyExpr<VertexColumn, Day>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<
        ProjectExpr<SLPropertyExpr<VertexColumn, Day>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case RTAnyType::kTimestamp: {
    auto expr =
        SLPropertyExpr<VertexColumn, Date>(graph, column, property_name);
    PropertyValueCollector<decltype(expr)> collector(ctx);
    if (expr.is_optional()) {
      return nullptr;
    }
    return std::make_unique<
        ProjectExpr<SLPropertyExpr<VertexColumn, Date>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  default:
    LOG(INFO) << "not implemented - " << static_cast<int>(type);
  }
  return nullptr;
}

template <typename VertexColumn>
std::unique_ptr<ProjectExprBase> create_ml_property_expr(
    const Context& ctx, const GraphReadInterface& graph,
    const VertexColumn& column, const std::string& property_name,
    RTAnyType type, int alias) {
  switch (type) {
  case RTAnyType::kI32Value: {
    auto expr =
        MLPropertyExpr<VertexColumn, int32_t>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<ProjectExpr<MLPropertyExpr<VertexColumn, int32_t>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }
  case RTAnyType::kI64Value: {
    auto expr =
        MLPropertyExpr<VertexColumn, int64_t>(graph, column, property_name);
    if (expr.is_optional()) {
      return nullptr;
    }
    PropertyValueCollector<decltype(expr)> collector(ctx);
    return std::make_unique<ProjectExpr<MLPropertyExpr<VertexColumn, int64_t>,
                                        decltype(collector)>>(std::move(expr),
                                                              collector, alias);
  }

  case RTAnyType::kDate32: {
    auto expr = MLPropertyExpr<VertexColumn, Day>(graph, column, property_name);
    PropertyValueCollector<decltype(expr)> collector(ctx);
    if (expr.is_optional()) {
      return nullptr;
    }
    return std::make_unique<
        ProjectExpr<MLPropertyExpr<VertexColumn, Day>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  case RTAnyType::kTimestamp: {
    auto expr =
        MLPropertyExpr<VertexColumn, Date>(graph, column, property_name);
    PropertyValueCollector<decltype(expr)> collector(ctx);
    if (expr.is_optional()) {
      return nullptr;
    }
    return std::make_unique<
        ProjectExpr<MLPropertyExpr<VertexColumn, Date>, decltype(collector)>>(
        std::move(expr), collector, alias);
  }
  default:
    LOG(INFO) << "not implemented - " << static_cast<int>(type);
  }
  return nullptr;
}

template <typename T>
struct OptionalValueCollector {
  struct OptionalExprWrapper {
    using V = std::optional<T>;
    OptionalExprWrapper(Expr&& expr) : expr(std::move(expr)) {}
    std::optional<T> operator()(size_t idx) const {
      auto val = expr.eval_path(idx, 0);
      if (val.is_null()) {
        return std::nullopt;
      }
      return TypedConverter<T>::to_typed(val);
    }
    Expr expr;
  };
  using EXPR = OptionalExprWrapper;
  void collect(const EXPR& expr, size_t idx) {
    auto val = expr.expr.eval_path(idx, 0);
    if (val.is_null()) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(TypedConverter<T>::to_typed(val), true);
    }
  }
  auto get(const EXPR&) { return builder.finish(); }
  OptionalValueColumnBuilder<T> builder;
};

struct VertexExprWrapper {
  using V = VertexRecord;
  VertexExprWrapper(Expr&& expr) : expr(std::move(expr)) {}
  VertexRecord operator()(size_t idx) const {
    return expr.eval_path(idx).as_vertex();
  }
  Expr expr;
};
struct SLVertexCollector {
  using EXPR = VertexExprWrapper;
  SLVertexCollector(label_t v_label) : builder(v_label) {}
  void collect(const EXPR& expr, size_t idx) {
    auto v = expr.expr.eval_path(idx).as_vertex();
    builder.push_back_opt(v.vid_);
  }
  auto get(const EXPR&) { return builder.finish(); }
  SLVertexColumnBuilder builder;
};

struct MLVertexCollector {
  using EXPR = VertexExprWrapper;
  void collect(const EXPR& expr, size_t idx) {
    auto v = expr.expr.eval_path(idx).as_vertex();
    builder.push_back_vertex(v);
  }
  auto get(const EXPR&) { return builder.finish(); }
  MLVertexColumnBuilder builder;
};

struct EdgeCollector {
  struct EdgeExprWrapper {
    using V = EdgeRecord;

    EdgeExprWrapper(Expr&& expr) : expr(std::move(expr)) {}
    EdgeRecord operator()(size_t idx) const {
      return expr.eval_path(idx).as_edge();
    }
    Expr expr;
  };
  using EXPR = EdgeExprWrapper;
  void collect(const EXPR& expr, size_t idx) {
    auto e = expr.expr.eval_path(idx);
    builder.push_back_elem(e);
  }
  auto get(const EXPR&) { return builder.finish(); }
  BDMLEdgeColumnBuilder builder;
};

struct ListCollector {
  struct ListExprWrapper {
    using V = List;
    ListExprWrapper(Expr&& expr) : expr(std::move(expr)) {}
    List operator()(size_t idx) const { return expr.eval_path(idx).as_list(); }
    Expr expr;
  };
  using EXPR = ListExprWrapper;
  ListCollector(const EXPR& expr) : builder_(expr.expr.builder()) {}
  void collect(const EXPR& expr, size_t idx) {
    builder_->push_back_elem(expr.expr.eval_path(idx));
  }
  auto get(const EXPR& expr) {
    auto& list_builder = dynamic_cast<ListValueColumnBuilderBase&>(*builder_);
    if (!list_builder.impls_has_been_set()) {
      list_builder.set_list_impls(expr.expr.get_list_impls());
    }
    return builder_->finish();
  }
  std::shared_ptr<IContextColumnBuilder> builder_;
};

struct TupleCollector {
  struct TupleExprWrapper {
    using V = Tuple;
    TupleExprWrapper(Expr&& expr) : expr(std::move(expr)) {}
    Tuple operator()(size_t idx) const {
      return expr.eval_path(idx).as_tuple();
    }
    Expr expr;
  };
  using EXPR = TupleExprWrapper;
  void collect(const EXPR& expr, size_t idx) {
    auto v = expr.expr.eval_path(idx);
    builder.push_back_elem(v);
  }
  auto get(const EXPR&) { return builder.finish(); }
  ValueColumnBuilder<Tuple> builder;
};

struct OptionalTupleCollector {
  struct OptionalTupleExprWrapper {
    using V = std::optional<Tuple>;
    OptionalTupleExprWrapper(Expr&& expr) : expr(std::move(expr)) {}
    std::optional<Tuple> operator()(size_t idx) const {
      auto val = expr.eval_path(idx, 0);
      if (val.is_null()) {
        return std::nullopt;
      }
      return val.as_tuple();
    }
    Expr expr;
  };
  using EXPR = OptionalTupleExprWrapper;
  void collect(const EXPR& expr, size_t idx) {
    auto v = expr.expr.eval_path(idx, 0);
    if (v.is_null()) {
      builder.push_back_null();
    } else {
      builder.push_back_elem(v);
    }
  }
  auto get(const EXPR&) { return builder.finish(); }
  OptionalValueColumnBuilder<Tuple> builder;
};

struct MapCollector {
  struct MapExprWrapper {
    using V = Map;
    MapExprWrapper(Expr&& expr) : expr(std::move(expr)) {}
    Map operator()(size_t idx) const { return expr.eval_path(idx).as_map(); }
    Expr expr;
  };
  using EXPR = MapExprWrapper;

  MapCollector(const EXPR& expr) : builder(expr.expr.builder()) {}
  void collect(const EXPR& expr, size_t idx) {
    auto v = expr.expr.eval_path(idx);
    builder->push_back_elem(v);
  }
  auto get(const EXPR&) { return builder->finish(); }
  std::shared_ptr<IContextColumnBuilder> builder;
};

struct OptionalMapCollector {
  struct OptionalMapExprWrapper {
    using V = std::optional<Map>;
    OptionalMapExprWrapper(Expr&& expr) : expr(std::move(expr)) {}
    std::optional<Map> operator()(size_t idx) const {
      auto val = expr.eval_path(idx, 0);
      if (val.is_null()) {
        return std::nullopt;
      }
      return val.as_map();
    }
    Expr expr;
  };
  using EXPR = OptionalMapExprWrapper;
  OptionalMapCollector(const EXPR& expr) : builder(expr.expr.builder()) {}
  void collect(const EXPR& expr, size_t idx) {
    auto v = expr.expr.eval_path(idx, 0);
    builder->push_back_elem(v);
  }
  auto get(const EXPR&) { return builder->finish(); }
  std::shared_ptr<IContextColumnBuilder> builder;
};

struct StringArrayCollector {
  struct StringArrayExprWrapper {
    using V = std::vector<std::string>;
    StringArrayExprWrapper(Expr&& expr) : expr(std::move(expr)) {}
    std::vector<std::string> operator()(size_t idx) const {
      // TODO: fix this
      auto v = expr.eval_path(idx).as_string_set();
      std::vector<std::string> ret;
      ret.reserve(v.size());
      for (auto& s : v) {
        ret.push_back(s);
      }
      return ret;
    }
    Expr expr;
  };

  using EXPR = StringArrayExprWrapper;
  StringArrayCollector(const EXPR& expr) : builder(expr.expr.builder()) {}
  void collect(const EXPR& expr, size_t idx) {
    auto v = expr.expr.eval_path(idx);
    builder->push_back_elem(v);
  }
  auto get(const EXPR&) { return builder->finish(); }
  std::shared_ptr<IContextColumnBuilder> builder;
};

template <typename EXPR, typename RESULT_T>
struct CaseWhenCollector {
  CaseWhenCollector() {}
  void collect(const EXPR& expr, size_t idx) {
    builder.push_back_opt(expr(idx));
  }
  auto get(const EXPR&) { return builder.finish(); }
  ValueColumnBuilder<RESULT_T> builder;
};

template <typename VERTEX_COL_PTR, typename SP_PRED_T, typename RESULT_T>
struct SPOpr {
  using V = RESULT_T;
  SPOpr(const VERTEX_COL_PTR& vertex_col, SP_PRED_T&& pred, RESULT_T then_value,
        RESULT_T else_value)
      : vertex_col(vertex_col),
        pred(std::move(pred)),
        then_value(then_value),
        else_value(else_value) {}
  inline RESULT_T operator()(size_t idx) const {
    auto v = vertex_col->get_vertex(idx);
    if (pred(v.label_, v.vid_)) {
      return then_value;
    } else {
      return else_value;
    }
  }
  VERTEX_COL_PTR vertex_col;
  SP_PRED_T pred;
  RESULT_T then_value;
  RESULT_T else_value;
};

template <typename PRED>
std::unique_ptr<ProjectExprBase> create_case_when_project(
    const std::shared_ptr<IVertexColumn>& vertex_col, PRED&& pred,
    const common::Value& then_value, const common::Value& else_value,
    int alias) {
  if (then_value.item_case() != else_value.item_case()) {
    return nullptr;
  }
  switch (then_value.item_case()) {
  case common::Value::kI32: {
    if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
      auto typed_vertex_col =
          std::dynamic_pointer_cast<SLVertexColumn>(vertex_col);
      SPOpr opr(typed_vertex_col, std::move(pred), then_value.i32(),
                else_value.i32());
      auto collector = CaseWhenCollector<decltype(opr), int32_t>();
      return std::make_unique<ProjectExpr<decltype(opr), decltype(collector)>>(
          std::move(opr), collector, alias);
    } else {
      SPOpr opr(vertex_col, std::move(pred), then_value.i32(),
                else_value.i32());
      auto collector = CaseWhenCollector<decltype(opr), int32_t>();
      return std::make_unique<ProjectExpr<decltype(opr), decltype(collector)>>(
          std::move(opr), collector, alias);
    }
  }
  case common::Value::kI64: {
    SPOpr opr(vertex_col, std::move(pred), then_value.i64(), else_value.i64());
    auto collector = CaseWhenCollector<decltype(opr), int64_t>();
    return std::make_unique<ProjectExpr<decltype(opr), decltype(collector)>>(
        std::move(opr), collector, alias);
  }

  default:
    LOG(ERROR) << "Unsupported type for case when collector";
    return nullptr;
  }
}

template <typename T>
static std::unique_ptr<ProjectExprBase> _make_project_expr(Expr&& expr,
                                                           int alias,
                                                           int row_num) {
  if (!expr.is_optional()) {
    ValueCollector<T> collector;
    collector.builder.reserve(row_num);
    return std::make_unique<
        ProjectExpr<typename ValueCollector<T>::EXPR, ValueCollector<T>>>(
        std::move(expr), collector, alias);
  } else {
    OptionalValueCollector<T> collector;
    collector.builder.reserve(row_num);
    return std::make_unique<ProjectExpr<
        typename OptionalValueCollector<T>::EXPR, OptionalValueCollector<T>>>(
        std::move(expr), collector, alias);
  }
}
template <typename T>
static std::function<std::unique_ptr<ProjectExprBase>(
    const GraphReadInterface& graph,
    const std::map<std::string, std::string>& params, const Context& ctx)>
_make_project_expr(const common::Expression& expr, int alias) {
  return [=](const GraphReadInterface& graph,
             const std::map<std::string, std::string>& params,
             const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
    Expr e(graph, ctx, params, expr, VarType::kPathVar);
    size_t row_num = ctx.row_num();
    if (!e.is_optional()) {
      ValueCollector<T> collector;
      collector.builder.reserve(row_num);
      return std::make_unique<
          ProjectExpr<typename ValueCollector<T>::EXPR, ValueCollector<T>>>(
          std::move(e), collector, alias);
    } else {
      OptionalValueCollector<T> collector;
      collector.builder.reserve(row_num);
      return std::make_unique<ProjectExpr<
          typename OptionalValueCollector<T>::EXPR, OptionalValueCollector<T>>>(
          std::move(e), collector, alias);
    }
  };
}

bool is_exchange_index(const common::Expression& expr, int alias, int& tag) {
  if (expr.operators().size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kVar) {
    auto var = expr.operators(0).var();
    tag = -1;
    if (var.has_property()) {
      return false;
    }

    if (var.has_tag()) {
      tag = var.tag().id();
    }
    // if (tag == alias) {
    return true;
    //}
  }
  return false;
}

bool is_check_property_in_range(const common::Expression& expr, int& tag,
                                std::string& name, std::string& lower,
                                std::string& upper, common::Value& then_value,
                                common::Value& else_value) {
  if (expr.operators_size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kCase) {
    auto opr = expr.operators(0).case_();
    if (opr.when_then_expressions_size() != 1) {
      return false;
    }
    auto when = opr.when_then_expressions(0).when_expression();
    if (when.operators_size() != 7) {
      return false;
    }
    {
      if (!when.operators(0).has_var()) {
        return false;
      }
      auto var = when.operators(0).var();
      if (!var.has_tag()) {
        return false;
      }
      tag = var.tag().id();
      if (!var.has_property()) {
        return false;
      }
      if (!var.property().has_key()) {
        return false;
      }
      name = var.property().key().name();
      if (name == "id" || name == "label") {
        return false;
      }
    }
    {
      auto op = when.operators(1);
      if (op.item_case() != common::ExprOpr::kLogical ||
          op.logical() != common::GE) {
        return false;
      }
    }
    auto lower_param = when.operators(2);
    if (lower_param.item_case() != common::ExprOpr::kParam) {
      return false;
    }
    lower = lower_param.param().name();
    {
      auto op = when.operators(3);
      if (op.item_case() != common::ExprOpr::kLogical ||
          op.logical() != common::AND) {
        return false;
      }
    }
    {
      if (!when.operators(4).has_var()) {
        return false;
      }
      auto var = when.operators(4).var();
      if (!var.has_tag()) {
        return false;
      }
      if (var.tag().id() != tag) {
        return false;
      }
      if (!var.has_property()) {
        return false;
      }
      if (!var.property().has_key() && name != var.property().key().name()) {
        return false;
      }
    }

    auto op = when.operators(5);
    if (op.item_case() != common::ExprOpr::kLogical ||
        op.logical() != common::LT) {
      return false;
    }
    auto upper_param = when.operators(6);
    if (upper_param.item_case() != common::ExprOpr::kParam) {
      return false;
    }
    upper = upper_param.param().name();
    auto then = opr.when_then_expressions(0).then_result_expression();
    if (then.operators_size() != 1) {
      return false;
    }
    if (!then.operators(0).has_const_()) {
      return false;
    }
    then_value = then.operators(0).const_();
    auto else_expr = opr.else_result_expression();
    if (else_expr.operators_size() != 1) {
      return false;
    }
    if (!else_expr.operators(0).has_const_()) {
      return false;
    }
    else_value = else_expr.operators(0).const_();
    if (then_value.item_case() != else_value.item_case()) {
      return false;
    }

    return true;
  }
  return false;
}

bool is_check_property_cmp(const common::Expression& expr, int& tag,
                           std::string& name, std::string& target,
                           common::Value& then_value, common::Value& else_value,
                           SPPredicateType& ptype) {
  if (expr.operators_size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kCase) {
    auto opr = expr.operators(0).case_();
    if (opr.when_then_expressions_size() != 1) {
      return false;
    }
    auto when = opr.when_then_expressions(0).when_expression();
    if (when.operators_size() != 3) {
      return false;
    }
    {
      if (!when.operators(0).has_var()) {
        return false;
      }
      auto var = when.operators(0).var();
      if (!var.has_tag()) {
        return false;
      }
      tag = var.tag().id();
      if (!var.has_property()) {
        return false;
      }
      if (!var.property().has_key()) {
        return false;
      }
      name = var.property().key().name();
      if (name == "id" || name == "label") {
        return false;
      }
    }
    {
      auto op = when.operators(1);
      if (op.item_case() != common::ExprOpr::kLogical) {
        return false;
      }
      switch (op.logical()) {
      case common::LT:
        ptype = SPPredicateType::kPropertyLT;
        break;
      case common::LE:
        ptype = SPPredicateType::kPropertyLE;
        break;
      case common::GT:
        ptype = SPPredicateType::kPropertyGT;
        break;
      case common::GE:
        ptype = SPPredicateType::kPropertyGE;
        break;
      case common::EQ:
        ptype = SPPredicateType::kPropertyEQ;
        break;
      case common::NE:
        ptype = SPPredicateType::kPropertyNE;
        break;
      default:
        return false;
      }
    }
    auto upper_param = when.operators(2);
    if (upper_param.item_case() != common::ExprOpr::kParam) {
      return false;
    }
    target = upper_param.param().name();
    auto then = opr.when_then_expressions(0).then_result_expression();
    if (then.operators_size() != 1) {
      return false;
    }
    if (!then.operators(0).has_const_()) {
      return false;
    }
    then_value = then.operators(0).const_();
    auto else_expr = opr.else_result_expression();
    if (else_expr.operators_size() != 1) {
      return false;
    }
    if (!else_expr.operators(0).has_const_()) {
      return false;
    }
    else_value = else_expr.operators(0).const_();
    if (then_value.item_case() != else_value.item_case()) {
      return false;
    }

    return true;
  }
  return false;
}

bool is_property_extract(const common::Expression& expr, int& tag,
                         std::string& name, RTAnyType& type) {
  if (expr.operators_size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kVar) {
    auto var = expr.operators(0).var();
    tag = -1;
    if (!var.has_property()) {
      return false;
    }

    if (var.has_tag()) {
      tag = var.tag().id();
    }
    if (var.has_property() && var.property().has_key()) {
      name = var.property().key().name();
      if (name == "id" || name == "label") {
        return false;
      }
      if (var.has_node_type()) {
        type = parse_from_ir_data_type(var.node_type());
      } else {
        return false;
      }
      if (type == RTAnyType::kUnknown) {
        return false;
      }
      // only support pod type
      if (type == RTAnyType::kTimestamp || type == RTAnyType::kDate32 ||
          type == RTAnyType::kI64Value || type == RTAnyType::kI32Value) {
        return true;
      }
    }
  }
  return false;
}

template <typename T>
static std::unique_ptr<ProjectExprBase> create_sp_pred_case_when(
    const GraphReadInterface& graph,
    const std::map<std::string, std::string>& params,
    const std::shared_ptr<IVertexColumn>& vertex, SPPredicateType type,
    const std::string& name, const std::string& target,
    const common::Value& then_value, const common::Value& else_value,
    int alias) {
  if (type == SPPredicateType::kPropertyLT) {
    VertexPropertyLTPredicateBeta<T> pred(graph, name, params.at(target));
    return create_case_when_project(vertex, std::move(pred), then_value,
                                    else_value, alias);
  } else if (type == SPPredicateType::kPropertyGT) {
    VertexPropertyGTPredicateBeta<T> pred(graph, name, params.at(target));
    return create_case_when_project(vertex, std::move(pred), then_value,
                                    else_value, alias);
  } else if (type == SPPredicateType::kPropertyLE) {
    VertexPropertyLEPredicateBeta<T> pred(graph, name, params.at(target));
    return create_case_when_project(vertex, std::move(pred), then_value,
                                    else_value, alias);
  } else if (type == SPPredicateType::kPropertyGE) {
    VertexPropertyGEPredicateBeta<T> pred(graph, name, params.at(target));
    return create_case_when_project(vertex, std::move(pred), then_value,
                                    else_value, alias);
  } else if (type == SPPredicateType::kPropertyEQ) {
    VertexPropertyEQPredicateBeta<T> pred(graph, name, params.at(target));
    return create_case_when_project(vertex, std::move(pred), then_value,
                                    else_value, alias);
  } else if (type == SPPredicateType::kPropertyNE) {
    VertexPropertyNEPredicateBeta<T> pred(graph, name, params.at(target));

    return create_case_when_project(vertex, std::move(pred), then_value,
                                    else_value, alias);
  }
  return nullptr;
}

// in the case of data_type is not set, we need to infer the type from the
// expr
static std::function<std::unique_ptr<ProjectExprBase>(
    const GraphReadInterface& graph,
    const std::map<std::string, std::string>& params, const Context& ctx)>
make_project_expr(const common::Expression& expr, int alias) {
  return [=](const GraphReadInterface& graph,
             const std::map<std::string, std::string>& params,
             const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
    Expr e(graph, ctx, params, expr, VarType::kPathVar);

    switch (e.type()) {
    case RTAnyType::kI64Value: {
      return _make_project_expr<int64_t>(std::move(e), alias, ctx.row_num());
    } break;
    case RTAnyType::kStringValue: {
      return _make_project_expr<std::string_view>(std::move(e), alias,
                                                  ctx.row_num());
    } break;
    case RTAnyType::kDate32: {
      return _make_project_expr<Day>(std::move(e), alias, ctx.row_num());
    } break;
    case RTAnyType::kTimestamp: {
      return _make_project_expr<Date>(std::move(e), alias, ctx.row_num());
    } break;
    case RTAnyType::kVertex: {
      MLVertexCollector collector;
      collector.builder.reserve(ctx.row_num());
      return std::make_unique<
          ProjectExpr<typename MLVertexCollector::EXPR, MLVertexCollector>>(
          std::move(e), collector, alias);
    } break;
    case RTAnyType::kI32Value: {
      return _make_project_expr<int32_t>(std::move(e), alias, ctx.row_num());
    } break;
    case RTAnyType::kF64Value: {
      return _make_project_expr<double>(std::move(e), alias, ctx.row_num());
    } break;
    case RTAnyType::kEdge: {
      EdgeCollector collector;
      return std::make_unique<
          ProjectExpr<typename EdgeCollector::EXPR, EdgeCollector>>(
          std::move(e), collector, alias);
    } break;
    case RTAnyType::kTuple: {
      if (e.is_optional()) {
        OptionalTupleCollector collector;
        collector.builder.reserve(ctx.row_num());
        return std::make_unique<ProjectExpr<
            typename OptionalTupleCollector::EXPR, OptionalTupleCollector>>(
            std::move(e), collector, alias);
      } else {
        TupleCollector collector;
        collector.builder.reserve(ctx.row_num());
        return std::make_unique<
            ProjectExpr<typename TupleCollector::EXPR, TupleCollector>>(
            std::move(e), collector, alias);
      }
    } break;
    case RTAnyType::kList: {
      ListCollector::EXPR expr(std::move(e));
      ListCollector collector(expr);
      return std::make_unique<
          ProjectExpr<typename ListCollector::EXPR, ListCollector>>(
          std::move(expr), collector, alias);
    } break;
    case RTAnyType::kMap: {
      if (!e.is_optional()) {
        MapCollector::EXPR expr(std::move(e));
        MapCollector collector(expr);
        return std::make_unique<
            ProjectExpr<typename MapCollector::EXPR, MapCollector>>(
            std::move(expr), collector, alias);
      } else {
        OptionalMapCollector::EXPR expr(std::move(e));
        OptionalMapCollector collector(expr);
        return std::make_unique<ProjectExpr<typename OptionalMapCollector::EXPR,
                                            OptionalMapCollector>>(
            std::move(expr), collector, alias);
      }
    } break;
    default:
      LOG(FATAL) << "not support - " << static_cast<int>(e.type());
      break;
    }
    return nullptr;
  };
}

static std::optional<std::function<std::unique_ptr<ProjectExprBase>(
    const GraphReadInterface& graph,
    const std::map<std::string, std::string>& params, const Context& ctx)>>
parse_special_expr(const common::Expression& expr, int alias) {
  int tag = -1;
  if (is_exchange_index(expr, alias, tag)) {
    return [=](const GraphReadInterface& graph,
               const std::map<std::string, std::string>& params,
               const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
      return std::make_unique<DummyGetter>(tag, alias);
    };
  }
  {
    int tag;
    std::string name;
    RTAnyType type;
    if (is_property_extract(expr, tag, name, type)) {
      return [=](const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params,
                 const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
        auto col = ctx.get(tag);
        if ((!col->is_optional()) &&
            col->column_type() == ContextColumnType::kVertex) {
          auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
          if (vertex_col->get_labels_set().size() == 1) {
            if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
              auto typed_vertex_col =
                  std::dynamic_pointer_cast<SLVertexColumn>(vertex_col);
              return create_sl_property_expr(ctx, graph, *typed_vertex_col,
                                             name, type, alias);

            } else {
              return create_sl_property_expr(ctx, graph, *vertex_col, name,
                                             type, alias);
            }
          } else {
            if (vertex_col->vertex_column_type() ==
                VertexColumnType::kMultiple) {
              auto typed_vertex_col =
                  std::dynamic_pointer_cast<MLVertexColumn>(vertex_col);
              return create_ml_property_expr(ctx, graph, *typed_vertex_col,
                                             name, type, alias);
            } else {
              auto typed_vertex_col =
                  std::dynamic_pointer_cast<MSVertexColumn>(vertex_col);
              return create_ml_property_expr(ctx, graph, *typed_vertex_col,
                                             name, type, alias);
            }
          }
        }
        return make_project_expr(expr, alias)(graph, params, ctx);
      };
    }
  }
  std::string name, lower, upper, target;
  common::Value then_value, else_value;
  if (is_check_property_in_range(expr, tag, name, lower, upper, then_value,
                                 else_value)) {
    return [=](const GraphReadInterface& graph,
               const std::map<std::string, std::string>& params,
               const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
      auto col = ctx.get(tag);
      if (col->column_type() == ContextColumnType::kVertex) {
        auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);

        auto type = expr.operators(0)
                        .case_()
                        .when_then_expressions(0)
                        .when_expression()
                        .operators(2)
                        .param()
                        .data_type();
        auto type_ = parse_from_ir_data_type(type);
        if (then_value.item_case() != else_value.item_case() ||
            then_value.item_case() != common::Value::kI32) {
          return make_project_expr(expr, alias)(graph, params, ctx);
        }

        if (type_ == RTAnyType::kI32Value) {
          SPOpr sp(vertex_col,
                   VertexPropertyBetweenPredicateBeta<int32_t>(
                       graph, name, params.at(lower), params.at(upper)),
                   then_value.i32(), else_value.i32());
          CaseWhenCollector<decltype(sp), int32_t> collector;
          return std::make_unique<
              ProjectExpr<decltype(sp), decltype(collector)>>(std::move(sp),
                                                              collector, alias);

        } else if (type_ == RTAnyType::kI64Value) {
          SPOpr sp(vertex_col,
                   VertexPropertyBetweenPredicateBeta<int64_t>(
                       graph, name, params.at(lower), params.at(upper)),
                   then_value.i32(), else_value.i32());
          CaseWhenCollector<decltype(sp), int32_t> collector;
          return std::make_unique<
              ProjectExpr<decltype(sp), decltype(collector)>>(std::move(sp),
                                                              collector, alias);
        } else if (type_ == RTAnyType::kTimestamp) {
          if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
            auto typed_vertex_col =
                std::dynamic_pointer_cast<SLVertexColumn>(vertex_col);
            SPOpr sp(typed_vertex_col,
                     VertexPropertyBetweenPredicateBeta<Date>(
                         graph, name, params.at(lower), params.at(upper)),
                     then_value.i32(), else_value.i32());
            CaseWhenCollector<decltype(sp), int32_t> collector;
            return std::make_unique<
                ProjectExpr<decltype(sp), decltype(collector)>>(
                std::move(sp), collector, alias);
          } else {
            SPOpr sp(vertex_col,
                     VertexPropertyBetweenPredicateBeta<Date>(
                         graph, name, params.at(lower), params.at(upper)),
                     then_value.i32(), else_value.i32());
            CaseWhenCollector<decltype(sp), int32_t> collector;
            return std::make_unique<
                ProjectExpr<decltype(sp), decltype(collector)>>(
                std::move(sp), collector, alias);
          }
        }
      }
      return make_project_expr(expr, alias)(graph, params, ctx);
    };
  }
  SPPredicateType ptype;
  if (is_check_property_cmp(expr, tag, name, target, then_value, else_value,
                            ptype)) {
    return [=](const GraphReadInterface& graph,
               const std::map<std::string, std::string>& params,
               const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
      auto col = ctx.get(tag);
      if (col->column_type() == ContextColumnType::kVertex) {
        auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
        auto type = expr.operators(0)
                        .case_()
                        .when_then_expressions(0)
                        .when_expression()
                        .operators(2)
                        .param()
                        .data_type();
        auto type_ = parse_from_ir_data_type(type);

        if (type_ == RTAnyType::kI32Value) {
          auto ptr = create_sp_pred_case_when<int32_t>(
              graph, params, vertex_col, ptype, name, target, then_value,
              else_value, alias);
          if (ptr) {
            return ptr;
          }
        } else if (type_ == RTAnyType::kI64Value) {
          auto ptr = create_sp_pred_case_when<int64_t>(
              graph, params, vertex_col, ptype, name, target, then_value,
              else_value, alias);
          if (ptr) {
            return ptr;
          }
        } else if (type_ == RTAnyType::kTimestamp) {
          auto ptr = create_sp_pred_case_when<Date>(
              graph, params, vertex_col, ptype, name, target, then_value,
              else_value, alias);
          if (ptr) {
            return ptr;
          }
        } else if (type_ == RTAnyType::kStringValue) {
          auto ptr = create_sp_pred_case_when<std::string_view>(
              graph, params, vertex_col, ptype, name, target, then_value,
              else_value, alias);
          if (ptr) {
            return ptr;
          }
        }
      }
      return make_project_expr(expr, alias)(graph, params, ctx);
    };
  }
  return std::nullopt;
}

std::optional<std::function<std::unique_ptr<ProjectExprBase>(
    const GraphReadInterface& graph,
    const std::map<std::string, std::string>& params, const Context& ctx)>>
make_project_expr(const common::Expression& expr,
                  const common::IrDataType& data_type, int alias) {
  switch (data_type.type_case()) {
  case common::IrDataType::kDataType: {
    auto type = parse_from_ir_data_type(data_type);
    switch (type) {
    case RTAnyType::kI64Value: {
      return _make_project_expr<int64_t>(expr, alias);
    } break;
    case RTAnyType::kI32Value: {
      return _make_project_expr<int32_t>(expr, alias);
    } break;
    case RTAnyType::kF64Value: {
      return _make_project_expr<double>(expr, alias);
    } break;
    case RTAnyType::kBoolValue: {
      return _make_project_expr<bool>(expr, alias);
    } break;
    case RTAnyType::kStringValue: {
      return _make_project_expr<std::string_view>(expr, alias);
    } break;
    case RTAnyType::kTimestamp: {
      return _make_project_expr<Date>(expr, alias);
    } break;
    case RTAnyType::kDate32: {
      return _make_project_expr<Day>(expr, alias);
    } break;
    // todo: fix this
    case RTAnyType::kList: {
      return [=](const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params,
                 const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
        Expr e(graph, ctx, params, expr, VarType::kPathVar);
        StringArrayCollector::EXPR expr(std::move(e));
        StringArrayCollector collector(expr);
        collector.builder->reserve(ctx.row_num());
        return std::make_unique<ProjectExpr<typename StringArrayCollector::EXPR,
                                            StringArrayCollector>>(
            std::move(expr), collector, alias);
      };
    } break;
    // compiler bug here
    case RTAnyType::kUnknown: {
      return make_project_expr(expr, alias);
    } break;
    default: {
      LOG(INFO) << "not support" << data_type.DebugString();
      return std::nullopt;
    }
    }
  }
  case common::IrDataType::kGraphType: {
    const common::GraphDataType& graph_data_type = data_type.graph_type();
    common::GraphDataType_GraphElementOpt elem_opt =
        graph_data_type.element_opt();
    int label_num = graph_data_type.graph_data_type_size();
    if (elem_opt == common::GraphDataType_GraphElementOpt::
                        GraphDataType_GraphElementOpt_VERTEX) {
      if (label_num == 1) {
        label_t v_label = static_cast<label_t>(
            graph_data_type.graph_data_type(0).label().label());
        return [=](const GraphReadInterface& graph,
                   const std::map<std::string, std::string>& params,
                   const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
          Expr e(graph, ctx, params, expr, VarType::kPathVar);
          SLVertexCollector collector(v_label);
          collector.builder.reserve(ctx.row_num());
          return std::make_unique<
              ProjectExpr<typename SLVertexCollector::EXPR, SLVertexCollector>>(
              std::move(e), collector, alias);
        };
      } else if (label_num > 1) {
        return [=](const GraphReadInterface& graph,
                   const std::map<std::string, std::string>& params,
                   const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
          Expr e(graph, ctx, params, expr, VarType::kPathVar);
          MLVertexCollector collector;
          collector.builder.reserve(ctx.row_num());
          return std::make_unique<
              ProjectExpr<typename MLVertexCollector::EXPR, MLVertexCollector>>(
              std::move(e), collector, alias);
        };
      } else {
        LOG(INFO) << "unexpected type";
      }
    } else if (elem_opt == common::GraphDataType_GraphElementOpt::
                               GraphDataType_GraphElementOpt_EDGE) {
      return [=](const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params,
                 const Context& ctx) -> std::unique_ptr<ProjectExprBase> {
        Expr e(graph, ctx, params, expr, VarType::kPathVar);
        EdgeCollector collector;
        return std::make_unique<
            ProjectExpr<typename EdgeCollector::EXPR, EdgeCollector>>(
            std::move(e), collector, alias);
      };
    } else {
      LOG(INFO) << "unexpected type";
    }
  } break;
  case common::IrDataType::TYPE_NOT_SET: {
    return make_project_expr(expr, alias);
  } break;

  default:
    LOG(INFO) << "unexpected type" << data_type.DebugString();
    break;
  }
  return std::nullopt;
}

class ProjectOpr : public IReadOperator {
 public:
  ProjectOpr(const std::vector<std::function<std::unique_ptr<ProjectExprBase>(
                 const GraphReadInterface& graph,
                 const std::map<std::string, std::string>& params,
                 const Context& ctx)>>& exprs,
             bool is_append)
      : exprs_(exprs), is_append_(is_append) {}

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    std::vector<std::unique_ptr<ProjectExprBase>> exprs;
    for (size_t i = 0; i < exprs_.size(); ++i) {
      exprs.push_back(exprs_[i](graph, params, ctx));
    }
    return Project::project(std::move(ctx), exprs, is_append_);
  }

 private:
  std::vector<std::function<std::unique_ptr<ProjectExprBase>(
      const GraphReadInterface& graph,
      const std::map<std::string, std::string>& params, const Context& ctx)>>
      exprs_;
  bool is_append_;
};

auto _make_project_expr(const common::Expression& expr, int alias,
                        const std::optional<common::IrDataType>& data_type) {
  auto func = parse_special_expr(expr, alias);
  if (func.has_value()) {
    return func.value();
  }
  if (data_type.has_value() &&
      data_type.value().type_case() != common::IrDataType::TYPE_NOT_SET) {
    auto func = make_project_expr(expr, data_type.value(), alias);
    if (func.has_value()) {
      return func.value();
    }
  }
  return make_project_expr(expr, alias);
}

bl::result<ReadOpBuildResultT> ProjectOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  std::vector<common::IrDataType> data_types;
  int mappings_size = plan.plan(op_idx).opr().project().mappings_size();
  std::vector<std::function<std::unique_ptr<ProjectExprBase>(
      const GraphReadInterface& graph,
      const std::map<std::string, std::string>& params, const Context& ctx)>>
      exprs;
  ContextMeta ret_meta;
  bool is_append = plan.plan(op_idx).opr().project().is_append();
  if (is_append) {
    ret_meta = ctx_meta;
  }
  if (plan.plan(op_idx).meta_data_size() == mappings_size) {
    for (int i = 0; i < plan.plan(op_idx).meta_data_size(); ++i) {
      data_types.push_back(plan.plan(op_idx).meta_data(i).type());
      const auto& m = plan.plan(op_idx).opr().project().mappings(i);
      int alias = m.has_alias() ? m.alias().value() : -1;
      ret_meta.set(alias);
      if (!m.has_expr()) {
        LOG(ERROR) << "expr is not set" << m.DebugString();
        return std::make_pair(nullptr, ret_meta);
      }
      auto expr = m.expr();
      exprs.emplace_back(_make_project_expr(expr, alias, data_types[i]));
    }
  } else {
    for (int i = 0; i < mappings_size; ++i) {
      auto& m = plan.plan(op_idx).opr().project().mappings(i);

      int alias = m.has_alias() ? m.alias().value() : -1;

      ret_meta.set(alias);
      if (!m.has_expr()) {
        LOG(ERROR) << "expr is not set" << m.DebugString();
        return std::make_pair(nullptr, ret_meta);
      }
      auto expr = m.expr();
      exprs.emplace_back(_make_project_expr(expr, alias, std::nullopt));
    }
  }

  return std::make_pair(
      std::make_unique<ProjectOpr>(std::move(exprs), is_append), ret_meta);
}

class ProjectOrderByOprBeta : public IReadOperator {
 public:
  ProjectOrderByOprBeta(
      const std::vector<std::function<std::unique_ptr<ProjectExprBase>(
          const GraphReadInterface& graph,
          const std::map<std::string, std::string>& params,
          const Context& ctx)>>& exprs,
      const std::set<int>& order_by_keys,
      const std::vector<std::pair<common::Variable, bool>>& order_by_pairs,
      int lower_bound, int upper_bound,
      const std::tuple<int, int, bool>& first_pair)
      : exprs_(exprs),
        order_by_keys_(order_by_keys),
        order_by_pairs_(order_by_pairs),
        lower_bound_(lower_bound),
        upper_bound_(upper_bound),
        first_pair_(first_pair) {}

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    auto cmp_func = [&](const Context& ctx) -> GeneralComparer {
      GeneralComparer cmp;
      for (const auto& pair : order_by_pairs_) {
        Var v(graph, ctx, pair.first, VarType::kPathVar);
        cmp.add_keys(std::move(v), pair.second);
      }
      return cmp;
    };
    return Project::project_order_by_fuse<GeneralComparer>(
        graph, params, std::move(ctx), exprs_, cmp_func, lower_bound_,
        upper_bound_, order_by_keys_, first_pair_);
  }

 private:
  std::vector<std::function<std::unique_ptr<ProjectExprBase>(
      const GraphReadInterface& graph,
      const std::map<std::string, std::string>& params, const Context& ctx)>>
      exprs_;
  std::set<int> order_by_keys_;
  std::vector<std::pair<common::Variable, bool>> order_by_pairs_;
  int lower_bound_, upper_bound_;
  std::tuple<int, int, bool> first_pair_;
};

static bool project_order_by_fusable_beta(
    const physical::Project& project_opr, const algebra::OrderBy& order_by_opr,
    const ContextMeta& ctx_meta,
    const std::vector<common::IrDataType>& data_types,
    std::set<int>& order_by_keys) {
  if (!order_by_opr.has_limit()) {
    return false;
  }
  if (project_opr.is_append()) {
    return false;
  }

  int mappings_size = project_opr.mappings_size();
  if (static_cast<size_t>(mappings_size) != data_types.size()) {
    return false;
  }

  std::set<int> new_generate_columns;
  for (int i = 0; i < mappings_size; ++i) {
    const physical::Project_ExprAlias& m = project_opr.mappings(i);
    if (m.has_alias()) {
      int alias = m.alias().value();
      if (ctx_meta.exist(alias)) {
        return false;
      }
      if (new_generate_columns.find(alias) != new_generate_columns.end()) {
        return false;
      }
      new_generate_columns.insert(alias);
    }
  }

  int order_by_keys_num = order_by_opr.pairs_size();
  for (int k_i = 0; k_i < order_by_keys_num; ++k_i) {
    if (!order_by_opr.pairs(k_i).has_key()) {
      return false;
    }
    if (!order_by_opr.pairs(k_i).key().has_tag()) {
      return false;
    }
    if (!(order_by_opr.pairs(k_i).key().tag().item_case() ==
          common::NameOrId::ItemCase::kId)) {
      return false;
    }
    order_by_keys.insert(order_by_opr.pairs(k_i).key().tag().id());
  }
  if (data_types.size() == order_by_keys.size()) {
    return false;
  }
  for (auto key : order_by_keys) {
    if (new_generate_columns.find(key) == new_generate_columns.end() &&
        !ctx_meta.exist(key)) {
      return false;
    }
  }
  return true;
}

bl::result<ReadOpBuildResultT> ProjectOrderByOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  std::vector<common::IrDataType> data_types;
  int mappings_size = plan.plan(op_idx).opr().project().mappings_size();
  if (plan.plan(op_idx).meta_data_size() == mappings_size) {
    for (int i = 0; i < plan.plan(op_idx).meta_data_size(); ++i) {
      data_types.push_back(plan.plan(op_idx).meta_data(i).type());
    }
  }
  std::set<int> order_by_keys;
  if (project_order_by_fusable_beta(plan.plan(op_idx).opr().project(),
                                    plan.plan(op_idx + 1).opr().order_by(),
                                    ctx_meta, data_types, order_by_keys)) {
    ContextMeta ret_meta;
    std::vector<std::function<std::unique_ptr<ProjectExprBase>(
        const GraphReadInterface& graph,
        const std::map<std::string, std::string>& params, const Context& ctx)>>
        exprs;
    std::set<int> index_set;
    int first_key =
        plan.plan(op_idx + 1).opr().order_by().pairs(0).key().tag().id();
    int first_idx = -1;
    for (int i = 0; i < mappings_size; ++i) {
      auto& m = plan.plan(op_idx).opr().project().mappings(i);
      int alias = -1;
      if (m.has_alias()) {
        alias = m.alias().value();
      }
      ret_meta.set(alias);
      if (alias == first_key) {
        first_idx = i;
      }
      if (!m.has_expr()) {
        LOG(ERROR) << "expr is not set" << m.DebugString();
        return std::make_pair(nullptr, ret_meta);
      }
      auto expr = m.expr();
      exprs.emplace_back(_make_project_expr(expr, alias, data_types[i]));
      if (order_by_keys.find(alias) != order_by_keys.end()) {
        index_set.insert(i);
      }
    }

    auto order_by_opr = plan.plan(op_idx + 1).opr().order_by();
    int pair_size = order_by_opr.pairs_size();
    std::vector<std::pair<common::Variable, bool>> order_by_pairs;
    std::tuple<int, int, bool> first_tuple;
    for (int i = 0; i < pair_size; ++i) {
      const auto& pair = order_by_opr.pairs(i);
      if (pair.order() != algebra::OrderBy_OrderingPair_Order::
                              OrderBy_OrderingPair_Order_ASC &&
          pair.order() != algebra::OrderBy_OrderingPair_Order::
                              OrderBy_OrderingPair_Order_DESC) {
        LOG(ERROR) << "order by order is not set" << pair.DebugString();
        return std::make_pair(nullptr, ContextMeta());
      }
      bool asc =
          pair.order() ==
          algebra::OrderBy_OrderingPair_Order::OrderBy_OrderingPair_Order_ASC;
      order_by_pairs.emplace_back(pair.key(), asc);
      if (i == 0) {
        first_tuple = std::make_tuple(first_key, first_idx, asc);
        if (pair.key().has_property()) {
          LOG(ERROR) << "key has property" << pair.DebugString();
          return std::make_pair(nullptr, ContextMeta());
        }
      }
    }
    int lower = 0;
    int upper = std::numeric_limits<int>::max();
    if (order_by_opr.has_limit()) {
      lower = order_by_opr.limit().lower();
      upper = order_by_opr.limit().upper();
    }
    return std::make_pair(std::make_unique<ProjectOrderByOprBeta>(
                              std::move(exprs), index_set, order_by_pairs,
                              lower, upper, first_tuple),
                          ret_meta);
  } else {
    return std::make_pair(nullptr, ContextMeta());
  }
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
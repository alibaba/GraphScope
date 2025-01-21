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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/path.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/path_expand.h"
#include "flex/engines/graph_db/runtime/utils/predicates.h"
#include "flex/engines/graph_db/runtime/utils/utils.h"

namespace gs {
namespace runtime {
namespace ops {

static bool is_shortest_path_with_order_by_limit(
    const physical::PhysicalPlan& plan, int i, int& path_len_alias,
    int& vertex_alias, int& limit_upper) {
  int opr_num = plan.plan_size();
  const auto& opr = plan.plan(i).opr();
  int start_tag = opr.path().start_tag().value();
  // must be any shortest path
  if (opr.path().path_opt() !=
          physical::PathExpand_PathOpt::PathExpand_PathOpt_ANY_SHORTEST ||
      opr.path().result_opt() !=
          physical::PathExpand_ResultOpt::PathExpand_ResultOpt_ALL_V_E) {
    return false;
  }
  if (i + 4 < opr_num) {
    const auto& get_v_opr = plan.plan(i + 1).opr();
    const auto& get_v_filter_opr = plan.plan(i + 2).opr();
    const auto& select_opr = plan.plan(i + 3).opr();
    const auto& project_opr = plan.plan(i + 4).opr();
    const auto& order_by_opr = plan.plan(i + 5).opr();
    if (!get_v_opr.has_vertex() || !get_v_filter_opr.has_vertex() ||
        !project_opr.has_project() || !order_by_opr.has_order_by()) {
      return false;
    }
    if (get_v_opr.vertex().opt() != physical::GetV::END) {
      return false;
    }

    if (get_v_filter_opr.vertex().opt() != physical::GetV::ITSELF) {
      return false;
    }

    int path_alias = opr.path().has_alias() ? opr.path().alias().value() : -1;
    int get_v_tag =
        get_v_opr.vertex().has_tag() ? get_v_opr.vertex().tag().value() : -1;
    int get_v_alias = get_v_opr.vertex().has_alias()
                          ? get_v_opr.vertex().alias().value()
                          : -1;
    if (path_alias != get_v_tag && get_v_tag != -1) {
      return false;
    }
    int get_v_filter_tag = get_v_filter_opr.vertex().has_tag()
                               ? get_v_filter_opr.vertex().tag().value()
                               : -1;
    if (get_v_filter_tag != get_v_alias && get_v_filter_tag != -1) {
      return false;
    }
    if (!select_opr.has_select()) {
      return false;
    }
    if (!select_opr.select().has_predicate()) {
      return false;
    }
    auto pred = select_opr.select().predicate();
    if (pred.operators_size() != 3) {
      return false;
    }
    if (!pred.operators(0).has_var() ||
        !(pred.operators(1).item_case() == common::ExprOpr::kLogical) ||
        pred.operators(1).logical() != common::Logical::NE ||
        !pred.operators(2).has_var()) {
      return false;
    }

    if (!pred.operators(0).var().has_tag() ||
        !pred.operators(2).var().has_tag()) {
      return false;
    }
    if (pred.operators(0).var().tag().id() != get_v_alias &&
        pred.operators(2).var().tag().id() != get_v_alias) {
      return false;
    }

    if (pred.operators(0).var().tag().id() != start_tag &&
        pred.operators(2).var().tag().id() != start_tag) {
      return false;
    }

    // only vertex and length(path)
    if (project_opr.project().mappings_size() != 2 ||
        project_opr.project().is_append()) {
      return false;
    }

    auto mapping = project_opr.project().mappings();
    if (!mapping[0].has_expr() || !mapping[1].has_expr()) {
      return false;
    }
    if (mapping[0].expr().operators_size() != 1 ||
        mapping[1].expr().operators_size() != 1) {
      return false;
    }
    if (!mapping[0].expr().operators(0).has_var() ||
        !mapping[1].expr().operators(0).has_var()) {
      return false;
    }
    if (!mapping[0].expr().operators(0).var().has_tag() ||
        !mapping[1].expr().operators(0).var().has_tag()) {
      return false;
    }
    common::Variable path_len_var0;
    common::Variable vertex_var;
    if (mapping[0].expr().operators(0).var().tag().id() == path_alias) {
      path_len_var0 = mapping[0].expr().operators(0).var();
      vertex_var = mapping[1].expr().operators(0).var();
      path_len_alias = mapping[0].alias().value();
      vertex_alias = mapping[1].alias().value();

    } else if (mapping[1].expr().operators(0).var().tag().id() == path_alias) {
      path_len_var0 = mapping[1].expr().operators(0).var();
      vertex_var = mapping[0].expr().operators(0).var();
      path_len_alias = mapping[1].alias().value();
      vertex_alias = mapping[0].alias().value();
    } else {
      return false;
    }
    if (!path_len_var0.has_property() || !path_len_var0.property().has_len()) {
      return false;
    }

    if (vertex_var.has_property()) {
      return false;
    }

    // must has order by limit
    if (!order_by_opr.order_by().has_limit()) {
      return false;
    }
    limit_upper = order_by_opr.order_by().limit().upper();
    if (order_by_opr.order_by().pairs_size() < 0) {
      return false;
    }
    if (!order_by_opr.order_by().pairs()[0].has_key()) {
      return false;
    }
    if (!order_by_opr.order_by().pairs()[0].key().has_tag()) {
      return false;
    }
    if (order_by_opr.order_by().pairs()[0].key().tag().id() != path_len_alias) {
      return false;
    }
    if (order_by_opr.order_by().pairs()[0].order() !=
        algebra::OrderBy_OrderingPair_Order::OrderBy_OrderingPair_Order_ASC) {
      return false;
    }
    return true;
  }
  return false;
}

static bool is_all_shortest_path(const physical::PhysicalPlan& plan, int i) {
  int opr_num = plan.plan_size();
  const auto& opr = plan.plan(i).opr();
  if (opr.path().path_opt() !=
          physical::PathExpand_PathOpt::PathExpand_PathOpt_ALL_SHORTEST ||
      opr.path().result_opt() !=
          physical::PathExpand_ResultOpt::PathExpand_ResultOpt_ALL_V_E) {
    return false;
  }

  if (i + 2 < opr_num) {
    const auto& get_v_opr = plan.plan(i + 1).opr();
    const auto& get_v_filter_opr = plan.plan(i + 2).opr();
    if (!get_v_filter_opr.has_vertex() || !get_v_opr.has_vertex()) {
      return false;
    }
    if (get_v_opr.vertex().opt() != physical::GetV::END) {
      return false;
    }
    if (get_v_filter_opr.vertex().opt() != physical::GetV::ITSELF) {
      return false;
    }

    int path_alias = opr.path().has_alias() ? opr.path().alias().value() : -1;
    int get_v_tag =
        get_v_opr.vertex().has_tag() ? get_v_opr.vertex().tag().value() : -1;
    int get_v_alias = get_v_opr.vertex().has_alias()
                          ? get_v_opr.vertex().alias().value()
                          : -1;
    if (path_alias != get_v_tag && get_v_tag != -1) {
      return false;
    }
    int get_v_filter_tag = get_v_filter_opr.vertex().has_tag()
                               ? get_v_filter_opr.vertex().tag().value()
                               : -1;
    if (get_v_filter_tag != get_v_alias && get_v_filter_tag != -1) {
      return false;
    }

    return true;
  }
  return false;
}

static bool is_shortest_path(const physical::PhysicalPlan& plan, int i) {
  int opr_num = plan.plan_size();
  const auto& opr = plan.plan(i).opr();
  // must be any shortest path
  if (opr.path().path_opt() !=
          physical::PathExpand_PathOpt::PathExpand_PathOpt_ANY_SHORTEST ||
      opr.path().result_opt() !=
          physical::PathExpand_ResultOpt::PathExpand_ResultOpt_ALL_V_E) {
    return false;
  }
  if (i + 2 < opr_num) {
    const auto& get_v_opr = plan.plan(i + 1).opr();
    const auto& get_v_filter_opr = plan.plan(i + 2).opr();
    if (!get_v_filter_opr.has_vertex() || !get_v_opr.has_vertex()) {
      return false;
    }
    if (get_v_opr.vertex().opt() != physical::GetV::END) {
      return false;
    }
    if (get_v_filter_opr.vertex().opt() != physical::GetV::ITSELF) {
      return false;
    }

    int path_alias = opr.path().has_alias() ? opr.path().alias().value() : -1;
    int get_v_tag =
        get_v_opr.vertex().has_tag() ? get_v_opr.vertex().tag().value() : -1;
    int get_v_alias = get_v_opr.vertex().has_alias()
                          ? get_v_opr.vertex().alias().value()
                          : -1;
    if (path_alias != get_v_tag && get_v_tag != -1) {
      return false;
    }
    int get_v_filter_tag = get_v_filter_opr.vertex().has_tag()
                               ? get_v_filter_opr.vertex().tag().value()
                               : -1;
    if (get_v_filter_tag != get_v_alias && get_v_filter_tag != -1) {
      return false;
    }

    return true;
  }
  return false;
}

class SPOrderByLimitOpr : public IReadOperator {
 public:
  SPOrderByLimitOpr(
      const ShortestPathParams& spp, int limit,
      std::function<std::unique_ptr<SPVertexPredicate>(
          const GraphReadInterface&, const std::map<std::string, std::string>&)>
          pred)
      : spp_(spp), limit_(limit), pred_(std::move(pred)) {}

  template <typename T>
  bl::result<gs::runtime::Context> _invoke(
      const GraphReadInterface& graph, Context&& ctx,
      std::unique_ptr<SPVertexPredicate>&& pred) {
    if (pred->type() == SPPredicateType::kPropertyEQ) {
      const auto& casted_pred =
          dynamic_cast<const VertexPropertyEQPredicateBeta<T>&>(*pred);
      return PathExpand::single_source_shortest_path_with_order_by_length_limit(
          graph, std::move(ctx), spp_, casted_pred, limit_);
    } else if (pred->type() == SPPredicateType::kPropertyLT) {
      const auto& casted_pred =
          dynamic_cast<const VertexPropertyLTPredicateBeta<T>&>(*pred);
      return PathExpand::single_source_shortest_path_with_order_by_length_limit(
          graph, std::move(ctx), spp_, casted_pred, limit_);
    } else if (pred->type() == SPPredicateType::kPropertyGT) {
      const auto& casted_pred =
          dynamic_cast<const VertexPropertyGTPredicateBeta<T>&>(*pred);
      return PathExpand::single_source_shortest_path_with_order_by_length_limit(
          graph, std::move(ctx), spp_, casted_pred, limit_);
    } else if (pred->type() == SPPredicateType::kPropertyLE) {
      const auto& casted_pred =
          dynamic_cast<const VertexPropertyLEPredicateBeta<T>&>(*pred);
      return PathExpand::single_source_shortest_path_with_order_by_length_limit(
          graph, std::move(ctx), spp_, casted_pred, limit_);
    } else if (pred->type() == SPPredicateType::kPropertyGE) {
      const auto& casted_pred =
          dynamic_cast<const VertexPropertyGEPredicateBeta<T>&>(*pred);
      return PathExpand::single_source_shortest_path_with_order_by_length_limit(
          graph, std::move(ctx), spp_, casted_pred, limit_);
    } else {
      LOG(ERROR) << "type not supported currently"
                 << static_cast<int>(pred->type());
      RETURN_UNSUPPORTED_ERROR("type not supported currently" +
                               std::to_string(static_cast<int>(pred->type())));
    }
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    auto sp_vertex_pred = pred_(graph, params);
    bl::result<gs::runtime::Context> ret;
    if (sp_vertex_pred->data_type() == RTAnyType::kStringValue) {
      ret = _invoke<std::string_view>(graph, std::move(ctx),
                                      std::move(sp_vertex_pred));
    } else if (sp_vertex_pred->data_type() == RTAnyType::kI32Value) {
      ret = _invoke<int32_t>(graph, std::move(ctx), std::move(sp_vertex_pred));
    } else if (sp_vertex_pred->data_type() == RTAnyType::kI64Value) {
      ret = _invoke<int64_t>(graph, std::move(ctx), std::move(sp_vertex_pred));
    } else if (sp_vertex_pred->data_type() == RTAnyType::kF64Value) {
      ret = _invoke<double>(graph, std::move(ctx), std::move(sp_vertex_pred));
    } else if (sp_vertex_pred->data_type() == RTAnyType::kTimestamp) {
      ret = _invoke<Date>(graph, std::move(ctx), std::move(sp_vertex_pred));
    } else {
      LOG(ERROR) << "type not supported currently"
                 << static_cast<int>(sp_vertex_pred->data_type());
      RETURN_UNSUPPORTED_ERROR(
          "type not supported currently" +
          std::to_string(static_cast<int>(sp_vertex_pred->data_type())));
    }
    return ret;
  }

 private:
  ShortestPathParams spp_;
  int limit_;
  std::function<std::unique_ptr<SPVertexPredicate>(
      const GraphReadInterface&, const std::map<std::string, std::string>&)>
      pred_;
};

class SPOrderByLimitWithOutPredOpr : public IReadOperator {
 public:
  SPOrderByLimitWithOutPredOpr(const ShortestPathParams& spp, int limit)
      : spp_(spp), limit_(limit) {}

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    return PathExpand::single_source_shortest_path_with_order_by_length_limit(
        graph, std::move(ctx), spp_, [](label_t, vid_t) { return true; },
        limit_);
  }

 private:
  ShortestPathParams spp_;
  int limit_;
};

class SPOrderByLimitWithGPredOpr : public IReadOperator {
 public:
  SPOrderByLimitWithGPredOpr(const ShortestPathParams& spp, int limit,
                             const common::Expression& pred)
      : spp_(spp), limit_(limit), pred_(pred) {}

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    Context tmp;
    auto v_pred =
        parse_expression(graph, tmp, params, pred_, VarType::kVertexVar);
    auto pred = [&v_pred](label_t label, vid_t vid) {
      return v_pred->eval_vertex(label, vid, 0).as_bool();
    };

    return PathExpand::single_source_shortest_path_with_order_by_length_limit(
        graph, std::move(ctx), spp_, pred, limit_);
  }

 private:
  ShortestPathParams spp_;
  int limit_;
  common::Expression pred_;
};

std::pair<std::unique_ptr<IReadOperator>, ContextMeta>
SPOrderByLimitOprBuilder::Build(const gs::Schema& schema,
                                const ContextMeta& ctx_meta,
                                const physical::PhysicalPlan& plan,
                                int op_idx) {
  const auto& opr = plan.plan(op_idx).opr().path();
  int path_len_alias = -1;
  int vertex_alias = -1;
  int limit_upper = -1;
  if (is_shortest_path_with_order_by_limit(plan, op_idx, path_len_alias,
                                           vertex_alias, limit_upper)) {
    ContextMeta ret_meta = ctx_meta;
    ret_meta.set(vertex_alias);
    ret_meta.set(path_len_alias);
    CHECK(opr.has_start_tag());
    int start_tag = opr.start_tag().value();
    CHECK(!opr.is_optional());
    ShortestPathParams spp;
    spp.start_tag = start_tag;
    spp.dir = parse_direction(opr.base().edge_expand().direction());
    spp.v_alias = vertex_alias;
    spp.alias = path_len_alias;
    spp.hop_lower = opr.hop_range().lower();
    spp.hop_upper = opr.hop_range().upper();
    spp.labels = parse_label_triplets(plan.plan(op_idx).meta_data(0));
    CHECK(spp.labels.size() == 1) << "only support one label triplet";
    const auto& get_v_opr = plan.plan(op_idx + 2).opr().vertex();
    if (get_v_opr.has_params() && get_v_opr.params().has_predicate()) {
      auto sp_vertex_pred =
          parse_special_vertex_predicate(get_v_opr.params().predicate());
      if (sp_vertex_pred.has_value()) {
        return std::make_pair(std::make_unique<SPOrderByLimitOpr>(
                                  spp, limit_upper, sp_vertex_pred.value()),
                              ret_meta);
      } else {
        return std::make_pair(
            std::make_unique<SPOrderByLimitWithGPredOpr>(
                spp, limit_upper, get_v_opr.params().predicate()),
            ret_meta);
      }
    } else {
      return std::make_pair(
          std::make_unique<SPOrderByLimitWithOutPredOpr>(spp, limit_upper),
          ret_meta);
    }
  } else {
    return std::make_pair(nullptr, ContextMeta());
  }
}

class SPSPredOpr : public IReadOperator {
 public:
  SPSPredOpr(
      const ShortestPathParams& spp,
      std::function<std::unique_ptr<SPVertexPredicate>(
          const GraphReadInterface&, const std::map<std::string, std::string>&)>
          pred)
      : spp_(spp), pred_(std::move(pred)) {}

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    auto sp_vertex_pred = pred_(graph, params);
    return PathExpand::
        single_source_shortest_path_with_special_vertex_predicate(
            graph, std::move(ctx), spp_, *sp_vertex_pred);
  }

 private:
  ShortestPathParams spp_;
  std::function<std::unique_ptr<SPVertexPredicate>(
      const GraphReadInterface&, const std::map<std::string, std::string>&)>
      pred_;
};

class SPGPredOpr : public IReadOperator {
 public:
  SPGPredOpr(const ShortestPathParams& spp, const common::Expression& pred)
      : spp_(spp), pred_(pred) {}

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    Context tmp;
    auto predicate =
        parse_expression(graph, tmp, params, pred_, VarType::kVertexVar);
    auto pred = [&predicate](label_t label, vid_t v) {
      return predicate->eval_vertex(label, v, 0).as_bool();
    };

    return PathExpand::single_source_shortest_path(graph, std::move(ctx), spp_,
                                                   pred);
  }

 private:
  ShortestPathParams spp_;
  common::Expression pred_;
};
class SPWithoutPredOpr : public IReadOperator {
 public:
  SPWithoutPredOpr(const ShortestPathParams& spp) : spp_(spp) {}

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    return PathExpand::single_source_shortest_path(
        graph, std::move(ctx), spp_, [](label_t, vid_t) { return true; });
  }

 private:
  ShortestPathParams spp_;
};

class ASPOpr : public IReadOperator {
 public:
  ASPOpr(const physical::PathExpand& opr,
         const physical::PhysicalOpr_MetaData& meta,
         const physical::GetV& get_v_opr, int v_alias) {
    CHECK(opr.has_start_tag());
    int start_tag = opr.start_tag().value();
    CHECK(!opr.is_optional());
    aspp_.start_tag = start_tag;
    aspp_.dir = parse_direction(opr.base().edge_expand().direction());
    aspp_.v_alias = v_alias;
    aspp_.alias = opr.has_alias() ? opr.alias().value() : -1;
    aspp_.hop_lower = opr.hop_range().lower();
    aspp_.hop_upper = opr.hop_range().upper();

    aspp_.labels = parse_label_triplets(meta);
    CHECK(aspp_.labels.size() == 1) << "only support one label triplet";
    CHECK(aspp_.labels[0].src_label == aspp_.labels[0].dst_label)
        << "only support same src and dst label";
    CHECK(get_v_opr.has_params() && get_v_opr.params().has_predicate());
    CHECK(is_pk_oid_exact_check(get_v_opr.params().predicate(), oid_getter_));
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    Any oid = oid_getter_(params);
    vid_t vid;
    CHECK(graph.GetVertexIndex(aspp_.labels[0].dst_label, oid, vid))
        << "vertex not found";
    auto v = std::make_pair(aspp_.labels[0].dst_label, vid);
    return PathExpand::all_shortest_paths_with_given_source_and_dest(
        graph, std::move(ctx), aspp_, v);
  }

 private:
  ShortestPathParams aspp_;
  std::function<Any(const std::map<std::string, std::string>&)> oid_getter_;
};

class SSSDSPOpr : public IReadOperator {
 public:
  SSSDSPOpr(const ShortestPathParams& spp,
            const std::function<Any(const std::map<std::string, std::string>&)>&
                oid_getter)
      : spp_(spp), oid_getter_(oid_getter) {}

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    Any vertex = oid_getter_(params);
    vid_t vid;
    CHECK(graph.GetVertexIndex(spp_.labels[0].dst_label, vertex, vid))
        << "vertex not found";
    auto v = std::make_pair(spp_.labels[0].dst_label, vid);

    return PathExpand::single_source_single_dest_shortest_path(
        graph, std::move(ctx), spp_, v);
  }

 private:
  ShortestPathParams spp_;
  std::function<Any(const std::map<std::string, std::string>&)> oid_getter_;
};
std::pair<std::unique_ptr<IReadOperator>, ContextMeta> SPOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  if (is_shortest_path(plan, op_idx)) {
    auto vertex = plan.plan(op_idx + 2).opr().vertex();
    int v_alias = -1;
    if (!vertex.has_alias()) {
      v_alias = plan.plan(op_idx + 1).opr().vertex().has_alias()
                    ? plan.plan(op_idx + 1).opr().vertex().alias().value()
                    : -1;
    } else {
      v_alias = vertex.alias().value();
    }
    int alias = -1;
    if (plan.plan(op_idx).opr().path().has_alias()) {
      alias = plan.plan(op_idx).opr().path().alias().value();
    }
    ret_meta.set(v_alias);
    ret_meta.set(alias);

    const auto& opr = plan.plan(op_idx).opr().path();
    CHECK(opr.has_start_tag());
    int start_tag = opr.start_tag().value();
    CHECK(!opr.is_optional());
    ShortestPathParams spp;
    spp.start_tag = start_tag;
    spp.dir = parse_direction(opr.base().edge_expand().direction());
    spp.v_alias = v_alias;
    spp.alias = alias;
    spp.hop_lower = opr.hop_range().lower();
    spp.hop_upper = opr.hop_range().upper();
    spp.labels = parse_label_triplets(plan.plan(op_idx).meta_data(0));
    CHECK(spp.labels.size() == 1) << "only support one label triplet";
    CHECK(spp.labels[0].src_label == spp.labels[0].dst_label)
        << "only support same src and dst label";
    std::function<Any(const std::map<std::string, std::string>&)> oid_getter;
    if (vertex.has_params() && vertex.params().has_predicate() &&
        is_pk_oid_exact_check(vertex.params().predicate(), oid_getter)) {
      return std::make_pair(std::make_unique<SSSDSPOpr>(spp, oid_getter),
                            ret_meta);
    } else {
      if (vertex.has_params() && vertex.params().has_predicate()) {
        auto sp_vertex_pred =
            parse_special_vertex_predicate(vertex.params().predicate());
        if (sp_vertex_pred.has_value()) {
          return std::make_pair(
              std::make_unique<SPSPredOpr>(spp, sp_vertex_pred.value()),
              ret_meta);
        } else {
          return std::make_pair(
              std::make_unique<SPGPredOpr>(spp, vertex.params().predicate()),
              ret_meta);
        }
      } else {
        return std::make_pair(std::make_unique<SPWithoutPredOpr>(spp),
                              ret_meta);
      }
    }
  } else if (is_all_shortest_path(plan, op_idx)) {
    auto vertex = plan.plan(op_idx + 2).opr().vertex();
    int v_alias = -1;
    if (!vertex.has_alias()) {
      v_alias = plan.plan(op_idx + 1).opr().vertex().has_alias()
                    ? plan.plan(op_idx + 1).opr().vertex().alias().value()
                    : -1;
    } else {
      v_alias = vertex.alias().value();
    }
    int alias = -1;
    if (plan.plan(op_idx).opr().path().has_alias()) {
      alias = plan.plan(op_idx).opr().path().alias().value();
    }
    ret_meta.set(v_alias);
    ret_meta.set(alias);

    return std::make_pair(std::make_unique<ASPOpr>(
                              plan.plan(op_idx).opr().path(),
                              plan.plan(op_idx).meta_data(0), vertex, v_alias),
                          ret_meta);
  } else {
    return std::make_pair(nullptr, ContextMeta());
  }
}

class PathExpandVOpr : public IReadOperator {
 public:
  PathExpandVOpr(const PathExpandParams& pep) : pep_(pep) {}

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    return PathExpand::edge_expand_v(graph, std::move(ctx), pep_);
  }

 private:
  PathExpandParams pep_;
};

std::pair<std::unique_ptr<IReadOperator>, ContextMeta>
PathExpandVOprBuilder::Build(const gs::Schema& schema,
                             const ContextMeta& ctx_meta,
                             const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr().path();
  const auto& next_opr = plan.plan(op_idx + 1).opr().vertex();
  if (opr.result_opt() ==
          physical::PathExpand_ResultOpt::PathExpand_ResultOpt_END_V &&
      opr.base().edge_expand().expand_opt() ==
          physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    int alias = -1;
    if (next_opr.has_alias()) {
      alias = next_opr.alias().value();
    }
    ContextMeta ret_meta = ctx_meta;
    ret_meta.set(alias);
    int start_tag = opr.has_start_tag() ? opr.start_tag().value() : -1;
    CHECK(opr.path_opt() ==
          physical::PathExpand_PathOpt::PathExpand_PathOpt_ARBITRARY);
    CHECK(!opr.is_optional());
    Direction dir = parse_direction(opr.base().edge_expand().direction());
    CHECK(!opr.base().edge_expand().is_optional());
    const algebra::QueryParams& query_params =
        opr.base().edge_expand().params();
    PathExpandParams pep;
    pep.alias = alias;
    pep.dir = dir;
    pep.hop_lower = opr.hop_range().lower();
    pep.hop_upper = opr.hop_range().upper();
    pep.start_tag = start_tag;
    pep.labels = parse_label_triplets(plan.plan(op_idx).meta_data(0));
    CHECK(opr.base().edge_expand().expand_opt() ==
          physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
    CHECK(!query_params.has_predicate());
    return std::make_pair(std::make_unique<PathExpandVOpr>(pep), ret_meta);
  } else {
    return std::make_pair(nullptr, ContextMeta());
  }
}

class PathExpandOpr : public IReadOperator {
 public:
  PathExpandOpr(PathExpandParams pep) : pep_(pep) {}

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    return PathExpand::edge_expand_p(graph, std::move(ctx), pep_);
  }

 private:
  PathExpandParams pep_;
};

std::pair<std::unique_ptr<IReadOperator>, ContextMeta>
PathExpandOprBuilder::Build(const gs::Schema& schema,
                            const ContextMeta& ctx_meta,
                            const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr().path();
  int alias = -1;
  if (opr.has_alias()) {
    alias = opr.alias().value();
  }
  ContextMeta ret_meta = ctx_meta;
  ret_meta.set(alias);

  CHECK(opr.has_start_tag());
  int start_tag = opr.start_tag().value();
  CHECK(opr.path_opt() ==
        physical::PathExpand_PathOpt::PathExpand_PathOpt_ARBITRARY);

  CHECK(!opr.is_optional());

  Direction dir = parse_direction(opr.base().edge_expand().direction());
  CHECK(!opr.base().edge_expand().is_optional());
  const algebra::QueryParams& query_params = opr.base().edge_expand().params();
  PathExpandParams pep;
  pep.alias = alias;
  pep.dir = dir;
  pep.hop_lower = opr.hop_range().lower();
  pep.hop_upper = opr.hop_range().upper();
  pep.start_tag = start_tag;
  pep.labels = parse_label_triplets(plan.plan(op_idx).meta_data(0));
  CHECK(!query_params.has_predicate());
  return std::make_pair(std::make_unique<PathExpandOpr>(pep), ret_meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
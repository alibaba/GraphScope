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
#include "flex/engines/graph_db/runtime/common/operators/retrieve/edge_expand.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/edge.h"
#include "flex/engines/graph_db/runtime/utils/utils.h"

namespace gs {
namespace runtime {
namespace ops {

template <typename T1, typename T2, typename T3>
class TCOpr : public IReadOperator {
 public:
  TCOpr(const physical::EdgeExpand& ee_opr0,
        const physical::EdgeExpand& ee_opr1, const physical::GetV& v_opr1,
        const physical::EdgeExpand& ee_opr2, const LabelTriplet& label0,
        const LabelTriplet& label1, const LabelTriplet& label2)
      : label0_(label0), label1_(label1), label2_(label2) {
    input_tag_ = ee_opr0.has_v_tag() ? ee_opr0.v_tag().value() : -1;
    dir0_ = parse_direction(ee_opr0.direction());
    dir1_ = parse_direction(ee_opr1.direction());
    dir2_ = parse_direction(ee_opr2.direction());
    alias1_ = -1;
    if (ee_opr1.has_alias()) {
      alias1_ = ee_opr1.alias().value();
    }
    if (v_opr1.has_alias()) {
      alias1_ = v_opr1.alias().value();
    }
    alias2_ = -1;
    if (ee_opr2.has_alias()) {
      alias2_ = ee_opr2.alias().value();
    }
    is_lt_ = ee_opr0.params().predicate().operators(1).logical() ==
             common::Logical::LT;
    auto val = ee_opr0.params().predicate().operators(2);
    param_name_ = val.param().name();
    if (dir0_ == Direction::kOut) {
      labels_[0] = std::make_tuple(label0_.src_label, label0_.dst_label,
                                   label0_.edge_label, dir0_);
    } else {
      labels_[0] = std::make_tuple(label0_.dst_label, label0_.src_label,
                                   label0_.edge_label, dir0_);
    }
    if (dir1_ == Direction::kOut) {
      labels_[1] = std::make_tuple(label1_.src_label, label1_.dst_label,
                                   label1_.edge_label, dir1_);
    } else {
      labels_[1] = std::make_tuple(label1_.dst_label, label1_.src_label,
                                   label1_.edge_label, dir1_);
    }
    if (dir2_ == Direction::kOut) {
      labels_[2] = std::make_tuple(label2_.src_label, label2_.dst_label,
                                   label2_.edge_label, dir2_);
    } else {
      labels_[2] = std::make_tuple(label2_.dst_label, label2_.src_label,
                                   label2_.edge_label, dir2_);
    }
  }

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    const std::string& param_value = params.at(param_name_);
    return EdgeExpand::tc<T1, T2, T3>(graph, std::move(ctx), labels_,
                                      input_tag_, alias1_, alias2_, is_lt_,
                                      param_value);
  }

 private:
  LabelTriplet label0_, label1_, label2_;
  Direction dir0_, dir1_, dir2_;
  int input_tag_;
  int alias1_;
  int alias2_;
  bool is_lt_;
  std::array<std::tuple<label_t, label_t, label_t, Direction>, 3> labels_;
  std::string param_name_;
};

bool tc_fusable(const physical::EdgeExpand& ee_opr0,
                const physical::GroupBy& group_by_opr,
                const physical::EdgeExpand& ee_opr1,
                const physical::GetV& v_opr1,
                const physical::EdgeExpand& ee_opr2,
                const algebra::Select& select_opr) {
  // ee_opr0
  if (ee_opr0.is_optional() || (!ee_opr0.has_v_tag()) ||
      (!ee_opr0.has_alias())) {
    return false;
  }
  // predicate
  if (!ee_opr0.params().has_predicate()) {
    return false;
  }

  auto sp_pred = parse_sp_pred(ee_opr0.params().predicate());
  if (sp_pred != SPPredicateType::kPropertyGT &&
      sp_pred != SPPredicateType::kPropertyLT) {
    return false;
  }
  auto op2 = ee_opr0.params().predicate().operators(2);
  if (op2.item_case() != common::ExprOpr::ItemCase::kParam) {
    return false;
  }

  int start_tag = ee_opr0.v_tag().value();
  auto dir0 = ee_opr0.direction();
  if (dir0 == physical::EdgeExpand_Direction::EdgeExpand_Direction_BOTH) {
    return false;
  }
  int alias0 = ee_opr0.alias().value();

  // group_by_opr
  if (group_by_opr.mappings_size() != 1 || group_by_opr.functions_size() != 1) {
    return false;
  }
  auto mapping = group_by_opr.mappings(0);
  if ((!mapping.has_key()) || mapping.key().tag().id() != start_tag) {
    return false;
  }
  int alias1 = mapping.alias().value();
  auto func = group_by_opr.functions(0);
  if (func.aggregate() != physical::GroupBy_AggFunc::TO_SET) {
    return false;
  }
  if (func.vars_size() != 1 || (!func.vars(0).has_tag()) ||
      func.vars(0).tag().id() != alias0 || func.vars(0).has_property()) {
    return false;
  }
  int alias2 = func.alias().value();

  // ee_opr1 and v_opr1
  if (ee_opr1.is_optional() || (!ee_opr1.has_v_tag()) ||
      ee_opr1.v_tag().value() != alias1 || ee_opr1.has_alias()) {
    return false;
  }

  if (ee_opr1.direction() ==
      physical::EdgeExpand_Direction::EdgeExpand_Direction_BOTH) {
    return false;
  }
  if (ee_opr1.params().has_predicate()) {
    return false;
  }
  // tag -1
  if (v_opr1.has_tag() || (!v_opr1.has_alias()) ||
      v_opr1.opt() != physical::GetV_VOpt::GetV_VOpt_ITSELF) {
    return false;
  }
  // int alias3 = v_opr1.alias().value();

  // ee_opr2, tag -1
  if (ee_opr2.is_optional() || (ee_opr2.has_v_tag()) ||
      (!ee_opr2.has_alias())) {
    return false;
  }
  if (ee_opr2.direction() ==
      physical::EdgeExpand_Direction::EdgeExpand_Direction_BOTH) {
    return false;
  }
  if (ee_opr2.params().has_predicate()) {
    return false;
  }

  int alias4 = ee_opr2.alias().value();

  // select_opr
  if (select_opr.predicate().operators_size() != 3) {
    return false;
  }
  auto& var = select_opr.predicate().operators(0);
  auto& within = select_opr.predicate().operators(1);
  auto& v_set = select_opr.predicate().operators(2);
  if ((!var.has_var()) || (!var.var().has_tag()) || var.var().has_property()) {
    return false;
  }
  if (var.var().tag().id() != alias4) {
    return false;
  }
  if (within.item_case() != common::ExprOpr::ItemCase::kLogical ||
      within.logical() != common::Logical::WITHIN) {
    return false;
  }

  if ((!v_set.has_var()) || (!v_set.var().has_tag()) ||
      v_set.var().has_property()) {
    return false;
  }

  int v_set_tag = v_set.var().tag().id();
  if (v_set_tag != alias2) {
    return false;
  }
  return true;
}

inline bool parse_edge_type(const Schema& schema, const LabelTriplet& label,
                            PropertyType& ep) {
  const auto& properties0 = schema.get_edge_properties(
      label.src_label, label.dst_label, label.edge_label);
  if (properties0.empty()) {
    ep = PropertyType::Empty();
    return true;
  } else {
    if (1 == properties0.size()) {
      ep = properties0[0];
      return true;
    }
  }
  return false;
}

template <typename T1, typename T2>
std::unique_ptr<IReadOperator> _make_tc_opr(
    const physical::EdgeExpand& ee_opr0, const physical::EdgeExpand& ee_opr1,
    const physical::GetV& v_opr1, const physical::EdgeExpand& ee_opr2,
    const LabelTriplet& label0, const LabelTriplet& label1,
    const LabelTriplet& label2, const std::array<PropertyType, 3>& eps) {
  if (eps[2] == PropertyType::Empty()) {
    return std::make_unique<TCOpr<T1, T2, grape::EmptyType>>(
        ee_opr0, ee_opr1, v_opr1, ee_opr2, label0, label1, label2);
  } else if (eps[2] == PropertyType::Date()) {
    return std::make_unique<TCOpr<T1, T2, Date>>(
        ee_opr0, ee_opr1, v_opr1, ee_opr2, label0, label1, label2);
  } else if (eps[2] == PropertyType::Int64()) {
    return std::make_unique<TCOpr<T1, T2, int64_t>>(
        ee_opr0, ee_opr1, v_opr1, ee_opr2, label0, label1, label2);
  }
  return nullptr;
}

std::unique_ptr<IReadOperator> make_tc_opr(
    const physical::EdgeExpand& ee_opr0, const physical::EdgeExpand& ee_opr1,
    const physical::GetV& v_opr1, const physical::EdgeExpand& ee_opr2,
    const LabelTriplet& label0, const LabelTriplet& label1,
    const LabelTriplet& label2, const std::array<PropertyType, 3>& eps) {
  if (eps[0] == PropertyType::Date()) {
    if (eps[1] == PropertyType::Date()) {
      return _make_tc_opr<Date, Date>(ee_opr0, ee_opr1, v_opr1, ee_opr2, label0,
                                      label1, label2, eps);
    } else if (eps[1] == PropertyType::Empty()) {
      return _make_tc_opr<Date, grape::EmptyType>(
          ee_opr0, ee_opr1, v_opr1, ee_opr2, label0, label1, label2, eps);
    }
  } else if (eps[0] == PropertyType::Int64()) {
    if (eps[1] == PropertyType::Date()) {
      return _make_tc_opr<int64_t, Date>(ee_opr0, ee_opr1, v_opr1, ee_opr2,
                                         label0, label1, label2, eps);
    } else if (eps[1] == PropertyType::Empty()) {
      return _make_tc_opr<int64_t, grape::EmptyType>(
          ee_opr0, ee_opr1, v_opr1, ee_opr2, label0, label1, label2, eps);
    }
  }
  return nullptr;
}
std::pair<std::unique_ptr<IReadOperator>, ContextMeta> TCOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
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
    auto labels0 = parse_label_triplets(plan.plan(op_idx).meta_data(0));
    auto labels1 = parse_label_triplets(plan.plan(op_idx + 2).meta_data(0));
    auto labels2 = parse_label_triplets(plan.plan(op_idx + 4).meta_data(0));

    if (labels0.size() != 1 || labels1.size() != 1 || labels2.size() != 1) {
      return std::make_pair(nullptr, ContextMeta());
    }
    std::array<PropertyType, 3> eps;
    if (!parse_edge_type(schema, labels0[0], eps[0]) ||
        !parse_edge_type(schema, labels1[0], eps[1]) ||
        !parse_edge_type(schema, labels2[0], eps[2])) {
      return std::make_pair(nullptr, ContextMeta());
    }
    auto opr = make_tc_opr(plan.plan(op_idx).opr().edge(),
                           plan.plan(op_idx + 2).opr().edge(),
                           plan.plan(op_idx + 3).opr().vertex(),
                           plan.plan(op_idx + 4).opr().edge(), labels0[0],
                           labels1[0], labels2[0], eps);
    if (opr == nullptr) {
      return std::make_pair(nullptr, ContextMeta());
    }
    ContextMeta meta = ctx_meta;
    meta.set(alias1);
    meta.set(alias2);

    return std::make_pair(std::move(opr), meta);
  } else {
    return std::make_pair(nullptr, ContextMeta());
  }
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
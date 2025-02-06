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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/edge.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/edge_expand.h"
#include "flex/engines/graph_db/runtime/utils/predicates.h"
#include "flex/engines/graph_db/runtime/utils/utils.h"

namespace gs {
namespace runtime {
namespace ops {

bool edge_expand_get_v_fusable(const physical::EdgeExpand& ee_opr,
                               const physical::GetV& v_opr,
                               const physical::PhysicalOpr_MetaData& meta) {
  if (ee_opr.expand_opt() !=
          physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE &&
      ee_opr.expand_opt() !=
          physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    return false;
  }
  if (ee_opr.params().has_predicate()) {
    return false;
  }
  int alias = -1;
  if (ee_opr.has_alias()) {
    alias = ee_opr.alias().value();
  }
  if (alias != -1) {
    return false;
  }

  int tag = -1;
  if (v_opr.has_tag()) {
    tag = v_opr.tag().value();
  }
  if (tag != -1) {
    return false;
  }

  Direction dir = parse_direction(ee_opr.direction());
  if (ee_opr.expand_opt() ==
      physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    if (v_opr.opt() == physical::GetV_VOpt::GetV_VOpt_ITSELF) {
      return true;
    }
  } else if (ee_opr.expand_opt() ==
             physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
    if (dir == Direction::kOut &&
        v_opr.opt() == physical::GetV_VOpt::GetV_VOpt_END) {
      return true;
    }
    if (dir == Direction::kIn &&
        v_opr.opt() == physical::GetV_VOpt::GetV_VOpt_START) {
      return true;
    }
  }
  return false;
}

struct VertexPredicateWrapper {
  VertexPredicateWrapper(const GeneralVertexPredicate& pred) : pred_(pred) {}
  inline bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, Direction dir,
                         size_t path_idx) const {
    if (dir == Direction::kOut) {
      return pred_(label.dst_label, dst, path_idx);
    } else {
      return pred_(label.src_label, src, path_idx);
    }
  }
  const GeneralVertexPredicate& pred_;
};

struct VertexEdgePredicateWrapper {
  VertexEdgePredicateWrapper(const GeneralVertexPredicate& v_pred,
                             const GeneralEdgePredicate& e_pred)
      : v_pred_(v_pred), e_pred_(e_pred) {}

  inline bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, Direction dir,
                         size_t path_idx) const {
    if (dir == Direction::kOut) {
      return v_pred_(label.dst_label, dst, path_idx) &&
             e_pred_(label, src, dst, edata, dir, path_idx);
    } else {
      return v_pred_(label.src_label, src, path_idx) &&
             e_pred_(label, src, dst, edata, dir, path_idx);
    }
  }

  const GeneralVertexPredicate& v_pred_;
  const GeneralEdgePredicate& e_pred_;
};

struct ExactVertexEdgePredicateWrapper {
  ExactVertexEdgePredicateWrapper(const ExactVertexPredicate& v_pred,
                                  const GeneralEdgePredicate& e_pred)
      : v_pred_(v_pred), e_pred_(e_pred) {}

  inline bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, Direction dir,
                         size_t path_idx) const {
    if (dir == Direction::kOut) {
      return v_pred_(label.dst_label, dst, path_idx) &&
             e_pred_(label, src, dst, edata, dir, path_idx);
    } else {
      return v_pred_(label.src_label, src, path_idx) &&
             e_pred_(label, src, dst, edata, dir, path_idx);
    }
  }

  const ExactVertexPredicate& v_pred_;
  const GeneralEdgePredicate& e_pred_;
};

struct ExactVertexPredicateWrapper {
  ExactVertexPredicateWrapper(const ExactVertexPredicate& pred) : pred_(pred) {}

  inline bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& edata, Direction dir,
                         size_t path_idx) const {
    if (dir == Direction::kOut) {
      return pred_(label.dst_label, dst, path_idx);
    } else {
      return pred_(label.src_label, src, path_idx);
    }
  }

  const ExactVertexPredicate& pred_;
};

class EdgeExpandVWithoutPredOpr : public IReadOperator {
 public:
  EdgeExpandVWithoutPredOpr(const EdgeExpandParams& eep) : eep_(eep) {}

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    return EdgeExpand::expand_vertex_without_predicate(graph, std::move(ctx),
                                                       eep_);
  }

  std::string get_operator_name() const override {
    return "EdgeExpandVWithoutPredOpr";
  }

 private:
  EdgeExpandParams eep_;
};

class EdgeExpandVWithEPGTOpr : public IReadOperator {
 public:
  EdgeExpandVWithEPGTOpr(const EdgeExpandParams& eep, const std::string& param,
                         const common::Expression& pred)
      : eep_(eep), param_(param), pred_(pred) {}

  std::string get_operator_name() const override {
    return "EdgeExpandVWithEPGTOpr";
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    std::string param_value = params.at(param_);
    auto ret = EdgeExpand::expand_vertex_ep_gt(graph, std::move(ctx), eep_,
                                               param_value);
    if (ret) {
      return ret.value();
    }
    GeneralEdgePredicate pred(graph, ctx, params, pred_);
    return EdgeExpand::expand_vertex<GeneralEdgePredicate>(
        graph, std::move(ctx), eep_, pred);
  }

 private:
  EdgeExpandParams eep_;
  std::string param_;
  common::Expression pred_;
};

class EdgeExpandVWithEPLTOpr : public IReadOperator {
 public:
  EdgeExpandVWithEPLTOpr(const EdgeExpandParams& eep, const std::string& param,
                         const common::Expression& pred)
      : eep_(eep), param_(param), pred_(pred) {}

  std::string get_operator_name() const override {
    return "EdgeExpandVWithEPLTOpr";
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    std::string param_value = params.at(param_);
    auto ret = EdgeExpand::expand_vertex_ep_lt(graph, std::move(ctx), eep_,
                                               param_value);
    if (ret) {
      return ret.value();
    }
    GeneralEdgePredicate pred(graph, ctx, params, pred_);
    return EdgeExpand::expand_vertex<GeneralEdgePredicate>(
        graph, std::move(ctx), eep_, pred);
  }

 private:
  EdgeExpandParams eep_;
  std::string param_;
  common::Expression pred_;
};

class EdgeExpandVWithEdgePredOpr : public IReadOperator {
 public:
  EdgeExpandVWithEdgePredOpr(const EdgeExpandParams& eep,
                             const common::Expression& pred)
      : eep_(eep), pred_(pred) {}

  std::string get_operator_name() const override {
    return "EdgeExpandVWithEdgePredOpr";
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    GeneralEdgePredicate pred(graph, ctx, params, pred_);
    return EdgeExpand::expand_vertex<GeneralEdgePredicate>(
        graph, std::move(ctx), eep_, pred);
  }

 private:
  EdgeExpandParams eep_;
  common::Expression pred_;
};

class EdgeExpandEWithoutPredicateOpr : public IReadOperator {
 public:
  EdgeExpandEWithoutPredicateOpr(const EdgeExpandParams& eep) : eep_(eep) {}

  std::string get_operator_name() const override {
    return "EdgeExpandEWithoutPredicateOpr";
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    return EdgeExpand::expand_edge_without_predicate(graph, std::move(ctx),
                                                     eep_, timer);
  }

 private:
  EdgeExpandParams eep_;
};

class EdgeExpandEWithSPredOpr : public IReadOperator {
 public:
  EdgeExpandEWithSPredOpr(
      const EdgeExpandParams& eep,
      const std::function<std::unique_ptr<SPEdgePredicate>(
          const GraphReadInterface&,
          const std::map<std::string, std::string>&)>& sp_edge_pred,
      const common::Expression& pred)
      : eep_(eep), sp_edge_pred_(sp_edge_pred), pred_(pred) {}

  std::string get_operator_name() const override {
    return "EdgeExpandEWithSPredOpr";
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    auto pred = sp_edge_pred_(graph, params);
    if (pred) {
      auto ret = EdgeExpand::expand_edge_with_special_edge_predicate(
          graph, std::move(ctx), eep_, *pred);
      if (ret) {
        return ret.value();
      }
    }
    GeneralEdgePredicate gpred(graph, ctx, params, pred_);
    return EdgeExpand::expand_edge<GeneralEdgePredicate>(graph, std::move(ctx),
                                                         eep_, gpred);
  }

 private:
  EdgeExpandParams eep_;
  std::function<std::unique_ptr<SPEdgePredicate>(
      const GraphReadInterface&, const std::map<std::string, std::string>&)>
      sp_edge_pred_;
  common::Expression pred_;
};

class EdgeExpandEWithGPredOpr : public IReadOperator {
 public:
  EdgeExpandEWithGPredOpr(const EdgeExpandParams& eep,
                          const common::Expression& pred)
      : eep_(eep), pred_(pred) {}

  std::string get_operator_name() const override {
    return "EdgeExpandEWithGPredOpr";
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    GeneralEdgePredicate pred(graph, ctx, params, pred_);
    return EdgeExpand::expand_edge<GeneralEdgePredicate>(graph, std::move(ctx),
                                                         eep_, pred);
  }

 private:
  EdgeExpandParams eep_;
  common::Expression pred_;
};

class EdgeExpandVWithExactVertexOpr : public IReadOperator {
 public:
  EdgeExpandVWithExactVertexOpr(const EdgeExpandParams& eep, label_t pk_label,
                                const std::string& pk,
                                const algebra::QueryParams& query_params)
      : eep_(eep), pk_label_(pk_label), pk_(pk), query_params_(query_params) {}

  std::string get_operator_name() const override {
    return "EdgeExpandVWithExactVertexOpr";
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    std::string param_value = params.at(pk_);
    int64_t oid = std::stoll(param_value);
    vid_t vid = std::numeric_limits<vid_t>::max();
    if (!graph.GetVertexIndex(pk_label_, oid, vid)) {
      LOG(ERROR) << "vertex not found with label " << pk_label_ << " and oid "
                 << oid;
      RETURN_UNSUPPORTED_ERROR("vertex not found with label " +
                               std::to_string(static_cast<int>(pk_label_)) +
                               " and oid " + std::to_string(oid));
    }
    ExactVertexPredicate v_pred(pk_label_, vid);
    if (query_params_.has_predicate()) {
      GeneralEdgePredicate e_pred(graph, ctx, params,
                                  query_params_.predicate());
      ExactVertexEdgePredicateWrapper ve_pred(v_pred, e_pred);

      return EdgeExpand::expand_vertex<ExactVertexEdgePredicateWrapper>(
          graph, std::move(ctx), eep_, ve_pred);
    } else {
      return EdgeExpand::expand_vertex<ExactVertexPredicateWrapper>(
          graph, std::move(ctx), eep_, v_pred);
    }
  }

 private:
  EdgeExpandParams eep_;
  label_t pk_label_;
  std::string pk_;
  algebra::QueryParams query_params_;
};

class EdgeExpandVWithVertexEdgePredOpr : public IReadOperator {
 public:
  EdgeExpandVWithVertexEdgePredOpr(const EdgeExpandParams& eep,
                                   const common::Expression& v_pred,
                                   const common::Expression& e_pred)
      : eep_(eep), v_pred_(v_pred), e_pred_(e_pred) {}

  std::string get_operator_name() const override {
    return "EdgeExpandVWithVertexEdgePredOpr";
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    GeneralVertexPredicate v_pred(graph, ctx, params, v_pred_);
    GeneralEdgePredicate e_pred(graph, ctx, params, e_pred_);
    VertexEdgePredicateWrapper ve_pred(v_pred, e_pred);
    auto ret = EdgeExpand::expand_vertex<VertexEdgePredicateWrapper>(
        graph, std::move(ctx), eep_, ve_pred);
    return ret;
  }

 private:
  EdgeExpandParams eep_;
  common::Expression v_pred_;
  common::Expression e_pred_;
};

class EdgeExpandVWithSPVertexPredOpr : public IReadOperator {
 public:
  EdgeExpandVWithSPVertexPredOpr(
      const EdgeExpandParams& eep,
      const std::function<std::unique_ptr<SPVertexPredicate>(
          const GraphReadInterface&,
          const std::map<std::string, std::string>&)>& sp_vertex_pred,
      const common::Expression& pred)
      : eep_(eep), sp_vertex_pred_(sp_vertex_pred), pred_(pred) {}

  std::string get_operator_name() const override {
    return "EdgeExpandVWithSPVertexPredOpr";
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    auto pred = sp_vertex_pred_(graph, params);
    if (pred) {
      auto ret = EdgeExpand::expand_vertex_with_special_vertex_predicate(
          graph, std::move(ctx), eep_, *pred);
      if (ret) {
        return ret.value();
      }
    }
    GeneralVertexPredicate v_pred(graph, ctx, params, pred_);
    VertexPredicateWrapper vpred(v_pred);
    return EdgeExpand::expand_vertex<VertexPredicateWrapper>(
        graph, std::move(ctx), eep_, v_pred);
  }

 private:
  EdgeExpandParams eep_;
  std::function<std::unique_ptr<SPVertexPredicate>(
      const GraphReadInterface&, const std::map<std::string, std::string>&)>
      sp_vertex_pred_;
  common::Expression pred_;
};

class EdgeExpandVWithGPVertexPredOpr : public IReadOperator {
 public:
  EdgeExpandVWithGPVertexPredOpr(const EdgeExpandParams& eep,
                                 const common::Expression& pred)
      : eep_(eep), pred_(pred) {}
  std::string get_operator_name() const override {
    return "EdgeExpandVWithGPVertexPredOpr";
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    GeneralVertexPredicate v_pred(graph, ctx, params, pred_);
    VertexPredicateWrapper vpred(v_pred);
    return EdgeExpand::expand_vertex<VertexPredicateWrapper>(
        graph, std::move(ctx), eep_, v_pred);
  }

 private:
  EdgeExpandParams eep_;
  common::Expression pred_;
};

static bool check_label_in_set(const Direction& dir,
                               const std::vector<LabelTriplet>& edge_labels,
                               const std::set<label_t>& labels_set) {
  bool within = true;
  if (dir == Direction::kOut) {
    for (auto& triplet : edge_labels) {
      if (labels_set.find(triplet.dst_label) == labels_set.end()) {
        within = false;
        break;
      }
    }
  } else if (dir == Direction::kIn) {
    for (auto& triplet : edge_labels) {
      if (labels_set.find(triplet.src_label) == labels_set.end()) {
        within = false;
        break;
      }
    }
  } else {
    for (auto& triplet : edge_labels) {
      if (labels_set.find(triplet.dst_label) == labels_set.end()) {
        within = false;
        break;
      }
      if (labels_set.find(triplet.src_label) == labels_set.end()) {
        within = false;
        break;
      }
    }
  }
  return within;
}

bl::result<ReadOpBuildResultT> EdgeExpandOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  int alias = -1;
  if (plan.plan(op_idx).opr().edge().has_alias()) {
    alias = plan.plan(op_idx).opr().edge().alias().value();
  }
  ContextMeta meta = ctx_meta;
  meta.set(alias);
  auto opr = plan.plan(op_idx).opr().edge();
  int v_tag = opr.has_v_tag() ? opr.v_tag().value() : -1;
  Direction dir = parse_direction(opr.direction());
  bool is_optional = opr.is_optional();
  if (!opr.has_params()) {
    LOG(ERROR) << "EdgeExpandOprBuilder::Build: query_params is empty";
    return std::make_pair(nullptr, ContextMeta());
  }
  const auto& query_params = opr.params();
  EdgeExpandParams eep;
  eep.v_tag = v_tag;
  eep.labels = parse_label_triplets(plan.plan(op_idx).meta_data(0));
  eep.dir = dir;
  eep.alias = alias;
  eep.is_optional = is_optional;
  if (opr.expand_opt() == physical::EdgeExpand_ExpandOpt_VERTEX) {
    if (query_params.has_predicate()) {
      if (parse_sp_pred(query_params.predicate()) ==
          SPPredicateType::kPropertyGT) {
        std::string param_name =
            query_params.predicate().operators(2).param().name();
        return std::make_pair(std::make_unique<EdgeExpandVWithEPGTOpr>(
                                  eep, param_name, query_params.predicate()),
                              meta);
      } else if (parse_sp_pred(query_params.predicate()) ==
                 SPPredicateType::kPropertyLT) {
        std::string param_name =
            query_params.predicate().operators(2).param().name();
        return std::make_pair(std::make_unique<EdgeExpandVWithEPLTOpr>(
                                  eep, param_name, query_params.predicate()),
                              meta);
      } else {
        return std::make_pair(std::make_unique<EdgeExpandVWithEdgePredOpr>(
                                  eep, query_params.predicate()),
                              meta);
      }
    } else {
      return std::make_pair(std::make_unique<EdgeExpandVWithoutPredOpr>(eep),
                            meta);
    }
  } else if (opr.expand_opt() == physical::EdgeExpand_ExpandOpt_EDGE) {
    if (query_params.has_predicate()) {
      auto sp_edge_pred =
          parse_special_edge_predicate(query_params.predicate());
      if (sp_edge_pred.has_value()) {
        return std::make_pair(
            std::make_unique<EdgeExpandEWithSPredOpr>(eep, sp_edge_pred.value(),
                                                      query_params.predicate()),
            meta);
      } else {
        return std::make_pair(std::make_unique<EdgeExpandEWithGPredOpr>(
                                  eep, query_params.predicate()),
                              meta);
      }
    } else {
      return std::make_pair(
          std::make_unique<EdgeExpandEWithoutPredicateOpr>(eep), meta);
    }
  }
  return std::make_pair(nullptr, ContextMeta());
}

bl::result<ReadOpBuildResultT> EdgeExpandGetVOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  if (edge_expand_get_v_fusable(plan.plan(op_idx).opr().edge(),
                                plan.plan(op_idx + 1).opr().vertex(),
                                plan.plan(op_idx).meta_data(0))) {
    int alias = -1;
    if (plan.plan(op_idx + 1).opr().vertex().has_alias()) {
      alias = plan.plan(op_idx + 1).opr().vertex().alias().value();
    }
    ContextMeta meta = ctx_meta;
    meta.set(alias);
    const auto& ee_opr = plan.plan(op_idx).opr().edge();
    const auto& v_opr = plan.plan(op_idx + 1).opr().vertex();
    int v_tag = ee_opr.has_v_tag() ? ee_opr.v_tag().value() : -1;
    Direction dir = parse_direction(ee_opr.direction());
    bool is_optional = ee_opr.is_optional();
    if (!ee_opr.has_params()) {
      LOG(ERROR) << "EdgeExpandGetVOprBuilder::Build: query_params is empty"
                 << ee_opr.DebugString();
      return std::make_pair(nullptr, ContextMeta());
    }
    const auto& query_params = ee_opr.params();
    if (ee_opr.expand_opt() !=
            physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_EDGE &&
        ee_opr.expand_opt() !=
            physical::EdgeExpand_ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
      LOG(ERROR) << "EdgeExpandGetVOprBuilder::Build: expand_opt is not EDGE "
                    "or VERTEX"
                 << ee_opr.DebugString();
      return std::make_pair(nullptr, ContextMeta());
    }
    if (query_params.has_predicate()) {
      LOG(ERROR)
          << "EdgeExpandGetVOprBuilder::Build: query_params has predicate"
          << query_params.predicate().DebugString();
      return std::make_pair(nullptr, ContextMeta());
    }

    EdgeExpandParams eep;
    eep.v_tag = v_tag;
    eep.labels = parse_label_triplets(plan.plan(op_idx).meta_data(0));
    eep.dir = dir;
    eep.alias = alias;
    eep.is_optional = is_optional;
    if (!v_opr.params().has_predicate()) {
      if (query_params.has_predicate()) {
        return std::make_pair(std::make_unique<EdgeExpandVWithEdgePredOpr>(
                                  eep, query_params.predicate()),
                              meta);
      } else {
        return std::make_pair(std::make_unique<EdgeExpandVWithoutPredOpr>(eep),
                              meta);
      }
    }

    if (parse_sp_pred(v_opr.params().predicate()) == SPPredicateType::kWithIn) {
      auto property = v_opr.params().predicate().operators(0);
      if (property.has_var() && property.var().has_property() &&
          property.var().property().has_label()) {
        auto labels = v_opr.params().predicate().operators(2);
        // label with in
        if (labels.has_const_() && labels.const_().has_i64_array()) {
          std::set<label_t> labels_set;
          const auto& label_array = labels.const_().i64_array();
          size_t num = label_array.item_size();
          for (size_t i = 0; i < num; ++i) {
            labels_set.insert(label_array.item(i));
          }
          if (check_label_in_set(dir, eep.labels, labels_set)) {
            if (query_params.has_predicate()) {
              return std::make_pair(
                  std::make_unique<EdgeExpandVWithEdgePredOpr>(
                      eep, query_params.predicate()),
                  meta);
            } else {
              return std::make_pair(
                  std::make_unique<EdgeExpandVWithoutPredOpr>(eep), meta);
            }
          } else {
            if (query_params.has_predicate()) {
              return std::make_pair(
                  std::make_unique<EdgeExpandVWithVertexEdgePredOpr>(
                      eep, v_opr.params().predicate(),
                      query_params.predicate()),
                  meta);
            } else {
              return std::make_pair(
                  std::make_unique<EdgeExpandVWithGPVertexPredOpr>(
                      eep, v_opr.params().predicate()),
                  meta);
            }
          }
        }
      }
    }

    // Exact vertex predicate
    label_t exact_pk_label;
    std::string pk_name;
    if (is_pk_exact_check(schema, v_opr.params().predicate(), exact_pk_label,
                          pk_name)) {
      return std::make_pair(std::make_unique<EdgeExpandVWithExactVertexOpr>(
                                eep, exact_pk_label, pk_name, v_opr.params()),
                            meta);
    }

    if (query_params.has_predicate()) {
      return std::make_pair(
          std::make_unique<EdgeExpandVWithVertexEdgePredOpr>(
              eep, v_opr.params().predicate(), query_params.predicate()),
          meta);
    } else {
      auto sp_vertex_pred =
          parse_special_vertex_predicate(v_opr.params().predicate());
      if (sp_vertex_pred.has_value()) {
        return std::make_pair(
            std::make_unique<EdgeExpandVWithSPVertexPredOpr>(
                eep, *sp_vertex_pred, v_opr.params().predicate()),
            meta);
      } else {
        return std::make_pair(std::make_unique<EdgeExpandVWithGPVertexPredOpr>(
                                  eep, v_opr.params().predicate()),
                              meta);
      }
    }
  }
  return std::make_pair(nullptr, ContextMeta());
}
}  // namespace ops

}  // namespace runtime
}  // namespace gs
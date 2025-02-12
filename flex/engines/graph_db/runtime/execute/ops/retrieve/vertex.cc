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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/vertex.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/get_v.h"
#include "flex/engines/graph_db/runtime/utils/predicates.h"
#include "flex/engines/graph_db/runtime/utils/special_predicates.h"
#include "flex/engines/graph_db/runtime/utils/utils.h"

namespace gs {
namespace runtime {
namespace ops {

class GetVFromVerticesWithLabelWithInOpr : public IReadOperator {
 public:
  GetVFromVerticesWithLabelWithInOpr(const physical::GetV& opr,
                                     const GetVParams& p,
                                     const std::set<label_t>& labels)
      : opr_(opr), v_params_(p), labels_set_(labels) {}

  std::string get_operator_name() const override {
    return "GetVFromVerticesWithLabelWithInOpr";
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    auto input_vertex_list_ptr =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(v_params_.tag));
    CHECK(input_vertex_list_ptr) << ctx.get(v_params_.tag)->column_info();
    bool flag = true;
    for (auto label : input_vertex_list_ptr->get_labels_set()) {
      if (labels_set_.find(label) == labels_set_.end()) {
        flag = false;
        break;
      }
    }
    if (v_params_.tag == -1 && flag) {
      ctx.set(v_params_.alias, input_vertex_list_ptr);
      return ctx;
    } else {
      GeneralVertexPredicate pred(graph, ctx, params,
                                  opr_.params().predicate());
      return GetV::get_vertex_from_vertices(graph, std::move(ctx), v_params_,
                                            pred);
    }
  }

 private:
  physical::GetV opr_;
  GetVParams v_params_;
  std::set<label_t> labels_set_;
};

class GetVFromVerticesWithPKExactOpr : public IReadOperator {
 public:
  GetVFromVerticesWithPKExactOpr(const physical::GetV& opr, const GetVParams& p,
                                 label_t exact_pk_label,
                                 const std::string& exact_pk)
      : opr_(opr),
        v_params_(p),
        exact_pk_label_(exact_pk_label),
        exact_pk_(exact_pk) {}

  std::string get_operator_name() const override {
    return "GetVFromVerticesWithPKExact";
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    int64_t pk = std::stoll(params.at(exact_pk_));
    vid_t index = std::numeric_limits<vid_t>::max();
    graph.GetVertexIndex(exact_pk_label_, pk, index);
    ExactVertexPredicate pred(exact_pk_label_, index);
    return GetV::get_vertex_from_vertices(graph, std::move(ctx), v_params_,
                                          pred);
  }

 private:
  physical::GetV opr_;
  GetVParams v_params_;
  label_t exact_pk_label_;
  std::string exact_pk_;
};

class GetVFromVerticesWithPredicateOpr : public IReadOperator {
 public:
  GetVFromVerticesWithPredicateOpr(const physical::GetV& opr,
                                   const GetVParams& p)
      : opr_(opr), v_params_(p) {}

  std::string get_operator_name() const override {
    return "GetVFromVerticesWithPredicate";
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    GeneralVertexPredicate pred(graph, ctx, params, opr_.params().predicate());
    return GetV::get_vertex_from_vertices(graph, std::move(ctx), v_params_,
                                          pred);
  }

 private:
  physical::GetV opr_;
  GetVParams v_params_;
};

class GetVFromEdgesWithPredicateOpr : public IReadOperator {
 public:
  GetVFromEdgesWithPredicateOpr(const physical::GetV& opr, const GetVParams& p)
      : opr_(opr), v_params_(p) {}

  std::string get_operator_name() const override {
    return "GetVFromEdgesWithPredicate";
  }

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    if (opr_.params().has_predicate()) {
      GeneralVertexPredicate pred(graph, ctx, params,
                                  opr_.params().predicate());
      return GetV::get_vertex_from_edges(graph, std::move(ctx), v_params_,
                                         pred);
    } else {
      return GetV::get_vertex_from_edges(graph, std::move(ctx), v_params_,
                                         DummyVertexPredicate());
    }
  }

 private:
  physical::GetV opr_;
  GetVParams v_params_;
};

bl::result<ReadOpBuildResultT> VertexOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& vertex = plan.plan(op_idx).opr().vertex();

  int alias = -1;
  if (vertex.has_alias()) {
    alias = plan.plan(op_idx).opr().vertex().alias().value();
  }

  ContextMeta ret_meta = ctx_meta;
  ret_meta.set(alias);

  int tag = -1;
  if (vertex.has_tag()) {
    tag = vertex.tag().value();
  }
  VOpt opt = parse_opt(vertex.opt());

  if (!vertex.has_params()) {
    LOG(ERROR) << "GetV should have params" << vertex.DebugString();
    return std::make_pair(nullptr, ContextMeta());
  }
  GetVParams p;
  p.opt = opt;
  p.tag = tag;
  p.tables = parse_tables(vertex.params());
  p.alias = alias;

  if (vertex.params().has_predicate()) {
    if (opt == VOpt::kItself) {
      // label within predicate
      {
        std::set<label_t> labels_set;
        if (is_label_within_predicate(vertex.params().predicate(),
                                      labels_set)) {
          return std::make_pair(
              std::make_unique<GetVFromVerticesWithLabelWithInOpr>(
                  plan.plan(op_idx).opr().vertex(), p, labels_set),
              ctx_meta);
        }
      }

      // pk exact check
      {
        label_t exact_pk_label;
        std::string exact_pk;
        if (is_pk_exact_check(schema, vertex.params().predicate(),
                              exact_pk_label, exact_pk)) {
          return std::make_pair(
              std::make_unique<GetVFromVerticesWithPKExactOpr>(
                  plan.plan(op_idx).opr().vertex(), p, exact_pk_label,
                  exact_pk),
              ctx_meta);
        }
      }
      // general predicate
      return std::make_pair(std::make_unique<GetVFromVerticesWithPredicateOpr>(
                                plan.plan(op_idx).opr().vertex(), p),
                            ctx_meta);
    } else if (opt == VOpt::kEnd || opt == VOpt::kStart) {
      return std::make_pair(std::make_unique<GetVFromEdgesWithPredicateOpr>(
                                plan.plan(op_idx).opr().vertex(), p),
                            ctx_meta);
    }
  } else {
    if (opt == VOpt::kEnd || opt == VOpt::kStart || opt == VOpt::kOther) {
      return std::make_pair(std::make_unique<GetVFromEdgesWithPredicateOpr>(
                                plan.plan(op_idx).opr().vertex(), p),
                            ctx_meta);
    }
  }

  LOG(ERROR) << "not support" << vertex.DebugString();
  return std::make_pair(nullptr, ContextMeta());
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs
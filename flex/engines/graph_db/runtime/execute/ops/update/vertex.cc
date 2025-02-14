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

#include "flex/engines/graph_db/runtime/execute/ops/update/vertex.h"
#include "flex/engines/graph_db/runtime/common/operators/update/get_v.h"
#include "flex/engines/graph_db/runtime/utils/expr_impl.h"
#include "flex/engines/graph_db/runtime/utils/params.h"
#include "flex/engines/graph_db/runtime/utils/utils.h"

namespace gs {
namespace runtime {
namespace ops {

class UGetVFromEdgeOpr : public IUpdateOperator {
 public:
  UGetVFromEdgeOpr(const GetVParams& params) : params_(params) {}
  ~UGetVFromEdgeOpr() = default;

  std::string get_operator_name() const override { return "UGetVFromEdgeOpr"; }

  bl::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer& timer) override {
    return UGetV::get_vertex_from_edge(
        graph, std::move(ctx), params_,
        [&](size_t idx, label_t label, vid_t vid) { return true; });
  }

 private:
  GetVParams params_;
};

class UGetVFromEdgeWithPredOpr : public IUpdateOperator {
 public:
  UGetVFromEdgeWithPredOpr(const GetVParams& params,
                           const common::Expression& expr)
      : params_(params), expr_(expr) {}
  ~UGetVFromEdgeWithPredOpr() = default;

  std::string get_operator_name() const override {
    return "UGetVFromEdgeWithPredOpr";
  }

  bl::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer& timer) override {
    auto expr = parse_expression<GraphUpdateInterface>(
        graph, ctx, params, expr_, VarType::kPathVar);
    if (expr->is_optional()) {
      LOG(ERROR) << "GetV does not support optional expression now";
      RETURN_BAD_REQUEST_ERROR("GetV does not support optional expression now");
    }
    LOG(INFO) << "GetVFromEdgeWithPredOpr";
    return UGetV::get_vertex_from_edge(
        graph, std::move(ctx), params_,
        [&](size_t idx, label_t label, vid_t vid) {
          return expr->eval_vertex(label, vid, idx).as_bool();
        });
  }

 private:
  GetVParams params_;
  common::Expression expr_;
};

class UGetVFromVerticesWithPredOpr : public IUpdateOperator {
 public:
  UGetVFromVerticesWithPredOpr(const GetVParams& params,
                               const common::Expression& expr)
      : params_(params), expr_(expr) {}
  ~UGetVFromVerticesWithPredOpr() = default;

  std::string get_operator_name() const override {
    return "UGetVFromVerticesWithPredOpr";
  }

  bl::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer& timer) override {
    auto expr = parse_expression<GraphUpdateInterface>(
        graph, ctx, params, expr_, VarType::kPathVar);
    if (expr->is_optional()) {
      LOG(ERROR) << "GetV does not support optional expression now";
      RETURN_BAD_REQUEST_ERROR("GetV does not support optional expression now");
    }
    return UGetV::get_vertex_from_vertices(
        graph, std::move(ctx), params_,
        [&](size_t idx, label_t label, vid_t vid) {
          return expr->eval_vertex(label, vid, idx).as_bool();
        });
  }

 private:
  GetVParams params_;
  common::Expression expr_;
};
std::unique_ptr<IUpdateOperator> UVertexOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  const auto& vertex = plan.plan(op_idx).opr().vertex();
  int alias = vertex.has_alias() ? vertex.alias().value() : -1;
  int tag = vertex.has_tag() ? vertex.tag().value() : -1;
  GetVParams params;
  VOpt opt = parse_opt(vertex.opt());
  params.opt = opt;
  params.tag = tag;
  params.alias = alias;
  params.tables = parse_tables(vertex.params());
  if (vertex.params().has_predicate()) {
    if (opt == VOpt::kEnd || opt == VOpt::kStart || opt == VOpt::kOther) {
      return std::make_unique<UGetVFromEdgeWithPredOpr>(
          params, vertex.params().predicate());
    } else if (opt == VOpt::kItself) {
      return std::make_unique<UGetVFromVerticesWithPredOpr>(
          params, vertex.params().predicate());
    }
    return std::make_unique<UGetVFromEdgeWithPredOpr>(
        params, vertex.params().predicate());
  }
  if (opt == VOpt::kEnd || opt == VOpt::kStart || opt == VOpt::kOther) {
    return std::make_unique<UGetVFromEdgeOpr>(params);
  }
  LOG(ERROR) << "GetV does not support opt " << static_cast<int>(opt);
  return nullptr;
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs
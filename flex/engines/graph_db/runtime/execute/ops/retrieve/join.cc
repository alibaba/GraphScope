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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/join.h"
#include "flex/engines/graph_db/runtime/execute/plan_parser.h"

namespace gs {
namespace runtime {
namespace ops {

class JoinOpr : public IReadOperator {
 public:
  JoinOpr(gs::runtime::ReadPipeline&& left_pipeline,
          gs::runtime::ReadPipeline&& right_pipeline,
          const JoinParams& join_params)
      : left_pipeline_(std::move(left_pipeline)),
        right_pipeline_(std::move(right_pipeline)),
        params_(join_params) {}

  gs::runtime::Context Eval(const gs::runtime::GraphReadInterface& graph,
                            const std::map<std::string, std::string>& params,
                            gs::runtime::Context&& ctx,
                            gs::runtime::OprTimer& timer) override {
    gs::runtime::Context ret_dup(ctx);

    auto left_ctx =
        left_pipeline_.Execute(graph, std::move(ctx), params, timer);
    auto right_ctx =
        right_pipeline_.Execute(graph, std::move(ret_dup), params, timer);

    return Join::join(std::move(left_ctx), std::move(right_ctx), params_);
  }

 private:
  gs::runtime::ReadPipeline left_pipeline_;
  gs::runtime::ReadPipeline right_pipeline_;

  JoinParams params_;
};

std::pair<std::unique_ptr<IReadOperator>, ContextMeta> JoinOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta;
  std::vector<int> right_columns;
  auto& opr = plan.plan(op_idx).opr().join();
  JoinParams p;
  CHECK(opr.left_keys().size() == opr.right_keys().size())
      << "join keys size mismatch";
  auto left_keys = opr.left_keys();

  for (int i = 0; i < left_keys.size(); i++) {
    if (!left_keys.Get(i).has_tag()) {
      LOG(FATAL) << "left_keys should have tag";
    }
    p.left_columns.push_back(left_keys.Get(i).tag().id());
  }
  auto right_keys = opr.right_keys();
  for (int i = 0; i < right_keys.size(); i++) {
    if (!right_keys.Get(i).has_tag()) {
      LOG(FATAL) << "right_keys should have tag";
    }
    p.right_columns.push_back(right_keys.Get(i).tag().id());
  }

  switch (opr.join_kind()) {
  case physical::Join_JoinKind::Join_JoinKind_INNER:
    p.join_type = JoinKind::kInnerJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_SEMI:
    p.join_type = JoinKind::kSemiJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_ANTI:
    p.join_type = JoinKind::kAntiJoin;
    break;
  case physical::Join_JoinKind::Join_JoinKind_LEFT_OUTER:
    p.join_type = JoinKind::kLeftOuterJoin;
    break;
  default:
    LOG(FATAL) << "unsupported join kind" << opr.join_kind();
  }
  auto join_kind = plan.plan(op_idx).opr().join().join_kind();

  auto pair1 = PlanParser::get().parse_read_pipeline_with_meta(
      schema, ctx_meta, plan.plan(op_idx).opr().join().left_plan());
  auto pair2 = PlanParser::get().parse_read_pipeline_with_meta(
      schema, ctx_meta, plan.plan(op_idx).opr().join().right_plan());
  auto& ctx_meta1 = pair1.second;
  auto& ctx_meta2 = pair2.second;
  if (join_kind == physical::Join_JoinKind::Join_JoinKind_SEMI ||
      join_kind == physical::Join_JoinKind::Join_JoinKind_ANTI) {
    ret_meta = ctx_meta1;
  } else if (join_kind == physical::Join_JoinKind::Join_JoinKind_INNER) {
    ret_meta = ctx_meta1;
    for (auto k : ctx_meta2.columns()) {
      ret_meta.set(k);
    }
  } else {
    CHECK(join_kind == physical::Join_JoinKind::Join_JoinKind_LEFT_OUTER);
    ret_meta = ctx_meta1;
    for (auto k : ctx_meta2.columns()) {
      if (std::find(p.right_columns.begin(), p.right_columns.end(), k) ==
          p.right_columns.end()) {
        ret_meta.set(k);
      }
    }
  }
  return std::make_pair(std::make_unique<JoinOpr>(std::move(pair1.first),
                                                  std::move(pair2.first), p),
                        ret_meta);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs
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

#include "flex/engines/graph_db/runtime/execute/plan_parser.h"

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/dedup.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/edge.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/group_by.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/intersect.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/join.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/limit.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/order_by.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/path.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/procedure_call.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/project.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/scan.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/select.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/sink.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/unfold.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/union.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/vertex.h"

#include "flex/engines/graph_db/runtime/execute/ops/update/dedup.h"
#include "flex/engines/graph_db/runtime/execute/ops/update/load.h"
#include "flex/engines/graph_db/runtime/execute/ops/update/project.h"
#include "flex/engines/graph_db/runtime/execute/ops/update/sink.h"
#include "flex/engines/graph_db/runtime/execute/ops/update/unfold.h"

namespace gs {

namespace runtime {

void PlanParser::init() {
  register_read_operator_builder(std::make_unique<ops::ScanOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::TCOprBuilder>());
  register_read_operator_builder(
      std::make_unique<ops::EdgeExpandGetVOprBuilder>());
  register_read_operator_builder(std::make_unique<ops::EdgeExpandOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::VertexOprBuilder>());

  register_read_operator_builder(
      std::make_unique<ops::ProjectOrderByOprBuilder>());
  register_read_operator_builder(std::make_unique<ops::ProjectOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::OrderByOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::GroupByOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::DedupOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::SelectOprBuilder>());

  register_read_operator_builder(
      std::make_unique<ops::SPOrderByLimitOprBuilder>());
  register_read_operator_builder(std::make_unique<ops::SPOprBuilder>());
  register_read_operator_builder(
      std::make_unique<ops::PathExpandVOprBuilder>());
  register_read_operator_builder(std::make_unique<ops::PathExpandOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::JoinOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::IntersectOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::LimitOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::UnfoldOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::UnionOprBuilder>());

  register_read_operator_builder(std::make_unique<ops::SinkOprBuilder>());
  register_read_operator_builder(
      std::make_unique<ops::ProcedureCallOprBuilder>());

  register_write_operator_builder(std::make_unique<ops::LoadOprBuilder>());
  register_write_operator_builder(
      std::make_unique<ops::DedupInsertOprBuilder>());
  register_write_operator_builder(
      std::make_unique<ops::ProjectInsertOprBuilder>());
  register_write_operator_builder(
      std::make_unique<ops::SinkInsertOprBuilder>());
  register_write_operator_builder(
      std::make_unique<ops::UnfoldInsertOprBuilder>());
}

PlanParser& PlanParser::get() {
  static PlanParser parser;
  return parser;
}

void PlanParser::register_read_operator_builder(
    std::unique_ptr<IReadOperatorBuilder>&& builder) {
  auto ops = builder->GetOpKinds();
  read_op_builders_[*ops.begin()].emplace_back(ops, std::move(builder));
}

void PlanParser::register_write_operator_builder(
    std::unique_ptr<IInsertOperatorBuilder>&& builder) {
  auto op = builder->GetOpKind();
  write_op_builders_[op] = std::move(builder);
}

#if 1
static std::string get_opr_name(
    physical::PhysicalOpr_Operator::OpKindCase op_kind) {
  switch (op_kind) {
  case physical::PhysicalOpr_Operator::OpKindCase::kScan: {
    return "scan";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kEdge: {
    return "edge_expand";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kVertex: {
    return "get_v";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kOrderBy: {
    return "order_by";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kProject: {
    return "project";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kSink: {
    return "sink";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kDedup: {
    return "dedup";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kGroupBy: {
    return "group_by";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kSelect: {
    return "select";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kPath: {
    return "path";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kJoin: {
    return "join";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kRoot: {
    return "root";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kIntersect: {
    return "intersect";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kUnion: {
    return "union";
  }
  case physical::PhysicalOpr_Operator::OpKindCase::kUnfold: {
    return "unfold";
  }
  default:
    return "unknown";
  }
}
#endif

bl::result<std::pair<ReadPipeline, ContextMeta>>
PlanParser::parse_read_pipeline_with_meta(const gs::Schema& schema,
                                          const ContextMeta& ctx_meta,
                                          const physical::PhysicalPlan& plan) {
  int opr_num = plan.plan_size();
  std::vector<std::unique_ptr<IReadOperator>> operators;
  ContextMeta cur_ctx_meta = ctx_meta;
  for (int i = 0; i < opr_num;) {
    physical::PhysicalOpr_Operator::OpKindCase cur_op_kind =
        plan.plan(i).opr().op_kind_case();
    if (cur_op_kind == physical::PhysicalOpr_Operator::OpKindCase::kSink) {
      // break;
    }
    if (cur_op_kind == physical::PhysicalOpr_Operator::OpKindCase::kRoot) {
      ++i;
      continue;
    }
    auto& builders = read_op_builders_[cur_op_kind];
    int old_i = i;
    gs::Status status = gs::Status::OK();
    for (auto& pair : builders) {
      auto pattern = pair.first;
      auto& builder = pair.second;
      if (pattern.size() > static_cast<size_t>(opr_num - i)) {
        continue;
      }
      bool match = true;
      for (size_t j = 1; j < pattern.size(); ++j) {
        if (plan.plan(i + j).opr().op_kind_case() != pattern[j]) {
          match = false;
        }
      }
      if (match) {
        bl::result<ReadOpBuildResultT> res_pair_status = bl::try_handle_some(
            [&builder, &schema, &cur_ctx_meta, &plan,
             &i]() -> bl::result<ReadOpBuildResultT> {
              return builder->Build(schema, cur_ctx_meta, plan, i);
            },
            [&status](const gs::Status& err) {
              status = err;
              return ReadOpBuildResultT(nullptr, ContextMeta());
            },
            [&](const bl::error_info& err) {
              status =
                  gs::Status(gs::StatusCode::INTERNAL_ERROR,
                             "Error: " + std::to_string(err.error().value()) +
                                 ", Exception: " + err.exception()->what());
              return ReadOpBuildResultT(nullptr, ContextMeta());
            },
            [&]() {
              status = gs::Status(gs::StatusCode::UNKNOWN, "Unknown error");
              return ReadOpBuildResultT(nullptr, ContextMeta());
            });
        if (res_pair_status) {
          auto& opr = res_pair_status.value().first;
          auto& new_ctx_meta = res_pair_status.value().second;
          if (opr) {
            operators.emplace_back(std::move(opr));
            cur_ctx_meta = new_ctx_meta;
            i = builder->stepping(i);
            // Reset status to OK after a successful match.
            status = gs::Status::OK();
            break;
          } else {
            // If the operator is null, it means the builder has failed, we need
            // to stage the error.
            status = gs::Status(gs::StatusCode::INTERNAL_ERROR,
                                "Failed to build operator at index " +
                                    std::to_string(i) +
                                    ", op_kind: " + get_opr_name(cur_op_kind));
          }
        }
      }
    }
    if (i == old_i) {
      std::stringstream ss;
      ss << "Failed to parse plan at index " << i << ": "
         << get_opr_name(cur_op_kind)
         << ", last match error: " << status.ToString();
      auto err = gs::Status(gs::StatusCode::INTERNAL_ERROR, ss.str());
      LOG(ERROR) << err.ToString();
      return bl::new_error(err);
    }
  }
  return std::make_pair(ReadPipeline(std::move(operators)), cur_ctx_meta);
}

bl::result<ReadPipeline> PlanParser::parse_read_pipeline(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan) {
  auto ret = parse_read_pipeline_with_meta(schema, ctx_meta, plan);
  if (!ret) {
    return ret.error();
  }
  return std::move(ret.value().first);
}

bl::result<InsertPipeline> PlanParser::parse_write_pipeline(
    const gs::Schema& schema, const physical::PhysicalPlan& plan) {
  std::vector<std::unique_ptr<IInsertOperator>> operators;
  for (int i = 0; i < plan.plan_size(); ++i) {
    auto op_kind = plan.plan(i).opr().op_kind_case();
    auto op = write_op_builders_.at(op_kind)->Build(schema, plan, i);
    if (!op) {
      RETURN_UNSUPPORTED_ERROR("Failed to parse write pipeline at index " +
                               std::to_string(i) + ": " +
                               get_opr_name(op_kind));
    }
    operators.emplace_back(std::move(op));
  }
  return InsertPipeline(std::move(operators));
}

}  // namespace runtime

}  // namespace gs

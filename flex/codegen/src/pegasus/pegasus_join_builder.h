/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_JOIN_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_JOIN_BUILDER_H_

#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/pegasus/pegasus_order_by_builder.h"
#include "flex/codegen/src/pegasus/pegasus_project_builder.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/expr.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {
namespace pegasus {
class JoinOpBuilder {
 public:
  JoinOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  JoinOpBuilder& operator_index(const int32_t operator_index) {
    operator_index_ = operator_index;
    return *this;
  }

  JoinOpBuilder& add_plan(const physical::PhysicalPlan& left_plan,
                          const physical::PhysicalPlan& right_plan) {
    left_plan_ = left_plan;
    right_plan_ = right_plan;
    return *this;
  }

  JoinOpBuilder& set_join_kind(const physical::Join::JoinKind& join_kind) {
    join_kind_ = join_kind;
    return *this;
  }

  JoinOpBuilder& set_join_key(const std::vector<common::Variable> left_keys,
                              const std::vector<common::Variable> right_keys) {
    left_keys_ = left_keys;
    right_keys_ = right_keys;
    return *this;
  }

  std::string Build() {
    VLOG(10) << "Start build join";
    std::stringstream ss;
    // codegen for stream copy
    ss << "let (mut left_stream, mut right_stream) = stream.copied();\n";

    boost::format join_code_fmter(
        "let stream_%1% = {\n"
        "let (mut left_stream, mut right_stream) = stream_%2%.copied();\n"
        "left_stream = {\n"
        "let stream_0 = left_stream;\n"
        "%3%"
        "};\n"
        "right_stream = {\n"
        "let stream_0 = right_stream;\n"
        "%4%"
        "};\n"
        "%5%"
        "};\n"  // code for copied
    );
    join_code_fmter % operator_index_ % (operator_index_ - 1);
    // codegen for left & right plan
    auto left_context = ctx_.CreateSubTaskContext("left_");
    auto right_context = ctx_.CreateSubTaskContext("right_");

    join_code_fmter % write_sub_plan(left_context, left_plan_);

    join_code_fmter % write_sub_plan(right_context, right_plan_);
    switch (join_kind_) {
    case physical::Join::JoinKind::Join_JoinKind_INNER: {
      join_code_fmter % "left_stream.inner_join(right_stream)?\n";
      break;
    }
    case physical::Join::JoinKind::Join_JoinKind_LEFT_OUTER: {
      join_code_fmter % "left_stream.left_outer_join(right_stream)?\n";
      break;
    }
    case physical::Join::JoinKind::Join_JoinKind_RIGHT_OUTER: {
      join_code_fmter % "left_stream.right_outer_join(right_stream)?\n";
      break;
    }
    case physical::Join::JoinKind::Join_JoinKind_SEMI: {
      join_code_fmter % "left_stream.semi_join(right_stream)?\n";
      break;
    }
    case physical::Join::JoinKind::Join_JoinKind_ANTI: {
      join_code_fmter % "left_stream.anti_join(right_stream)?\n";
      break;
    }
    default:
      LOG(FATAL) << "Unsupported join type";
    }

    // combine building context after join

    // codegen for final join value combination

    return join_code_fmter.str();
  }

 private:
  std::string write_sub_plan(BuildingContext& context,
                             physical::PhysicalPlan& plan) {
    auto plan_size = plan.plan_size();
    std::stringstream sub_plan_code_ss;
    for (auto i = 0; i < plan_size; i++) {
      auto op = plan.plan(i);
      auto& meta_datas = op.meta_data();

      auto opr = op.opr();
      switch (opr.op_kind_case()) {
      case physical::PhysicalOpr::Operator::kRepartition: {
        physical::PhysicalOpr::MetaData meta_data;

        VLOG(10) << "Found a repartition operator";
        auto& repartition_op = opr.repartition();
        auto repartition_codegen = pegasus::BuildRepartitionOp(
            context, i + 1, repartition_op, meta_data);
        VLOG(10) << repartition_codegen;
        sub_plan_code_ss << repartition_codegen;
        break;
      }
      case physical::PhysicalOpr::Operator::kGroupBy: {
        std::vector<physical::PhysicalOpr::MetaData> meta_datas;
        for (auto i = 0; i < op.meta_data_size(); i++) {
          meta_datas.push_back(op.meta_data(i));
        }

        VLOG(10) << "Found a groupby operator";
        auto& groupby_op = opr.group_by();

        sub_plan_code_ss << pegasus::BuildGroupByOp(context, i + 1, groupby_op,
                                                    meta_datas);
        break;
      }
      case physical::PhysicalOpr::Operator::kOrderBy: {
        physical::PhysicalOpr::MetaData meta_data;

        VLOG(10) << "Found a order_by operator";
        auto& orderby_op = opr.order_by();

        sub_plan_code_ss << pegasus::BuildOrderByOp(context, i + 1, orderby_op,
                                                    meta_data);
        break;
      }
      case physical::PhysicalOpr::Operator::kProject: {
        std::vector<physical::PhysicalOpr::MetaData> meta_data;
        for (auto i = 0; i < op.meta_data_size(); i++) {
          meta_data.push_back(op.meta_data(i));
        }

        VLOG(10) << "Found a project operator";
        auto& project_op = opr.project();

        sub_plan_code_ss << pegasus::BuildProjectOp(context, i + 1, project_op,
                                                    meta_data);
        break;
      }
      case physical::PhysicalOpr::Operator::kEdge: {  // edge expand
        auto& meta_data = meta_datas[0];
        VLOG(10) << "Found a edge expand operator";
        auto& edge_op = opr.edge();
        auto edge_codegen = pegasus::BuildEdgeExpandOp<int32_t>(
            context, i + 1, edge_op, meta_data);
        VLOG(10) << edge_codegen;
        sub_plan_code_ss << edge_codegen;
        break;
      }
      case physical::PhysicalOpr::Operator::kVertex: {
        physical::PhysicalOpr::MetaData meta_data;

        VLOG(10) << "Found a get_v operator";
        auto& vertex_op = opr.vertex();
        auto vertex_codegen =
            pegasus::BuildGetVOp<uint8_t>(context, i + 1, vertex_op, meta_data);
        VLOG(10) << vertex_codegen;
        sub_plan_code_ss << vertex_codegen;

        break;
      }
      case physical::PhysicalOpr::Operator::kDedup: {
        physical::PhysicalOpr::MetaData meta_data;
        VLOG(10) << "Found a dedup operator";
        auto& dedup_op = opr.dedup();
        auto dedup_codegen =
            pegasus::BuildDedupOp(context, i + 1, dedup_op, meta_data);
        VLOG(10) << dedup_codegen;
        sub_plan_code_ss << dedup_codegen;
        break;
      }
      default:
        LOG(FATAL) << "Not supported in union.";
      }
    }
    sub_plan_code_ss << "stream_" << plan_size;
    return sub_plan_code_ss.str();
  }

  BuildingContext ctx_;
  int32_t operator_index_;
  physical::PhysicalPlan left_plan_;
  physical::PhysicalPlan right_plan_;
  std::vector<common::Variable> left_keys_;
  std::vector<common::Variable> right_keys_;
  physical::Join::JoinKind join_kind_;
};

static std::string BuildJoinOp(
    BuildingContext& ctx, int32_t operator_index, const physical::Join& join_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  JoinOpBuilder builder(ctx);
  builder.set_join_kind(join_pb.join_kind())
      .add_plan(join_pb.left_plan(), join_pb.right_plan());
  std::vector<common::Variable> left_keys, right_keys;
  for (auto i = 0; i < join_pb.left_keys_size(); i++) {
    left_keys.emplace_back(join_pb.left_keys(i));
  }
  for (auto i = 0; i < join_pb.right_keys_size(); i++) {
    right_keys.emplace_back(join_pb.right_keys(i));
  }
  builder.set_join_key(left_keys, right_keys);
  return builder.operator_index(operator_index).Build();
}
}  // namespace pegasus
}  // namespace gs

#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_JOIN_BUILDER_H_

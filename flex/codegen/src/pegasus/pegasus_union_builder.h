
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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_UNION_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_UNION_BUILDER_H_

#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/expr.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {
namespace pegasus {
class UnionOpBuilder {
 public:
  UnionOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  UnionOpBuilder& operator_index(const int32_t operator_index) {
    operator_index_ = operator_index;
    return *this;
  }

  UnionOpBuilder& add_plan(const physical::PhysicalPlan& plan) {
    sub_plans_.push_back(plan);
    return *this;
  }

  std::string Build() {
    VLOG(10) << "Start build union";
    int32_t sub_plan_size = sub_plans_.size();

    boost::format union_fmter(
        "let stream_%1% = {\n"
        "%2%"  // stream copied
        "%3%"  // sub_plan_code
        "%4%"  // union stream
        "}\n");

    std::string copied_code = write_copied_code();

    // codegen for sub plans
    std::stringstream plan_ss;
    for (int32_t i = 0; i < sub_plan_size; i++) {
      auto sub_ctx = ctx_.CreateSubTaskContext();
      auto sub_plan = sub_plans_[i];
      plan_ss << generate_sub_plan(sub_ctx, sub_plan, i);

      // aggregate sub plan context
    }
    std::string merge_code = write_merge_code();

    union_fmter % operator_index_ % copied_code % plan_ss.str() % merge_code;
    return union_fmter.str();
  }

 private:
  std::string write_copied_code() {
    boost::format copied_code_fmter(
        "let stream_%1%_0 = stream_%2%;\n"
        "%3%"  // code for copied
    );
    std::stringstream copied_ss;
    for (size_t i = 0; i + 1 < sub_plans_.size(); i++) {
      boost::format copied_fmter(
          "let (mut stream_%1%_%2%, mut stream_%1%_%3%) = "
          "stream_%1%_%2%.copied();\n");
      copied_fmter % operator_index_ % i % (i + 1);
      copied_ss << copied_fmter.str();
    }
    copied_code_fmter % operator_index_ % (operator_index_ - 1) %
        copied_ss.str();
    return copied_code_fmter.str();
  }

  std::string generate_sub_plan(BuildingContext& sub_plan_context,
                                physical::PhysicalPlan& sub_plan,
                                int32_t index) {
    boost::format union_fmter(
        "stream_%1%_%2% = {\n"
        "let stream_0 = stream_%1%_%2%;\n"
        "%3%"
        "};\n");
    std::stringstream sub_plan_code_ss;
    for (int32_t i = 0; i < sub_plan.plan_size(); i++) {
      auto op = sub_plan.plan(i);
      auto& meta_datas = op.meta_data();

      auto opr = op.opr();
      switch (opr.op_kind_case()) {
      case physical::PhysicalOpr::Operator::kRepartition: {
        physical::PhysicalOpr::MetaData meta_data;

        VLOG(10) << "Found a repartition operator";
        auto& repartition_op = opr.repartition();
        auto repartition_codegen = pegasus::BuildRepartitionOp(
            sub_plan_context, i + 1, repartition_op, meta_data);
        VLOG(10) << repartition_codegen;
        sub_plan_code_ss << repartition_codegen;
        break;
      }
      case physical::PhysicalOpr::Operator::kGroupBy: {
        std::vector<physical::PhysicalOpr::MetaData> meta_datas;
        for (int32_t i = 0; i < op.meta_data_size(); i++) {
          meta_datas.push_back(op.meta_data(i));
        }

        VLOG(10) << "Found a groupby operator";
        auto& groupby_op = opr.group_by();

        sub_plan_code_ss << pegasus::BuildGroupByOp(sub_plan_context, i + 1,
                                                    groupby_op, meta_datas);
        break;
      }
      case physical::PhysicalOpr::Operator::kOrderBy: {
        physical::PhysicalOpr::MetaData meta_data;

        VLOG(10) << "Found a order_by operator";
        auto& orderby_op = opr.order_by();

        sub_plan_code_ss << pegasus::BuildOrderByOp(sub_plan_context, i + 1,
                                                    orderby_op, meta_data);
        break;
      }
      case physical::PhysicalOpr::Operator::kProject: {
        std::vector<physical::PhysicalOpr::MetaData> meta_data;
        for (int32_t i = 0; i < op.meta_data_size(); i++) {
          meta_data.push_back(op.meta_data(i));
        }

        VLOG(10) << "Found a project operator";
        auto& project_op = opr.project();

        sub_plan_code_ss << pegasus::BuildProjectOp(sub_plan_context, i + 1,
                                                    project_op, meta_data);
        break;
      }
      case physical::PhysicalOpr::Operator::kEdge: {  // edge expand
        auto& meta_data = meta_datas[0];
        VLOG(10) << "Found a edge expand operator";
        auto& edge_op = opr.edge();
        auto edge_codegen = pegasus::BuildEdgeExpandOp<int32_t>(
            sub_plan_context, i + 1, edge_op, meta_data);
        VLOG(10) << edge_codegen;
        sub_plan_code_ss << edge_codegen;
        break;
      }
      case physical::PhysicalOpr::Operator::kVertex: {
        physical::PhysicalOpr::MetaData meta_data;

        VLOG(10) << "Found a get_v operator";
        auto& vertex_op = opr.vertex();
        auto vertex_codegen = pegasus::BuildGetVOp<uint8_t>(
            sub_plan_context, i + 1, vertex_op, meta_data);
        VLOG(10) << vertex_codegen;
        sub_plan_code_ss << vertex_codegen;

        break;
      }
      case physical::PhysicalOpr::Operator::kDedup: {
        physical::PhysicalOpr::MetaData meta_data;
        VLOG(10) << "Found a dedup operator";
        auto& dedup_op = opr.dedup();
        auto dedup_codegen =
            pegasus::BuildDedupOp(sub_plan_context, i + 1, dedup_op, meta_data);
        VLOG(10) << dedup_codegen;
        sub_plan_code_ss << dedup_codegen;
        break;
      }
      default:
        LOG(FATAL) << "Not supported in union.";
      }
    }
    union_fmter % operator_index_ % index % sub_plan_code_ss.str();
    return union_fmter.str();
  }

  std::string write_merge_code() {
    boost::format merge_code_fmter(
        "let result_stream = stream_%1%_0%2%;\n"
        "result_stream");
    std::stringstream merge_ss;
    for (size_t i = 1; i < sub_plans_.size(); i++) {
      boost::format merge_fmter(".merge(stream_%1%_%2%)?");
      merge_fmter % operator_index_ % i;
      merge_ss << merge_fmter.str();
    }
    merge_code_fmter % operator_index_ % merge_ss.str();
    return merge_code_fmter.str();
  }

  BuildingContext ctx_;
  int32_t operator_index_;
  std::vector<physical::PhysicalPlan> sub_plans_;
};

static std::string BuildUnionOp(
    BuildingContext& ctx, int32_t operator_index,
    const physical::Union& union_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  UnionOpBuilder builder(ctx);
  for (int32_t i = 0; i < union_pb.sub_plans_size(); i++) {
    builder.add_plan(union_pb.sub_plans(i));
  }
  return builder.operator_index(operator_index).Build();
}

}  // namespace pegasus
}  // namespace gs
#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_UNION_BUILDER_H_

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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_INTERSECT_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_INTERSECT_BUILDER_H_

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
class IntersectOpBuilder {
 public:
  IntersectOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  IntersectOpBuilder& intersect_key(int32_t intersect_key) {
    intersect_key_ = intersect_key;
    return *this;
  }

  IntersectOpBuilder& add_plan(const physical::PhysicalPlan& plan) {
    sub_plans_.push_back(plan);
    return *this;
  }

  std::string Build() {
    VLOG(10) << "Start Build intersect";
    std::stringstream ss;
    for (size_t i = 0; i < sub_plans_.size(); i++) {
      auto operator_size = sub_plans_[i].plan_size();
      for (auto j = 0; j < operator_size; j++) {
        VLOG(10) << "Get " << j << "th operator from sub plan " << i;
        auto op = sub_plans_[i].plan(j);
        auto& meta_datas = op.meta_data();
        // CHECK(meta_datas.size() == 1) << "meta data size: " <<
        // meta_datas.size();
        // physical::PhysicalOpr::MetaData meta_data; //fake meta
        auto opr = op.opr();
        switch (opr.op_kind_case()) {
        case physical::PhysicalOpr::Operator::kRepartition: {
          physical::PhysicalOpr::MetaData meta_data;

          VLOG(10) << "Found a repartition operator";
          auto& repartition_op = opr.repartition();
          auto repartition_codegen = pegasus::BuildRepartitionOp(
              ctx_, i + 1, repartition_op, meta_data);
          VLOG(10) << repartition_codegen;
          ss << repartition_codegen;
          break;
        }
        case physical::PhysicalOpr::Operator::kEdge: {  // edge expand
          auto& meta_data = meta_datas[0];
          VLOG(10) << "Found a edge expand operator";
          auto& edge_op = opr.edge();
          auto edge_expand_codegen = pegasus::BuildEdgeExpandOp<int32_t>(
              ctx_, i + 1, edge_op, meta_data);
          VLOG(10) << edge_expand_codegen;
          ss << edge_expand_codegen;
          break;
        }
        case physical::PhysicalOpr::Operator::kVertex: {
          physical::PhysicalOpr::MetaData meta_data;

          VLOG(10) << "Found a get_v operator";
          auto& vertex_op = opr.vertex();
          auto vertex_codegen =
              pegasus::BuildGetVOp<uint8_t>(ctx_, i + 1, vertex_op, meta_data);
          VLOG(10) << vertex_codegen;
          ss << vertex_codegen;

          break;
        }
        default:
          LOG(FATAL) << "Not supported in intersect.";
        }
      }
    }
    VLOG(10) << "Finish Build intersect";
    return ss.str();
  }

 private:
  BuildingContext ctx_;
  int32_t intersect_key_;
  std::vector<physical::PhysicalPlan> sub_plans_;
};

static std::string BuildIntersectOp(
    BuildingContext& ctx, const physical::Intersect& intersect_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  IntersectOpBuilder builder(ctx);
  for (auto i = 0; i < intersect_pb.sub_plans_size(); i++) {
    builder.add_plan(intersect_pb.sub_plans(i));
  }
  return builder.intersect_key(intersect_pb.key()).Build();
}
}  // namespace pegasus
}  // namespace gs

#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_INTERSECT_BUILDER_H_

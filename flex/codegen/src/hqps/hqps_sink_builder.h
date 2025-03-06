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
#ifndef CODEGEBN_SRC_HQPS_HQPS_SINK_BUILDER_H_
#define CODEGEBN_SRC_HQPS_HQPS_SINK_BUILDER_H_

#include <sstream>
#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/hqps/hqps_expr_builder.h"
#include "flex/codegen/src/pb_parser/query_params_parser.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {

static constexpr const char* SINK_OP_TEMPLATE_STR =
    "return Engine::Sink(%1%, %2%, std::array<int32_t, %3%>{%4%});";
class SinkOpBuilder {
 public:
  SinkOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  std::string Build() {
    std::string prev_ctx_name = ctx_.GetCurCtxName();
    // We need to sink the result along with the alias_id, which is maintained
    // in the context
    auto& tag_ind_2_tag_ids = ctx_.GetTagIdAndIndMapping().GetTagInd2TagIds();
    CHECK(tag_ind_2_tag_ids.size() > 0);

    std::string tag_ids_str;
    {
      std::stringstream ss;
      for (size_t i = 0; i < tag_ind_2_tag_ids.size(); ++i) {
        if (i == tag_ind_2_tag_ids.size() - 1) {
          ss << tag_ind_2_tag_ids[i];
        } else {
          ss << tag_ind_2_tag_ids[i] << ",";
        }
      }
      tag_ids_str = ss.str();
    }
    boost::format formatter(SINK_OP_TEMPLATE_STR);
    formatter % ctx_.GraphVar() % prev_ctx_name % tag_ind_2_tag_ids.size() %
        tag_ids_str;
    return formatter.str();
  }

 private:
  BuildingContext ctx_;
};

std::string BuildSinkOp(BuildingContext& ctx, const physical::Sink& sink_op_pb,
                        const physical::PhysicalOpr::MetaData& meta_data) {
  SinkOpBuilder builder(ctx);
  return builder.Build();
}

}  // namespace gs

#endif  // CODEGEBN_SRC_HQPS_HQPS_SINK_BUILDER_H_
#ifndef SINK_BUILDER_H
#define SINK_BUILDER_H

#include <sstream>
#include <string>
#include <vector>

#include "proto_generated_gie/algebra.pb.h"
#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

#include "flex/codegen/building_context.h"
#include "flex/codegen/graph_types.h"
#include "flex/codegen/op_builder/expr_builder.h"
#include "flex/codegen/pb_parser/query_params_parser.h"
#include "flex/codegen/codegen_utils.h"

namespace gs {
class SinkOpBuilder {
 public:
  SinkOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  std::string Build() {
    std::stringstream ss;
    std::string prev_ctx_name = ctx_.GetCurCtxName();
    // We need to sink the result along with the alias_id, which is maintained
    // in the context
    auto& tag_ind_2_tag_ids = ctx_.GetTagIdAndIndMapping().GetTagInd2TagIds();
    CHECK(tag_ind_2_tag_ids.size() > 0);
    ss << "return Engine::Sink(" << prev_ctx_name << ",";
    ss << "std::array<int32_t, " << tag_ind_2_tag_ids.size() << ">{";
    for (auto i = 0; i < tag_ind_2_tag_ids.size(); ++i) {
      if (i == tag_ind_2_tag_ids.size() - 1) {
        ss << tag_ind_2_tag_ids[i] << "}";
      } else {
        ss << tag_ind_2_tag_ids[i] << ",";
      }
    }
    ss << ");" << std::endl;
    return ss.str();
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

#endif  // SINK_BUILDER_H
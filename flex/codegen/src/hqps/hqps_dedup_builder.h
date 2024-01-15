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
#ifndef CODEGEN_SRC_HQPS_HQPS_DEDUP_BUILDER_H_
#define CODEGEN_SRC_HQPS_HQPS_DEDUP_BUILDER_H_

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

enum DedupProp {
  kInnerId = 0,
  kProp = 1,
};

static constexpr const char* DEDUP_OP_TEMPLATE_STR =
    "auto %1%= Engine::template Dedup<%2%>(std::move(%3%));";

class DedupOpBuilder {
 public:
  DedupOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  // dedup on kId
  DedupOpBuilder& dedup_on_inner_id(int32_t tag_id) {
    // dedup_prop_ = "none";
    // dedup_prop_type_ = DedupProp::kInnerId;
    int32_t real_tag_ind = ctx_.GetTagInd(tag_id);
    dedup_tag_ids_.emplace_back(real_tag_ind);
    return *this;
  }

  std::string Build() const {
    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();
    std::string dedup_tag_ids_str;
    {
      std::stringstream ss;
      for (size_t i = 0; i < dedup_tag_ids_.size(); ++i) {
        if (i + 1 == dedup_tag_ids_.size()) {
          ss << dedup_tag_ids_[i];
        } else {
          ss << dedup_tag_ids_[i] << ",";
        }
      }
      dedup_tag_ids_str = ss.str();
    }
    boost::format formater(DEDUP_OP_TEMPLATE_STR);
    formater % next_ctx_name % dedup_tag_ids_str % prev_ctx_name;
    return formater.str();
  }

 private:
  BuildingContext& ctx_;
  std::vector<int32_t> dedup_tag_ids_;
};

static std::string BuildDedupOp(
    BuildingContext& ctx, const algebra::Dedup& dedup,
    const physical::PhysicalOpr::MetaData& meta_data) {
  DedupOpBuilder dedup_builder(ctx);
  auto keys = dedup.keys();
  CHECK(keys.size() > 0) << "Dedup keys size should be gt 0";

  for (auto& key : keys) {
    if (key.has_property()) {
      LOG(FATAL) << "dedup on property" << key.property().DebugString()
                 << "not supported";
      // dedup_builder.dedup_prop(key.property());
    } else {
      VLOG(10) << "dedup on innerid";
      dedup_builder.dedup_on_inner_id(key.tag().id());
    }
  }

  return dedup_builder.Build();
}

}  // namespace gs

#endif  // CODEGEN_SRC_HQPS_HQPS_DEDUP_BUILDER_H_
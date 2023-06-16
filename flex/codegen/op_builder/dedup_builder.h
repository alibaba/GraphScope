#ifndef DEDUP_BUILDER_H
#define DEDUP_BUILDER_H

#include <sstream>
#include <string>
#include <vector>

#include "proto_generated_gie/algebra.pb.h"
#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

#include "flex/codegen/building_context.h"
#include "flex/codegen/graph_types.h"
#include "flex/codegen/pb_parser/query_params_parser.h"
#include "flex/codegen/codegen_utils.h"

#include "flex/codegen/op_builder/expr_builder.h"
namespace gs {

enum DedupProp {
  kInnerId = 0,
  kProp = 1,
};
class DedupOpBuilder {
 public:
  DedupOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  // DedupOpBuilder& dedup_prop(const common::Property& property) {
  //   switch (property.item_case()) {
  //   case common::Property::ItemCase::kKey:
  //     dedup_prop_ = property.key().name();
  //     dedup_prop_type_ = DedupProp::kProp;
  //     break;
  //   case common::Property::ItemCase::kId:
  //     dedup_prop_ = "none";
  //     dedup_prop_type_ = DedupProp::kInnerId;
  //     break;
  //   default:
  //     throw std::runtime_error("Unknown dedup property type");
  //   }
  //   return *this;
  // }

  // dedup on kId
  DedupOpBuilder& dedup_on_inner_id(int32_t tag_id) {
    // dedup_prop_ = "none";
    // dedup_prop_type_ = DedupProp::kInnerId;
    int32_t real_tag_ind = ctx_.GetTagInd(tag_id);
    dedup_tag_ids_.emplace_back(real_tag_ind);
    return *this;
  }

  std::string Build() const {
    std::stringstream ss;
    std::string prev_ctx_name, cur_ctx_name;
    std::tie(prev_ctx_name, cur_ctx_name) = ctx_.GetPrevAndNextCtxName();
    ss << "auto " << cur_ctx_name << " " << _ASSIGN_STR_
       << " Engine::template ";
    ss << "Dedup<";
    for (auto i = 0; i < dedup_tag_ids_.size(); ++i) {
      if (i + 1 == dedup_tag_ids_.size()) {
        ss << dedup_tag_ids_[i];
      } else {
        ss << dedup_tag_ids_[i] << ",";
      }
    }
    ss << ">";
    ss << "(std::move(" << prev_ctx_name << "));";
    ss << std::endl;
    return ss.str();
  }

 private:
  BuildingContext& ctx_;
  std::vector<int32_t> dedup_tag_ids_;
  // std::string dedup_prop_;
  // DedupProp dedup_prop_type_;
};

static std::string BuildDedupOp(
    BuildingContext& ctx, const algebra::Dedup& dedup,
    const physical::PhysicalOpr::MetaData& meta_data) {
  // LOG(INFO) << "Dedup: " << dedup.DebugString();
  DedupOpBuilder dedup_builder(ctx);
  auto keys = dedup.keys();
  CHECK(keys.size() > 0) << "Dedup keys size should be gt 0";

  for (auto& key : keys) {
    // if (key.has_tag()) {
    //   dedup_builder.dedup_tag(key.tag().id());
    // } else {
    //   dedup_builder.dedup_tag(-1);
    // }

    if (key.has_property()) {
      LOG(FATAL) << "dedup on property" << key.property().DebugString()
                 << "not supported";
      // dedup_builder.dedup_prop(key.property());
    } else {
      LOG(INFO) << "dedup on innerid";
      dedup_builder.dedup_on_inner_id(key.tag().id());
    }
  }

  return dedup_builder.Build();
}

}  // namespace gs

#endif  // DEDUP_BUILDER_H
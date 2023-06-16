#ifndef SORT_BUILDER_H
#define SORT_BUILDER_H

#include <optional>
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

std::string sort_pair_pb_to_order_pair(
    const BuildingContext& ctx, const algebra::OrderBy::OrderingPair& pair) {
  std::stringstream ss;
  ss << SORT_PROPER_PAIR_NAME << "<";
  if (pair.order() ==
      algebra::OrderBy::OrderingPair::Order::OrderBy_OrderingPair_Order_ASC) {
    ss << "gs::SortOrder::ASC";
  } else if (pair.order() == algebra::OrderBy::OrderingPair::Order::
                                 OrderBy_OrderingPair_Order_DESC) {
    ss << "gs::SortOrder::DESC";
  } else {
    throw std::runtime_error("Unknown sort order: ");
  }
  auto real_key_tag_id = ctx.GetTagInd(pair.key().tag().id());
  ss << ", " << real_key_tag_id;
  CHECK(pair.key().node_type().type_case() == common::IrDataType::kDataType)
      << "sort ordering pair only support primitive";
  ss << ", " << common_data_type_pb_2_str(pair.key().node_type().data_type());
  // the type of sorted property.
  ss << ">(\"";
  // if the tag.property is specified, use that
  // if not, use "none"
  if (pair.key().has_property()) {
    ss << pair.key().property().key().name();
  } else {
    ss << "None";
  }

  ss << "\")";
  return ss.str();
}

class SortOpBuilder {
 public:
  SortOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  SortOpBuilder& add_sort_pair(const algebra::OrderBy::OrderingPair& pair) {
    sort_pairs_.push_back(pair);
    return *this;
  }

  // the range size can also be specified at runtime
  SortOpBuilder& range(const algebra::Range& limit) {
    // auto& lower_one = limit.lower();
    // auto& upper_one = limit.upper();
    // if (lower_one.has_value()) {
    //   lower_ = lower_one.value();
    // } else if (lower_one.has_param()) {
    //   lower_param_ = param_const_pb_to_param_const(lower_one.param());
    // } else {
    //   LOG(WARNING) << "no lower bound for sort";
    //   lower_ = 0;
    // }

    // if (upper_one.has_value()) {
    //   upper_ = upper_one.value();
    //   if (upper_ <= 0) {
    //     upper_ = std::numeric_limits<int32_t>::max();
    //   }
    // } else if (upper_one.has_param()) {
    //   upper_param_ = param_const_pb_to_param_const(upper_one.param());
    // } else {
    //   LOG(WARNING) << "no upper bound for sort";
    //   upper_ = std::numeric_limits<int32_t>::max();
    // }
    lower_ = limit.lower();
    upper_ = limit.upper();

    LOG(INFO) << "Sort Range: " << lower_.value()
              << ", upper: " << upper_.value();
    if (lower_param_.has_value()) {
      LOG(INFO) << "lower param: " << lower_param_.value().var_name;
    }
    if (upper_param_.has_value()) {
      LOG(INFO) << "upper param: " << upper_param_.value().var_name;
    }
    return *this;
  }

  // return the sort opt and sort code.
  std::pair<std::string, std::string> Build() const {
    // make sort opt;
    std::string sort_opt_name;
    std::string sort_opt_code;
    std::string sort_code;
    // if there are any param_const in the range, we need to insert it into
    // context.
    {
      if (lower_param_.has_value()) {
        ctx_.AddParameterVar(lower_param_.value());
      }
      if (lower_param_.has_value()) {
        ctx_.AddParameterVar(upper_param_.value());
      }
    }
    {
      std::stringstream ss;
      sort_opt_name = ctx_.GetNextSortOptName();
      ss << _4_SPACES << "auto " << sort_opt_name << _ASSIGN_STR_
         << "gs::make_sort_opt(";
      ss << "gs::Range(";
      if (lower_.has_value()) {
        ss << lower_.value();
      } else if (lower_param_.has_value()) {
        ss << lower_param_.value().var_name;
      } else {
        LOG(FATAL) << "Lower param not set";
      }

      ss << ", ";
      if (upper_.has_value()) {
        ss << upper_.value();
      } else if (upper_param_.has_value()) {
        ss << upper_param_.value().var_name;
      } else {
        LOG(FATAL) << "Upper param not set";
      }
      ss << ")";

      for (auto i = 0; i < sort_pairs_.size(); ++i) {
        ss << ",";
        ss << sort_pair_pb_to_order_pair(ctx_, sort_pairs_[i]);
      }
      ss << ");" << std::endl;
      sort_opt_code = ss.str();
    }

    {
      std::stringstream ss;
      std::string prev_ctx_name, next_ctx_name;
      std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();
      ss << "auto " << next_ctx_name << _ASSIGN_STR_ << "Engine::Sort("
         << ctx_.TimeStampVar() << ", " << ctx_.GraphVar() << ", "
         << "std::move(" << prev_ctx_name << "), std::move(" << sort_opt_name
         << "));" << std::endl;
      sort_code = ss.str();
    }
    return std::make_pair(sort_opt_code, sort_code);
  }

 private:
  BuildingContext& ctx_;
  std::vector<algebra::OrderBy::OrderingPair> sort_pairs_;
  std::optional<int32_t> lower_, upper_;
  std::optional<codegen::ParamConst> lower_param_, upper_param_;
};

static std::pair<std::string, std::string> BuildSortOp(
    BuildingContext& ctx, const algebra::OrderBy& order_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  if (order_pb.pairs_size() <= 0) {
    throw std::runtime_error("Sort has no pairs");
  }
  SortOpBuilder sort_builder(ctx);
  auto& sort_pairs = order_pb.pairs();
  for (auto i = 0; i < sort_pairs.size(); ++i) {
    sort_builder.add_sort_pair(sort_pairs[i]);
  }
  return sort_builder.range(order_pb.limit()).Build();
}
}  // namespace gs

#endif  // SORT_BUILDER_H
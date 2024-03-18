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
#ifndef CODEGEN_SRC_HQPS_HQPS_SORT_BUILDER_H_
#define CODEGEN_SRC_HQPS_HQPS_SORT_BUILDER_H_

#include <optional>
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

static constexpr const char* ORDERING_PAIR_TEMPLATE_STR =
    "gs::OrderingPropPair<%1%, %2%, %3%>(\"%4%\")";

static constexpr const char* SORT_OP_TEMPLATE_STR =
    "auto %1% = Engine::Sort(%2%, std::move(%3%), gs::Range(%4%, %5%), "
    "std::tuple{%6%});";

std::string sort_pair_pb_to_order_pair(
    const BuildingContext& ctx, const algebra::OrderBy::OrderingPair& pair) {
  std::stringstream ss;
  std::string sort_order_str, sort_prop_type, sort_prop_name;
  if (pair.order() ==
      algebra::OrderBy::OrderingPair::Order::OrderBy_OrderingPair_Order_ASC) {
    sort_order_str = "gs::SortOrder::ASC";
  } else if (pair.order() == algebra::OrderBy::OrderingPair::Order::
                                 OrderBy_OrderingPair_Order_DESC) {
    sort_order_str = "gs::SortOrder::DESC";
  } else {
    throw std::runtime_error("Unknown sort order: ");
  }
  auto real_key_tag_id = ctx.GetTagInd(pair.key().tag().id());
  CHECK(pair.key().node_type().type_case() == common::IrDataType::kDataType)
      << "sort ordering pair only support primitive";
  if (pair.key().has_property()) {
    auto sort_property = pair.key().property();
    if (sort_property.has_label()) {
      sort_prop_name = "label";
      sort_prop_type = data_type_2_string(codegen::DataType::kLabelId);
    } else if (sort_property.has_key()) {
      sort_prop_name = pair.key().property().key().name();
      sort_prop_type =
          single_common_data_type_pb_2_str(pair.key().node_type().data_type());
    } else {
      throw std::runtime_error("Unknown sort property type" +
                               sort_property.DebugString());
    }
  } else {
    sort_prop_name = "";
    sort_prop_type =
        single_common_data_type_pb_2_str(pair.key().node_type().data_type());
  }

  boost::format formater(ORDERING_PAIR_TEMPLATE_STR);
  formater % sort_order_str % real_key_tag_id % sort_prop_type % sort_prop_name;
  return formater.str();
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
    lower_ = limit.lower();
    upper_ = limit.upper();
    if (upper_ == 0) {
      LOG(WARNING) << "Receive upper limit 0, set to INT_MAX";
      upper_ = std::numeric_limits<int32_t>::max();
    }

    VLOG(10) << "Sort Range: " << lower_.value()
             << ", upper: " << upper_.value();
    if (lower_param_.has_value()) {
      VLOG(10) << "lower param: " << lower_param_.value().var_name;
    }
    if (upper_param_.has_value()) {
      VLOG(10) << "upper param: " << upper_param_.value().var_name;
    }
    return *this;
  }

  // return the sort opt and sort code.
  std::string Build() const {
    // make sort opt;
    std::string range_lower, range_upper;
    std::string ordering_pairs_str;
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

    if (lower_.has_value()) {
      range_lower = std::to_string(lower_.value());
    } else if (lower_param_.has_value()) {
      range_lower = lower_param_.value().var_name;
    } else {
      LOG(FATAL) << "Lower param not set";
    }

    if (upper_.has_value()) {
      range_upper = std::to_string(upper_.value());
    } else if (upper_param_.has_value()) {
      range_upper = upper_param_.value().var_name;
    } else {
      LOG(FATAL) << "Upper param not set";
    }
    {
      std::stringstream ss;
      for (size_t i = 0; i < sort_pairs_.size(); ++i) {
        ss << sort_pair_pb_to_order_pair(ctx_, sort_pairs_[i]);
        if (i != sort_pairs_.size() - 1) {
          ss << ", ";
        }
      }
      ordering_pairs_str = ss.str();
    }

    boost::format formater(SORT_OP_TEMPLATE_STR);
    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();
    formater % next_ctx_name % ctx_.GraphVar() % prev_ctx_name % range_lower %
        range_upper % ordering_pairs_str;

    return formater.str();
  }

 private:
  BuildingContext& ctx_;
  std::vector<algebra::OrderBy::OrderingPair> sort_pairs_;
  std::optional<int32_t> lower_, upper_;
  std::optional<codegen::ParamConst> lower_param_, upper_param_;
};

static std::string BuildSortOp(
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

#endif  // CODEGEN_SRC_HQPS_HQPS_SORT_BUILDER_H_
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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_ORDER_BY_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_ORDER_BY_BUILDER_H_

#include <sstream>
#include <string>
#include <vector>

#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/pb_parser/query_params_parser.h"

namespace gs {
namespace pegasus {
class OrderByOpBuilder {
 public:
  OrderByOpBuilder(BuildingContext& ctx) : ctx_(ctx), limit_(-1) {}

  OrderByOpBuilder& operator_index(const int32_t operator_index) {
    operator_index_ = operator_index;
    return *this;
  }

  OrderByOpBuilder& AddOrderingPair(
      const algebra::OrderBy_OrderingPair& order_pair) {
    ordering_pair_.emplace_back(order_pair);
    return *this;
  }

  OrderByOpBuilder& SetLimit(const int32_t limit) {
    limit_ = limit;
    return *this;
  }

  std::string Build() {
    std::stringstream ss;
    boost::format order_by_fmt(
        "%1%"
        "%2%"
        "})?;\n");

    std::string head_code = write_head();

    std::string order_body_code;
    VLOG(10) << "Ordering pair size is " << ordering_pair_.size();
    for (size_t i = 0; i < ordering_pair_.size(); ++i) {
      boost::format cmp_fmter("x.%1%.%2%(&y.%1%)%3%");

      int32_t input_tag = ordering_pair_[i].key().tag().id();
      auto data_type = ordering_pair_[i].key().node_type().data_type();
      int32_t tag_index = ctx_.GetAliasIndex(input_tag);

      if (i > 0) {
        ss << ".then(";
      }
      std::string cmp_type;
      switch (data_type.item_case()) {
      case common::DataType::kPrimitiveType: {
        switch (data_type.primitive_type()) {
        case common::PrimitiveType::DT_BOOL:
        case common::PrimitiveType::DT_SIGNED_INT32:
        case common::PrimitiveType::DT_SIGNED_INT64:
          cmp_type = "cmp";
          break;
        case common::PrimitiveType::DT_DOUBLE: {
          cmp_type = "partial_cmp";
          break;
        }
        default:
          LOG(FATAL) << "Unsupported type "
                     << static_cast<int32_t>(data_type.primitive_type());
        }
      }
      case common::DataType::kString: {
        cmp_type = "cmp";
        break;
      }
      default:
        LOG(FATAL) << "Unsupported type " << data_type.DebugString();
      }
      std::string reverse_str;
      if (ordering_pair_[i].order() == algebra::OrderBy_OrderingPair_Order::
                                           OrderBy_OrderingPair_Order_DESC) {
        reverse_str = ".reverse()";
      }
      cmp_fmter % tag_index % cmp_type % reverse_str;
      if (i > 0) {
        order_body_code = order_body_code + ".then(" + cmp_fmter.str() + ")\n";
      } else {
        order_body_code += cmp_fmter.str();
      }
    }
    order_by_fmt % head_code % order_body_code;
    return order_by_fmt.str();
  }

 private:
  std::string write_head() const {
    boost::format head_fmter("let stream_%1% = stream_%2%.%3%(%4% |x, y| {\n");
    std::string operator_name;
    std::string limit_code;
    if (limit_ < 0) {
      operator_name = "sort_by";
    } else {
      operator_name = "sort_limit_by";
      limit_code = std::to_string(limit_) + ", ";
    }
    head_fmter % operator_index_ % (operator_index_ - 1) % operator_name %
        limit_code;
    return head_fmter.str();
  }

  BuildingContext& ctx_;
  int32_t operator_index_;
  std::vector<algebra::OrderBy_OrderingPair> ordering_pair_;
  int32_t limit_;
};

static std::string BuildOrderByOp(
    BuildingContext& ctx, int32_t operator_index,
    const algebra::OrderBy& order_by_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  OrderByOpBuilder builder(ctx);

  CHECK(order_by_pb.pairs_size() >= 1);
  for (int32_t i = 0; i < order_by_pb.pairs_size(); i++) {
    builder.AddOrderingPair(order_by_pb.pairs(i));
  }
  if (order_by_pb.has_limit()) {
    builder.SetLimit(order_by_pb.limit().upper());
  }

  return builder.operator_index(operator_index).Build();
}

}  // namespace pegasus
}  // namespace gs

#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_ORDER_BY_BUILDER_H_

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
#ifndef CODEGEN_SRC_HQPS_HQPS_LIMIT_BUILDER_H_
#define CODEGEN_SRC_HQPS_HQPS_LIMIT_BUILDER_H_

#include <string>
#include <vector>
#include "flex/codegen/src/building_context.h"

#include "boost/format.hpp"

namespace gs {
static constexpr const char* LIMIT_OP_TEMPLATE_STR =
    "auto %1% = Engine::Limit(std::move(%2%), %3%, %4%);";

class LimitOpBuilder {
 public:
  LimitOpBuilder(BuildingContext& context) : context_(context) {}

  LimitOpBuilder& range(const algebra::Range& range) {
    range_ = range;
    return *this;
  }

  std::string Build() const {
    boost::format formater(LIMIT_OP_TEMPLATE_STR);
    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = context_.GetPrevAndNextCtxName();
    formater % next_ctx_name % prev_ctx_name % range_.lower() % range_.upper();
    return formater.str();
  }

 private:
  BuildingContext& context_;
  algebra::Range range_;
};
static std::string BuildLimitOp(BuildingContext& ctx,
                                const algebra::Limit& limit_op) {
  VLOG(10) << "Building Limit Op: " << limit_op.DebugString();
  LimitOpBuilder builder(ctx);
  builder.range(limit_op.range());
  return builder.Build();
}
}  // namespace gs

#endif  // CODEGEN_SRC_HQPS_HQPS_LIMIT_BUILDER_H_
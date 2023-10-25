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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_LIMIT_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_LIMIT_BUILDER_H_

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

class LimitOpBuilder {
 public:
  LimitOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  LimitOpBuilder& operator_index(const int32_t operator_index) {
    operator_index_ = operator_index;
    return *this;
  }

  LimitOpBuilder& limit(int32_t limit) {
    limit_ = limit;
    return *this;
  }

  std::string Build() {
    VLOG(10) << "Start build limit";

    boost::format limit_fmter("let stream_%1% = stream_%2%.limit(%3%)?;");
    limit_fmter % operator_index_ % (operator_index_ - 1) % limit_;
    return limit_fmter.str();
  }

 private:
  BuildingContext ctx_;
  int32_t operator_index_;
  int32_t limit_;
};

static std::string BuildLimitOp(
    BuildingContext& ctx, int32_t operator_index,
    const algebra::Limit& limit_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  LimitOpBuilder builder(ctx);
  return builder.operator_index(operator_index)
      .limit(limit_pb.range().upper())
      .Build();
}
}  // namespace pegasus
}  // namespace gs

#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_LIMIT_BUILDER_H_

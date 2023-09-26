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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_SINK_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_SINK_BUILDER_H_

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
#include "flex/codegen/src/pegasus/pegasus_expr_builder.h"

namespace gs {
namespace pegasus {
class SinkOpBuilder {
 public:
  SinkOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  SinkOpBuilder& operator_index(const int32_t operator_index) {
    operator_index_ = operator_index;
    return *this;
  }

  std::string Build() {
    boost::format sink_fmter("stream_%1%.sink_into(output)\n");
    sink_fmter % (operator_index_ - 1);
    return sink_fmter.str();
  }

 private:
  BuildingContext ctx_;
  int32_t operator_index_;
};

std::string BuildSinkOp(BuildingContext& ctx, int32_t operator_index,
                        const physical::Sink& sink_op_pb,
                        const physical::PhysicalOpr::MetaData& meta_data) {
  SinkOpBuilder builder(ctx);
  return builder.operator_index(operator_index).Build();
}
}  // namespace pegasus
}  // namespace gs

#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_SINK_BUILDER_H_

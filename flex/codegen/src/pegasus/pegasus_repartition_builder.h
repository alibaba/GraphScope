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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_REPARTITION_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_REPARTITION_BUILDER_H_

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
class PePartitionOpBuilder {
 public:
  PePartitionOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  PePartitionOpBuilder& operator_index(const int32_t operator_index) {
    operator_index_ = operator_index;
    return *this;
  }

  PePartitionOpBuilder& input_tag(const int32_t input_tag) {
    in_tag_ = input_tag;
    return *this;
  }

  // return make_project code and call project code.
  std::string Build() const {
    boost::format repartition_fmter(
        "let stream_%1% = stream_%2%.repartition(move |input| {\n"
        "Ok(get_partition(&input.%3%, workers as usize, "
        "pegasus::get_servers_len()))\n"
        "});\n");
    int32_t index;
    if (in_tag_ == -1) {
      index = 0;
    } else {
      index = ctx_.GetAliasIndex(in_tag_);
    }
    repartition_fmter % operator_index_ % (operator_index_ - 1) % index;
    return repartition_fmter.str();
  }

 private:
  BuildingContext& ctx_;
  int32_t operator_index_;
  int32_t in_tag_ = -1;
};

static std::string BuildRepartitionOp(
    BuildingContext& ctx, int32_t operator_index,
    const physical::Repartition& repartition_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  PePartitionOpBuilder builder(ctx);
  if (repartition_pb.to_another().has_shuffle_key()) {
    builder.input_tag(repartition_pb.to_another().shuffle_key().value());
  }
  return builder.operator_index(operator_index).Build();
}
}  // namespace pegasus
}  // namespace gs

#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_REPARTITION_BUILDER_H_

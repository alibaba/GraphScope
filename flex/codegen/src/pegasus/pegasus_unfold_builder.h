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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_UNFOLD_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_UNFOLD_BUILDER_H_

#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/pegasus/pegasus_repartition_builder.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/expr.pb.h"

namespace gs {
namespace pegasus {
class UnfoldOpBuilder {
 public:
  UnfoldOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  UnfoldOpBuilder& operator_index(const int32_t operator_index) {
    operator_index_ = operator_index;
    return *this;
  }

  UnfoldOpBuilder& add_tag(int32_t in_tag_id, int32_t out_tag_id) {
    in_tag_id_ = in_tag_id;
    out_tag_id_ = out_tag_id;
    return *this;
  }

  std::string Build() {
    auto input_size = ctx_.InputSize();

    boost::format unfold_fmter(
        "let stream_%1% = stream_%2%\n"
        ".flat_map(|%3%| {\n"
        "Ok(i%4%.into_iter().map(|res| Ok(%5%)))\n"
        "})?;\n");

    std::string input_params = generate_arg_list("i", input_size);

    int32_t input_index = 0;
    if (in_tag_id_ != -1) {
      input_index = ctx_.GetAliasIndex(in_tag_id_);
    }
    ctx_.SetAlias(out_tag_id_);
    auto outputs = ctx_.GetOutput();
    auto output_index = ctx_.GetAliasIndex(out_tag_id_);
    ctx_.SetOutput(0, outputs[input_index]);
    ctx_.SetOutput(output_index, outputs[input_index]);
    std::string output_params = generate_output_list(
        "i", input_size, "res", output_index, ctx_.ContainHead());

    unfold_fmter % operator_index_ % (operator_index_ - 1) % input_params %
        input_index % output_params;
    return unfold_fmter.str();
  }

 private:
  BuildingContext& ctx_;
  int32_t operator_index_;
  int32_t in_tag_id_, out_tag_id_;
};

static std::string BuildUnfoldOp(
    BuildingContext& ctx, int32_t operator_index,
    const physical::Unfold& unfold_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  UnfoldOpBuilder builder(ctx);
  builder.add_tag(unfold_pb.tag().value(), unfold_pb.alias().value());
  return builder.operator_index(operator_index).Build();
}
}  // namespace pegasus
}  // namespace gs
#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_UNFOLD_BUILDER_H_

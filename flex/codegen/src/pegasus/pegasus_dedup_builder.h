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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_DEDUP_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_DEDUP_BUILDER_H_

#include <set>
#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/pegasus/pegasus_repartition_builder.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/expr.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {
namespace pegasus {
class DedupOpBuilder {
 public:
  DedupOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  DedupOpBuilder& operator_index(const int32_t operator_index) {
    operator_index_ = operator_index;
    return *this;
  }

  DedupOpBuilder& add_in_tag(int32_t in_tag_id) {
    in_tag_ids_.push_back(in_tag_id);
    return *this;
  }

  std::string Build() {
    std::stringstream ss;
    auto input_size = ctx_.InputSize();

    // write key_by head
    boost::format key_by_head_fmter(
        "let stream_%1% = stream_%2%.key_by|%3%| {\n");
    std::string key_by_input = generate_arg_list("i", input_size);
    key_by_head_fmter % operator_index_ % (operator_index_ - 1) % key_by_input;

    boost::format key_by_output_fmter("Ok((%1%, %2%))\n})?\n");
    std::stringstream key_ss;
    key_ss << "(";
    std::unordered_set<int32_t> key_sets;
    for (size_t i = 0; i < in_tag_ids_.size(); i++) {
      int32_t input_index = 0;
      if (in_tag_ids_[i] != -1) {
        input_index = ctx_.GetAliasIndex(in_tag_ids_[i]);
      }
      key_sets.insert(input_index);
      key_ss << "i" << input_index;
      if (i + 1 != in_tag_ids_.size()) {
        key_ss << ", ";
      }
    }
    key_ss << ")";
    std::string key_code = key_ss.str();

    std::stringstream value_ss;
    value_ss << "(";
    std::vector<int32_t> value_list;
    for (int32_t i = 0; i < input_size; i++) {
      if (key_sets.find(i) == key_sets.end()) {
        value_list.push_back(i);
      }
    }
    for (size_t i = 0; i < value_list.size(); i++) {
      value_ss << "i" << value_list[i];
      if (i != value_list.size() - 1) {
        value_ss << ", ";
      }
    }
    value_ss << ")";
    std::string value_code = value_ss.str();

    key_by_output_fmter % key_code % value_code;
    std::string key_by_code =
        key_by_head_fmter.str() + key_by_output_fmter.str();

    boost::format dedup_fmter(
        ".dedup()?\n"
        ".map(|%1%| Ok(%2%))?;\n");
    std::string params = key_code + ", " + value_code;
    std::string outputs = generate_arg_list("i", input_size);
    dedup_fmter % params % outputs;

    return key_by_code + dedup_fmter.str();
  }

 private:
  BuildingContext& ctx_;
  int32_t operator_index_;
  std::vector<int32_t> in_tag_ids_;
};

static std::string BuildDedupOp(
    BuildingContext& ctx, int32_t operator_index, const algebra::Dedup& dedup,
    const physical::PhysicalOpr::MetaData& meta_data) {
  DedupOpBuilder builder(ctx);
  auto tag_size = dedup.keys_size();
  for (int32_t i = 0; i < tag_size; i++) {
    builder.add_in_tag(dedup.keys(i).tag().id());
  }
  return builder.operator_index(operator_index).Build();
}
}  // namespace pegasus
}  // namespace gs
#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_DEDUP_BUILDER_H_

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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_SELECT_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_SELECT_BUILDER_H_

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
class SelectOpBuilder {
 public:
  SelectOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  SelectOpBuilder& operator_index(const int32_t operator_index) {
    operator_index_ = operator_index;
    return *this;
  }

  SelectOpBuilder& predicate(common::Expression expr) {
    expr_ = expr;
    return *this;
  }

  std::string Build() {
    VLOG(10) << "Start build select";

    int32_t input_size = ctx_.InputSize();
    boost::format select_head_fmter(
        "let stream_%1% = stream_%2%\n"
        ".filter_map(move |%3%| {\n");
    std::string input_params = generate_arg_list("i", input_size);
    select_head_fmter % operator_index_ % (operator_index_ - 1) % input_params;

    auto expr_builder = ExprBuilder(ctx_);
    expr_builder.AddAllExprOpr(expr_.operators());
    std::string predicate_expr;
    std::vector<std::string> var_names;
    std::vector<int32_t> var_tags;
    std::vector<codegen::ParamConst> properties;
    std::vector<std::string> case_exprs;
    std::tie(predicate_expr, var_names, var_tags, properties, case_exprs) =
        expr_builder.BuildRust();

    // Codegen for property expression
    std::string vars_code;
    for (auto i : case_exprs) {
      vars_code += i;
    }
    for (size_t i = 0; i < var_names.size(); i++) {
      int32_t input_index;
      std::pair<int32_t, std::vector<int32_t>> input_type;
      VLOG(10) << "Input tag is " << var_tags[i];
      if (var_tags[i] == -1) {
        input_index = 0;
        input_type = ctx_.GetHeadType();
      } else {
        input_index = ctx_.GetAliasIndex(var_tags[i]);
        input_type = ctx_.GetAliasType(var_tags[i]);
      }
      VLOG(10) << "Property is " << properties[i].var_name << ", var name is "
               << var_names[i];

      boost::format itself_fmter("let %1% = i%2%;\n");
      boost::format property_fmter(
          "let vertex_id = CSR.get_internal_id(i%1% as usize);\n"
          "%2%");
      if (properties[i].var_name == "none") {
        itself_fmter % var_names[i] % input_index;
        vars_code += itself_fmter.str();
      } else {
        CHECK(input_type.first == 0);
        if (input_type.second.size() == 1) {
          boost::format property_fmter("let %1% = %2%[vertex_id];\n");
          int32_t label_id = input_type.second[0];
          std::string property_name =
              get_vertex_prop_column_name(properties[i].var_name, label_id);
          property_fmter % var_names[i] % property_name;
          vars_code += property_fmter.str();
        } else {
          boost::format properties_fmter(
              "let vertex_label = LDBCVertexParser::<usize>::get_label_id(i%1% "
              "as usize);\n"
              "let %2% = \n"
              "%3%"  // get property for different labels
              "else {\n"
              "panic!(\"Unexpected label: {}\", vertex_label)"
              "}\n");

          std::string condition_code;
          for (size_t j = 0; j < input_type.second.size(); j++) {
            boost::format condition_fmter(
                "if vertex_label == %1% {\n"
                "%2%[vertex_id]\n"
                "}\n");
            std::string property_name =
                get_vertex_prop_column_name(properties[i].var_name, j);
            int32_t label_id = input_type.second[j];
            condition_fmter % label_id % property_name;
            if (j > 0) {
              condition_code += "else";
            }
            condition_code += condition_fmter.str();
          }

          properties_fmter % input_index % var_names[i] % condition_code;
          vars_code += properties_fmter.str();
        }
      }
    }

    boost::format select_result_fmter(
        "%1%\n"
        "if %2% {\n"
        "Ok(Some(%3%))\n"
        "} else {\n"
        "Ok(None)\n"
        "}\n"
        "})?;\n");
    select_result_fmter % vars_code % predicate_expr % input_params;

    return select_head_fmter.str() + select_result_fmter.str();
  }

 private:
  BuildingContext ctx_;
  int32_t operator_index_;
  common::Expression expr_;
};

static std::string BuildSelectOp(
    BuildingContext& ctx, int32_t operator_index,
    const algebra::Select& select_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  SelectOpBuilder builder(ctx);
  builder.predicate(select_pb.predicate());
  return builder.operator_index(operator_index).Build();
}

}  // namespace pegasus
}  // namespace gs
#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_SELECT_BUILDER_H_

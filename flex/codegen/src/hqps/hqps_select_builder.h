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
#ifndef CODEGEN_SRC_HQPS_HQPS_SELECT_BUILDER_H_
#define CODEGEN_SRC_HQPS_HQPS_SELECT_BUILDER_H_

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

static constexpr const char* SELECT_OP_TEMPLATE_STR =
    "auto %1% = gs::make_filter(%2%(%3%), %4%);\n"
    "auto %5% = Engine::template Select<%6%>(%7%, std::move(%8%), "
    "std::move(%1%));\n";

class SelectOpBuilder {
 public:
  SelectOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  SelectOpBuilder& expr(const common::Expression expr) {
    ExprBuilder expr_builder(ctx_);
    common::DataType data_type;
    data_type.set_primitive_type(common::PrimitiveType::DT_BOOL);
    expr_builder.set_return_type(data_type);
    expr_builder.AddAllExprOpr(expr.operators());

    std::string func_code;
    std::vector<codegen::ParamConst> func_call_params;
    std::vector<std::pair<int32_t, std::string>> tag_props;
    std::vector<common::DataType> unused_expr_ret_type;
    std::tie(expr_name_, func_call_params, tag_props, func_code,
             unused_expr_ret_type) = expr_builder.Build();

    // add func_call_params to ctx's param const;
    for (size_t i = 0; i < func_call_params.size(); ++i) {
      ctx_.AddParameterVar(func_call_params[i]);
    }

    ctx_.AddExprCode(func_code);

    expr_var_name_ = ctx_.GetNextExprVarName();
    {
      std::stringstream ss;
      for (size_t i = 0; i < func_call_params.size(); ++i) {
        ss << func_call_params[i].var_name;
        if (i != func_call_params.size() - 1) {
          ss << ",";
        }
      }
      func_call_param_str_ = ss.str();
    }
    {
      std::stringstream ss;
      for (size_t i = 0; i < tag_props.size(); ++i) {
        ss << tag_props[i].second;
        if (i != tag_props.size() - 1) {
          ss << ",";
        }
      }
      selectors_str_ = ss.str();
    }
    {
      std::stringstream ss;
      for (size_t i = 0; i < tag_props.size(); ++i) {
        ss << format_input_col(tag_props[i].first);
        if (i != tag_props.size() - 1) {
          ss << ",";
        }
      }
      in_col_ids_str_ = ss.str();
    }

    return *this;
  }

  std::string Build() const {
    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();

    boost::format formatter(SELECT_OP_TEMPLATE_STR);
    formatter % expr_var_name_ % expr_name_ % func_call_param_str_ %
        selectors_str_ % next_ctx_name % in_col_ids_str_ % ctx_.GraphVar() %
        prev_ctx_name;
    return formatter.str();
  }

 private:
  BuildingContext& ctx_;
  std::string expr_name_;
  std::string expr_call_code_;
  std::string expr_var_name_;
  std::string func_call_param_str_;
  std::string selectors_str_;
  std::string in_col_ids_str_;
};

// return expression code and select op code(including expr calling)
static std::string BuildSelectOp(
    BuildingContext& ctx, const algebra::Select& select_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  if (!select_pb.has_predicate()) {
    throw std::runtime_error("Select expression is not set");
  }

  ////build select op code
  SelectOpBuilder select_builder(ctx);
  select_builder.expr(select_pb.predicate());
  std::string select_op_code = select_builder.Build();
  return select_op_code;
}

}  // namespace gs

#endif  // CODEGEN_SRC_HQPS_HQPS_SELECT_BUILDER_H_
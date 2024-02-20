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
#ifndef CODEGEN_SRC_HQPS_HQPS_PROJECT_BUILDER_H_
#define CODEGEN_SRC_HQPS_HQPS_PROJECT_BUILDER_H_

#include <sstream>
#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/hqps/hqps_case_when_builder.h"
#include "flex/codegen/src/hqps/hqps_expr_builder.h"
#include "flex/codegen/src/pb_parser/query_params_parser.h"
#include "flex/codegen/src/string_utils.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {
static constexpr const char* PROJECT_MAPPER_VAR_TEMPLATE_STR =
    "gs::make_mapper_with_variable<INPUT_COL_ID(%1%)>(%2%)";

static constexpr const char* PROJECT_MAPPER_EXPR_TEMPLATE_STR =
    "gs::make_mapper_with_expr<%1%>(%2%(%3%) %4%)";

static constexpr const char* PROJECT_OP_TEMPLATE_STR =
    "auto %1% = Engine::Project<%2%>(%3%, std::move(%4%), std::tuple{%5%});\n";

// to check the output type of case when is the same.
bool sanity_check(const common::Case& expr_case) {
  // TODO: implement this check
  return true;
}

std::tuple<std::string, std::string, std::string> concatenate_expr_built_result(
    BuildingContext& ctx,
    const std::vector<codegen::ParamConst>& func_construct_param_const,
    const std::vector<std::pair<int32_t, std::string>>& expr_selectors) {
  std::string in_col_ids, expr_constructor_param_str, expr_selector_str;
  {
    std::stringstream ss;
    for (size_t i = 0; i < expr_selectors.size(); ++i) {
      ss << expr_selectors[i].first;
      if (i != expr_selectors.size() - 1) {
        ss << ", ";
      }
    }
    in_col_ids = ss.str();
  }
  {
    std::stringstream ss;
    if (func_construct_param_const.size() > 0) {
      ss << ", ";
    }
    for (size_t i = 0; i < func_construct_param_const.size(); ++i) {
      ss << func_construct_param_const[i].var_name;
      if (i != func_construct_param_const.size() - 1) {
        ss << ", ";
      }
    }
    expr_constructor_param_str = ss.str();
  }
  {
    std::stringstream ss;
    if (expr_selectors.size() > 0) {
      ss << ", ";
    }
    for (size_t i = 0; i < expr_selectors.size(); ++i) {
      ss << expr_selectors[i].second;
      if (i != expr_selectors.size() - 1) {
        ss << ", ";
      }
    }
    expr_selector_str = ss.str();
  }
  return std::make_tuple(in_col_ids, expr_constructor_param_str,
                         expr_selector_str);
}

// There can be expression in project's mappings
// 0. project common expression
// 1. project case when.
// NOTE: the return type of all case-when and else should be all the
// same(excluding null)
std::string project_case_when_from_project_mapping(
    BuildingContext& ctx, const common::Case& expr_case,
    common::DataType data_type, int32_t out_alias_tag) {
  // the common::case contains three expression, input-expr, when-expr,
  // then-expr the input-expr can be null. if it is null, it means we just
  // evaluate when-expr, then-expr like case when.

  // if input-expr is not null, we need to evaluate input-expr, and then
  // evaluate when-expr, then-expr, and else_expr.

  // if input-expr is null, we need to evaluate when-expr, then-expr, and
  // else_expr.

  // check the return type is the same.
  if (!sanity_check(expr_case)) {
    throw std::runtime_error("case when sanity check failed");
  }
  CaseWhenBuilder builder(ctx);
  builder.when_then_exprs(expr_case.when_then_expressions())
      .else_expr(expr_case.else_result_expression());

  std::string expr_func_name, expr_code;
  std::vector<codegen::ParamConst> func_construct_param_const;
  std::vector<std::pair<int32_t, std::string>> expr_selectors;
  std::vector<common::DataType>
      ret_data_type;  // returned data type for case when building
  // ret_data_type is not used.
  std::tie(expr_func_name, func_construct_param_const, expr_selectors,
           expr_code, ret_data_type) = builder.Build();

  ctx.AddExprCode(expr_code);
  if (func_construct_param_const.size() > 0) {
    for (auto& param_const : func_construct_param_const) {
      ctx.AddParameterVar(param_const);
    }
  }

  // make_project_with_expr
  std::string in_col_ids, expr_constructor_param_str, expr_selector_str;
  std::tie(in_col_ids, expr_constructor_param_str, expr_selector_str) =
      concatenate_expr_built_result(ctx, func_construct_param_const,
                                    expr_selectors);
  boost::format formater(PROJECT_MAPPER_EXPR_TEMPLATE_STR);
  formater % in_col_ids % expr_func_name % expr_constructor_param_str %
      expr_selector_str;
  return formater.str();
}

std::string project_expression_from_project_mapping(
    BuildingContext& ctx, const common::Expression& expr,
    int32_t out_alias_tag) {
  auto expr_builder = ExprBuilder(ctx);
  VLOG(10) << "Projecting expression: " << expr.DebugString();
  auto ret_data_type = eval_expr_return_type(expr);
  LOG(INFO) << "Expression return type: "
            << single_common_data_type_pb_2_str(ret_data_type);

  expr_builder.AddAllExprOpr(expr.operators());
  expr_builder.set_return_type(ret_data_type);
  std::string expr_func_name, expr_code;
  std::vector<codegen::ParamConst> func_construct_param_const;
  std::vector<std::pair<int32_t, std::string>> expr_selectors;
  std::vector<common::DataType> unused_expr_ret_type;
  std::tie(expr_func_name, func_construct_param_const, expr_selectors,
           expr_code, unused_expr_ret_type) = expr_builder.Build();

  ctx.AddExprCode(expr_code);
  // make_project_with_expr
  if (func_construct_param_const.size() > 0) {
    for (auto& param_const : func_construct_param_const) {
      ctx.AddParameterVar(param_const);
    }
  }

  // make_project_with_expr
  std::string in_col_ids, expr_constructor_param_str, expr_selector_str;
  std::tie(in_col_ids, expr_constructor_param_str, expr_selector_str) =
      concatenate_expr_built_result(ctx, func_construct_param_const,
                                    expr_selectors);
  boost::format formater(PROJECT_MAPPER_EXPR_TEMPLATE_STR);
  formater % in_col_ids % expr_func_name % expr_constructor_param_str %
      expr_selector_str;
  return formater.str();
}

std::string project_variable_mapping_to_string(BuildingContext& ctx,
                                               const common::ExprOpr& expr_op,
                                               int32_t real_res_col_id) {
  int32_t in_tag_id = -2;
  std::vector<std::string> prop_names;
  std::vector<codegen::DataType> data_types;
  bool project_self = false;
  switch (expr_op.item_case()) {
  case common::ExprOpr::kCase: {
    VLOG(10) << "Got case when in projecting";
    auto case_when = expr_op.case_();
    VLOG(10) << case_when.DebugString();
    return project_case_when_from_project_mapping(
        ctx, case_when, expr_op.node_type().data_type(), real_res_col_id);
  }
  case common::ExprOpr::kVar: {
    VLOG(10) << "Got var in projecting";
    auto& var = expr_op.var();
    in_tag_id = var.tag().id();
    if (var.has_property()) {
      auto& prop = var.property();
      if (prop.item_case() == common::Property::kId) {
        project_self = true;
      } else if (prop.item_case() == common::Property::kKey) {
        prop_names.push_back(prop.key().name());
        data_types.push_back(
            common_data_type_pb_2_data_type(var.node_type().data_type()));
      } else if (prop.item_case() == common::Property::kLen) {
        prop_names.push_back("length");
        data_types.push_back(codegen::DataType::kLength);
      } else if (prop.item_case() == common::Property::kLabel) {
        // return the label id.
        prop_names.push_back("label");
        data_types.push_back(codegen::DataType::kLabelId);
      } else {
        LOG(FATAL) << "Unknown property type" << prop.DebugString();
      }
    } else {
      VLOG(10) << "receives no property, project itself";
      project_self = true;
    }
    break;
  }
  case common::ExprOpr::kVarMap: {
    VLOG(10) << "Got variable map in projecting";
    LOG(WARNING) << "CURRENTLY we flat the var map to a list of variables";
  }

  case common::ExprOpr::kVars: {
    VLOG(10) << "Got variable keys in projecting";
    // project properties to a list.
    auto& vars =
        expr_op.has_vars() ? expr_op.vars().keys() : expr_op.var_map().keys();
    for (int32_t i = 0; i < vars.size(); ++i) {
      auto& var = vars[i];
      if (in_tag_id == -2) {
        in_tag_id = var.tag().id();
      } else {
        CHECK(in_tag_id == var.tag().id()) << "can only support one tag";
      }

      auto& prop = var.property();
      // if (prop.has_id()) {
      if (prop.item_case() == common::Property::kId) {
        LOG(FATAL) << "Not support project id in projecting with vars";
        // } else if (prop.has_key()) {
      } else if (prop.item_case() == common::Property::kKey) {
        prop_names.push_back(prop.key().name());
        data_types.push_back(
            common_data_type_pb_2_data_type(var.node_type().data_type()));
      } else {
        LOG(FATAL) << "Unknown property type" << prop.DebugString();
      }
    }
    break;
  }
  default:
    LOG(FATAL) << "Unknown variable type";
  }
  auto real_in_col_id = ctx.GetTagInd(in_tag_id);
  VLOG(10) << "real_in_tag_id: " << real_in_col_id
           << " in_tag_id: " << in_tag_id;

  if (project_self) {
    VLOG(10) << "Projecting self";
    CHECK(prop_names.size() == 0 && data_types.size() == 0);
    std::string selector_str;
    {
      boost::format select_formater(PROPERTY_SELECTOR);
      select_formater % EMPTY_TYPE % "";
      selector_str = select_formater.str();
    }
    boost::format formater(PROJECT_MAPPER_VAR_TEMPLATE_STR);

    formater % real_in_col_id % selector_str;
    return formater.str();
  } else {
    VLOG(10) << "Projecting properties" << gs::to_string(prop_names);
    CHECK(prop_names.size() == data_types.size());
    CHECK(prop_names.size() == 1);
    std::string selector_str;
    {
      boost::format select_formater(PROPERTY_SELECTOR);
      select_formater % data_type_2_string(data_types[0]) % prop_names[0];
      selector_str = select_formater.str();
    }
    boost::format formater(PROJECT_MAPPER_VAR_TEMPLATE_STR);
    formater % real_in_col_id % selector_str;
    return formater.str();
  }
}

std::string project_mapping_to_string(
    BuildingContext& ctx, const physical::Project::ExprAlias& mapping,
    TagIndMapping& new_tag_ind_map) {
  int32_t res_alias = mapping.alias().value();
  // TODO: Currently we assume each expr_alias contains only property for that
  // input tag

  auto real_res_alias = new_tag_ind_map.CreateOrGetTagInd(res_alias);
  auto& expr = mapping.expr();
  // CHECK(expr.operators_size() == 1) << "can only support one variable";
  if (expr.operators_size() > 1) {
    return project_expression_from_project_mapping(ctx, expr, real_res_alias);
  } else if (expr.operators_size() == 1) {
    auto& expr_op = expr.operators(0);
    return project_variable_mapping_to_string(ctx, expr_op, real_res_alias);
  } else {
    LOG(FATAL) << "expect at least one expr opr";
    return "";
  }
}

/**
 * @brief Build project operator.
 * Project op will create a brand new context, which means we should create a
 * new tag_id tag_ind mapping.
 *
 */
class ProjectOpBuilder {
 public:
  ProjectOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  ProjectOpBuilder& is_append(bool is_append) {
    is_append_ = is_append;
    return *this;
  }

  ProjectOpBuilder& add_mapping(const physical::Project::ExprAlias& mapping) {
    mappings_.push_back(mapping);
    return *this;
  }

  // return make_project code and call project code.
  std::string Build() const {
    TagIndMapping new_tag_id_mapping;
    std::string project_cols_code;
    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();
    std::stringstream ss;
    for (size_t i = 0; i < mappings_.size(); ++i) {
      ss << project_mapping_to_string(ctx_, mappings_[i], new_tag_id_mapping);
      if (i != mappings_.size() - 1) {
        ss << ", ";
      }
    }
    project_cols_code = ss.str();

    boost::format formater(PROJECT_OP_TEMPLATE_STR);
    formater % next_ctx_name % project_is_append_str(is_append_) %
        ctx_.GraphVar() % prev_ctx_name % project_cols_code;
    ctx_.UpdateTagIdAndIndMapping(new_tag_id_mapping);

    return formater.str();
  }

 private:
  BuildingContext& ctx_;
  bool is_append_;
  std::vector<physical::Project::ExprAlias> mappings_;
};

static std::string BuildProjectOp(
    BuildingContext& ctx, const physical::Project& project_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  ProjectOpBuilder builder(ctx);
  builder.is_append(project_pb.is_append());
  auto& mappings = project_pb.mappings();
  for (int32_t i = 0; i < mappings.size(); ++i) {
    builder.add_mapping(mappings[i]);
  }
  return builder.Build();
}
}  // namespace gs

#endif  // CODEGEN_SRC_HQPS_HQPS_PROJECT_BUILDER_H_
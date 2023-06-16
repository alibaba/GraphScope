#ifndef PROJECT_BUILDER_H
#define PROJECT_BUILDER_H

#include <sstream>
#include <string>
#include <vector>

#include "proto_generated_gie/algebra.pb.h"
#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

#include "flex/codegen/building_context.h"
#include "flex/codegen/graph_types.h"
#include "flex/codegen/op_builder/case_when_builder.h"
#include "flex/codegen/op_builder/expr_builder.h"
#include "flex/codegen/pb_parser/query_params_parser.h"
#include "flex/codegen/codegen_utils.h"

namespace gs {

// to check the output type of case when is the same.
bool sanity_check(const common::Case& expr_case) {
  auto& when_exprs = expr_case.when_then_expressions();
  auto& else_expr = expr_case.else_result_expression();

  // TODO: implement this check
  return true;
}

// There can be expression in project's mappings
// 0. project common expression
// 1. project case when.
// NOTE: the return type of all case-when and else should be all the
// same(excluding null)
void parse_case_when_from_project_mapping(BuildingContext& ctx,
                                          const common::Case& expr_case,
                                          common::DataType data_type,
                                          std::stringstream& ss,
                                          int32_t out_alias_tag) {
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
  builder.return_type(data_type)
      .input_expr(expr_case.input_expression())
      .when_then_exprs(expr_case.when_then_expressions())
      .else_expr(expr_case.else_result_expression());

  std::string expr_func_name, expr_code;
  std::vector<codegen::ParamConst> func_call_param_const;
  std::vector<std::string> expr_tag_props;
  common::DataType ret_data_type;  // returned data type for case when building
  std::tie(expr_func_name, func_call_param_const, expr_tag_props, expr_code,
           ret_data_type) = builder.Build();

  ctx.AddExprCode(expr_code);
  if (func_call_param_const.size() > 0) {
    for (auto& param_const : func_call_param_const) {
      ctx.AddParameterVar(param_const);
    }
  }

  auto data_type_name = common_data_type_pb_2_str(ret_data_type);
  // make_project_with_expr
  ss << MAKE_PROJECT_EXPR << "<";
  ss << out_alias_tag << ",";
  ss << data_type_name;
  ss << ">";
  ss << "(";

  ss << expr_func_name << "(";
  for (auto i = 0; i < func_call_param_const.size(); ++i) {
    ss << func_call_param_const[i].var_name;
    if (i != func_call_param_const.size() - 1) {
      ss << ", ";
    }
  }
  if (expr_tag_props.size() > 0) {
    for (auto i = 0; i < expr_tag_props.size() - 1; ++i) {
      ss << expr_tag_props[i] << ", ";
    }
    ss << expr_tag_props[expr_tag_props.size() - 1];
  }
  ss << ")";
  ss << ")";
}

void prase_expression_from_project_mapping(BuildingContext& ctx,
                                           const common::Expression& expr,
                                           std::stringstream& ss,
                                           int32_t out_alias_tag) {
  auto expr_builder = ExprBuilder(ctx);
  CHECK(expr.operators_size() == 3)
      << "Current only support binary expression for project";
  CHECK(expr.operators(1).has_node_type());
  auto data_type_name =
      common_data_type_pb_2_str(expr.operators(1).node_type().data_type());
  LOG(INFO) << "data_typename: " << data_type_name;
  expr_builder.AddAllExprOpr(expr.operators());
  std::string expr_func_name, expr_code;
  std::vector<codegen::ParamConst> func_call_param_const;
  std::vector<std::string> expr_tag_props;
  common::DataType unused_expr_ret_type;
  std::tie(expr_func_name, func_call_param_const, expr_tag_props, expr_code,
           unused_expr_ret_type) = expr_builder.Build();

  ctx.AddExprCode(expr_code);
  // make_project_with_expr
  ss << MAKE_PROJECT_EXPR << "<";
  ss << out_alias_tag << ",";
  ss << data_type_name;
  ss << ">";
  ss << "(";

  ss << expr_func_name << "(";
  for (auto i = 0; i < func_call_param_const.size(); ++i) {
    ss << func_call_param_const[i].var_name;
    if (i != func_call_param_const.size() - 1) {
      ss << ", ";
    }
  }
  if (expr_tag_props.size() > 0) {
    for (auto i = 0; i < expr_tag_props.size() - 1; ++i) {
      ss << expr_tag_props[i] << ", ";
    }
    ss << expr_tag_props[expr_tag_props.size() - 1];
  }
  ss << ")";
  ss << ")";
}

std::string project_mapping_to_string(
    BuildingContext& ctx, const physical::Project::ExprAlias& mapping,
    TagIndMapping& new_tag_ind_map) {
  std::stringstream ss;
  int32_t res_alias = mapping.alias().value();
  // TODO: Currenly we assume each expr_alias contains only property for that
  // input tag
  int32_t in_tag_id = -2;
  std::vector<std::string> prop_names;
  std::vector<codegen::DataType> data_types;
  bool project_self = false;

  auto real_res_alias = new_tag_ind_map.CreateOrGetTagInd(res_alias);

  auto& expr = mapping.expr();
  // CHECK(expr.operators_size() == 1) << "can only support one variable";
  if (expr.operators_size() > 1) {
    prase_expression_from_project_mapping(ctx, expr, ss, real_res_alias);
  } else if (expr.operators_size() == 1) {
    auto& expr_op = expr.operators(0);
    switch (expr_op.item_case()) {
    case common::ExprOpr::kCase: {
      LOG(INFO) << "Got case when in projecting";
      auto case_when = expr_op.case_();
      LOG(INFO) << case_when.DebugString();
      CHECK(expr_op.node_type().type_case() == common::IrDataType::kDataType);
      parse_case_when_from_project_mapping(
          ctx, case_when, expr_op.node_type().data_type(), ss, real_res_alias);
      // just reuturn.
      return ss.str();
    }
    case common::ExprOpr::kVar: {
      LOG(INFO) << "Got var in projecting";
      auto& var = expr_op.var();
      in_tag_id = var.tag().id();
      if (var.has_property()) {
        auto& prop = var.property();
        // if (prop.has_id()) {
        if (prop.item_case() == common::Property::kId) {
          // project itself.
          project_self = true;
          // } else if (prop.has_key()) {
        } else if (prop.item_case() == common::Property::kKey) {
          prop_names.push_back(prop.key().name());
          data_types.push_back(
              common_data_type_pb_2_data_type(var.node_type().data_type()));
        } else {
          LOG(FATAL) << "Unknown property type" << prop.DebugString();
        }
      } else {
        LOG(INFO) << "receives no property, project itself";
        project_self = true;
      }
      break;
    }
    case common::ExprOpr::kVarMap: {
      LOG(INFO) << "Got variable map in projecting";
      LOG(WARNING) << "CURRENTLY we flat the var map to a list of variables";
    }

    case common::ExprOpr::kVars: {
      LOG(INFO) << "Got variable keys in projecting";
      // project properties to a list.
      auto& vars =
          expr_op.has_vars() ? expr_op.vars().keys() : expr_op.var_map().keys();
      for (auto i = 0; i < vars.size(); ++i) {
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

    auto real_in_tag_id = ctx.GetTagInd(in_tag_id);
    LOG(INFO) << "real_in_tag_id: " << real_in_tag_id
              << " in_tag_id: " << in_tag_id;

    if (project_self) {
      LOG(INFO) << "Projecting self";
      CHECK(prop_names.size() == 0 && data_types.size() == 0);
      ss << PROJECT_SELF_STR << "<" << real_in_tag_id << ", " << real_res_alias
         << ">()";
    } else {
      LOG(INFO) << "Projecting properties" << gs::to_string(prop_names);
      CHECK(prop_names.size() == data_types.size());
      CHECK(prop_names.size() > 0);
      ss << PROJECT_PROPS_STR << "<" << real_in_tag_id << ", "
         << real_res_alias;
      for (auto i = 0; i < data_types.size(); ++i) {
        ss << "," << data_type_2_string(data_types[i]);
      }
      ss << ">({";
      for (auto i = 0; i < prop_names.size() - 1; ++i) {
        ss << "\"" << prop_names[i] << "\", ";
      }
      ss << "\"" << prop_names[prop_names.size() - 1] << "\"";
      ss << "})";
    }
  } else {
    LOG(FATAL) << "expect at least one expr opr";
  }

  return ss.str();
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
  std::pair<std::string, std::string> Build() const {
    std::string project_opt_name;
    std::string project_opt_code;
    std::string call_project_code;
    TagIndMapping new_tag_id_mapping;
    {
      std::stringstream ss;
      project_opt_name = ctx_.GetNextProjectOptName();
      ss << "auto " << project_opt_name << " " << _ASSIGN_STR_ << " "
         << MAKE_PROJECT_OPT_NAME;
      ss << "(";
      for (auto i = 0; i < mappings_.size() - 1; ++i) {
        ss << project_mapping_to_string(ctx_, mappings_[i], new_tag_id_mapping)
           << ", ";
      }
      ss << project_mapping_to_string(ctx_, mappings_[mappings_.size() - 1],
                                      new_tag_id_mapping);
      ss << ");" << std::endl;

      project_opt_code = ss.str();
    }
    ctx_.UpdateTagIdAndIndMapping(new_tag_id_mapping);

    {
      std::stringstream ss;
      std::string prev_ctx_name, next_ctx_name;
      std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();
      ss << "auto " << next_ctx_name << " " << _ASSIGN_STR_
         << " Engine::template ";
      ss << "Project<" << std::to_string(is_append_) << ">";

      ss << "(";
      ss << ctx_.TimeStampVar() << "," << ctx_.GraphVar() << ", std::move("
         << prev_ctx_name << "), ";
      ss << "std::move(" << project_opt_name << ")";
      ss << ");" << std::endl;
      call_project_code = ss.str();
    }

    return std::make_pair(project_opt_code, call_project_code);
  }

 private:
  BuildingContext& ctx_;
  bool is_append_;
  std::vector<physical::Project::ExprAlias> mappings_;
};

static std::pair<std::string, std::string> BuildProjectOp(
    BuildingContext& ctx, const physical::Project& project_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  ProjectOpBuilder builder(ctx);
  builder.is_append(project_pb.is_append());
  auto& mappings = project_pb.mappings();
  for (auto i = 0; i < mappings.size(); ++i) {
    builder.add_mapping(mappings[i]);
  }
  return builder.Build();
}
}  // namespace gs

#endif  // PROJECT_BUILDER_H
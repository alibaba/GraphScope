#ifndef UTILS_H
#define UTILS_H

#include <sstream>
#include <string>

#include "flex/codegen/string_utils.h"
#include "flex/engines/hqps/engine/hqps_utils.h"
#include "proto_generated_gie/physical.pb.h"

namespace gs {

// check type consistent
bool data_type_consistent(const common::DataType& left,
                          const common::DataType& right) {
  if (left == common::DataType::NONE || right == common::DataType::NONE) {
    return true;
  }
  return left == right;
}

std::string decode_param_from_decoder(std::stringstream& ss,
                                      const codegen::ParamConst& param_const,
                                      int32_t ind,
                                      const std::string& var_prefix,
                                      const std::string& decoder_name) {
  std::string var_name = var_prefix + std::to_string(ind);
  ss << _4_SPACES;
  ss << data_type_2_string(param_const.type) << " " << var_name << " = ";
  ss << decoder_name << "." << decode_type_as_str(param_const.type) << ";";
  ss << std::endl;
  return var_name;
}

template <typename T>
void intersection(std::vector<T>& v1, std::vector<T>& v2) {
  std::vector<T> res;
  for (auto num : v1) {
    for (int i = 0; i < v2.size(); i++) {
      if (num == v2[i]) {
        res.push_back(num);
        break;
      }
    }
  }
  res.swap(v1);
}

static std::vector<std::string> add_quotes(
    const std::vector<std::string>& strs) {
  std::vector<std::string> res;
  for (auto& str : strs) {
    res.emplace_back("\"" + str + "\"");
  }
  return res;
}

static std::string with_quote(std::string res) { return "\"" + res + "\""; }

static std::string make_named_property(
    const std::vector<std::string>& prop_names,
    const std::vector<std::string>& prop_types) {
  std::stringstream ss;
  auto quoted_prop_names = add_quotes(prop_names);
  std::string prop_names_str = gs::to_string(quoted_prop_names);
  std::string prop_types_str = gs::to_string(prop_types);
  ss << NAMED_PROPERTY_CLASS_NAME << "<" << prop_types_str << ">";
  ss << "(";
  // ss << "{" << prop_names_str << "}";
  ss << prop_names_str;
  ss << ")";
  return ss.str();
}

static std::string make_inner_id_property(int tag_id, std::string prop_type) {
  std::stringstream ss;
  ss << INNER_ID_PROPERTY_NAME << "<" << tag_id << ">{}";
  return ss.str();
}

// The input variable can have property or not, if property is not present, we
// take that as a IdKey
static std::string variable_to_named_property(BuildingContext& ctx,
                                              const common::Variable& var) {
  if (var.has_property()) {
    std::vector<std::string> prop_names{var.property().key().name()};
    std::vector<std::string> prop_types{data_type_2_string(
        common_data_type_pb_2_data_type(var.node_type().data_type()))};
    LOG(INFO) << "extract prop names: " << gs::to_string(prop_names);
    LOG(INFO) << "extract prop types: " << gs::to_string(prop_types);
    return make_named_property(prop_names, prop_types);
  } else {
    // if variable has no property, we assume it means get the innerIdProperty
    // there are two cases:
    // 0 : vertex, but the node type is passed as all property and types.
    // 1: collection, just take the value;
    std::string prop_types;
    if (var.node_type().type_case() == common::IrDataType::kDataType) {
      prop_types = data_type_2_string(
          common_data_type_pb_2_data_type(var.node_type().data_type()));
    } else {
      prop_types = VERTEX_ID_T;
    }
    // get tag_id
    int tag_id = -1;
    if (var.has_tag()) {
      tag_id = var.tag().id();
    }
    int real_tag_ind = ctx.GetTagInd(tag_id);

    return make_inner_id_property(real_tag_ind, prop_types);
  }
}

static void fill_sample_expr(common::Expression& expr) {
  auto left = expr.add_operators();
  {
    auto var = left->mutable_var();
    auto prop = var->mutable_property();
    auto prop_name = prop->mutable_key();
    prop_name->set_name("prop1");
  }
  auto mid = expr.add_operators();
  { mid->set_logical(common::Logical::EQ); }

  auto right = expr.add_operators();
  {
    auto val = right->mutable_const_();
    val->set_i64(1);
  }
}

// fill paramterize expression
static void fill_oid_param_expr(common::Expression& expr) {
  auto left = expr.add_operators();
  {
    auto var = left->mutable_var();
    auto prop = var->mutable_property();
    auto prop_name = prop->mutable_key();
    prop_name->set_name("prop1");
  }
  auto mid = expr.add_operators();
  { mid->set_logical(common::Logical::EQ); }

  auto right = expr.add_operators();
  {
    auto val = right->mutable_param();
    // val->set_data_type(common::DataType::INT64);
    val->mutable_data_type()->set_data_type(common::DataType::INT64);
    val->set_name("oid");
  }
}

// @.joinDate > 2019-01-01
static void fill_join_date_expr(common::Expression& expr) {
  auto left = expr.add_operators();
  {
    auto var = left->mutable_var();
    auto prop = var->mutable_property();
    auto prop_name = prop->mutable_key();
    var->mutable_node_type()->set_data_type(common::DataType::INT64);
    prop_name->set_name("joinDate");
  }
  auto mid = expr.add_operators();
  { mid->set_logical(common::Logical::LE); }

  auto right = expr.add_operators();
  {
    auto val = right->mutable_param();
    // val->set_data_type(common::DataType::INT64);
    val->mutable_data_type()->set_data_type(common::DataType::INT64);
    val->set_name("min_join_date");
  }
}

// (label_key == 1) && (id == 2)
static void make_expr_with_label_key(common::Expression& expr) {
  {
    // left brace
    auto left_brace = expr.add_operators();
    left_brace->set_brace(common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE);
  }
  {
    auto left = expr.add_operators();
    auto var = left->mutable_var();
    auto prop = var->mutable_property();
    auto prop_name = prop->mutable_label();
  }

  {
    auto mid = expr.add_operators();
    mid->set_logical(common::Logical::WITHIN);
  }

  {
    auto right = expr.add_operators();
    auto array = right->mutable_const_()->mutable_i64_array();
    array->add_item(1);
  }
  {
    // left brace
    auto left_brace = expr.add_operators();
    left_brace->set_brace(common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE);
  }

  {
    auto op = expr.add_operators();
    op->set_logical(common::Logical::AND);
  }

  {
    // left brace
    auto left_brace = expr.add_operators();
    left_brace->set_brace(common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE);
  }

  {
    auto left2 = expr.add_operators();
    auto var = left2->mutable_var();
    auto prop = var->mutable_property();
    auto prop_name = prop->mutable_key();
    var->mutable_node_type()->set_data_type(common::DataType::INT64);
    prop_name->set_name("id");
  }

  {
    auto mid2 = expr.add_operators();
    mid2->set_logical(common::Logical::EQ);
  }

  {
    auto right2 = expr.add_operators();
    auto val = right2->mutable_const_();
    val->set_i64(2);
  }

  {
    // left brace
    auto left_brace = expr.add_operators();
    left_brace->set_brace(common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE);
  }
}

static void make_scan_op_pb(physical::Scan& scan_op,
                            physical::PhysicalOpr::MetaData& meta_data,
                            int res_alias) {
  algebra::QueryParams* query_params = scan_op.mutable_params();

  auto table = query_params->add_tables();
  // table->set_name("person");
  table->set_id(0);

  auto predicate = query_params->mutable_predicate();
  fill_sample_expr(*predicate);

  scan_op.set_scan_opt(physical::Scan::ScanOpt::Scan_ScanOpt_VERTEX);
  auto res_alias_ = scan_op.mutable_alias();
  res_alias_->set_value(res_alias);

  // meta
  auto vertex_ele = meta_data.mutable_type()->mutable_graph_type();
  vertex_ele->set_element_opt(common::GraphDataType::GraphElementOpt::
                                  GraphDataType_GraphElementOpt_VERTEX);
}

static void make_edge_expand_v_single_label_no_expr_op_pb(
    physical::EdgeExpand& edge_expand_op,
    physical::PhysicalOpr::MetaData& meta_data, int v_tag, int res_alias) {
  auto ir_data_type = meta_data.mutable_type();
  auto ir_graph_type = ir_data_type->mutable_graph_type();
  ir_graph_type->set_element_opt(common::GraphDataType::GraphElementOpt::
                                     GraphDataType_GraphElementOpt_VERTEX);

  algebra::QueryParams* query_params = edge_expand_op.mutable_params();

  auto table = query_params->add_tables();
  // table->set_name("knows");
  table->set_id(1);

  edge_expand_op.set_expand_opt(
      physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
  edge_expand_op.mutable_alias()->set_value(res_alias);

  // direction
  edge_expand_op.set_direction(
      physical::EdgeExpand::Direction::EdgeExpand_Direction_OUT);

  // applied tag
  edge_expand_op.mutable_v_tag()->set_value(v_tag);
}

static void make_edge_expand_v_single_label_op_pb(
    physical::EdgeExpand& edge_expand_op,
    physical::PhysicalOpr::MetaData& meta_data, int v_tag, int res_alias) {
  auto ir_data_type = meta_data.mutable_type();
  auto ir_graph_type = ir_data_type->mutable_graph_type();
  ir_graph_type->set_element_opt(common::GraphDataType::GraphElementOpt::
                                     GraphDataType_GraphElementOpt_VERTEX);

  algebra::QueryParams* query_params = edge_expand_op.mutable_params();

  auto table = query_params->add_tables();
  // table->set_name("knows");
  table->set_id(1);
  {
    // add expression
    auto expr = query_params->mutable_predicate();
    fill_join_date_expr(*expr);
  }

  edge_expand_op.set_expand_opt(
      physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
  edge_expand_op.mutable_alias()->set_value(res_alias);

  // direction
  edge_expand_op.set_direction(
      physical::EdgeExpand::Direction::EdgeExpand_Direction_OUT);

  // applied tag
  edge_expand_op.mutable_v_tag()->set_value(v_tag);
}

static void make_edge_expand_v_two_label_op_pb(
    physical::EdgeExpand& edge_expand_op,
    physical::PhysicalOpr::MetaData& meta_data, int v_tag, int res_alias) {
  auto ir_data_type = meta_data.mutable_type();
  auto ir_graph_type = ir_data_type->mutable_graph_type();
  ir_graph_type->set_element_opt(common::GraphDataType::GraphElementOpt::
                                     GraphDataType_GraphElementOpt_VERTEX);

  algebra::QueryParams* query_params = edge_expand_op.mutable_params();

  auto table = query_params->add_tables();
  // table->set_name("knows");
  table->set_id(1);

  edge_expand_op.set_expand_opt(
      physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX);
  edge_expand_op.mutable_alias()->set_value(res_alias);

  // direction
  edge_expand_op.set_direction(
      physical::EdgeExpand::Direction::EdgeExpand_Direction_OUT);

  // applied tag
  edge_expand_op.mutable_v_tag()->set_value(v_tag);
}

static void make_edge_expand_e_one_label_op_pb(
    physical::EdgeExpand& edge_expand_op,
    physical::PhysicalOpr::MetaData& meta_data, int v_tag, int res_alias) {
  auto ir_data_type = meta_data.mutable_type();
  auto ir_graph_type = ir_data_type->mutable_graph_type();
  ir_graph_type->set_element_opt(common::GraphDataType::GraphElementOpt::
                                     GraphDataType_GraphElementOpt_EDGE);
  auto graph_ele_type = ir_data_type->mutable_graph_type();
  auto first_ele_type = graph_ele_type->add_graph_data_type();
  {
    // first_ele_type.label is ignoreed;
    auto prop = first_ele_type->add_props();
    auto prop_name_or_id = prop->mutable_prop_id();
    prop_name_or_id->set_name("creationDate");
    prop->set_type(common::DataType::INT64);

    auto prop2 = first_ele_type->add_props();
    auto prop_name_or_id2 = prop2->mutable_prop_id();
    prop_name_or_id2->set_name("weight");
    prop2->set_type(common::DataType::DOUBLE);
  }

  algebra::QueryParams* query_params = edge_expand_op.mutable_params();

  auto table = query_params->add_tables();
  // table->set_name("knows");
  table->set_id(0);

  edge_expand_op.set_expand_opt(
      physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_EDGE);
  edge_expand_op.mutable_alias()->set_value(res_alias);

  // direction
  edge_expand_op.set_direction(
      physical::EdgeExpand::Direction::EdgeExpand_Direction_IN);

  // applied tag
  edge_expand_op.mutable_v_tag()->set_value(v_tag);
}

// id == 1
void make_sample_exprs(common::Expression& exprs) {
  {
    auto expr = exprs.add_operators();
    auto left_var = expr->mutable_var();
    left_var->mutable_tag()->set_id(-1);
    left_var->mutable_property()->mutable_key()->set_name("id");
    left_var->mutable_node_type()->set_data_type(common::DataType::INT64);
    expr->mutable_node_type()->set_data_type(common::DataType::INT64);
  }
  {
    auto expr2 = exprs.add_operators();
    expr2->set_logical(common::Logical::EQ);
    expr2->mutable_node_type()->set_data_type(common::DataType::INT64);
  }
  {
    auto expr3 = exprs.add_operators();
    expr3->mutable_const_()->set_i64(1);
    expr3->mutable_node_type()->set_data_type(common::DataType::INT64);
  }
}

// @.id != 1
void make_sample_exprs_with_params(common::Expression& exprs) {
  {
    auto expr = exprs.add_operators();
    auto left_var = expr->mutable_var();
    left_var->mutable_tag()->set_id(-1);
    left_var->mutable_property()->mutable_key()->set_name("id");
    left_var->mutable_node_type()->set_data_type(common::DataType::INT64);
    expr->mutable_node_type()->set_data_type(common::DataType::INT64);
  }
  {
    auto expr2 = exprs.add_operators();
    expr2->set_logical(common::Logical::EQ);
    expr2->mutable_node_type()->set_data_type(common::DataType::INT64);
  }
  {
    auto expr3 = exprs.add_operators();
    expr3->mutable_param()->set_name("person_id");
    // expr3->mutable_param()->set_data_type(common::DataType::INT64);
    expr3->mutable_param()->mutable_data_type()->set_data_type(
        common::DataType::INT64);
    // expr3->mutable_const_()->set_i64(1);
    // expr3->mutable_node_type()->set_data_type(common::DataType::INT64);
  }
}

void make_select_op_pb(algebra::Select& select,
                       physical::PhysicalOpr::MetaData& meta_data) {
  auto exprs = select.mutable_predicate();
  make_sample_exprs_with_params(*exprs);
}

void make_sort_op_pb(algebra::OrderBy& sort_pb) {
  // add order_pairs
  {
    auto pair = sort_pb.add_pairs();
    pair->set_order(
        algebra::OrderBy::OrderingPair::Order::OrderBy_OrderingPair_Order_ASC);
    pair->mutable_key()->mutable_tag()->set_id(-1);
    pair->mutable_key()->mutable_property()->mutable_key()->set_name("id");
    pair->mutable_key()->mutable_node_type()->set_data_type(
        common::DataType::INT64);
  }
  {
    auto pair = sort_pb.add_pairs();
    pair->set_order(
        algebra::OrderBy::OrderingPair::Order::OrderBy_OrderingPair_Order_DESC);
    pair->mutable_key()->mutable_tag()->set_id(-1);
    pair->mutable_key()->mutable_property()->mutable_key()->set_name("name");
    pair->mutable_key()->mutable_node_type()->set_data_type(
        common::DataType::STRING);
  }
  //  sort_pb.mutable_limit()->mutable_lower()->set_value(0);
  //  sort_pb.mutable_limit()->mutable_upper()->set_value(10);
  sort_pb.mutable_limit()->set_lower(0);
  sort_pb.mutable_limit()->set_upper(10);
}

void make_dedup_op_pb(algebra::Dedup& dedup_pb) {
  auto key = dedup_pb.add_keys();
  key->mutable_tag()->set_id(-1);
  key->mutable_property()->mutable_id();
}

void make_project_op_pb(physical::Project& project_pb) {
  project_pb.set_is_append(true);
  {
    auto mapping = project_pb.add_mappings();
    mapping->mutable_alias()->set_value(1);
    auto expr = mapping->mutable_expr();
    {
      auto expr_opr = expr->add_operators();
      auto var = expr_opr->mutable_var();
      var->mutable_tag()->set_id(-1);
      var->mutable_property()->mutable_key()->set_name("id");
      var->mutable_node_type()->set_data_type(common::DataType::INT64);
      expr_opr->mutable_node_type()->set_data_type(common::DataType::INT64);
    }
  }
  {
    auto mapping = project_pb.add_mappings();
    mapping->mutable_alias()->set_value(2);
    auto expr = mapping->mutable_expr();
    {
      auto expr_opr = expr->add_operators();
      auto vars = expr_opr->mutable_var_map();
      {
        auto var = vars->add_keys();
        var->mutable_tag()->set_id(-1);
        var->mutable_property()->mutable_key()->set_name("id");
        var->mutable_node_type()->set_data_type(common::DataType::INT64);
        expr_opr->mutable_node_type()->set_data_type(common::DataType::INT64);
      }
      {
        auto var = vars->add_keys();
        var->mutable_tag()->set_id(-1);
        var->mutable_property()->mutable_key()->set_name("creationDate");
        var->mutable_node_type()->set_data_type(common::DataType::INT64);
        expr_opr->mutable_node_type()->set_data_type(common::DataType::INT64);
      }
    }
  }
}

void make_getv_op_pb(physical::GetV& getv_pb) {
  getv_pb.set_opt(physical::GetV::VOpt::GetV_VOpt_ITSELF);
  getv_pb.mutable_tag()->set_value(-1);
  getv_pb.mutable_alias()->set_value(1);
  auto table = getv_pb.mutable_params()->add_tables();
  table->set_id(1);
  auto expr = *getv_pb.mutable_params()->mutable_predicate();
  {
    auto expr_opr = expr.add_operators();
    auto left_var = expr_opr->mutable_var();
    left_var->mutable_tag()->set_id(-1);
    left_var->mutable_property()->mutable_key()->set_name("id");
    left_var->mutable_node_type()->set_data_type(common::DataType::INT64);
    expr_opr->mutable_node_type()->set_data_type(common::DataType::INT64);
  }
  {
    auto expr_opr = expr.add_operators();
    expr_opr->set_logical(common::Logical::EQ);
    expr_opr->mutable_node_type()->set_data_type(common::DataType::INT64);
  }
  {
    auto expr_opr = expr.add_operators();
    expr_opr->mutable_param()->set_name("person_id");
    expr_opr->mutable_param()->mutable_data_type()->set_data_type(
        common::DataType::INT64);
    // expr3->mutable_const_()->set_i64(1);
    // expr3->mutable_node_type()->set_data_type(common::DataType::INT64);
  }
}

void make_sinple_getv_op_pb(physical::GetV& getv_pb) {
  getv_pb.set_opt(physical::GetV::VOpt::GetV_VOpt_END);
  getv_pb.mutable_tag()->set_value(-1);
  getv_pb.mutable_alias()->set_value(1);
  auto table = getv_pb.mutable_params()->add_tables();
  table->set_id(1);
}

void make_path_expand_op_pb(
    physical::PathExpand& path_expand_op_pb,
    google::protobuf::RepeatedPtrField<physical::PhysicalOpr::MetaData>&
        meta_data,
    int32_t in_tag, int32_t out_tag, int32_t lower, int32_t upper) {
  path_expand_op_pb.mutable_start_tag()->set_value(in_tag);
  path_expand_op_pb.mutable_alias()->set_value(out_tag);
  // path_expand_op_pb.mutable_hop_range()->mutable_lower()->set_value(lower);
  // path_expand_op_pb.mutable_hop_range()->mutable_upper()->set_value(upper);
  path_expand_op_pb.mutable_hop_range()->set_lower(lower);
  path_expand_op_pb.mutable_hop_range()->set_upper(upper);
  path_expand_op_pb.set_path_opt(
      physical::PathExpand::PathOpt::PathExpand_PathOpt_ARBITRARY);
  path_expand_op_pb.set_result_opt(
      physical::PathExpand::ResultOpt::PathExpand_ResultOpt_END_V);
  {
    auto base = path_expand_op_pb.mutable_base();
    {
      auto edge_expand = base->mutable_edge_expand();
      make_edge_expand_v_single_label_no_expr_op_pb(*edge_expand, meta_data[0],
                                                    -1, 0);
    }
    {
      auto getv = base->mutable_get_v();
      make_sinple_getv_op_pb(*getv);
    }
  }
}

void make_fold_op_pb(physical::GroupBy& group_by_op,
                     physical::PhysicalOpr::MetaData& meta_data) {
  {
    auto agg_func = group_by_op.add_functions();
    agg_func->set_aggregate(
        physical::GroupBy::AggFunc::Aggregate::GroupBy_AggFunc_Aggregate_SUM);
    agg_func->mutable_alias()->set_value(2);
    auto agg_var = agg_func->add_vars();
    agg_var->mutable_tag()->set_id(1);
    agg_var->mutable_property()->mutable_key()->set_name("weight");
    agg_var->mutable_node_type()->set_data_type(common::DataType::INT64);
  }
}

void make_group_count_op_pb(physical::GroupBy& group_by_op) {
  // add key_alias;
  {
    auto key_alias = group_by_op.add_mappings();
    key_alias->mutable_alias()->set_value(3);
    key_alias->mutable_key()->mutable_tag()->set_id(1);
    // just create
    key_alias->mutable_key()->mutable_property()->mutable_id();
    key_alias->mutable_key()->mutable_node_type()->set_data_type(
        common::DataType::INT64);
  }

  {
    auto agg_func = group_by_op.add_functions();
    agg_func->set_aggregate(
        physical::GroupBy::AggFunc::Aggregate::GroupBy_AggFunc_Aggregate_COUNT);
    agg_func->mutable_alias()->set_value(2);
    auto agg_var = agg_func->add_vars();
    agg_var->mutable_tag()->set_id(0);
    agg_var->mutable_property()->mutable_id();
    agg_var->mutable_node_type()->set_data_type(common::DataType::INT64);
  }
}

void make_apply_op_pb(physical::Apply& apply_op,
                      physical::PhysicalOpr::MetaData& meta_data,
                      int32_t in_tag, int32_t out_tag) {
  apply_op.mutable_alias()->set_value(out_tag);
  apply_op.set_join_kind(physical::Join::JoinKind::Join_JoinKind_INNER);
  {
    auto inner_op = apply_op.mutable_sub_plan()->add_plan();
    auto inner_edge_expand = inner_op->mutable_opr()->mutable_edge();
    auto inner_meta = inner_op->add_meta_data();
    make_edge_expand_v_single_label_no_expr_op_pb(*inner_edge_expand,
                                                  *inner_meta, -1, 2);
  }

  {
    auto inner_op = apply_op.mutable_sub_plan()->add_plan();
    auto inner_edge_expand = inner_op->mutable_opr()->mutable_edge();
    auto inner_meta = inner_op->add_meta_data();
    make_edge_expand_v_single_label_no_expr_op_pb(*inner_edge_expand,
                                                  *inner_meta, 2, -1);
  }

  {
    // select
    auto inner_op = apply_op.mutable_sub_plan()->add_plan();
    auto inner_select = inner_op->mutable_opr()->mutable_select();
    auto inner_meta = inner_op->add_meta_data();
    make_select_op_pb(*inner_select, *inner_meta);
  }
  {
    // fold
    auto inner_op = apply_op.mutable_sub_plan()->add_plan();
    auto inner_fold = inner_op->mutable_opr()->mutable_group_by();
    auto inner_meta = inner_op->add_meta_data();
    make_fold_op_pb(*inner_fold, *inner_meta);
  }
}

void make_join_op_pb(physical::Join& join_op_pb) {
  join_op_pb.set_join_kind(physical::Join_JoinKind_INNER);
  {
    auto left_key = join_op_pb.add_left_keys();
    left_key->mutable_tag()->set_id(1);
    left_key->mutable_property()->mutable_id();
    left_key->mutable_node_type()->set_data_type(common::DataType::INT64);
  }
  {
    auto right_key = join_op_pb.add_right_keys();
    right_key->mutable_tag()->set_id(2);
    right_key->mutable_property()->mutable_id();
    right_key->mutable_node_type()->set_data_type(common::DataType::INT64);
  }
  {
    auto left_plans = join_op_pb.mutable_left_plan();
    {
      auto left_plan = left_plans->add_plan();
      auto left_edge_expand = left_plan->mutable_opr()->mutable_edge();
      auto left_meta = left_plan->add_meta_data();
      make_edge_expand_v_single_label_no_expr_op_pb(*left_edge_expand,
                                                    *left_meta, 0, 1);
    }
  }
  {
    auto right_plans = join_op_pb.mutable_right_plan();
    {
      auto right_plan = right_plans->add_plan();
      auto right_edge_expand = right_plan->mutable_opr()->mutable_edge();
      auto right_meta = right_plan->add_meta_data();
      make_edge_expand_v_single_label_no_expr_op_pb(*right_edge_expand,
                                                    *right_meta, 0, 1);
    }
    {
      auto right_plan = right_plans->add_plan();
      auto right_edge_expand = right_plan->mutable_opr()->mutable_edge();
      auto right_meta = right_plan->add_meta_data();
      make_edge_expand_v_single_label_no_expr_op_pb(*right_edge_expand,
                                                    *right_meta, 1, 2);
    }
  }
}

void make_sink_op_pb(physical::Sink& sink_op_pb,
                     physical::PhysicalOpr::MetaData& meta_data) {
  return;
}

// make a full query plan
auto make_query_pb(physical::PhysicalPlan& query) {
  auto scan_op = query.add_plan();
  auto scan_op_plan = scan_op->mutable_opr()->mutable_scan();
  auto scan_op_meta = scan_op->add_meta_data();
  make_scan_op_pb(*scan_op_plan, *scan_op_meta, -1);

  auto path_expand_op = query.add_plan();
  auto path_expand_op_plan = path_expand_op->mutable_opr()->mutable_path();
  auto path_expand_op_meta = path_expand_op->add_meta_data();
  make_path_expand_op_pb(*path_expand_op_plan,
                         *path_expand_op->mutable_meta_data(), -1, 0, 1, 2);

  auto edge_expand_op = query.add_plan();
  auto edge_expand_op_plan = edge_expand_op->mutable_opr()->mutable_edge();
  auto edge_expand_op_meta = edge_expand_op->add_meta_data();
  make_edge_expand_v_single_label_op_pb(*edge_expand_op_plan,
                                        *edge_expand_op_meta, 0, 1);

  // apply
  auto apply_op = query.add_plan();
  auto apply_op_plan = apply_op->mutable_opr()->mutable_apply();
  auto apply_op_meta = apply_op->add_meta_data();
  make_apply_op_pb(*apply_op_plan, *apply_op_meta, 1, 2);

  // add_sink_op
  auto sink_op = query.add_plan();
  auto sink_op_plan = sink_op->mutable_opr()->mutable_sink();
  auto sink_op_meta = sink_op->add_meta_data();
  make_sink_op_pb(*sink_op_plan, *sink_op_meta);
}

}  // namespace gs

#endif  // UTILS_H
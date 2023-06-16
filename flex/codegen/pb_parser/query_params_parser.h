#ifndef CODEGEN_SRC_QUERY_PARAMS_PARSE_H_
#define CODEGEN_SRC_QUERY_PARAMS_PARSE_H_

#include <vector>

#include "proto_generated_gie/algebra.pb.h"
#include "proto_generated_gie/common.pb.h"

#include "flex/codegen/graph_types.h"

namespace gs {

bool try_to_get_label_name_from_query_params(const algebra::QueryParams& params,
                                             std::string& label_name) {
  // CHECK(params.tables_size() == 1) << "Only 1 lable is possible";
  // if (!params.tables(0).has_name()) {
  if (params.tables(0).item_case() != common::NameOrId::kName) {
    return false;
  }
  label_name = params.tables(0).name();
  return true;
}

bool try_to_get_label_id_from_query_params(const algebra::QueryParams& params,
                                           int32_t& label_id) {
  // CHECK(params.tables_size() == 1) << "Only 1 lable is possible";
  // if (!params.tables(0).has_id()) {
  if (params.tables(0).item_case() != common::NameOrId::kId) {
    return false;
  }
  label_id = params.tables(0).id();
  return true;
}

bool try_to_get_label_id_from_query_params(const algebra::QueryParams& params,
                                           std::vector<int32_t>& label_ids) {
  // CHECK(params.tables_size() == 1) << "Only 1 lable is possible";
  if (params.tables_size() > 0) {
    LOG(WARNING) << "params has more than 1 labels";
  }
  for (auto i = 0; i < params.tables_size(); i++) {
    // if (!params.tables(i).has_id()) {
    if (params.tables(i).item_case() != common::NameOrId::kId) {
      return false;
    }
    label_ids.push_back(params.tables(i).id());
  }
  return true;
}

bool try_to_get_label_ids_from_expr(const common::Expression& expression,
                                    std::vector<int32_t>& label_ids) {
  auto opr_size = expression.operators_size();
  for (auto i = 0; i < opr_size; ++i) {
    auto opr = expression.operators(i);
    if (opr.has_var() && opr.var().property().has_label()) {
      CHECK(i + 2 < opr_size) << "expr is not valid";
      auto mid = expression.operators(i + 1);
      auto right = expression.operators(i + 2);
      if (mid.item_case() == common::ExprOpr::kLogical &&
          mid.logical() == common::Logical::EQ &&
          right.item_case() == common::ExprOpr::kConst) {
        auto const_ = right.const_();
        // if (const_.has_i32()) {
        if (const_.item_case() == common::Value::kI32) {
          label_ids.push_back(right.const_().i32());
          // } else if (const_.has_i64()) {
        } else if (const_.item_case() == common::Value::kI64) {
          label_ids.push_back(right.const_().i64());
        } else {
          LOG(FATAL) << "expect i32 or i64 for label id";
          return false;
        }
        return true;
      } else if (mid.item_case() == common::ExprOpr::kLogical &&
                 mid.logical() == common::Logical::WITHIN &&
                 right.item_case() == common::ExprOpr::kConst) {
        auto const_ = right.const_();
        // if (const_.has_i32_array()) {
        if (const_.item_case() == common::Value::kI32Array) {
          auto array = const_.i32_array();
          CHECK(array.item_size() == 1);
          label_ids.push_back(array.item(0));
          // } else if (const_.has_i64_array()) {
        } else if (const_.item_case() == common::Value::kI64Array) {
          auto array = const_.i64_array();
          // CHECK(array.item_size() == 1);
          for (auto i = 0; i < array.item_size(); ++i) {
            label_ids.push_back(array.item(i));
          }
        } else {
          LOG(FATAL) << "expect i32 or i64 for label id";
          return false;
        }
        return true;
      }
    }
  }
  return false;
}

bool try_to_get_oid_from_expr_impl(const common::Expression& expression,
                                   int64_t& oid) {
  LOG(INFO) << "try get oid from expression";
  if (expression.operators_size() != 3) {
    LOG(INFO) << "operator size gt 3, return false";
    return false;
  }
  auto& left = expression.operators(0);
  auto& mid = expression.operators(1);
  auto& right = expression.operators(2);
  if (!left.has_var()) {
    LOG(INFO) << "First item is not var";
    return false;
  }
  if (mid.item_case() != common::ExprOpr::kLogical ||
      mid.logical() != common::Logical::EQ) {
    LOG(INFO) << "mid item is not eq";
    return false;
  }

  if (!right.has_const_()) {
    LOG(INFO) << "right item is not const";
    return false;
  }
  auto& con_val = right.const_();
  // if (!con_val.has_i64() && !con_val.has_i32()) {
  if (con_val.item_case() != common::Value::kI64 &&
      con_val.item_case() != common::Value::kI32) {
    LOG(INFO) << "right value is not int64 or int32";
    return false;
  }
  // if (con_val.has_i64()) {
  if (con_val.item_case() == common::Value::kI64) {
    oid = con_val.i64();
    return true;
  }
  oid = (int64_t) con_val.i32();
  return true;
}

// entry for parse oid from expression, expression can contains 3 ops or 6 ops.
bool try_to_get_oid_from_expr(const common::Expression& expression,
                              int64_t& oid) {
  auto num_oprs = expression.operators_size();
  LOG(INFO) << "try get oid from expression, size: " << num_oprs;
  if (num_oprs != 3 && num_oprs != 11) {
    LOG(INFO) << "can only support 3 ops or 11 ops expression";
    return false;
  }
  if (num_oprs == 3) {
    return try_to_get_oid_from_expr_impl(expression, oid);
  }
  // TODO: current hacks the implementaion. (label within 1) && (id == 8780)
  common::Expression new_expr;
  new_expr.add_operators()->CopyFrom(expression.operators(7));
  new_expr.add_operators()->CopyFrom(expression.operators(8));
  new_expr.add_operators()->CopyFrom(expression.operators(9));
  return try_to_get_oid_from_expr_impl(new_expr, oid);
}

bool try_to_get_oid_param_from_expr_impl(const common::Expression& expression,
                                         codegen::ParamConst& param_const) {
  LOG(INFO) << "try get oid param from expression";
  if (expression.operators_size() != 3) {
    LOG(INFO) << "operator size gt 3, return false";
    return false;
  }
  auto& left = expression.operators(0);
  auto& mid = expression.operators(1);
  auto& right = expression.operators(2);
  if (!left.has_var()) {
    LOG(INFO) << "First item is not var";
    return false;
  }
  if (mid.item_case() != common::ExprOpr::kLogical ||
      mid.logical() != common::Logical::EQ) {
    LOG(INFO) << "mid item is not eq";
    return false;
  }

  // if (!right.has_param()) {
  if (right.item_case() != common::ExprOpr::kParam) {
    LOG(INFO) << "right item is not param const";
    return false;
  }
  auto& con_val = right.param();
  parse_param_const_from_pb(con_val, right.node_type(), param_const);
  return true;
}

bool try_to_get_oid_param_from_expr(const common::Expression& expression,
                                    codegen::ParamConst& param_const) {
  auto num_oprs = expression.operators_size();
  LOG(INFO) << "try get oid param from expression, size: " << num_oprs;
  if (num_oprs != 3 && num_oprs != 11) {
    LOG(INFO) << "can only support 3 ops or 11 ops expression";
    return false;
  }
  if (num_oprs == 3) {
    return try_to_get_oid_param_from_expr_impl(expression, param_const);
  }
  common::Expression new_expr;
  new_expr.add_operators()->CopyFrom(expression.operators(7));
  new_expr.add_operators()->CopyFrom(expression.operators(8));
  new_expr.add_operators()->CopyFrom(expression.operators(9));
  return try_to_get_oid_param_from_expr_impl(new_expr, param_const);
}

}  // namespace gs

#endif  // CODEGEN_SRC_QUERY_PARAMS_PARSE_H_

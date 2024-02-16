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
#ifndef CODEGEN_SRC_HQPS_HQPS_SCAN_BUILDER_H_
#define CODEGEN_SRC_HQPS_HQPS_SCAN_BUILDER_H_

#include <sstream>
#include <string>
#include <vector>

#include "boost/format.hpp"
#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/pb_parser/query_params_parser.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

// #define FAST_SCAN

namespace gs {

/// Args
/// 1. expr_var_name
/// 2. expr_class_name(const_params)
/// 3. selectors,concatenated string
/// 4. res_ctx_name
/// 5. AppendOpt
/// 6. graph name
/// 7. vertex label
static constexpr const char* SCAN_OP_TEMPLATE_STR =
    "auto %1% = gs::make_filter(%2%(%3%) %4%);\n"
    "auto %5% = Engine::template ScanVertex<%6%>(%7%, %8%, std::move(%1%));\n";

static constexpr const char* SCAN_OP_TEMPLATE_NO_EXPR_STR =
    "auto %1% = Engine::template ScanVertex<%2%>(%3%, %4%, "
    "Filter<TruePredicate>());\n";

/// Args
/// 1. res_ctx_name
/// 2. AppendOpt,
/// 3. graph name
/// 4. vertex label
/// 5. oid
static constexpr const char* SCAN_OP_WITH_OID_ONE_LABEL_TEMPLATE_STR =
    "auto %1% = Engine::template ScanVertexWithOid<%2%,%3%>(%4%, %5%, %6%);\n";

/// Args
/// 1. res_ctx_name
/// 2. AppendOpt,
/// 3. graph name
/// 4. vertex label
/// 5. oid
static constexpr const char* SCAN_OP_WITH_OID_MUL_LABEL_TEMPLATE_STR =
    "auto %1% = Engine::template ScanVertexWithOid<%2%>(%3%, "
    "std::array<label_id_t, %4%>{%5%}, %6%);\n";

/**
 * @brief When building scanOp, we ignore the data type provided in the pb.
 *
 */
class ScanOpBuilder {
 public:
  ScanOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  ScanOpBuilder& scanOpt(const physical::Scan::ScanOpt& opt) {
    if (opt != physical::Scan::ScanOpt::Scan_ScanOpt_VERTEX) {
      throw std::runtime_error(
          std::string("Currently only support from vertex"));
    }
    scan_opt_ = opt;
    return *this;
  }

  ScanOpBuilder& resAlias(const int32_t& res_alias) {
    res_alias_ = ctx_.CreateOrGetTagInd(res_alias);
    CHECK(res_alias_ == -1 || res_alias_ == 0);
    return *this;
  }

  // get required oid from query params
  ScanOpBuilder& queryParams(const algebra::QueryParams& query_params) {
    if (!query_params.has_predicate()) {
      VLOG(10) << "No expr in params";
    }
    CHECK(labels_ids_.empty()) << "label ids should be empty";
    if (!try_to_get_label_id_from_query_params(query_params, labels_ids_)) {
      LOG(FATAL) << "fail to label id from expr";
    }

    // the user provide oid can be a const or a param const
    if (query_params.has_predicate()) {
      auto& predicate = query_params.predicate();
      VLOG(10) << "predicate: " << predicate.DebugString();
      // We first scan the predicate to find whether there is conditions on
      // labels.
      std::vector<int32_t> expr_label_ids;
      if (try_to_get_label_ids_from_expr(predicate, expr_label_ids)) {
        // join expr_label_ids with table_label_ids;
        VLOG(10) << "Found label ids in expr: "
                 << gs::to_string(expr_label_ids);
        intersection(labels_ids_, expr_label_ids);
      }

      auto expr_builder = ExprBuilder(ctx_);
      expr_builder.set_return_type(common::DataType::BOOLEAN);
      expr_builder.AddAllExprOpr(query_params.predicate().operators());

      std::string expr_code;
      std::vector<codegen::ParamConst> func_call_param_const;
      std::vector<std::pair<int32_t, std::string>> expr_tag_props;
      std::vector<common::DataType> unused_expr_ret_type;
      std::tie(expr_func_name_, func_call_param_const, expr_tag_props,
               expr_code, unused_expr_ret_type) = expr_builder.Build();
      VLOG(10) << "Found expr in scan:  " << expr_func_name_;
      // generate code.
      ctx_.AddExprCode(expr_code);
      expr_var_name_ = ctx_.GetNextExprVarName();
      {
        std::stringstream ss;
        for (size_t i = 0; i < func_call_param_const.size(); ++i) {
          ss << func_call_param_const[i].var_name;
          if (i != func_call_param_const.size() - 1) {
            ss << ",";
          }
        }
        expr_construct_params_ = ss.str();
      }
      {
        std::stringstream ss;
        if (expr_tag_props.size() > 0) {
          ss << ",";
          for (size_t i = 0; i + 1 < expr_tag_props.size(); ++i) {
            ss << expr_tag_props[i].second << ", ";
          }
          ss << expr_tag_props[expr_tag_props.size() - 1].second;
        }
        selectors_str_ = ss.str();
      }
    }
    return *this;
  }

  ScanOpBuilder& idx_predicate(const algebra::IndexPredicate& predicate) {
    // check query_params not has predicate.

    // Currently we only support one predicate.
    if (predicate.or_predicates_size() < 1) {
      VLOG(10) << "No predicate in index predicate";
      return *this;
    }
    if (predicate.or_predicates_size() != 1) {
      throw std::runtime_error(
          std::string("Currently only support one predicate"));
    }
    CHECK(expr_func_name_.empty()) << "Predicate is already given by expr";
    auto or_predicate = predicate.or_predicates(0);
    if (or_predicate.predicates_size() != 1) {
      throw std::runtime_error(
          std::string("Currently only support one and predicate"));
    }
    auto triplet = or_predicate.predicates(0);
    if (triplet.value_case() == algebra::IndexPredicate::Triplet::kConst) {
      // FUTURE: check property is really the primary key.
      auto const_value = triplet.const_();
      switch (const_value.item_case()) {
      case common::Value::kI32:
        oid_ = std::to_string(const_value.i32());
        oid_type_name_ = "int32_t";
        break;
      case common::Value::kI64:
        oid_ = std::to_string(const_value.i64());
        oid_type_name_ = "int64_t";
        break;
      case common::Value::kStr:
        oid_ = const_value.str();
        oid_type_name_ = "std::string_view";
      default:
        LOG(FATAL) << "Currently only support int, long as primary key";
      }
      VLOG(1) << "Found oid: " << oid_
              << " in index scan, type: " << oid_type_name_;
    } else {
      // dynamic param
      auto dyn_param_pb = triplet.param();
      auto param_const = param_const_pb_to_param_const(dyn_param_pb);
      VLOG(10) << "receive param const in index predicate: "
               << dyn_param_pb.DebugString();
      ctx_.AddParameterVar(param_const);
      // set to oid_ and oid_type_name_
      oid_ = param_const.var_name;
      oid_type_name_ = data_type_2_string(param_const.type);
    }

    return *this;
  }

  std::string Build() const {
    // 1. If common expression predicate presents, scan with expression
    if (!expr_func_name_.empty()) {
      VLOG(1) << "Scan with expression";
      return scan_with_expr(labels_ids_, expr_var_name_, expr_func_name_,
                            expr_construct_params_, selectors_str_);
    } else {
      // If oid_ not empty, scan with oid
      if (!oid_.empty()) {
        VLOG(1) << "Scan with oid: " << oid_;
        return scan_with_oid(labels_ids_, oid_, oid_type_name_);
      } else {
        // If no oid, scan without expression
        VLOG(1) << "Scan without expression";
        return scan_without_expr(labels_ids_);
      }
    }
  }

 private:
  std::string scan_with_oid(const std::vector<int32_t>& label_ids,
                            const std::string& oid,
                            const std::string& oid_type_name) const {
    VLOG(10) << "Scan with oid: " << oid;
    std::string next_ctx_name = ctx_.GetCurCtxName();
    auto append_opt = res_alias_to_append_opt(res_alias_);

    if (label_ids.size() == 1) {
      boost::format formater(SCAN_OP_WITH_OID_ONE_LABEL_TEMPLATE_STR);
      formater % next_ctx_name % append_opt % oid_type_name % ctx_.GraphVar() %
          label_ids[0] % oid;
      return formater.str();
    } else {
      boost::format formater(SCAN_OP_WITH_OID_MUL_LABEL_TEMPLATE_STR);
      std::stringstream ss;
      for (size_t i = 0; i + 1 < label_ids.size(); ++i) {
        ss << std::to_string(label_ids[i]) << ", ";
      }
      ss << std::to_string(label_ids[label_ids.size() - 1]);
      formater % next_ctx_name % append_opt % ctx_.GraphVar() %
          label_ids.size() % ss.str() % oid;
      return formater.str();
    }
  }

  std::string scan_without_expr(const std::vector<int32_t>& label_ids) const {
    std::string label_ids_str;
    {
      std::stringstream ss;
      CHECK(label_ids.size() > 0);
      if (label_ids.size() == 1) {
        ss << label_ids[0];
      } else {
        ss << "std::array<label_id_t, " << label_ids.size() << "> {";
        for (size_t i = 0; i + 1 < label_ids.size(); ++i) {
          ss << std::to_string(label_ids[i]) << ", ";
        }
        ss << std::to_string(label_ids[label_ids.size() - 1]);
        ss << "}";
      }
      label_ids_str = ss.str();
    }
    boost::format formater(SCAN_OP_TEMPLATE_NO_EXPR_STR);
    formater % ctx_.GetCurCtxName() % res_alias_to_append_opt(res_alias_) %
        ctx_.GraphVar() % label_ids_str;
    return formater.str();
  }

  std::string scan_with_expr(const std::vector<int32_t>& label_ids,
                             const std::string& expr_var_name,
                             const std::string& expr_func_name,
                             const std::string& expr_construct_params,
                             const std::string& selectors_str) const {
    std::string next_ctx_name = ctx_.GetCurCtxName();
    std::string label_ids_str;
    {
      std::stringstream ss;
      CHECK(label_ids.size() > 0);
      if (label_ids.size() == 1) {
        ss << label_ids[0];
      } else {
        ss << "std::array<label_id_t, " << label_ids.size() << "> {";
        for (size_t i = 0; i + 1 < label_ids.size(); ++i) {
          ss << std::to_string(label_ids[i]) << ", ";
        }
        ss << std::to_string(label_ids[label_ids.size() - 1]);
        ss << "}";
      }
      label_ids_str = ss.str();
    }

    boost::format formater(SCAN_OP_TEMPLATE_STR);
    formater % expr_var_name % expr_func_name % expr_construct_params %
        selectors_str % next_ctx_name % res_alias_to_append_opt(res_alias_) %
        ctx_.GraphVar() % label_ids_str;
    return formater.str();
  }

  BuildingContext& ctx_;
  physical::Scan::ScanOpt scan_opt_;
  std::vector<int32_t> labels_ids_;
  std::string expr_var_name_, expr_func_name_, expr_construct_params_,
      selectors_str_;  // The expression decode from params.
  std::string oid_;    // the oid decode from idx predicate, or param name.
  std::string oid_type_name_;
  int res_alias_;
};

static std::string BuildScanOp(
    BuildingContext& ctx, const physical::Scan& scan_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  if (!scan_pb.has_params()) {
    throw std::runtime_error(std::string("expect scan pb has params"));
  }
  auto builder = ScanOpBuilder(ctx).scanOpt(scan_pb.scan_opt());
  if (scan_pb.has_alias()) {
    VLOG(10) << "scan pb has alias" << scan_pb.alias().value();
    builder.resAlias(scan_pb.alias().value());
  } else {
    builder.resAlias(-1);
  }
  builder.queryParams(scan_pb.params());
  if (scan_pb.has_idx_predicate()) {
    builder.idx_predicate(scan_pb.idx_predicate());
  }
  return builder.Build();
}

}  // namespace gs

#endif  // CODEGEN_SRC_HQPS_HQPS_SCAN_BUILDER_H_

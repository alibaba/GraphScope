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

#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "boost/format.hpp"
#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/hqps/hqps_expr_builder.h"
#include "flex/codegen/src/pb_parser/query_params_parser.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"
#include "flex/storages/rt_mutable_graph/schema.h"

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

static constexpr const char* SCAN_OP_BOTH_OID_EXPR_TEMPLATE_STR =
    "auto %1% = gs::make_filter(%2%(%3%) %4%);\n"
    "auto %5% = Engine::template ScanVertexWithOidExpr<%6%, %7%>(%8%, %9%, "
    "%10%, std::move(%1%));\n";

static constexpr const char* SCAN_OP_BOTH_GID_EXPR_TEMPLATE_STR =
    "auto %1% = gs::make_filter(%2%(%3%) %4%);\n"
    "auto %5% = Engine::template ScanVertexWithGidExpr<%6%, %7%>(%8%, %9%, "
    "%10%, std::move(%1%));\n";

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
/// 5. gid
static constexpr const char* SCAN_OP_WITH_GID_ONE_LABEL_TEMPLATE_STR =
    "auto %1% = Engine::template ScanVertexWithGid<%2%,%3%>(%4%, %5%, %6%);\n";

/// Args
/// 1. res_ctx_name
/// 2. AppendOpt,
/// 3. graph name
/// 4. vertex label
/// 5. oid
static constexpr const char* SCAN_OP_WITH_OID_MUL_LABEL_TEMPLATE_STR =
    "auto %1% = Engine::template ScanVertexWithOid<%2%, %3%>(%4%, "
    "std::array<label_id_t, %5%>{%6%}, %7%);\n";

/// Args
/// 1. res_ctx_name
/// 2. AppendOpt,
/// 3. graph name
/// 4. vertex label
/// 5. gid
static constexpr const char* SCAN_OP_WITH_GID_MUL_LABEL_TEMPLATE_STR =
    "auto %1% = Engine::template ScanVertexWithGid<%2%, %3%>(%4%, "
    "std::array<label_id_t, %5%>{%6%}, %7%);\n";

/**
 * @brief When building scanOp, we ignore the data type provided in the pb.
 * The expression can from index predicate or query params, or both.
 */
class ScanOpBuilder {
 public:
  ScanOpBuilder(BuildingContext& ctx, const std::optional<Schema>& schema)
      : ctx_(ctx), schema_(schema), expr_builder_(ctx_) {}

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

      // TODO: make expr_builder a member of ScanOpBuilder
      // auto expr_builder = ExprBuilder(ctx_);
      expr_builder_.set_return_type(common::DataType::BOOLEAN);
      // Add extra (, ) to wrap the code, since we may append index_predicate
      // afterwards.
      common::ExprOpr left_brace, right_brace;
      left_brace.set_brace(common::ExprOpr_Brace_LEFT_BRACE);
      right_brace.set_brace(common::ExprOpr_Brace_RIGHT_BRACE);
      expr_builder_.AddExprOpr(left_brace);
      expr_builder_.AddAllExprOpr(query_params.predicate().operators());
      expr_builder_.AddExprOpr(right_brace);
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

    auto or_predicate = predicate.or_predicates(0);
    if (or_predicate.predicates_size() != 1) {
      throw std::runtime_error(
          std::string("Currently only support one and predicate"));
    }
    auto triplet = or_predicate.predicates(0);
    if (!triplet.has_key()) {
      LOG(FATAL) << "Expect key in index predicate.";
    }
    auto& key = triplet.key();
    if (key.has_id()) {
      // scan with global id.
      scan_with_oid_gid_ = false;
    } else if (key.has_key()) {
      // scan with primary key.We only support one primary key.
      scan_with_oid_gid_ = true;
    } else {
      LOG(FATAL) << "Expect id or key in index predicate.";
    }

    if (triplet.value_case() == algebra::IndexPredicate::Triplet::kConst) {
      // FUTURE: check property is really the primary key.
      auto const_value = triplet.const_();
      switch (const_value.item_case()) {
      case common::Value::kI32:
        oid_or_gid_ = std::to_string(const_value.i32());
        oid_or_gid_type_name_ = "int32_t";
        break;
      case common::Value::kI64:
        oid_or_gid_ = std::to_string(const_value.i64());
        oid_or_gid_type_name_ = "int64_t";
        break;
      case common::Value::kStr:
        oid_or_gid_ = const_value.str();
        oid_or_gid_type_name_ = "std::string_view";
      case common::Value::kI32Array:
        oid_or_gid_ = "";
        for (int32_t i = 0; i < const_value.i32_array().item_size(); ++i) {
          oid_or_gid_ += std::to_string(const_value.i32_array().item(i));
          if (i + 1 != const_value.i32_array().item_size()) {
            oid_or_gid_ += ", ";
          }
        }
        oid_or_gid_type_name_ = "int32_t";
        break;
      case common::Value::kI64Array:
        oid_or_gid_ = "";
        for (int32_t i = 0; i < const_value.i64_array().item_size(); ++i) {
          oid_or_gid_ += std::to_string(const_value.i64_array().item(i));
          if (i + 1 != const_value.i64_array().item_size()) {
            oid_or_gid_ += ", ";
          }
        }
        oid_or_gid_type_name_ = "int64_t";
        break;

      default:
        LOG(FATAL) << "Currently only support int, long as primary key: "
                   << const_value.DebugString();
      }
      VLOG(1) << "Found oid/gid: " << oid_or_gid_
              << " in index scan, type: " << oid_or_gid_type_name_;
    } else {
      // dynamic param
      auto dyn_param_pb = triplet.param();
      auto param_const = param_const_pb_to_param_const(dyn_param_pb);
      VLOG(10) << "receive param const in index predicate: "
               << dyn_param_pb.DebugString();
      ctx_.AddParameterVar(param_const);
      // set to oid_or_gid_ and oid_or_gid_type_name_
      oid_or_gid_ = param_const.var_name;
      oid_or_gid_type_name_ = data_type_2_string(param_const.type);
    }

    get_real_oid_gid_type_name();
    // add std::vector{ %} to oid_or_gid_
    oid_or_gid_ =
        "std::vector<" + oid_or_gid_type_name_ + ">{" + oid_or_gid_ + "}";
    return *this;
  }

  void build_expr(std::string& expr_var_name, std::string& expr_func_name,
                  std::string& expr_construct_params,
                  std::string& selectors_str) const {
    std::string expr_code;
    std::vector<codegen::ParamConst> func_call_param_const;
    std::vector<std::pair<int32_t, std::string>> expr_tag_props;
    std::vector<common::DataType> unused_expr_ret_type;
    std::tie(expr_func_name, func_call_param_const, expr_tag_props, expr_code,
             unused_expr_ret_type) = expr_builder_.Build();
    VLOG(10) << "Found expr in scan:  " << expr_func_name;
    // generate code.
    ctx_.AddExprCode(expr_code);
    expr_var_name = ctx_.GetNextExprVarName();
    {
      std::stringstream ss;
      for (size_t i = 0; i < func_call_param_const.size(); ++i) {
        ss << func_call_param_const[i].var_name;
        if (i != func_call_param_const.size() - 1) {
          ss << ",";
        }
      }
      expr_construct_params = ss.str();
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
      selectors_str = ss.str();
    }
  }

  std::string Build() const {
    // 1. If common expression predicate presents, scan with expression
    if (!expr_builder_.get_return_type().empty()) {
      VLOG(1) << "Scan with expression";

      std::string expr_var_name, expr_func_name, expr_construct_params,
          selectors_str;  // The expression decode from params.

      build_expr(expr_var_name, expr_func_name, expr_construct_params,
                 selectors_str);
      if (oid_or_gid_.empty()) {
        return scan_with_expr(expr_var_name, expr_func_name,
                              expr_construct_params, selectors_str);
      } else {
        return scan_with_expr_and_oid_gid(expr_var_name, expr_func_name,
                                          expr_construct_params, selectors_str);
      }
    } else {
      // If oid_or_gid_ not empty, scan with oid
      if (!oid_or_gid_.empty()) {
        VLOG(1) << "Scan with oid: " << oid_or_gid_;
        return scan_with_oid_gid();
      } else {
        // If no oid, scan without expression
        VLOG(1) << "Scan without expression";
        return scan_without_expr();
      }
    }
  }

 private:
  std::string scan_with_oid_gid() const {
    VLOG(10) << "Scan with oid/gid: " << oid_or_gid_;

    std::string next_ctx_name = ctx_.GetCurCtxName();
    auto append_opt = res_alias_to_append_opt(res_alias_);

    boost::format formater;
    if (labels_ids_.size() == 1) {
      if (scan_with_oid_gid_) {
        formater = boost::format(SCAN_OP_WITH_OID_ONE_LABEL_TEMPLATE_STR);
      } else {
        formater = boost::format(SCAN_OP_WITH_GID_ONE_LABEL_TEMPLATE_STR);
      }
      formater % next_ctx_name % append_opt % oid_or_gid_type_name_ %
          ctx_.GraphVar() % labels_ids_[0] % oid_or_gid_;
      return formater.str();
    } else {
      if (scan_with_oid_gid_) {
        formater = boost::format(SCAN_OP_WITH_OID_MUL_LABEL_TEMPLATE_STR);
      } else {
        formater = boost::format(SCAN_OP_WITH_GID_MUL_LABEL_TEMPLATE_STR);
      }
      std::stringstream ss;
      for (size_t i = 0; i + 1 < labels_ids_.size(); ++i) {
        ss << std::to_string(labels_ids_[i]) << ", ";
      }
      ss << std::to_string(labels_ids_[labels_ids_.size() - 1]);
      formater % next_ctx_name % append_opt % oid_or_gid_type_name_ %
          ctx_.GraphVar() % labels_ids_.size() % ss.str() % oid_or_gid_;
      return formater.str();
    }
  }

  std::string scan_without_expr() const {
    std::string label_ids_str;
    {
      std::stringstream ss;
      CHECK(labels_ids_.size() > 0);
      if (labels_ids_.size() == 1) {
        ss << labels_ids_[0];
      } else {
        ss << "std::array<label_id_t, " << labels_ids_.size() << "> {";
        for (size_t i = 0; i + 1 < labels_ids_.size(); ++i) {
          ss << std::to_string(labels_ids_[i]) << ", ";
        }
        ss << std::to_string(labels_ids_[labels_ids_.size() - 1]);
        ss << "}";
      }
      label_ids_str = ss.str();
    }
    boost::format formater(SCAN_OP_TEMPLATE_NO_EXPR_STR);
    formater % ctx_.GetCurCtxName() % res_alias_to_append_opt(res_alias_) %
        ctx_.GraphVar() % label_ids_str;
    return formater.str();
  }

  std::string scan_with_expr(const std::string& expr_var_name,
                             const std::string& expr_func_name,
                             const std::string& expr_construct_params,
                             const std::string& selectors_str) const {
    std::string next_ctx_name = ctx_.GetCurCtxName();
    std::string label_ids_str;
    {
      std::stringstream ss;
      CHECK(labels_ids_.size() > 0);
      if (labels_ids_.size() == 1) {
        ss << labels_ids_[0];
      } else {
        ss << "std::array<label_id_t, " << labels_ids_.size() << "> {";
        for (size_t i = 0; i + 1 < labels_ids_.size(); ++i) {
          ss << std::to_string(labels_ids_[i]) << ", ";
        }
        ss << std::to_string(labels_ids_[labels_ids_.size() - 1]);
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

  std::string scan_with_expr_and_oid_gid(
      const std::string& expr_var_name, const std::string& expr_func_name,
      const std::string& expr_construct_params,
      const std::string& selectors_str) const {
    std::string next_ctx_name = ctx_.GetCurCtxName();
    std::string label_ids_str;
    {
      std::stringstream ss;
      CHECK(labels_ids_.size() > 0);
      if (labels_ids_.size() == 1) {
        ss << labels_ids_[0];
      } else {
        ss << "std::array<label_id_t, " << labels_ids_.size() << "> {";
        for (size_t i = 0; i + 1 < labels_ids_.size(); ++i) {
          ss << std::to_string(labels_ids_[i]) << ", ";
        }
        ss << std::to_string(labels_ids_[labels_ids_.size() - 1]);
        ss << "}";
      }
      label_ids_str = ss.str();
    }

    boost::format formater;
    if (scan_with_oid_gid_) {
      formater = boost::format(SCAN_OP_BOTH_OID_EXPR_TEMPLATE_STR);
    } else {
      formater = boost::format(SCAN_OP_BOTH_GID_EXPR_TEMPLATE_STR);
    }
    formater % expr_var_name % expr_func_name % expr_construct_params %
        selectors_str % next_ctx_name % res_alias_to_append_opt(res_alias_) %
        oid_or_gid_type_name_ % ctx_.GraphVar() % label_ids_str % oid_or_gid_;
    return formater.str();
  }

  void get_real_oid_gid_type_name() {
    if (scan_with_oid_gid_) {
      if (!schema_.has_value()) {
        LOG(INFO) << "No schema found";
        return;
      }
      auto& real_schema = schema_.value();
      if (labels_ids_.size() < 1) {
        LOG(FATAL) << "No label id found";
      }
      std::unordered_set<std::string> oid_types;
      for (auto label_id : labels_ids_) {
        if (label_id >= real_schema.vertex_label_num()) {
          LOG(FATAL) << "label id " << label_id << " is not a valid label id";
        }
        auto pk_types = real_schema.get_vertex_primary_key(label_id);
        CHECK(pk_types.size() == 1) << "Currently only support one primary key";
        oid_types.insert(pk_type_to_string(std::get<0>(pk_types[0])));
      }
      if (oid_types.size() > 1) {
        LOG(FATAL) << "Find different oid types in different labels: "
                   << oid_types.size()
                   << " types, currently can only support 1.";
      }
      if (oid_types.size() < 1) {
        LOG(FATAL) << "No oid type found in schema";
      }
      LOG(INFO) << "parsed oid type name: " << oid_or_gid_type_name_
                << ", real oid type "
                   "name: "
                << *oid_types.begin();
      oid_or_gid_type_name_ = *oid_types.begin();
    } else {
      // We have builtin gid type. We don't need to check the schema.
      oid_or_gid_type_name_ = "gid_t";
    }
  }

  std::string pk_type_to_string(const gs::PropertyType type) const {
    if (type == gs::PropertyType::Int32()) {
      return "int32_t";
    } else if (type == gs::PropertyType::Int64()) {
      return "int64_t";
    } else if (type == gs::PropertyType::UInt32()) {
      return "uint32_t";
    } else if (type == gs::PropertyType::UInt64()) {
      return "uint64_t";
    } else if (type == gs::PropertyType::StringView()) {
      return "std::string_view";
    } else {
      LOG(FATAL) << "Currently only support int, long, string as primary key";
    }
  }

  BuildingContext& ctx_;
  const std::optional<Schema>& schema_;
  ExprBuilder expr_builder_;
  physical::Scan::ScanOpt scan_opt_;
  std::vector<int32_t> labels_ids_;
  bool scan_with_oid_gid_;  // true if oid , false if gid
  std::string oid_or_gid_;  // the oid decode from idx predicate, or param name,
                            // or the global vertex id.
  std::string oid_or_gid_type_name_;  // the type of oid_or_gid_
  int res_alias_;
};

static std::string BuildScanOp(BuildingContext& ctx,
                               const physical::Scan& scan_pb,
                               const physical::PhysicalOpr::MetaData& meta_data,
                               const std::optional<Schema>& schema) {
  if (!scan_pb.has_params()) {
    throw std::runtime_error(std::string("expect scan pb has params"));
  }
  auto builder = ScanOpBuilder(ctx, schema).scanOpt(scan_pb.scan_opt());
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

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
#include "proto_generated_gie/algebra.pb.h"
#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

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
    "auto %1% = gs::make_filter(%2%(%3%), %4%);\n"
    "auto %5% = Engine::template ScanVertex<%6%>(%7%, %8%, std::move(%1%));\n";

/// Args
/// 1. res_ctx_name
/// 2. AppendOpt,
/// 3. graph name
/// 4. vertex label
/// 5. oid
static constexpr const char* SCAN_OP_WITH_OID_TEMPLATE_STR =
    "auto %1% = Engine::template ScanVertex<%2%>(%3%, %4%, %5%));\n";

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
          std::string("Currently only suppor from vertex"));
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
      throw std::runtime_error(std::string("expect expr in params"));
    }
    query_params_ = query_params;
    return *this;
  }

  std::string Build() const {
    std::string label_name;
    std::vector<int32_t> labels_ids;
    if (!try_to_get_label_name_from_query_params(query_params_, label_name)) {
      LOG(WARNING) << "fail to label name from expr";
      if (!try_to_get_label_id_from_query_params(query_params_, labels_ids)) {
        LOG(FATAL) << "fail to label id from expr";
      }
    }

    // the user provide oid can be a const or a param const
    auto& predicate = query_params_.predicate();
    VLOG(10) << "predicate: " << predicate.DebugString();
    // We first scan the predicate to find whether there is conditions on
    // labels.
    std::vector<int32_t> expr_label_ids;
    if (try_to_get_label_ids_from_expr(predicate, expr_label_ids)) {
      // join expr_label_ids with table_lable_ids;
      VLOG(10) << "Found label ids in expr: " << gs::to_string(expr_label_ids);
      labels_ids = expr_label_ids;
    }
    // CHECK(labels_ids.size() == 1) << "only support one label in scan";

#ifdef FAST_SCAN
    gs::codegen::oid_t oid;
    gs::codegen::ParamConst oid_param;
    if (try_to_get_oid_from_expr(predicate, oid)) {
      VLOG(10) << "Parse oid: " << oid << "from expr";
      return scan_with_oid(label_name, label_id, oid);
    } else if (try_to_get_oid_param_from_expr(predicate, oid_param)) {
      VLOG(10) << "Parse oid param: " << oid_param.var_name << "from expr";
      return scan_with_oid(label_name, label_id, oid_param.var_name);
    } else {
      VLOG(10) << "Fail to parse oid from expr";
      {
#endif
        auto expr_builder = ExprBuilder(ctx_);
        expr_builder.set_return_type(common::DataType::BOOLEAN);
        expr_builder.AddAllExprOpr(predicate.operators());

        std::string expr_func_name, expr_code;
        std::vector<codegen::ParamConst> func_call_param_const;
        std::vector<std::pair<int32_t, std::string>> expr_tag_props;
        common::DataType unused_expr_ret_type;
        std::tie(expr_func_name, func_call_param_const, expr_tag_props,
                 expr_code, unused_expr_ret_type) = expr_builder.Build();
        VLOG(10) << "Found expr in edge_expand_opt:  " << expr_func_name;
        // generate code.
        ctx_.AddExprCode(expr_code);
        std::string expr_var_name = ctx_.GetNextExprVarName();
        std::string expr_construct_params;  // function construction params and
        std::string selectors_str;          // selectors str, concatenated
        {
          std::stringstream ss;
          for (auto i = 0; i < func_call_param_const.size(); ++i) {
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
            for (auto i = 0; i + 1 < expr_tag_props.size(); ++i) {
              ss << expr_tag_props[i].second << ", ";
            }
            ss << expr_tag_props[expr_tag_props.size() - 1].second;
          }
          selectors_str = ss.str();
        }

        // use expression to filter.
        return scan_with_expr(labels_ids, expr_var_name, expr_func_name,
                              expr_construct_params, selectors_str);

#ifdef FAST_SCAN
      }
    }
#endif
  }

 private:
  std::string scan_with_oid(const std::string& label_name,
                            const int32_t& label_id, codegen::oid_t oid) const {
    VLOG(10) << "Scan with fixed oid" << oid;
    std::string next_ctx_name = ctx_.GetCurCtxName();
    auto append_opt = res_alias_to_append_opt(res_alias_);

    boost::format formater(SCAN_OP_WITH_OID_TEMPLATE_STR);
    formater % next_ctx_name % append_opt % ctx_.GraphVar() % label_id % oid;
    return formater.str();
  }
  std::string scan_with_oid(const std::string& label_name,
                            const int32_t& label_id,
                            const std::string& oid) const {
    VLOG(10) << "Scan with dynamic param oid";
    std::string next_ctx_name = ctx_.GetCurCtxName();
    auto append_opt = res_alias_to_append_opt(res_alias_);

    boost::format formater(SCAN_OP_WITH_OID_TEMPLATE_STR);
    formater % next_ctx_name % append_opt % ctx_.GraphVar() % label_id % oid;
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
        for (auto i = 0; i + 1 < label_ids.size(); ++i) {
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
  algebra::QueryParams query_params_;
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
  return builder.queryParams(scan_pb.params()).Build();
}

}  // namespace gs

#endif  // CODEGEN_SRC_HQPS_HQPS_SCAN_BUILDER_H_
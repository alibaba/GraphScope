#ifndef CODEGEN_SRC_SCAN_BUILDER_H_
#define CODEGEN_SRC_SCAN_BUILDER_H_

#include <sstream>
#include <string>
#include <vector>

#include "proto_generated_gie/algebra.pb.h"
#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

#include "flex/codegen/building_context.h"
#include "flex/codegen/graph_types.h"
#include "flex/codegen/pb_parser/query_params_parser.h"
#include "flex/codegen/codegen_utils.h"

//#define FAST_SCAN

namespace gs {

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
    std::stringstream ss;

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
    LOG(INFO) << "predicate: " << predicate.DebugString();
    // We first scan the predicate to find whether there is conditions on
    // labels.
    std::vector<int32_t> expr_label_ids;
    if (try_to_get_label_ids_from_expr(predicate, expr_label_ids)) {
      // join expr_label_ids with table_lable_ids;
      LOG(INFO) << "Found label ids in expr: " << gs::to_string(expr_label_ids);
      labels_ids = expr_label_ids;
    }
    // CHECK(labels_ids.size() == 1) << "only support one label in scan";

#ifdef FAST_SCAN
    gs::codegen::oid_t oid;
    gs::codegen::ParamConst oid_param;
    if (try_to_get_oid_from_expr(predicate, oid)) {
      LOG(INFO) << "Parse oid: " << oid << "from expr";
      {
        scan_with_oid(ss, label_name, label_id);
        ss << oid;
        ss << ");";
        ss << std::endl;
      }
    } else if (try_to_get_oid_param_from_expr(predicate, oid_param)) {
      LOG(INFO) << "Parse oid param: " << oid_param.var_name << "from expr";
      {
        scan_with_oid(ss, label_name, label_id);
        ss << oid_param.var_name;
        // add param name to BuildingContext.
        ctx_.AddParameterVar(oid_param);
        ss << ");";
        ss << std::endl;
      }
    } else {
      LOG(INFO) << "Fail to parse oid from expr";
      // try to generate expr.
      {
#endif
        auto expr_builder = ExprBuilder(ctx_);
        expr_builder.AddAllExprOpr(predicate.operators());
        std::string expr_func_name, expr_code;
        std::vector<codegen::ParamConst> func_call_param_const;
        std::vector<std::string> expr_tag_props;
        common::DataType unused_expr_ret_type;
        std::tie(expr_func_name, func_call_param_const, expr_tag_props,
                 expr_code, unused_expr_ret_type) = expr_builder.Build();
        LOG(INFO) << "Found expr in edge_expand_opt:  " << expr_func_name;
        // generate code.
        ctx_.AddExprCode(expr_code);
        std::string expr_var_name = ctx_.GetNextExprVarName();
        ss << "auto " << expr_var_name << " = " << expr_func_name << "(";
        for (auto i = 0; i < func_call_param_const.size(); ++i) {
          ss << func_call_param_const[i].var_name;
          if (i != func_call_param_const.size() - 1) {
            ss << ", ";
          }
        }
        if (expr_tag_props.size() > 0) {
          if (func_call_param_const.size() > 0) {
            ss << ",";
          }
          for (auto i = 0; i + 1 < expr_tag_props.size(); ++i) {
            ss << expr_tag_props[i] << ", ";
          }
          ss << expr_tag_props[expr_tag_props.size() - 1];
        }
        ss << ");" << std::endl;

        // use expression to filter.
        scan_with_expr(ss, labels_ids, label_name, expr_var_name);

#ifdef FAST_SCAN
      }
    }
#endif

    return ss.str();
  }

 private:
  void scan_with_oid(std::stringstream& ss, const std::string& label_name,
                     const int32_t& label_id) const {
    std::string next_ctx_name = ctx_.GetCurCtxName();
    ss << "auto " << next_ctx_name << _ASSIGN_STR_ << "Engine::template ";
    ss << "ScanVertexWithOid<" << res_alias_ << ">"
       << "(";
    ss << ctx_.TimeStampVar() << ",";
    ss << ctx_.GraphVar() << ",";
    if (!label_name.empty()) {
      ss << label_name << ", ";
    } else {
      ss << label_id << ", ";
    }
  }

  void scan_with_expr(std::stringstream& ss,
                      const std::vector<int32_t>& label_ids,
                      const std::string& label_name,
                      const std::string& expr_var_name) const {
    std::string next_ctx_name = ctx_.GetCurCtxName();
    ss << "auto " << next_ctx_name << _ASSIGN_STR_ << "Engine::template ";
    ss << "ScanVertex<" << res_alias_ << ">"
       << "(";
    ss << ctx_.TimeStampVar() << ",";
    ss << ctx_.GraphVar() << ",";
    if (label_name.empty()) {
      // if label_id only contains 1 label.
      if (label_ids.size() == 1) {
        ss << label_ids[0] << ", ";
      } else {
        ss << "std::array<label_id_t, " << label_ids.size() << "> {";
        for (auto i = 0; i + 1 < label_ids.size(); ++i) {
          ss << label_ids[i] << ", ";
        }
        ss << label_ids[label_ids.size() - 1];
        ss << "},";
      }
    } else {
      ss << label_name << ",";
    }

    ss << "std::move(" << expr_var_name << ")";
    ss << ");";
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
    LOG(INFO) << "scan pb has alias" << scan_pb.alias().value();
    builder.resAlias(scan_pb.alias().value());
  } else {
    builder.resAlias(-1);
  }
  return builder.queryParams(scan_pb.params()).Build();
}

}  // namespace gs

#endif  // CODEGEN_SRC_SCAN_BUILDER_H_
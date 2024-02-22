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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_SCAN_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_SCAN_BUILDER_H_

#include <sstream>
#include <string>
#include <vector>

#include <boost/format.hpp>

#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/pb_parser/query_params_parser.h"

namespace gs {
namespace pegasus {

class ScanOpBuilder {
 public:
  ScanOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  ScanOpBuilder& operator_index(const int32_t operator_index) {
    operator_index_ = operator_index;
    return *this;
  }

  ScanOpBuilder& scanOpt(const physical::Scan::ScanOpt& opt) {
    if (opt != physical::Scan::ScanOpt::Scan_ScanOpt_VERTEX) {
      throw std::runtime_error(
          std::string("Currently only support from vertex"));
    }
    scan_opt_ = opt;
    return *this;
  }

  ScanOpBuilder& resAlias(const int32_t& res_alias) {
    res_alias_ = res_alias;
    return *this;
  }

  // get required oid from query params
  ScanOpBuilder& queryParams(const algebra::QueryParams& query_params) {
    query_params_ = query_params;
    return *this;
  }

  std::string Build() const {
    VLOG(10) << "[Scan Builder] Start build scan operator";

    boost::format scan_fmter("%1%%2%%3%");

    VLOG(10) << "[Scan Builder] Start write head";
    std::string head_code = write_head();

    std::vector<int32_t> label_ids;
    try_to_get_label_id_from_query_params(query_params_, label_ids);
    int32_t label_nums = label_ids.size();

    auto& predicate = query_params_.predicate();
    auto expr_builder = ExprBuilder(ctx_);
    VLOG(10) << "operators size is: " << predicate.operators().size() << "\n";
    expr_builder.AddAllExprOpr(predicate.operators());

    std::string predicate_expr;
    std::vector<std::string> var_names;
    std::vector<int32_t> var_tags;
    std::vector<codegen::ParamConst> properties;
    std::vector<std::string> case_exprs;
    std::tie(predicate_expr, var_names, var_tags, properties, case_exprs) =
        expr_builder.BuildRust();

    VLOG(10) << "[Scan Builder] Start write scan body";
    std::string scan_body_code;
    for (auto i = 0; i < label_nums; i++) {
      for (auto property : properties) {
        ctx_.AddVertexProperty(label_ids[i], property);
      }
      scan_body_code += write_scan_body(i, label_ids[i], predicate_expr,
                                        var_names, properties);
    }

    VLOG(10) << "[Scan Builder] Start write end";
    std::string end_code = write_end();

    VLOG(10) << "[Scan Builder] Set output";
    ctx_.SetHead(true);
    ctx_.SetHeadType(0, label_ids);
    if (res_alias_ != -1) {
      ctx_.SetAliasType(res_alias_, 0, label_ids);
    }

    std::vector<codegen::DataType> output;
    output.push_back(codegen::DataType::kInt64);
    ctx_.SetOutput(0, output);
    if (res_alias_ != -1) {
      ctx_.SetOutput(1, output);
    }

    scan_fmter % head_code % scan_body_code % end_code;
    return scan_fmter.str();
  }

 private:
  std::string write_head() const {
    boost::format head_fmter(
        "let stream_%1% = stream_%2%.flat_map(move |_| {\n"
        "let mut result = vec![];\n");
    head_fmter % operator_index_ % (operator_index_ - 1);
    return head_fmter.str();
  }

  std::string write_scan_body(
      int32_t index, const int32_t& label_id, const std::string& predicate_expr,
      const std::vector<std::string>& var_names,
      const std::vector<codegen::ParamConst>& properties) const {
    boost::format scan_body_fmter(
        "let vertex_%1%_num = CSR.get_vertices_num(%2%);\n"
        "let vertex_%1%_local_num = vertex_%1%_num / workers as usize +1;\n"
        "let mut vertex_%1%_start = vertex_%1%_local_num * worker_id as "
        "usize;\n"
        "let mut vertex_%1%_end = vertex_%1%_local_num * (worker_id + 1) as "
        "usize;\n"
        "vertex_%1%_start = std::cmp::min(vertex_%1%_start, vertex_%1%_num);\n"
        "vertex_%1%_end = std::cmp::min(vertex_%1%_end, vertex_%1%_num);\n"
        "for i in vertex_%1%_start..vertex_%1%_end { \n"
        "%3%"  // Filter by prediction
        "}\n");

    // Generate predicate code
    std::string predicate_code;
    if (query_params_.has_predicate()) {
      predicate_code =
          scan_with_expression(label_id, predicate_expr, var_names, properties);
    } else {
      predicate_code = scan_without_expression(label_id);
    }
    scan_body_fmter % index % label_id % predicate_code;
    return scan_body_fmter.str();
  }

  std::string scan_with_expression(
      const int32_t& label_id, const std::string& predicate_expr,
      const std::vector<std::string>& var_names,
      const std::vector<codegen::ParamConst>& properties) const {
    boost::format scan_vertex_fmter(
        "%1%"
        "if %2% {\n"
        "let vertex_global_id = CSR.get_global_id(i, %3%).unwrap() as u64;\n"
        "result.push(vertex_global_id);\n"
        "}\n");

    std::string vars_code;
    for (size_t i = 0; i < var_names.size(); i++) {
      std::string var_name = var_names[i];
      std::string prop_name = properties[i].var_name;
      std::string prop_column_name =
          get_vertex_prop_column_name(prop_name, label_id);

      boost::format var_fmter("let %1% = %2%[i];\n");
      var_fmter % var_name % prop_column_name;
      vars_code += var_fmter.str();
    }
    scan_vertex_fmter % vars_code % predicate_expr % label_id;
    return scan_vertex_fmter.str();
  }

  std::string scan_without_expression(const int32_t& label_id) const {
    boost::format scan_vertex_fmter(
        "let vertex_global_id = CSR.get_global_id(i, %1%).unwrap() as u64;\n"
        "result.push(vertex_global_id);\n");
    scan_vertex_fmter % label_id;
    return scan_vertex_fmter.str();
  }

  std::string write_end() const {
    boost::format end_fmter(
        "Ok(result.into_iter()%1%)\n"
        "})?;\n");

    std::string map_code;
    if (res_alias_ != -1) {
      ctx_.SetAlias(res_alias_);
      map_code = ".map(|res| (res, res))";
    }
    end_fmter % map_code;
    return end_fmter.str();
  }

  int32_t operator_index_;
  BuildingContext& ctx_;
  physical::Scan::ScanOpt scan_opt_;
  algebra::QueryParams query_params_;
  int res_alias_;
};

static std::string BuildScanOp(
    BuildingContext& ctx, int32_t operator_index, const physical::Scan& scan_pb,
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
  return builder.operator_index(operator_index)
      .queryParams(scan_pb.params())
      .Build();
}

}  // namespace pegasus
}  // namespace gs

#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_SCAN_BUILDER_H_

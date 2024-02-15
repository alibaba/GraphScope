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
#ifndef CODEGEN_SRC_HQPS_HQPS_GET_V_BUILDER_H_
#define CODEGEN_SRC_HQPS_HQPS_GET_V_BUILDER_H_

#include <array>
#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/pb_parser/name_id_parser.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/expr.pb.h"
#include "flex/proto_generated_gie/physical.pb.h"

#include <boost/format.hpp>

namespace gs {

static constexpr const char* GET_V_OPT_NO_FILTER_TEMPLATE_STR =
    "auto %1% = make_getv_opt(%2%, %3%);\n";

static constexpr const char* GET_V_NO_FILTER_TEMPLATE_STR =
    "auto %1% = make_getv_opt(%2%, %3%);\n"
    "auto %4% = Engine::template GetV<%5%,%6%>(%7%, std::move(%8%), "
    "std::move(%1%));\n";
static constexpr const char* GET_V_FILTER_TEMPLATE_STR =
    "auto %1% = gs::make_filter(%2%(%3%) %4%);\n"
    "auto %5% = make_getv_opt(%6%, %7%, std::move(%1%));\n"
    "auto %8% = Engine::template GetV<%9%,%10%>(%11%, std::move(%12%), "
    "std::move(%5%));\n";

namespace internal {
enum class GetVType {
  kStart = 0,
  kEnd = 1,
  kOther = 2,
  kBoth = 3,
  kItself = 4,
};

std::string get_v_type_2_str(GetVType get_v_type) {
  switch (get_v_type) {
  case GetVType::kStart:
    return "gs::VOpt::Start";
  case GetVType::kEnd:
    return "gs::VOpt::End";
  case GetVType::kOther:
    return "gs::VOpt::Other";
  case GetVType::kBoth:
    return "gs::VOpt::Both";
  case GetVType::kItself:
    return "gs::VOpt::Itself";
  default:
    throw std::runtime_error("unknown get_v_type");
  }
}
}  // namespace internal

template <typename LabelT>
std::pair<std::string, std::string> make_getv_opt_call_code(
    BuildingContext& ctx, const internal::GetVType& get_v_type,
    const std::vector<LabelT>& vertex_labels) {
  std::string var_name = ctx.GetNextGetVOptName();
  std::stringstream ss;

  boost::format formater(GET_V_OPT_NO_FILTER_TEMPLATE_STR);
  formater % var_name % internal::get_v_type_2_str(get_v_type) %
      label_ids_to_array_str(vertex_labels);
  return std::make_pair(var_name, formater.str());
}

internal::GetVType vopt_pb_to_internal(const physical::GetV::VOpt& v_opt) {
  switch (v_opt) {
  case physical::GetV_VOpt_START:
    return internal::GetVType::kStart;
  case physical::GetV_VOpt_END:
    return internal::GetVType::kEnd;
  case physical::GetV_VOpt_OTHER:
    return internal::GetVType::kOther;
  case physical::GetV_VOpt_BOTH:
    return internal::GetVType::kBoth;
  case physical::GetV_VOpt_ITSELF:
    return internal::GetVType::kItself;
  default:
    throw std::runtime_error("unknown vopt");
  }
}

template <typename LabelT>
class GetVOpBuilder {
 public:
  GetVOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  GetVOpBuilder& v_opt(const physical::GetV::VOpt& v_opt) {
    v_opt_ = vopt_pb_to_internal(v_opt);
    return *this;
  }

  GetVOpBuilder& in_tag(int32_t in_tag_id) {
    in_tag_id_ = ctx_.GetTagInd(in_tag_id);
    return *this;
  }

  GetVOpBuilder& out_tag(int32_t out_tag_id) {
    out_tag_id_ = ctx_.CreateOrGetTagInd(out_tag_id);
    return *this;
  }

  GetVOpBuilder& add_vertex_label(const common::NameOrId& vertex_label) {
    vertex_labels_.push_back(
        try_get_label_from_name_or_id<LabelT>(vertex_label));
    return *this;
  }

  GetVOpBuilder& filter(const common::Expression& expr) {
    auto size = expr.operators().size();
    if (size > 0) {
      ExprBuilder expr_builder(ctx_);

      auto& expr_oprs = expr.operators();
      expr_builder.AddAllExprOpr(expr_oprs);
      expr_builder.set_return_type(common::DataType::BOOLEAN);
      std::vector<common::DataType> unused_expr_ret_type;
      if (!expr_builder.empty()) {
        std::tie(expr_name_, expr_call_param_, tag_properties_, expr_code_,
                 unused_expr_ret_type) = expr_builder.Build();
        ctx_.AddExprCode(expr_code_);
      } else {
        VLOG(10) << "No valid expression in getv filter";
      }
    } else {
      VLOG(10) << "no expression in getv";
    }
    VLOG(10) << "Finish build getv filter";
    return *this;
  }

  std::string Build() const {
    std::string get_v_opt_var = ctx_.GetNextGetVOptName();
    std::string get_v_code;
    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();
    auto append_opt = res_alias_to_append_opt(out_tag_id_);
    auto input_col_str = format_input_col(in_tag_id_);
    std::vector<LabelT> tmp = remove_duplicate(vertex_labels_);
    VLOG(10) << "Before deduplicate: " << gs::to_string(vertex_labels_)
             << ", after dedup: " << gs::to_string(tmp);
    if (expr_name_.empty()) {
      boost::format formater(GET_V_NO_FILTER_TEMPLATE_STR);
      formater % get_v_opt_var % internal::get_v_type_2_str(v_opt_) %
          label_ids_to_array_str(tmp) % next_ctx_name % append_opt %
          input_col_str % ctx_.GraphVar() % prev_ctx_name;
      get_v_code = formater.str();
      // no filter
    } else {
      boost::format formater(GET_V_FILTER_TEMPLATE_STR);
      // with filter
      std::string expr_var_name = ctx_.GetNextExprVarName();
      std::string expr_call_str;
      std::string selectors_str;
      {
        std::stringstream ss;
        for (size_t i = 0; i < expr_call_param_.size(); ++i) {
          ss << expr_call_param_[i].var_name;
          if (i != expr_call_param_.size() - 1) {
            ss << ", ";
          }
        }
        expr_call_str = ss.str();
      }
      {
        std::stringstream ss;
        if (tag_properties_.size() > 0) {
          ss << ", ";
        }
        for (size_t i = 0; i < tag_properties_.size(); ++i) {
          ss << tag_properties_[i].second;
          if (i != tag_properties_.size() - 1) {
            ss << ", ";
          }
        }
        selectors_str = ss.str();
      }
      formater % expr_var_name % expr_name_ % expr_call_str % selectors_str %
          get_v_opt_var % internal::get_v_type_2_str(v_opt_) %
          label_ids_to_array_str(tmp) % next_ctx_name % append_opt %
          input_col_str % ctx_.GraphVar() % prev_ctx_name;
      get_v_code = formater.str();
    }
    VLOG(10) << "Finish building getv code";

    return get_v_code;
  }

 private:
  BuildingContext& ctx_;
  internal::GetVType v_opt_;
  int32_t in_tag_id_, out_tag_id_;
  std::vector<LabelT> vertex_labels_;
  std::vector<codegen::ParamConst> expr_call_param_;
  std::vector<std::pair<int32_t, std::string>> tag_properties_;
  std::string expr_name_, expr_code_;
};

template <typename LabelT>
static std::string BuildGetVOp(
    BuildingContext& ctx, const physical::GetV& get_v_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  GetVOpBuilder<LabelT> builder(ctx);
  builder.v_opt(get_v_pb.opt());
  if (get_v_pb.has_tag()) {
    builder.in_tag(get_v_pb.tag().value());
  } else {
    builder.in_tag(-1);
  }

  if (get_v_pb.has_alias()) {
    builder.out_tag(get_v_pb.alias().value());
  } else {
    builder.out_tag(-1);
  }
  auto& vertex_labels_pb = get_v_pb.params().tables();
  for (auto vertex_label_pb : vertex_labels_pb) {
    builder.add_vertex_label(vertex_label_pb);
  }

  return builder.filter(get_v_pb.params().predicate()).Build();
}
}  // namespace gs

#endif  // CODEGEN_SRC_HQPS_HQPS_GET_V_BUILDER_H_
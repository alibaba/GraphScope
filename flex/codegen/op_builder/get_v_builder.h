#ifndef GET_V_BUILDER_H
#define GET_V_BUILDER_H

#include <string>
#include <vector>

#include "flex/codegen/building_context.h"
#include "flex/codegen/graph_types.h"
#include "flex/codegen/pb_parser/name_id_parser.h"
#include "flex/codegen/codegen_utils.h"
#include "proto_generated_gie/algebra.pb.h"

#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/expr.pb.h"
#include "proto_generated_gie/physical.pb.h"

namespace gs {

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
std::string label_vec_to_str(const std::vector<LabelT>& labels) {
  std::stringstream ss;
  ss << "std::array<" << LABEL_ID_T << "," << labels.size() << ">{";
  if (labels.size() > 0) {
    for (auto i = 0; i < labels.size() - 1; ++i) {
      ss << std::to_string(labels[i]) << ", ";
    }
    ss << std::to_string(labels.back());
  }
  ss << "}";
  return ss.str();
}

template <typename LabelT>
std::pair<std::string, std::string> make_getv_opt_call_code(
    BuildingContext& ctx, const internal::GetVType& get_v_type,
    const std::vector<LabelT>& vertex_labels) {
  std::string var_name = ctx.GetNextGetVOptName();
  std::stringstream ss;
  ss << "auto " << var_name << _ASSIGN_STR_ << " " << MAKE_GETV_OPT_NAME;
  ss << "(";
  ss << internal::get_v_type_2_str(get_v_type) << ", ";
  ss << label_vec_to_str(vertex_labels);
  ss << ");";
  return std::make_pair(var_name, ss.str());
}

template <typename LabelT>
std::pair<std::string, std::string> make_getv_opt_call_code(
    BuildingContext& ctx, const internal::GetVType& get_v_type,
    const std::vector<LabelT>& vertex_labels,
    const std::string& expr_var_name) {
  std::string var_name = ctx.GetNextGetVOptName();
  std::stringstream ss;
  ss << "auto " << var_name << _ASSIGN_STR_ << " " << MAKE_GETV_OPT_NAME;
  ss << "(";
  ss << internal::get_v_type_2_str(get_v_type) << ", ";
  ss << label_vec_to_str(vertex_labels) << ",";
  ss << "std::move(" << expr_var_name << ")";
  ss << ");";
  return std::make_pair(var_name, ss.str());
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
      common::DataType unused_expr_ret_type;
      if (!expr_builder.empty()) {
        std::tie(expr_name_, expr_call_param_, tag_propertys_, expr_code_,
                 unused_expr_ret_type) = expr_builder.Build();
      } else {
        LOG(INFO) << "No valid expression in getv filter";
      }
    } else {
      LOG(INFO) << "no expression in getv";
    }
    return *this;
  }

  // expr call code
  // get_v_opt call code
  // get_v code.
  std::array<std::string, 3> Build() const {
    std::string expr_call_code;
    std::string expr_var_name;
    std::string get_v_opt_var;
    std::string get_v_opt_code;
    std::string get_v_code;
    {
      if (!expr_name_.empty()) {
        std::stringstream ss;
        expr_var_name = ctx_.GetNextExprVarName();
        ss << expr_name_ << " " << expr_var_name << "(";
        for (auto expr_call_param : expr_call_param_) {
          ss << expr_call_param.var_name << ", ";
        }
        for (auto i = 0; i < tag_propertys_.size() - 1; ++i) {
          ss << tag_propertys_[i] << ", ";
        }
        ss << tag_propertys_.back();
        ss << ");" << std::endl;
        expr_call_code = ss.str();
        ctx_.AddExprCode(expr_code_);
      } else {
        LOG(INFO) << "No expression in get_v builder";
      }
    }
    LOG(INFO) << "expr_call_code: " << expr_call_code
              << ", expr_var_name: " << expr_var_name;
    {
      // make_sure vertex_labels has no duplicate.
      std::vector<LabelT> tmp;
      {
        std::set<LabelT> label_set(vertex_labels_.begin(),
                                   vertex_labels_.end());
        tmp.assign(label_set.begin(), label_set.end());
      }

      if (expr_var_name.empty()) {
        std::tie(get_v_opt_var, get_v_opt_code) =
            make_getv_opt_call_code(ctx_, v_opt_, tmp);
      } else {
        std::tie(get_v_opt_var, get_v_opt_code) =
            make_getv_opt_call_code(ctx_, v_opt_, tmp, expr_var_name);
      }
    }
    LOG(INFO) << "get_v_opt_code: " << get_v_opt_code;
    {
      std::string prev_ctx_name, next_ctx_name;
      std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();
      std::stringstream ss;
      ss << "auto " << next_ctx_name << _ASSIGN_STR_ << "Engine::template GetV";
      ss << "<" << out_tag_id_ << "," << in_tag_id_ << ">";
      ss << "(";
      ss << ctx_.TimeStampVar() << ", " << ctx_.GraphVar() << ", ";
      ss << "std::move(" << prev_ctx_name << "), "
         << "std::move(" << get_v_opt_var << ")";
      ss << ");";
      get_v_code = ss.str();
    }
    return std::array<std::string, 3>{expr_call_code, get_v_opt_code,
                                      get_v_code};
  }

 private:
  BuildingContext& ctx_;
  internal::GetVType v_opt_;
  int32_t in_tag_id_, out_tag_id_;
  std::vector<LabelT> vertex_labels_;
  std::vector<codegen::ParamConst> expr_call_param_;
  std::vector<std::string> tag_propertys_;
  std::string expr_name_, expr_code_;
};

template <typename LabelT>
static std::array<std::string, 3> BuildGetVOp(
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

#endif  // GET_V_BUILDER_H
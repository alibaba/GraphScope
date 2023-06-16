#ifndef PATH_EXPAND_BUILDER_H
#define PATH_EXPAND_BUILDER_H

#include <string>
#include <vector>

#include "flex/codegen/building_context.h"
#include "flex/codegen/graph_types.h"
#include "flex/codegen/op_builder/get_v_builder.h"
#include "flex/codegen/pb_parser/expand_parser.h"
#include "flex/codegen/codegen_utils.h"
#include "proto_generated_gie/algebra.pb.h"
#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/expr.pb.h"

//#ifndef HOP_RANGE_PARAM
//#define HOP_RANGE_PARAM
//#endif

namespace gs {

std::string path_opt_pb_2_str(
    const physical::PathExpand::PathOpt& path_opt_pb) {
  switch (path_opt_pb) {
  case physical::PathExpand::PathOpt::PathExpand_PathOpt_ARBITRARY:
    return "gs::PathOpt::Arbitrary";
  case physical::PathExpand::PathOpt::PathExpand_PathOpt_SIMPLE:
    return "gs::PathOpt::Simple";
  default:
    throw std::runtime_error("unknown path_opt_pb");
  }
}

std::string result_opt_pb_2_str(
    const physical::PathExpand::ResultOpt& result_opt_pb) {
  switch (result_opt_pb) {
  case physical::PathExpand::ResultOpt::PathExpand_ResultOpt_END_V:
    return "gs::ResultOpt::EndV";
  case physical::PathExpand::ResultOpt::PathExpand_ResultOpt_ALL_V:
    return "gs::ResultOpt::AllV";
  default:
    throw std::runtime_error("unknown result_opt_pb");
  }
}

template <typename LabelT>
class PathExpandOpBuilder {
 public:
  PathExpandOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  PathExpandOpBuilder& in_tag(int32_t in_tag_id) {
    in_tag_id_ = ctx_.GetTagInd(in_tag_id);
    return *this;
  }

  PathExpandOpBuilder& out_tag(int32_t out_tag_id) {
    out_tag_id_ = ctx_.CreateOrGetTagInd(out_tag_id);
    LOG(INFO) << "out_tag_id: " << out_tag_id
              << ", out_tag_ind_: " << out_tag_id_;
    return *this;
  }

  // get the expand_opt name and the expand_opt code.
  PathExpandOpBuilder& edge_expand_opt() { return *this; }

  // get the getv name and the getv code.
  PathExpandOpBuilder& path_expand_opt(
      const physical::EdgeExpand& edge_expand_pb,
      const physical::GetV& get_v_pb,
      const google::protobuf::RepeatedPtrField<physical::PhysicalOpr::MetaData>&
          meta_data_pb) {
    direction_ =
        edge_expand_pb_2_internal_direction(edge_expand_pb.direction());

    if (meta_data_pb.size() >= 1) {
      CHECK(meta_data_pb.size() == 1) << "currently only support one meta_data";
      auto& graph_type = meta_data_pb[0].type();
      if (graph_type.type_case() == common::IrDataType::kDataType) {
        // throw runtime error
        throw std::runtime_error("Expect graphDataType in ir_data_type");
      } else if (graph_type.type_case() == common::IrDataType::kGraphType) {
        LOG(INFO) << "Parse edge triplet from meta_data";
        auto& act_graph_type = graph_type.graph_type();
        if (act_graph_type.element_opt() !=
            common::GraphDataType::GraphElementOpt::
                GraphDataType_GraphElementOpt_EDGE) {
          throw std::runtime_error("Expect edge graph type");
        }
        auto& edge_type = act_graph_type.graph_data_type();
        if (edge_type.size() != 1) {
          throw std::runtime_error("Expect only one edge type");
        }
        auto& edge_type0 = edge_type[0];
        auto& edge_labels = edge_type0.label();
        auto src_label = edge_labels.src_label().value();
        auto dst_label = edge_labels.dst_label().value();

        // if find edge triplets, we clear current
        LOG(INFO) << "Clear current dst labels:"
                  << gs::to_string(dst_vertex_labels_);
        dst_vertex_labels_.clear();

        if (direction_ == internal::Direction::kBoth) {
          CHECK(src_label == dst_label);
          dst_vertex_labels_.emplace_back(src_label);
        } else if (direction_ == internal::Direction::kOut) {
          dst_vertex_labels_.emplace_back(dst_label);
        } else if (direction_ == internal::Direction::kIn) {
          dst_vertex_labels_.emplace_back(src_label);
        } else {
          throw std::runtime_error("Unknown direction");
        }
      } else {
        throw std::runtime_error("Expect graphDataType in ir_data_type");
      }
    } else {
      LOG(INFO) << "No meta_data found";
    }

    {
      // build get_v
      auto v_opt = vopt_pb_to_internal(get_v_pb.opt());
      auto& v_labels_pb = get_v_pb.params().tables();

      if (dst_vertex_labels_.empty()) {
        for (auto i = 0; i < v_labels_pb.size(); ++i) {
          dst_vertex_labels_.push_back(
              try_get_label_from_name_or_id<LabelT>(v_labels_pb[i]));
        }
      }
      LOG(INFO) << "get vertex labels:" << gs::to_string(dst_vertex_labels_);
      CHECK(!get_v_pb.params().has_predicate()) << "currently don't suppport "
                                                   "getv with condition";
      // std::tie(get_v_expr_call_code, get_v_opt, getv_opt_) =
      // BuildGetVOpt(ctx_, get_v_pb);
      std::tie(getv_opt_name_, getv_opt_code_) =
          make_getv_opt_call_code(ctx_, v_opt, dst_vertex_labels_);
      LOG(INFO) << "Got getv_opt_name_: " << getv_opt_name_;
      LOG(INFO) << "Got getv_opt_code_: " << getv_opt_code_;
    }

    {
      // build edge_expand_opt
      auto& params = edge_expand_pb.params();
      auto expand_opt = edge_expand_pb.expand_opt();
      CHECK(dst_vertex_labels_.size() > 0) << "no dst lables found";

      physical::PhysicalOpr::MetaData meta_data;
      // pass an empty meta_data, since we need no meta_data for
      // edge_expand_opt.
      std::tie(edge_expand_opt_name_, edge_expand_opt_) = BuildEdgeExpandOpt(
          ctx_, direction_, params, dst_vertex_labels_, expand_opt, meta_data);
      VLOG(10) << "edge_expand_opt_name_: " << edge_expand_opt_name_;
      VLOG(10) << "edge_expand_opt_: " << edge_expand_opt_;
    }

    return *this;
  }

  PathExpandOpBuilder& hop_range(const algebra::Range& hop_range_pb) {
#ifdef HOP_RANGE_PARAM
    if (hop_range_pb.has_lower()) {
      auto& lower = hop_range_pb.lower();
      if (lower.has_value()) {
        range_lower_ = lower.value();
      } else if (lower.has_param()) {
        auto& param = lower.param();
        range_lower_param_ = param_const_pb_to_param_const(lower.param());
      } else {
        LOG(WARNING) << "hop_range_pb has no lower";
        range_lower_ = 0;
      }
    } else {
      LOG(WARNING) << "hop_range_pb has no lower";
      range_lower_ = 0;
    }
#else
    range_lower_ = hop_range_pb.lower();
#endif

#ifdef HOP_RANGE_PARAM
    if (hop_range_pb.has_upper()) {
      auto& upper = hop_range_pb.upper();
      if (upper.has_value()) {
        range_upper_ = upper.value();
      } else if (upper.has_param()) {
        auto& param = upper.param();
        range_upper_param_ = param_const_pb_to_param_const(upper.param());
      } else {
        LOG(WARNING) << "hop_range_pb has no upper";
        range_upper_ = std::numeric_limits<int32_t>::max();
      }
    } else {
      LOG(WARNING) << "hop_range_pb has no upper";
      range_upper_ = std::numeric_limits<int32_t>::max();
    }
#else
    range_upper_ = hop_range_pb.upper();
#endif

    LOG(INFO) << "got range: " << range_lower_.value() << " "
              << range_upper_.value();
    if (range_lower_param_) {
      LOG(INFO) << "got range_lower_param_: "
                << range_lower_param_.value().var_name;
    }
    if (range_upper_param_) {
      LOG(INFO) << "got range_upper_param_: "
                << range_upper_param_.value().var_name;
    }
    return *this;
  }

  PathExpandOpBuilder& path_opt(
      const physical::PathExpand::PathOpt& path_opt_pb) {
    path_opt_str_ = path_opt_pb_2_str(path_opt_pb);
    LOG(INFO) << "got path_opt: " << path_opt_str_;
    return *this;
  }

  PathExpandOpBuilder& result_opt(
      const physical::PathExpand::ResultOpt& result_opt_pb) {
    result_opt_str_ = result_opt_pb_2_str(result_opt_pb);
    LOG(INFO) << "got result_opt: " << result_opt_str_;
    return *this;
  }

  PathExpandOpBuilder& condition(const common::Expression& condition_pb) {
    LOG(WARNING) << "Skiped for path expand with condition";
    return *this;
  }

  std::array<std::string, 3> Build() const {
    {
      // first put the possible param vars into context
      if (range_lower_param_.has_value()) {
        ctx_.AddParameterVar(range_lower_param_.value());
      }
      if (range_upper_param_.has_value()) {
        ctx_.AddParameterVar(range_upper_param_.value());
      }
    }
    std::stringstream ss;
    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();
    std::string path_expand_opt_var = ctx_.GetNextPathOptName();
    ss << "auto " << path_expand_opt_var << _ASSIGN_STR_;
    ss << "gs::make_path_expand_opt(";
    ss << "std::move(" << edge_expand_opt_name_ << "), ";
    ss << "std::move(" << getv_opt_name_ << "), ";
    if (range_lower_.has_value()) {
      ss << "gs::Range(" << range_lower_.value();
    } else if (range_lower_param_.has_value()) {
      ss << "gs::Range(" << range_lower_param_.value().var_name;
    } else {
      LOG(FATAL) << "no id nor param found";
    }

    if (range_upper_.has_value()) {
      ss << ", " << range_upper_.value();
    } else if (range_upper_param_.has_value()) {
      ss << ", " << range_upper_param_.value().var_name;
    } else {
      LOG(FATAL) << "no id nor param found";
    }
    ss << "));" << std::endl;

    ss << " auto " << next_ctx_name << _ASSIGN_STR_ << " Engine::template";
    ss << " PathExpandV<" << out_tag_id_ << ", " << in_tag_id_ << ">";
    ss << "(";
    ss << ctx_.TimeStampVar() << ", ";
    ss << ctx_.GraphVar() << ", ";
    ss << "std::move(" << prev_ctx_name << "), ";
    ss << "std::move(" << path_expand_opt_var << ")";
    ss << ");" << std::endl;
    return std::array<std::string, 3>{edge_expand_opt_, getv_opt_code_,
                                      ss.str()};
  }

 private:
  BuildingContext& ctx_;
  int32_t in_tag_id_, out_tag_id_;
  std::string edge_expand_opt_name_, edge_expand_opt_;
  std::string getv_opt_name_, getv_opt_code_;
  std::optional<int32_t> range_lower_, range_upper_;
  std::optional<codegen::ParamConst> range_lower_param_, range_upper_param_;
  std::string path_opt_str_, result_opt_str_;
  std::vector<LabelT> dst_vertex_labels_;
  internal::Direction direction_;
};

// edge_expand_opt
// get_v_opt
// path_expand_opt
// op_code.
// NOTE: we currenly only support path expand v, the in_tag can be fetch fromn
// path_expand_pb itself, while the res_alilas shall be fetch from the later
// get_v
template <typename LabelT>
static std::array<std::string, 3> BuildPathExpandOp(
    BuildingContext& ctx, const physical::PathExpand& path_expand_pb,
    const google::protobuf::RepeatedPtrField<physical::PhysicalOpr::MetaData>&
        meta_data,
    int32_t out_tag_id) {
  PathExpandOpBuilder<LabelT> builder(ctx);
  if (path_expand_pb.has_start_tag()) {
    builder.in_tag(path_expand_pb.start_tag().value());
  } else {
    builder.in_tag(-1);
  }

  // CHECK(!path_expand_pb.has_alias());
  builder.out_tag(out_tag_id);

  return builder
      .path_expand_opt(path_expand_pb.base().edge_expand(),
                       path_expand_pb.base().get_v(),
                       meta_data)  // get_v_opt must be called first to
                                   // provide dst_label ids.
      .hop_range(path_expand_pb.hop_range())
      .path_opt(path_expand_pb.path_opt())
      .result_opt(path_expand_pb.result_opt())
      .condition(path_expand_pb.condition())

      .Build();
}
}  // namespace gs

#endif  // PATH_EXPAND_BUILDER_H
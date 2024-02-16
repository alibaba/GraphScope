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
#ifndef CODEGEN_SRC_HQPS_HQPS_PATH_EXPAND_BUILDER_H_
#define CODEGEN_SRC_HQPS_HQPS_PATH_EXPAND_BUILDER_H_

#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/hqps/hqps_get_v_builder.h"
#include "flex/codegen/src/pb_parser/expand_parser.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/expr.pb.h"

// #ifndef HOP_RANGE_PARAM
// #define HOP_RANGE_PARAM
// #endif

namespace gs {

static constexpr const char* PATH_EXPAND_V_OP_TEMPLATE_STR =
    "%1%\n"
    "%2%\n"
    "auto %3% = gs::make_path_expandv_opt(std::move(%4%), std::move(%5%), "
    "gs::Range(%6%, %7%));\n"
    "auto %8% = Engine::PathExpandV<%9%, %10%>(%11%, std::move(%12%), "
    "std::move(%13%));\n";

static constexpr const char* PATH_EXPAND_PATH_OP_TEMPLATE_STR =
    "%1%\n"
    "%2%\n"
    "auto %3% = gs::make_path_expandv_opt(std::move(%4%), std::move(%5%), "
    "gs::Range(%6%, %7%));\n"
    "auto %8% = Engine::PathExpandP<%9%, %10%>(%11%, std::move(%12%), "
    "std::move(%13%));\n";

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
    VLOG(10) << "out_tag_id: " << out_tag_id
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
        VLOG(10) << "Parse edge triplet from meta_data";
        auto& act_graph_type = graph_type.graph_type();
        if (act_graph_type.element_opt() !=
            common::GraphDataType::GraphElementOpt::
                GraphDataType_GraphElementOpt_EDGE) {
          throw std::runtime_error("Expect edge graph type");
        }
        auto& edge_type = act_graph_type.graph_data_type();
        if (edge_type.size() == 0) {
          throw std::runtime_error("Expect edge type size > 0");
        }
        std::vector<int32_t> src_labels, dst_labels;
        for (int i = 0; i < edge_type.size(); ++i) {
          auto& edge_type_i = edge_type[i];
          auto& edge_labels_i = edge_type_i.label();
          src_labels.push_back(edge_labels_i.src_label().value());
          dst_labels.push_back(edge_labels_i.dst_label().value());
        }

        // if find edge triplets, we clear current
        VLOG(10) << "Clear current dst labels:"
                 << gs::to_string(dst_vertex_labels_);
        dst_vertex_labels_.clear();

        if (direction_ == internal::Direction::kBoth) {
          // if direction is both, we need to check src_label == dst_label
          // dedup src_labels
          std::sort(src_labels.begin(), src_labels.end());
          src_labels.erase(std::unique(src_labels.begin(), src_labels.end()),
                           src_labels.end());
          // dedup dst_labels
          std::sort(dst_labels.begin(), dst_labels.end());
          dst_labels.erase(std::unique(dst_labels.begin(), dst_labels.end()),
                           dst_labels.end());
          for (size_t i = 0; i < src_labels.size(); ++i) {
            if (src_labels[i] != dst_labels[i]) {
              throw std::runtime_error(
                  "Expect src_label == dst_label for both direction");
            }
            dst_vertex_labels_.emplace_back(dst_labels[i]);
          }
        } else if (direction_ == internal::Direction::kOut) {
          for (size_t i = 0; i < dst_labels.size(); ++i) {
            dst_vertex_labels_.emplace_back(dst_labels[i]);
          }
        } else if (direction_ == internal::Direction::kIn) {
          for (size_t i = 0; i < src_labels.size(); ++i) {
            dst_vertex_labels_.emplace_back(src_labels[i]);
          }
        } else {
          throw std::runtime_error("Unknown direction");
        }
      } else {
        throw std::runtime_error("Expect graphDataType in ir_data_type");
      }
    } else {
      VLOG(10) << "No meta_data found";
    }

    {
      // build get_v
      auto v_opt = vopt_pb_to_internal(get_v_pb.opt());
      auto& v_labels_pb = get_v_pb.params().tables();

      if (dst_vertex_labels_.empty()) {
        for (int i = 0; i < v_labels_pb.size(); ++i) {
          dst_vertex_labels_.push_back(
              try_get_label_from_name_or_id<LabelT>(v_labels_pb[i]));
        }
      }
      VLOG(10) << "get vertex labels:" << gs::to_string(dst_vertex_labels_);
      CHECK(!get_v_pb.params().has_predicate()) << "currently don't support "
                                                   "getv with condition";
      // std::tie(get_v_expr_call_code, get_v_opt, getv_opt_) =
      // BuildGetVOpt(ctx_, get_v_pb);
      std::tie(getv_opt_name_, getv_opt_code_) =
          make_getv_opt_call_code(ctx_, v_opt, dst_vertex_labels_);
      VLOG(10) << "Got getv_opt_name_: " << getv_opt_name_;
      VLOG(10) << "Got getv_opt_code_: " << getv_opt_code_;
    }

    {
      // build edge_expand_opt
      auto& params = edge_expand_pb.params();
      auto expand_opt = edge_expand_pb.expand_opt();
      CHECK(dst_vertex_labels_.size() > 0) << "no dst labels found";

      if (params.tables().size() < 1) {
        throw std::runtime_error("no edge labels found");
      } else if (params.tables().size() == 1) {
        physical::PhysicalOpr::MetaData meta_data;
        // pass an empty meta_data, since we need no meta_data for
        std::tie(edge_expand_opt_name_, edge_expand_opt_) =
            BuildOneLabelEdgeExpandOpt(ctx_, direction_, params,
                                       dst_vertex_labels_, expand_opt,
                                       meta_data);
      } else {
        // get the first meta_data
        if (meta_data_pb.size() < 1) {
          throw std::runtime_error("no meta_data found");
        }
        auto& meta_data = meta_data_pb[0];
        std::tie(edge_expand_opt_name_, edge_expand_opt_) =
            BuildMultiLabelEdgeExpandOpt(ctx_, direction_, params, expand_opt,
                                         meta_data);
      }

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

    VLOG(10) << "got range: " << range_lower_.value() << " "
             << range_upper_.value();
    if (range_lower_param_) {
      VLOG(10) << "got range_lower_param_: "
               << range_lower_param_.value().var_name;
    }
    if (range_upper_param_) {
      VLOG(10) << "got range_upper_param_: "
               << range_upper_param_.value().var_name;
    }
    return *this;
  }

  PathExpandOpBuilder& path_opt(
      const physical::PathExpand::PathOpt& path_opt_pb) {
    path_opt_str_ = path_opt_pb_2_str(path_opt_pb);
    VLOG(10) << "got path_opt: " << path_opt_str_;
    return *this;
  }

  PathExpandOpBuilder& result_opt(
      const physical::PathExpand::ResultOpt& result_opt_pb) {
    result_opt_str_ = result_opt_pb_2_str(result_opt_pb);
    VLOG(10) << "got result_opt: " << result_opt_str_;
    return *this;
  }

  PathExpandOpBuilder& condition(const common::Expression& condition_pb) {
    LOG(WARNING) << "Skipped for path expand with condition";
    return *this;
  }

  PathExpandOpBuilder& set_output_to_vertices() {
    output_to_vertices_ = true;
    return *this;
  }

  PathExpandOpBuilder& set_output_paths() {
    output_to_vertices_ = false;
    return *this;
  }

  std::string Build() const {
    {
      // first put the possible param vars into context
      if (range_lower_param_.has_value()) {
        ctx_.AddParameterVar(range_lower_param_.value());
      }
      if (range_upper_param_.has_value()) {
        ctx_.AddParameterVar(range_upper_param_.value());
      }
    }
    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();
    std::string path_expand_opt_var = ctx_.GetNextPathOptName();
    std::string range_lower_value, range_upper_value;
    if (range_lower_.has_value()) {
      range_lower_value = std::to_string(range_lower_.value());
    } else if (range_lower_param_.has_value()) {
      range_lower_value = range_lower_param_.value().var_name;
    } else {
      LOG(FATAL) << "no id nor param found";
    }
    if (range_upper_.has_value()) {
      range_upper_value = std::to_string(range_upper_.value());
    } else if (range_upper_param_.has_value()) {
      range_upper_value = range_upper_param_.value().var_name;
    } else {
      LOG(FATAL) << "no id nor param found";
    }

    auto append_opt = res_alias_to_append_opt(out_tag_id_);
    auto input_col_str = format_input_col(in_tag_id_);
    boost::format formater("");
    if (output_to_vertices_) {
      formater = boost::format(PATH_EXPAND_V_OP_TEMPLATE_STR);
    } else {
      formater = boost::format(PATH_EXPAND_PATH_OP_TEMPLATE_STR);
    }

    formater % edge_expand_opt_ % getv_opt_code_ % path_expand_opt_var %
        edge_expand_opt_name_ % getv_opt_name_ % range_lower_value %
        range_upper_value % next_ctx_name % append_opt % input_col_str %
        ctx_.GraphVar() % prev_ctx_name % path_expand_opt_var;

    return formater.str();
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
  bool output_to_vertices_;  // true: output to vertices, false: output to paths
};

// edge_expand_opt
// get_v_opt
// path_expand_opt
// op_code.
// NOTE: we currently only support path expand v, the in_tag can be fetch from
// path_expand_pb itself, while the res_alias shall be fetch from the later
// get_v
template <typename LabelT>
static std::string BuildPathExpandVOp(
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
  builder.out_tag(out_tag_id);  // out_tag_id overrides alias

  return builder
      .path_expand_opt(path_expand_pb.base().edge_expand(),
                       path_expand_pb.base().get_v(),
                       meta_data)  // get_v_opt must be called first to
                                   // provide dst_label ids.
      .hop_range(path_expand_pb.hop_range())
      .path_opt(path_expand_pb.path_opt())
      .result_opt(path_expand_pb.result_opt())
      .condition(path_expand_pb.condition())
      .set_output_to_vertices()
      .Build();
}

// PathExpand without fusing with getv.
template <typename LabelT>
static std::string BuildPathExpandPathOp(
    BuildingContext& ctx, const physical::PathExpand& path_expand_pb,
    const google::protobuf::RepeatedPtrField<physical::PhysicalOpr::MetaData>&
        meta_data) {
  PathExpandOpBuilder<LabelT> builder(ctx);
  if (path_expand_pb.has_start_tag()) {
    builder.in_tag(path_expand_pb.start_tag().value());
  } else {
    builder.in_tag(-1);
  }

  // CHECK(path_expand_pb.has_alias());
  // if not, alias should be 0?
  if (path_expand_pb.has_alias()) {
    builder.out_tag(path_expand_pb.alias().value());
  } else {
    builder.out_tag(-1);
  }

  return builder
      .path_expand_opt(path_expand_pb.base().edge_expand(),
                       path_expand_pb.base().get_v(),
                       meta_data)  // get_v_opt must be called first to
                                   // provide dst_label ids.
      .hop_range(path_expand_pb.hop_range())
      .path_opt(path_expand_pb.path_opt())
      .result_opt(path_expand_pb.result_opt())
      .condition(path_expand_pb.condition())
      .set_output_paths()
      .Build();
}
}  // namespace gs

#endif  // CODEGEN_SRC_HQPS_HQPS_PATH_EXPAND_BUILDER_H_
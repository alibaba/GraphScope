#ifndef GEN_SRC_EDGE_EXPAND_BUILDER_H_
#define GEN_SRC_EDGE_EXPAND_BUILDER_H_

#include <string>

#include <sstream>

#include "flex/codegen/building_context.h"
#include "flex/codegen/pb_parser/internal_struct.h"
#include "flex/codegen/pb_parser/ir_data_type_parser.h"
#include "flex/codegen/pb_parser/name_id_parser.h"
#include "flex/codegen/pb_parser/query_params_parser.h"
#include "flex/codegen/codegen_utils.h"
#include "flex/codegen/string_utils.h"

//#define NO_EXTRACT_PROP_FROM_IR_DATA_TYPE

namespace gs {

// create edge expand opt
// the expression in query params are applied on edge.
// extract the edge property from ir_data_type
template <typename LabelT>
static std::pair<std::string, std::string> BuildEdgeExpandOpt(
    BuildingContext& ctx, const internal::Direction& direction,
    const algebra::QueryParams& params,
    const std::vector<LabelT>& dst_vertex_labels,
    const physical::EdgeExpand::ExpandOpt& expand_opt,
    const physical::PhysicalOpr::MetaData& meta_data) {
  std::string expr_var_name;

  std::stringstream ss;
  {
    // first check whether expand_opt contains expression.
    if (params.has_predicate()) {
      LOG(INFO) << "Found expr in edge expand";
      auto& expr = params.predicate();
      auto expr_builder = ExprBuilder(ctx);
      expr_builder.AddAllExprOpr(expr.operators());
      // for (auto i = 0; i < expr.operators_size(); ++i) {
      //   expr_builder.AddExprOpr(expr.operators(i));
      // }
      LOG(INFO) << "after add expr opr";
      std::string expr_func_name, expr_code;
      std::vector<codegen::ParamConst> func_call_param_const;
      std::vector<std::string> expr_tag_props;
      common::DataType unused_expr_ret_type;
      std::tie(expr_func_name, func_call_param_const, expr_tag_props, expr_code,
               unused_expr_ret_type) = expr_builder.Build();
      LOG(INFO) << "Found expr in edge_expand_opt:  " << expr_func_name;
      // generate code.
      ctx.AddExprCode(expr_code);
      expr_var_name = ctx.GetNextExprVarName();
      ss << "auto " << expr_var_name << " = " << expr_func_name << "(";
      for (auto i = 0; i < func_call_param_const.size(); ++i) {
        ss << func_call_param_const[i].var_name;
        if (i != func_call_param_const.size() - 1) {
          ss << ", ";
        }
      }
      if (expr_tag_props.size() > 0) {
        if (func_call_param_const.size() > 0) {
          ss << ", ";
        }
        for (auto i = 0; i < expr_tag_props.size() - 1; ++i) {
          ss << expr_tag_props[i] << ", ";
        }
        ss << expr_tag_props[expr_tag_props.size() - 1];
      }
      ss << ");" << std::endl;
    }
  }
  std::string var_name = ctx.GetNextEdgeOptName();

  ss << "auto " << var_name << " = ";
  if (expand_opt ==
      physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
    ss << "gs::make_edge_expande_opt";
  } else {
    ss << "gs::make_edge_expandv_opt";
  }
  // check data type
  auto& ir_data_type = meta_data.type();
  std::vector<std::vector<std::string>> prop_names;
  std::vector<std::vector<std::string>> prop_types;
// do we nneed to extract prop name from data_type?
// for edge_expand_e, we need the type info, but for edge_expand_v, we don't
// cause grape_graph need the exact type info.
#ifndef NO_EXTRACT_PROP_FROM_IR_DATA_TYPE
  if (try_to_get_prop_names_and_types_from_ir_data_type(
          ir_data_type, prop_names, prop_types)) {
    CHECK(prop_names.size() == 1);
    auto& cur_prop_names = prop_names[0];
    auto& cur_prop_types = prop_types[0];
    if (ctx.GetStorageType() == StorageBackend::kGrape &&
        expand_opt ==
            physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
      ss << "<";
      for (auto i = 0; i < cur_prop_types.size() - 1; ++i) {
        ss << cur_prop_types[i] << ", ";
      }
      ss << cur_prop_types[cur_prop_types.size() - 1] << ">";
    }

    if (expand_opt ==
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
      ss << "({";
      for (auto i = 0; i < cur_prop_names.size() - 1; ++i) {
        ss << "\"" << cur_prop_names[i] << "\", ";
      }
      ss << "\"" << cur_prop_names[cur_prop_names.size() - 1] << "\"";
      // auto named_property = make_named_property(cur_prop_names,
      // cur_prop_types);
      ss << "},";
    } else {
      ss << "(";
      LOG(WARNING) << "Currently expend no property for vertex";
    }
  } else {
    LOG(INFO) << "No prop types found.";
    ss << "(";
  }
#else
  LOG(INFO) << "not trying to find prop types since disabled.";
  ss << "(";
#endif

  ss << direction_pb_to_str(direction) << ", ";
  auto& edge_table = params.tables();
  CHECK(edge_table.size() == 1) << "edge table size should be 1";
  LabelT edge_label = try_get_label_from_name_or_id<LabelT>(params.tables()[0]);
  ss << ensure_label_id(edge_label) << ",";

  CHECK(dst_vertex_labels.size() > 0);
  if (dst_vertex_labels.size() == 1) {
    ss << ensure_label_id(dst_vertex_labels[0]);
  } else {
    ss << "std::array<label_id_t, " << dst_vertex_labels.size() << ">{";
    for (auto i = 0; i < dst_vertex_labels.size() - 1; ++i) {
      ss << std::to_string(dst_vertex_labels[i]) << ", ";
    }
    ss << std::to_string(dst_vertex_labels[dst_vertex_labels.size() - 1])
       << "}";
  }
  // append expr if it exists.
  if (expr_var_name.size() > 0) {
    ss << ", std::move(" << expr_var_name << ")";
  }
  ss << ");" << std::endl;
  ss << std::endl;

  return std::make_pair(var_name, ss.str());
}

template <typename LabelT>
class EdgeExpandOpBuilder {
 public:
  EdgeExpandOpBuilder(BuildingContext& ctx)
      : ctx_(ctx), direction_(internal::Direction::kNotSet) {}
  ~EdgeExpandOpBuilder() = default;

  EdgeExpandOpBuilder& resAlias(const int32_t res_alias) {
    res_alias_ = ctx_.CreateOrGetTagInd(res_alias);
    return *this;
  }

  EdgeExpandOpBuilder& dstVertexLabels(
      const std::vector<LabelT>& dst_vertex_labels) {
    get_v_vertex_labels_ = dst_vertex_labels;
    // dst_vertex_labels_.reserve(dst_vertex_labels.size());
    // for (auto i = 0; i < dst_vertex_labels.size(); ++i) {
    //   dst_vertex_labels_.emplace_back(dst_vertex_labels[i]);
    // }
    return *this;
  }

  EdgeExpandOpBuilder& query_params(const algebra::QueryParams& query_params) {
    query_params_ = query_params;
    // if dst_vertex_labels found, use it.
    return *this;
  }

  EdgeExpandOpBuilder& expand_opt(const physical::EdgeExpand::ExpandOpt& opt) {
    expand_opt_ = opt;
    return *this;
  }

  EdgeExpandOpBuilder& direction(const physical::EdgeExpand::Direction& dir) {
    switch (dir) {
    case physical::EdgeExpand::Direction::EdgeExpand_Direction_OUT:
      direction_ = internal::Direction::kOut;
      break;

    case physical::EdgeExpand::Direction::EdgeExpand_Direction_IN:
      direction_ = internal::Direction::kIn;
      break;

    case physical::EdgeExpand::Direction::EdgeExpand_Direction_BOTH:
      direction_ = internal::Direction::kBoth;
      break;
    default:
      LOG(FATAL) << "Unknown direction";
    }
    return *this;
  }

  EdgeExpandOpBuilder& v_tag(const int32_t& v_tag) {
    v_tag_ = ctx_.GetTagInd(v_tag);
    return *this;
  }

  EdgeExpandOpBuilder& meta_data(
      const physical::PhysicalOpr::MetaData& meta_data) {
    meta_data_ = meta_data;
    // we can get the edge tuplet from meta_data, in case we fail to extract
    // edge triplet from ir_data_type

    // if dst_vertex_labels is already given by params.tables, we skip infer
    // from meta_data
    // if (dst_vertex_labels_.size() > 0) {
    //   LOG(INFO)
    //       << " when try to use meta data for dst labels, already larger then
    //       0"
    //       << gs::to_string(dst_vertex_labels_);
    //   return *this;
    // }
    // try to inner_join dst_vertex_labels and get_v_vertex_labels.
    {
      auto& ir_data_type = meta_data_.type();
      LOG(INFO) << "str: " << ir_data_type.DebugString();
      CHECK(ir_data_type.has_graph_type());
      auto& graph_ele_type = ir_data_type.graph_type();
      LOG(INFO) << "debug string: " << graph_ele_type.DebugString();
      CHECK(graph_ele_type.element_opt() ==
                common::GraphDataType::GraphElementOpt::
                    GraphDataType_GraphElementOpt_EDGE ||
            graph_ele_type.element_opt() ==
                common::GraphDataType::GraphElementOpt::
                    GraphDataType_GraphElementOpt_VERTEX)
          << "expect edge meta for edge builder";
      auto& graph_data_type = graph_ele_type.graph_data_type();
      CHECK(graph_data_type.size() > 0);

      CHECK(direction_ != internal::Direction::kNotSet);
      for (auto ele_labe_type : graph_data_type) {
        auto& triplet = ele_labe_type.label();
        auto& dst_label = triplet.dst_label();
        if (direction_ == internal::Direction::kOut) {
          LOG(INFO) << "got dst_label : " << dst_label.value();
          dst_vertex_labels_.emplace_back(dst_label.value());
        } else if (direction_ == internal::Direction::kIn) {
          dst_vertex_labels_.emplace_back(triplet.src_label().value());
        } else {  // kBoth
          auto src = triplet.src_label().value();
          auto dst = triplet.dst_label().value();
          CHECK(src == dst) << "When expand with direction, both, src and dst "
                               "label should be the same";
          dst_vertex_labels_.emplace_back(src);
        }
      }
      LOG(INFO) << "before join: " << gs::to_string(dst_vertex_labels_);
      LOG(INFO) << "before join get_v: " << gs::to_string(get_v_vertex_labels_);
      // only interset if get_v_vertex_labels specify any labels
      if (get_v_vertex_labels_.size() > 0) {
        intersection(dst_vertex_labels_, get_v_vertex_labels_);
      }
      {
        std::unordered_set<LabelT> s(dst_vertex_labels_.begin(),
                                     dst_vertex_labels_.end());
        dst_vertex_labels_.assign(s.begin(), s.end());
      }
      LOG(INFO) << "after join " << gs::to_string(dst_vertex_labels_);
      LOG(INFO) << "extract dst vertex label: "
                << gs::to_string(dst_vertex_labels_) << ", from meta data";
    }
    return *this;
  }

  std::string Build() const {
    std::stringstream ss;

    std::string opt_name, opt_code;
    std::tie(opt_name, opt_code) =
        BuildEdgeExpandOpt(ctx_, direction_, query_params_, dst_vertex_labels_,
                           expand_opt_, meta_data_);

    ss << opt_code;

    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();
    ss << "auto " << next_ctx_name << _ASSIGN_STR_ << "Engine::template ";
    if (expand_opt_ ==
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
      if (dst_vertex_labels_.size() > 1) {
        ss << EDGE_EXPAND_V_MULTI_LABEL_METHOD_NAME;
      } else {
        ss << EDGE_EXPAND_V_METHOD_NAME;
      }
    } else if (expand_opt_ ==
               physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
      ss << EDGE_EXPAND_E_METHOD_NAME;
    } else {
      throw std::runtime_error("not support expand opt");
    }
    ss << "<" << res_alias_ << ", " << v_tag_ << ">"
       << "(";
    {
      ss << ctx_.TimeStampVar() << ", ";
      ss << ctx_.GraphVar() << ", ";
      ss << "std::move(" << prev_ctx_name << "), ";
      ss << "std::move(" << opt_name << ")";
    }

    ss << ");" << std::endl;
    return ss.str();
  }

 private:
  BuildingContext& ctx_;
  int32_t res_alias_;
  algebra::QueryParams query_params_;
  physical::EdgeExpand::ExpandOpt expand_opt_;
  // physical::EdgeExpand::Direction direction_;
  internal::Direction direction_;
  std::vector<LabelT> dst_vertex_labels_;
  std::vector<LabelT> get_v_vertex_labels_;
  int32_t v_tag_;
  physical::PhysicalOpr::MetaData meta_data_;
};

template <typename LabelT>
static std::string BuildEdgeExpandOp(
    BuildingContext& ctx, const physical::EdgeExpand& edge_expand,
    const physical::PhysicalOpr::MetaData& meta_data) {
  LOG(INFO) << "Building Edge Expand Op: " << edge_expand.DebugString();
  EdgeExpandOpBuilder<LabelT> builder(ctx);
  if (edge_expand.has_alias()) {
    builder.resAlias(edge_expand.alias().value());
  } else {
    builder.resAlias(-1);
  }
  builder.query_params(edge_expand.params())
      .expand_opt(edge_expand.expand_opt())
      .direction(edge_expand.direction());
  if (edge_expand.has_v_tag()) {
    builder.v_tag(edge_expand.v_tag().value());
  } else {
    builder.v_tag(-1);
  }
  builder.meta_data(meta_data);
  return builder.Build();
}

// build edge expand op with dst vertex labels.
// the extra dst_vertex_labels are extracted from get_v, It can be a larger
// collection or a smaller collection.
template <typename LabelT>
static std::string BuildEdgeExpandOp(
    BuildingContext& ctx, const physical::EdgeExpand& edge_expand,
    const physical::PhysicalOpr::MetaData& meta_data,
    std::vector<LabelT> dst_vertex_labels) {
  LOG(INFO) << "Building Edge Expand Op: " << edge_expand.DebugString();
  EdgeExpandOpBuilder<LabelT> builder(ctx);
  if (edge_expand.has_alias()) {
    builder.resAlias(edge_expand.alias().value());
  } else {
    builder.resAlias(-1);
  }
  builder.dstVertexLabels(dst_vertex_labels)
      .query_params(edge_expand.params())
      .expand_opt(edge_expand.expand_opt())
      .direction(edge_expand.direction());
  if (edge_expand.has_v_tag()) {
    builder.v_tag(edge_expand.v_tag().value());
  } else {
    builder.v_tag(-1);
  }
  builder.meta_data(meta_data);
  return builder.Build();
}

}  // namespace gs

#endif  // GEN_SRC_EDGE_EXPAND_BUILDER_H_
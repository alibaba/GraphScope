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
#ifndef CODEGEN_SRC_HQPS_HQPS_EDGE_EXPAND_BUILDER_H_
#define CODEGEN_SRC_HQPS_HQPS_EDGE_EXPAND_BUILDER_H_

#include <sstream>
#include <string>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/pb_parser/internal_struct.h"
#include "flex/codegen/src/pb_parser/ir_data_type_parser.h"
#include "flex/codegen/src/pb_parser/name_id_parser.h"
#include "flex/codegen/src/pb_parser/query_params_parser.h"
#include "flex/codegen/src/string_utils.h"

#include "boost/format.hpp"

// Expand to vertices with filter.
static constexpr const char* EDGE_EXPAND_V_OPT_FILTER_TEMPLATE_STR =
    "auto %1% = gs::make_filter(%2%(%3%) %4%);\n"
    "auto %5% = gs::make_edge_expandv_opt(%6%, %7%, %8%, std::move(%1%));\n";

// Expand to vertices with no filter.
static constexpr const char* EDGE_EXPAND_V_OPT_NO_FILTER_TEMPLATE_STR =
    "auto %1% = gs::make_edge_expandv_opt(%2%, %3%, %4%);\n";

// This opt can only be used by both edge expande, with multiple edge triplet.
static constexpr const char*
    EDGE_EXPAND_E_OPT_MULTI_EDGE_NO_FILTER_TEMPLATE_STR =
        "auto %1% = gs::make_edge_expand_multie_opt<%2%>(%3%, %4%, %5%);\n";

// This opt can only be used by both edge expandv, with multiple edge triplet,
static constexpr const char*
    EDGE_EXPAND_V_OPT_MULTI_EDGE_NO_FILTER_TEMPLATE_STR =
        "auto %1% = gs::make_edge_expand_multiv_opt(%2%, %3%);\n";

// Expand to Edges with Filter.
// propNames, direction, edge_label, vertex_label, filter
static constexpr const char* EDGE_EXPAND_E_OPT_FILTER_TEMPLATE_STR =
    "auto %1% = gs::make_filter(%2%(%3%) %4%);\n"
    "auto %5% = gs::make_edge_expande_opt<%6%>(%7%, %8%, %9%, %10%, "
    "std::move(%1%));\n";
// Expand to Edges with no filter
static constexpr const char* EDGE_EXPAND_E_OPT_NO_FILTER_TEMPLATE_STR =
    "auto %1% = gs::make_edge_expande_opt<%2%>(%3%, %4%, %5%, %6%);\n";

/* i.e.
  auto filter = gs::make_filter(TruePredicate(), EmptySelector);
  auto opt =
 gs::make_edge_expande_opt(std::tuple{PropertySelector<int64_t>("creationDate")},
         Direction::In, knows_label, person_label, std::move(filter));
*/

static constexpr const char* EDGE_EXPANDV_OP_TEMPLATE_STR =
    "auto %1% = Engine::template EdgeExpandV<%2%, %3%>(%4%, %5%, %6%);\n";

static constexpr const char* EDGE_EXPANDE_OP_TEMPLATE_STR =
    "auto %1% = Engine::template EdgeExpandE<%2%,%3%>(%4%, %5%, %6%);\n";

namespace gs {
// create edge expand opt
// the expression in query params are applied on edge.
// extract the edge property from ir_data_type
// Building for only one label.

std::string make_edge_expand_e_func_template_str(
    const std::vector<std::vector<std::string>>& edge_prop_types) {
  std::stringstream ss;
  ss << "label_id_t";
  if (edge_prop_types.size() > 0) {
    ss << ",";
  }
  // if empty type is given, use grape::EmptyType.
  for (size_t i = 0; i < edge_prop_types.size(); ++i) {
    ss << "std::tuple<";
    if (edge_prop_types[i].size() == 0) {
      ss << "grape::EmptyType";
    } else {
      for (size_t j = 0; j < edge_prop_types[i].size(); ++j) {
        ss << edge_prop_types[i][j];
        if (j != edge_prop_types[i].size() - 1) {
          ss << ", ";
        }
      }
    }
    ss << ">";
    if (i != edge_prop_types.size() - 1) {
      ss << ", ";
    }
  }
  return ss.str();
}

std::string edge_label_triplet_to_array_str(
    const std::vector<std::vector<int32_t>>& edge_label_triplet) {
  std::stringstream ss;
  ss << "std::array<std::array<label_id_t, 3>, " << edge_label_triplet.size()
     << ">{";
  for (size_t i = 0; i < edge_label_triplet.size(); ++i) {
    ss << "std::array<label_id_t, 3>{";
    CHECK(edge_label_triplet[i].size() == 3);
    for (size_t j = 0; j < edge_label_triplet[i].size(); ++j) {
      ss << edge_label_triplet[i][j];
      if (j != edge_label_triplet[i].size() - 1) {
        ss << ", ";
      }
    }
    ss << "}";
    if (i != edge_label_triplet.size() - 1) {
      ss << ", ";
    }
  }
  ss << "}";
  return ss.str();
}

std::string edge_label_triplet_to_vector_str(
    const std::vector<std::vector<int32_t>>& edge_label_triplet) {
  std::stringstream ss;
  ss << "std::vector<std::array<label_id_t, 3>>{";
  for (size_t i = 0; i < edge_label_triplet.size(); ++i) {
    ss << "std::array<label_id_t, 3>{";
    if (edge_label_triplet[i].size() != 3) {
      throw std::runtime_error("edge label triplet size must be 3");
    }
    for (size_t j = 0; j < edge_label_triplet[i].size(); ++j) {
      ss << edge_label_triplet[i][j];
      if (j != edge_label_triplet[i].size() - 1) {
        ss << ", ";
      }
    }
    ss << "}";
    if (i != edge_label_triplet.size() - 1) {
      ss << ", ";
    }
  }
  ss << "}";
  return ss.str();
}

std::string make_prop_tuple_array(const std::vector<std::string>& prop_names,
                                  const std::vector<std::string>& prop_types) {
  std::stringstream ss;
  static constexpr const char* PROP_TUPLE_ARRAY_TEMPLATE =
      "PropTupleArrayT<std::tuple<%1%>>{%2%}";
  // join prop_names with ,
  std::string prop_names_str;
  {
    std::stringstream ss;
    for (size_t i = 0; i < prop_names.size(); ++i) {
      ss << add_quote(prop_names[i]);
      if (i != prop_names.size() - 1) {
        ss << ", ";
      }
    }
    prop_names_str = ss.str();
  }
  // join prop_types with ,
  std::string prop_types_str;
  {
    std::stringstream ss;
    if (prop_types.size() == 0) {
      ss << "grape::EmptyType";
    } else {
      for (size_t i = 0; i < prop_types.size(); ++i) {
        if (prop_types[i].empty()) {
          ss << "grape::EmptyType";
        } else {
          ss << prop_types[i];
        }
        if (i != prop_types.size() - 1) {
          ss << ", ";
        }
      }
    }
    prop_types_str = ss.str();
  }
  boost::format formater(PROP_TUPLE_ARRAY_TEMPLATE);
  formater % prop_types_str % prop_names_str;
  return formater.str();
}

// for  properties of one label. construct a propTupleArray.
std::string make_prop_tuple_array_tuple(
    const std::vector<std::vector<std::string>>& prop_names,
    const std::vector<std::vector<std::string>>& prop_types) {
  std::stringstream ss;
  ss << "std::tuple{";
  CHECK(prop_names.size() == prop_types.size());
  for (size_t i = 0; i < prop_names.size(); ++i) {
    VLOG(10) << "prop_names: " << gs::to_string(prop_names[i])
             << ", prop_types: " << gs::to_string(prop_types[i]);
    ss << make_prop_tuple_array(prop_names[i], prop_types[i]);
    if (i != prop_names.size() - 1) {
      ss << ", ";
    }
  }
  ss << "}";
  return ss.str();
}

template <typename LabelT>
static std::pair<std::string, std::string> BuildOneLabelEdgeExpandOpt(
    BuildingContext& ctx, const internal::Direction& direction,
    const algebra::QueryParams& params,
    const std::vector<LabelT>& dst_vertex_labels,
    const physical::EdgeExpand::ExpandOpt& expand_opt,
    const physical::PhysicalOpr::MetaData& meta_data) {
  std::string expr_func_name;
  std::string expr_var_name = ctx.GetNextExprVarName();
  std::string opt_var_name = ctx.GetNextEdgeOptName();
  std::string func_construct_params_str, property_selectors_str,
      edge_label_id_str, dst_label_ids_str, edge_prop_selectors_str,
      edge_expand_e_types_str;
  {
    std::vector<std::vector<std::string>> prop_names;
    std::vector<std::vector<std::string>> prop_types;
    std::tie(prop_names, prop_types) =
        parse_prop_names_and_prop_types_from_ir_data_type(meta_data.type());
    // we only support one property.

    if (prop_names.size() > 0) {
      // check prop_names[0] is same with prop_names[1] and others
      for (size_t i = 1; i < prop_names.size(); ++i) {
        CHECK(prop_names[0] == prop_names[i]);
      }
      auto& cur_prop_names = prop_names[0];
      auto& cur_prop_types = prop_types[0];

      CHECK(cur_prop_names.size() == cur_prop_types.size());
      {
        std::string type_names;
        {
          std::stringstream ss;
          for (size_t i = 0; i < cur_prop_types.size(); ++i) {
            if (cur_prop_types[i].empty()) {
              ss << "grape::EmptyType";
            } else {
              ss << cur_prop_types[i];
            }
            if (i != cur_prop_types.size() - 1) {
              ss << ", ";
            }
          }
          edge_expand_e_types_str = ss.str();
          if (edge_expand_e_types_str.empty()) {
            edge_expand_e_types_str = "grape::EmptyType";
          }
        }
        {
          std::stringstream ss;
          for (size_t i = 0; i < cur_prop_names.size(); ++i) {
            ss << add_quote(cur_prop_names[i]);
            if (i != cur_prop_names.size() - 1) {
              ss << ", ";
            }
          }
          type_names = ss.str();
          if (type_names.empty()) {
            type_names = "\"\"";
          }
        }
        boost::format formater(PROP_NAME_ARRAY);
        formater % edge_expand_e_types_str % type_names;
        edge_prop_selectors_str = formater.str();
      }
    } else {
      VLOG(10) << "No property found for edge expand";
    }
  }
  // first check whether expand_opt contains expression.
  if (params.has_predicate()) {
    VLOG(10) << "Found expr in edge expand";
    auto& expr = params.predicate();
    auto expr_builder = ExprBuilder(ctx);
    expr_builder.set_return_type(common::DataType::BOOLEAN);
    expr_builder.AddAllExprOpr(expr.operators());
    std::string expr_code;
    std::vector<codegen::ParamConst> func_call_param_const;
    std::vector<std::pair<int32_t, std::string>> expr_tag_props;
    std::vector<common::DataType> unused_expr_ret_type;
    std::tie(expr_func_name, func_call_param_const, expr_tag_props, expr_code,
             unused_expr_ret_type) = expr_builder.Build();
    VLOG(10) << "Found expr in edge_expand_opt:  " << expr_func_name;
    // generate code.
    ctx.AddExprCode(expr_code);

    {
      std::stringstream ss;
      for (size_t i = 0; i < func_call_param_const.size(); ++i) {
        ss << func_call_param_const[i].var_name;
        if (i != func_call_param_const.size() - 1) {
          ss << ", ";
        }
      }
      func_construct_params_str = ss.str();
    }
    {
      std::stringstream ss;
      if (expr_tag_props.size() > 0) {
        ss << ",";
      }
      for (size_t i = 0; i < expr_tag_props.size(); ++i) {
        ss << expr_tag_props[i].second;
        if (i != expr_tag_props.size() - 1) {
          ss << ", ";
        }
      }
      property_selectors_str = ss.str();
    }
  }

  {
    LabelT edge_label =
        try_get_label_from_name_or_id<LabelT>(params.tables()[0]);
    edge_label_id_str = ensure_label_id(edge_label);
  }

  {
    CHECK(dst_vertex_labels.size() > 0);
    if (dst_vertex_labels.size() == 1) {
      dst_label_ids_str = ensure_label_id(dst_vertex_labels[0]);
    } else {
      dst_label_ids_str = label_ids_to_array_str(dst_vertex_labels);
    }
  }
  boost::format formater("");
  if (expand_opt ==
      physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
    if (params.has_predicate()) {
      VLOG(10) << "Building EdgeExpandE with predicate";
      formater = boost::format(EDGE_EXPAND_E_OPT_FILTER_TEMPLATE_STR);
      formater % expr_var_name % expr_func_name % func_construct_params_str %
          property_selectors_str % opt_var_name % edge_expand_e_types_str %
          edge_prop_selectors_str % gs::direction_pb_to_str(direction) %
          edge_label_id_str % dst_label_ids_str;
    } else {
      VLOG(10) << "Building EdgeExpandE without predicate";
      formater = boost::format(EDGE_EXPAND_E_OPT_NO_FILTER_TEMPLATE_STR);
      formater % opt_var_name % edge_expand_e_types_str %
          edge_prop_selectors_str % gs::direction_pb_to_str(direction) %
          edge_label_id_str % dst_label_ids_str;
    }
  } else {
    if (params.has_predicate()) {
      VLOG(10) << "Building EdgeExpandV with predicate";
      formater = boost::format(EDGE_EXPAND_V_OPT_FILTER_TEMPLATE_STR);
      formater % expr_var_name % expr_func_name % func_construct_params_str %
          property_selectors_str % opt_var_name %
          gs::direction_pb_to_str(direction) % edge_label_id_str %
          dst_label_ids_str;
    } else {
      VLOG(10) << "Building EdgeExpandV without predicate";
      formater = boost::format(EDGE_EXPAND_V_OPT_NO_FILTER_TEMPLATE_STR);
      formater % opt_var_name % gs::direction_pb_to_str(direction) %
          edge_label_id_str % dst_label_ids_str;
    }
  }

  return std::make_pair(opt_var_name, formater.str());
}

// Building edge expand opt with multiple edge triplet, no expression are
// supported in query currently.
static std::pair<std::string, std::string> BuildMultiLabelEdgeExpandOpt(
    BuildingContext& ctx, const internal::Direction& direction,
    const algebra::QueryParams& params,
    const physical::EdgeExpand::ExpandOpt& expand_opt,
    const physical::PhysicalOpr::MetaData& meta_data) {
  std::string opt_var_name = ctx.GetNextEdgeOptName();
  std::string func_construct_params_str, property_selectors_str,
      edge_label_id_str, dst_label_ids_str, edge_prop_selectors_str,
      edge_expand_e_types_str;

  std::vector<std::vector<std::string>> prop_names;
  std::vector<std::vector<std::string>> prop_types;
  std::tie(prop_names, prop_types) =
      parse_prop_names_and_prop_types_from_ir_data_type(meta_data.type());
  CHECK(prop_names.size() == prop_types.size());

  std::vector<std::vector<int32_t>> edge_label_triplet =
      parse_edge_label_triplet_from_ir_data_type(meta_data.type());
  CHECK(edge_label_triplet.size() == prop_names.size());
  LOG(INFO) << "Find multiple edge triplet: " << edge_label_triplet.size();

  auto func_template_str = make_edge_expand_e_func_template_str(prop_types);
  auto edge_named_prop_array =
      make_prop_tuple_array_tuple(prop_names, prop_types);

  boost::format formater;
  if (expand_opt ==
      physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
    auto edge_triplet_2d_array =
        edge_label_triplet_to_array_str(edge_label_triplet);
    formater =
        boost::format(EDGE_EXPAND_E_OPT_MULTI_EDGE_NO_FILTER_TEMPLATE_STR);
    formater % opt_var_name % func_template_str %
        gs::direction_pb_to_str(direction) % edge_triplet_2d_array %
        edge_named_prop_array;
  } else if (expand_opt ==
             physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_VERTEX) {
    auto edge_triplet_2d_vector =
        edge_label_triplet_to_vector_str(edge_label_triplet);
    formater =
        boost::format(EDGE_EXPAND_V_OPT_MULTI_EDGE_NO_FILTER_TEMPLATE_STR);
    formater % opt_var_name % gs::direction_pb_to_str(direction) %
        edge_triplet_2d_vector;
  } else {
    throw std::runtime_error("Unknown expand opt");
  }

  return std::make_pair(opt_var_name, formater.str());
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
    return *this;
  }

  EdgeExpandOpBuilder& query_params(const algebra::QueryParams& query_params) {
    query_params_ = query_params;
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
    {
      auto& ir_data_type = meta_data_.type();
      VLOG(10) << "str: " << ir_data_type.DebugString();
      CHECK(ir_data_type.has_graph_type());
      auto& graph_ele_type = ir_data_type.graph_type();
      VLOG(10) << "debug string: " << graph_ele_type.DebugString();
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
      for (auto ele_label_type : graph_data_type) {
        auto& triplet = ele_label_type.label();
        auto& dst_label = triplet.dst_label();
        edge_labels_.emplace_back(triplet.label());
        if (direction_ == internal::Direction::kOut) {
          VLOG(10) << "got dst_label : " << dst_label.value();
          dst_vertex_labels_.emplace_back(dst_label.value());
        } else if (direction_ == internal::Direction::kIn) {
          dst_vertex_labels_.emplace_back(triplet.src_label().value());
        } else {  // kBoth
          auto src = triplet.src_label().value();
          dst_vertex_labels_.emplace_back(src);
        }
      }
      VLOG(10) << "before join: " << gs::to_string(dst_vertex_labels_);
      VLOG(10) << "before join get_v: " << gs::to_string(get_v_vertex_labels_);
      // only interset if get_v_vertex_labels specify any labels
      if (get_v_vertex_labels_.size() > 0) {
        intersection(dst_vertex_labels_, get_v_vertex_labels_);
      }
      {
        std::unordered_set<LabelT> s(dst_vertex_labels_.begin(),
                                     dst_vertex_labels_.end());
        dst_vertex_labels_.assign(s.begin(), s.end());
      }
      VLOG(10) << "after join " << gs::to_string(dst_vertex_labels_);
      VLOG(10) << "extract dst vertex label: "
               << gs::to_string(dst_vertex_labels_) << ", from meta data";
    }
    return *this;
  }

  std::string Build() const {
    std::string opt_name, opt_code;
    // if edge expand contains only one edge_triplet, generate the simple
    // EdgeExpandOpt.
    std::unordered_set<LabelT> edge_label_set(edge_labels_.begin(),
                                              edge_labels_.end());
    if (edge_label_set.size() == 1) {
      LOG(INFO) << "Building simple edge expand opt, with only one edge label";
      std::tie(opt_name, opt_code) = BuildOneLabelEdgeExpandOpt(
          ctx_, direction_, query_params_, dst_vertex_labels_, expand_opt_,
          meta_data_);
    } else {
      std::tie(opt_name, opt_code) = BuildMultiLabelEdgeExpandOpt(
          ctx_, direction_, query_params_, expand_opt_, meta_data_);
    }

    std::string prev_ctx_name, next_ctx_name;
    std::tie(prev_ctx_name, next_ctx_name) = ctx_.GetPrevAndNextCtxName();
    boost::format formater("");
    if (expand_opt_ ==
        physical::EdgeExpand::ExpandOpt::EdgeExpand_ExpandOpt_EDGE) {
      formater = boost::format(EDGE_EXPANDE_OP_TEMPLATE_STR);
    } else {
      formater = boost::format(EDGE_EXPANDV_OP_TEMPLATE_STR);
    }

    auto append_opt = res_alias_to_append_opt(res_alias_);
    formater % next_ctx_name % append_opt % format_input_col(v_tag_) %
        ctx_.GraphVar() % make_move(prev_ctx_name) % make_move(opt_name);

    return opt_code + formater.str();
  }

 private:
  BuildingContext& ctx_;
  int32_t res_alias_;
  algebra::QueryParams query_params_;
  physical::EdgeExpand::ExpandOpt expand_opt_;
  internal::Direction direction_;
  std::vector<LabelT> dst_vertex_labels_;
  std::vector<LabelT> edge_labels_;
  std::vector<LabelT> get_v_vertex_labels_;
  int32_t v_tag_;
  physical::PhysicalOpr::MetaData meta_data_;
};

template <typename LabelT>
static std::string BuildEdgeExpandOp(
    BuildingContext& ctx, const physical::EdgeExpand& edge_expand,
    const physical::PhysicalOpr::MetaData& meta_data) {
  VLOG(10) << "Building Edge Expand Op: " << edge_expand.DebugString();
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
  VLOG(10) << "Building Edge Expand Op: " << edge_expand.DebugString();
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

#endif  // CODEGEN_SRC_HQPS_HQPS_EDGE_EXPAND_BUILDER_H_
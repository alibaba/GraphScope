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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_EDGE_EXPAND_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_EDGE_EXPAND_BUILDER_H_

#include <string>

#include <sstream>

#include "flex/proto_generated_gie/physical.pb.h"

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/pb_parser/ir_data_type_parser.h"
#include "flex/codegen/src/pb_parser/name_id_parser.h"
#include "flex/codegen/src/pb_parser/query_params_parser.h"
#include "flex/codegen/src/pegasus/pegasus_expr_builder.h"
#include "flex/codegen/src/string_utils.h"

#define NO_EXTRACT_PROP_FROM_IR_DATA_TYPE

namespace gs {
namespace pegasus {

template <typename LabelT>
class EdgeExpandOpBuilder {
 public:
  EdgeExpandOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}
  ~EdgeExpandOpBuilder() = default;

  EdgeExpandOpBuilder& operator_index(const int32_t operator_index) {
    operator_index_ = operator_index;
    return *this;
  }

  EdgeExpandOpBuilder& resAlias(const int32_t res_alias) {
    res_alias_ = res_alias;
    return *this;
  }

  EdgeExpandOpBuilder& query_params(const algebra::QueryParams& query_params) {
    query_params_ = query_params;
    if (query_params_.has_predicate()) {
      ExprBuilder expr_builder(ctx_);
      auto& expr_oprs = query_params_.predicate().operators();
      expr_builder.AddAllExprOpr(expr_oprs);
      std::vector<int32_t> predicate_tag;
      std::vector<std::string> case_exprs;
      std::tie(predicate_expr_, var_names_, predicate_tag, properties_,
               case_exprs) = expr_builder.BuildRust();
    }
    return *this;
  }

  EdgeExpandOpBuilder& expand_opt(const physical::EdgeExpand::ExpandOpt& opt) {
    expand_opt_ = opt;
    return *this;
  }

  EdgeExpandOpBuilder& direction(const physical::EdgeExpand::Direction& dir) {
    direction_ = dir;
    return *this;
  }

  EdgeExpandOpBuilder& v_tag(const int32_t& v_tag) {
    v_tag_ = v_tag;
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
      auto& graph_type = ir_data_type.graph_type();
      VLOG(10) << "debug string: " << graph_type.DebugString();
      CHECK(graph_type.element_opt() == common::GraphDataType::GraphElementOpt::
                                            GraphDataType_GraphElementOpt_EDGE)
          << "expect edge meta for edge builder";
      auto& ele_label_types = graph_type.graph_data_type();
      CHECK(ele_label_types.size() > 0);
      for (auto ele_label_type : ele_label_types) {
        auto& triplet = ele_label_type.label();
        auto& src_label = triplet.src_label();
        auto& dst_label = triplet.dst_label();
        src_vertex_labels_.emplace_back(src_label.value());
        dst_vertex_labels_.emplace_back(dst_label.value());
      }
      VLOG(10) << "extract dst vertex label: "
               << gs::to_string(dst_vertex_labels_) << ", from meta data";
    }
    return *this;
  }

  EdgeExpandOpBuilder& set_intersect(bool intersect) {
    is_intersect_ = intersect;
    return *this;
  }

  std::string Build() const {
    std::stringstream ss;

    VLOG(10) << "Start write head";
    std::string head_code = write_head();

    int32_t input_index;
    std::pair<int32_t, std::vector<int32_t>> in_type;
    if (v_tag_ == -1) {
      input_index = 0;
      in_type = ctx_.GetHeadType();
    } else {
      input_index = ctx_.GetAliasIndex(v_tag_);
      in_type = ctx_.GetAliasType(v_tag_);
    }

    VLOG(10) << "Start write edge expand";
    int32_t input_size = ctx_.InputSize();
    std::unordered_set<LabelT> start_labels_set;
    std::vector<LabelT> start_labels, end_labels;
    if (direction_ == physical::EdgeExpand_Direction::EdgeExpand_Direction_IN) {
      for (size_t i = 0; i < src_vertex_labels_.size(); i++) {
        end_labels.push_back(src_vertex_labels_[i]);
      }
      for (size_t i = 0; i < dst_vertex_labels_.size(); i++) {
        start_labels_set.insert(dst_vertex_labels_[i]);
        start_labels.push_back(dst_vertex_labels_[i]);
      }
    } else {
      for (size_t i = 0; i < dst_vertex_labels_.size(); i++) {
        end_labels.push_back(dst_vertex_labels_[i]);
      }
      for (size_t i = 0; i < src_vertex_labels_.size(); i++) {
        start_labels_set.insert(src_vertex_labels_[i]);
        start_labels.push_back(src_vertex_labels_[i]);
      }
    }

    int32_t edge_labels = query_params_.tables_size();
    std::stringstream expand_code_ss;
    for (int32_t i = 0; i < edge_labels; i++) {
      auto edge_label = query_params_.tables(i).id();
      if (start_labels_set.size() > 1) {
        boost::format multi_labels_fmter(
            "let vertex_label = LDBCVertexParser::<usize>::get_label_id(i%1% "
            "as usize);\n"
            "%2%");
        std::string labels_expand_code;
        for (size_t j = 0; j < src_vertex_labels_.size(); ++j) {
          boost::format with_label_fmter(
              "if vertex_label == %1% {\n"
              "%2%"
              "}\n");
          with_label_fmter % start_labels[j];
          std::string expand_code;
          if (direction_ ==
              physical::EdgeExpand_Direction::EdgeExpand_Direction_IN) {
            expand_code = write_edge_expand(
                src_vertex_labels_[j], edge_label, dst_vertex_labels_[j],
                physical::EdgeExpand_Direction::EdgeExpand_Direction_IN);
          } else if (direction_ ==
                     physical::EdgeExpand_Direction::EdgeExpand_Direction_OUT) {
            expand_code = write_edge_expand(
                src_vertex_labels_[j], edge_label, dst_vertex_labels_[j],
                physical::EdgeExpand_Direction::EdgeExpand_Direction_OUT);
          } else if (direction_ == physical::EdgeExpand_Direction::
                                       EdgeExpand_Direction_BOTH) {
            expand_code = write_edge_expand(
                src_vertex_labels_[j], edge_label, dst_vertex_labels_[j],
                physical::EdgeExpand_Direction::EdgeExpand_Direction_IN);
            expand_code += write_edge_expand(
                src_vertex_labels_[j], edge_label, dst_vertex_labels_[j],
                physical::EdgeExpand_Direction::EdgeExpand_Direction_OUT);
          }
          with_label_fmter % expand_code;
          labels_expand_code += with_label_fmter.str();
        }
        multi_labels_fmter % input_index % labels_expand_code;
        expand_code_ss << multi_labels_fmter.str();
      } else {
        for (size_t j = 0; j < src_vertex_labels_.size(); ++j) {
          if (direction_ ==
              physical::EdgeExpand_Direction::EdgeExpand_Direction_IN) {
            expand_code_ss << write_edge_expand(
                src_vertex_labels_[j], edge_label, dst_vertex_labels_[j],
                physical::EdgeExpand_Direction::EdgeExpand_Direction_IN);
          } else if (direction_ ==
                     physical::EdgeExpand_Direction::EdgeExpand_Direction_OUT) {
            expand_code_ss << write_edge_expand(
                src_vertex_labels_[j], edge_label, dst_vertex_labels_[j],
                physical::EdgeExpand_Direction::EdgeExpand_Direction_OUT);
          } else if (direction_ == physical::EdgeExpand_Direction::
                                       EdgeExpand_Direction_BOTH) {
            expand_code_ss << write_edge_expand(
                src_vertex_labels_[j], edge_label, dst_vertex_labels_[j],
                physical::EdgeExpand_Direction::EdgeExpand_Direction_IN);
            expand_code_ss << write_edge_expand(
                src_vertex_labels_[j], edge_label, dst_vertex_labels_[j],
                physical::EdgeExpand_Direction::EdgeExpand_Direction_OUT);
          }
        }
      }
    }
    VLOG(10) << "Start write body";
    boost::format edge_expand_body_fmter(
        "let vertex_id = graph.get_internal_id(i%1% as usize);\n"
        "%2%");
    edge_expand_body_fmter % input_index % expand_code_ss.str();

    std::vector<codegen::DataType> output;
    output.push_back(codegen::DataType::kInt64);
    auto outputs = ctx_.GetOutput();

    boost::format edge_expand_output_fmter(
        "Ok(result.into_iter().map(move |res| %1%))\n"
        "})?;\n");

    int32_t alias_index = -1;
    if (res_alias_ != -1) {
      ctx_.SetAlias(res_alias_);
      ctx_.SetAliasType(res_alias_, 0, end_labels);
      alias_index = ctx_.GetAliasIndex(res_alias_);
    }
    ctx_.SetHead(true);
    ctx_.SetHeadType(0, end_labels);

    std::string output_params = generate_output_list(
        "i", input_size, "res", alias_index, ctx_.ContainHead());
    edge_expand_output_fmter % output_params;

    return head_code + edge_expand_body_fmter.str() +
           edge_expand_output_fmter.str();
  }

 private:
  std::string write_head() const {
    int32_t input_size = ctx_.InputSize();
    boost::format head_fmter(
        "let stream_%1% = stream_%2%.flat_map(move |%3%| {\n"
        "let mut result = vec![];\n");
    std::string input_params = generate_arg_list("i", input_size);
    head_fmter % operator_index_ % (operator_index_ - 1) % input_params;
    return head_fmter.str();
  }

  std::string write_edge_expand(
      LabelT src_label, int32_t edge_label, LabelT dst_label,
      physical::EdgeExpand::Direction direction) const {
    std::string predicate_code;
    boost::format edge_expand_fmter(
        "if let Some(edges) = %1%.get_adj_list(vertex_id) {\n"
        "for e in edges{\n"
        "%2%"  // predicate & get global_id
        "}\n"
        "}\n");
    std::string subgraph_name =
        get_subgraph_name(src_label, edge_label, dst_label, direction);

    int32_t adj_label;
    if (direction_ == physical::EdgeExpand_Direction::EdgeExpand_Direction_IN) {
      adj_label = src_label;
    } else if (direction_ ==
               physical::EdgeExpand_Direction::EdgeExpand_Direction_OUT) {
      adj_label = dst_label;
    } else {
      LOG(FATAL) << "Unexpected direction";
    }
    std::string edge_traverse_code;
    if (query_params_.has_predicate()) {
      boost::format predicate_fmter(
          "%1%"
          "if %2% {\n"
          "result.push(graph.get_global_id(e.neighbor, %3%).unwrap() as u64);\n"
          "}\n");
      std::stringstream vars_stream;
      for (size_t i = 0; i < var_names_.size(); i++) {
        boost::format var_fmter("let %1% = %2%[e.neighbor];\n");
        var_fmter % var_names_[i] %
            get_edge_prop_column_name(properties_[i].var_name, src_label,
                                      edge_label, dst_label, direction);
        vars_stream << var_fmter.str();
      }
      predicate_fmter % vars_stream.str() % predicate_expr_ % adj_label;
      edge_traverse_code = predicate_fmter.str();
    } else {
      boost::format no_predicate_fmter(
          "result.push(graph.get_global_id(e.neighbor, %1%).unwrap() as "
          "u64);\n");
      no_predicate_fmter % adj_label;
      edge_traverse_code = no_predicate_fmter.str();
    }

    edge_expand_fmter % subgraph_name % edge_traverse_code;

    return edge_expand_fmter.str();
  }

  BuildingContext& ctx_;
  int32_t operator_index_;
  int32_t res_alias_;
  algebra::QueryParams query_params_;
  physical::EdgeExpand::ExpandOpt expand_opt_;
  physical::EdgeExpand::Direction direction_;
  std::vector<LabelT> src_vertex_labels_;
  std::vector<LabelT> dst_vertex_labels_;
  int32_t v_tag_;
  physical::PhysicalOpr::MetaData meta_data_;
  std::string predicate_expr_;
  std::vector<std::string> var_names_;
  std::vector<codegen::ParamConst> properties_;
  bool is_intersect_ = false;
};

template <typename LabelT>
static std::string BuildEdgeExpandOp(
    BuildingContext& ctx, int32_t operator_index,
    const physical::EdgeExpand& edge_expand,
    const physical::PhysicalOpr::MetaData& meta_data,
    bool is_intersect = false) {
  EdgeExpandOpBuilder<LabelT> builder(ctx);
  if (edge_expand.has_alias()) {
    builder.resAlias(edge_expand.alias().value());
  } else {
    builder.resAlias(-1);
  }
  builder.query_params(edge_expand.params())
      .expand_opt(edge_expand.expand_opt())
      .direction(edge_expand.direction())
      .meta_data(meta_data);
  if (edge_expand.has_v_tag()) {
    builder.v_tag(edge_expand.v_tag().value());
  } else {
    builder.v_tag(-1);
  }
  builder.set_intersect(is_intersect);
  return builder.operator_index(operator_index).Build();
}

}  // namespace pegasus
}  // namespace gs

#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_EDGE_EXPAND_BUILDER_H_
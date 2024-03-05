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
#ifndef CODEGEN_SRC_PEGASUS_PEGASUS_PATH_EXPAND_BUILDER_H_
#define CODEGEN_SRC_PEGASUS_PEGASUS_PATH_EXPAND_BUILDER_H_

#include <string>
#include <vector>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/codegen_utils.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/pegasus/pegasus_repartition_builder.h"
#include "flex/proto_generated_gie/algebra.pb.h"
#include "flex/proto_generated_gie/common.pb.h"
#include "flex/proto_generated_gie/expr.pb.h"

namespace gs {
namespace pegasus {
template <typename LabelT>
class PathExpandOpBuilder {
 public:
  PathExpandOpBuilder(BuildingContext& ctx) : ctx_(ctx) {}

  PathExpandOpBuilder& in_tag(int32_t in_tag_id) {
    in_tag_id_ = in_tag_id;
    return *this;
  }

  PathExpandOpBuilder& out_tag(int32_t out_tag_id) {
    out_tag_id_ = out_tag_id;
    return *this;
  }

  PathExpandOpBuilder& meta_data(
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
        if (edge_expand_.direction() ==
            physical::EdgeExpand_Direction::EdgeExpand_Direction_OUT) {
          src_vertex_labels_.emplace_back(src_label.value());
          dst_vertex_labels_.emplace_back(dst_label.value());
        } else {
          src_vertex_labels_.emplace_back(dst_label.value());
          dst_vertex_labels_.emplace_back(src_label.value());
        }
      }
      VLOG(10) << "extract dst vertex label: "
               << gs::to_string(dst_vertex_labels_) << ", from meta data";
    }
    return *this;
  }

  // get the expand_opt name and the expand_opt code.
  PathExpandOpBuilder& edge_expand_opt(
      const physical::EdgeExpand& edge_expand_pb) {
    edge_expand_ = edge_expand_pb;
    return *this;
  }

  // get the getv name and the getv code.
  PathExpandOpBuilder& getv_opt(const physical::GetV& get_v_pb) {
    get_v_ = get_v_pb;
    return *this;
  }

  PathExpandOpBuilder& hop_range(const algebra::Range& hop_range_pb) {
    range_lower_ = hop_range_pb.lower();
    range_upper_ = hop_range_pb.upper();
    VLOG(10) << "got range: " << range_lower_ << " " << range_upper_;
    return *this;
  }

  PathExpandOpBuilder& path_opt(
      const physical::PathExpand::PathOpt& path_opt_pb) {
    path_opt_ = path_opt_pb;
    return *this;
  }

  PathExpandOpBuilder& result_opt(
      const physical::PathExpand::ResultOpt& result_opt_pb) {
    result_opt_ = result_opt_pb;
    return *this;
  }

  PathExpandOpBuilder& condition(const common::Expression& condition_pb) {
    LOG(WARNING) << "Skipped for path expand with condition";
    return *this;
  }

  std::string Build() const {
    std::stringstream ss;
    int32_t input_size = ctx_.InputSize();
    VLOG(10) << "path_expand start";
    if (in_tag_id_ != -1 ||
        path_opt_ == physical::PathExpand::PathOpt::PathExpand_PathOpt_SIMPLE ||
        result_opt_ ==
            physical::PathExpand::ResultOpt::PathExpand_ResultOpt_ALL_V) {
      // move data to head
      ss << ".map(move |";
      write_arg_list(ss, "i", input_size);
      ss << "| {\n";
      int32_t input_index = 0;
      if (in_tag_id_ != -1) {
        input_index = ctx_.GetAliasIndex(in_tag_id_);
      }
      if (path_opt_ ==
              physical::PathExpand::PathOpt::PathExpand_PathOpt_SIMPLE ||
          result_opt_ ==
              physical::PathExpand::ResultOpt::PathExpand_ResultOpt_ALL_V) {
        ss << "let result = vec![i" << input_index << "];\n";
        write_result(ss, "result", -1);
      } else {
        std::string result_name = "i" + std::to_string(input_index);
        write_result(ss, result_name, -1);
      }
      ss << "})?\n";
    }
    VLOG(10) << "path_expand finished write head operator";

    // start iterate operator
    ss << ".iterate_emit_until(IterCondition::max_iters(" << range_upper_
       << "), EmitKind::Before, |start| {\nstart\n";
    // repartition for path_expand
    ss << ".repartition(move |input| {\n";

    ss << "Ok(get_partition(&input.0, workers as usize, "
          "pegasus::get_servers_len()))\n";
    ss << "})\n";
    VLOG(10) << "path_expand finished write iterate_emit & repartition";

    // edge_expand for path_expand
    ss << ".flat_map(move |";
    write_arg_list(ss, "i", input_size);
    ss << "| {\n";
    ss << "let mut result = vec![];\n";
    ss << "let vertex_id = CSR.get_internal_id(i0 as usize);\n";
    int32_t edge_labels = edge_expand_.params().tables_size();
    for (auto i = 0; i < edge_labels; i++) {
      auto edge_label = edge_expand_.params().tables(i).id();
      VLOG(10) << "src labels size: " << src_vertex_labels_.size();
      VLOG(10) << "dst labels size: " << dst_vertex_labels_.size();
      if (src_vertex_labels_.size() > 1) {
        ss << "let vertex_label = LDBCVertexParser::<usize>::get_label_id(i0 "
              "as usize);\n";
        for (size_t j = 0; j < src_vertex_labels_.size(); ++j) {
          VLOG(10) << "get vertex_label " << src_vertex_labels_[j];
          if (j == 0) {
            ss << "if vertex_label == " << src_vertex_labels_[j] << " {\n";
          } else {
            ss << "else if vertex_label == " << src_vertex_labels_[j] << " {\n";
          }
          write_edge_expand(ss, src_vertex_labels_[j], edge_label,
                            dst_vertex_labels_[j]);
          ss << "}";
        }
      } else {
        auto src_label = src_vertex_labels_[0];
        for (size_t j = 0; j < dst_vertex_labels_.size(); ++j) {
          write_edge_expand(ss, src_label, edge_label, dst_vertex_labels_[j]);
        }
      }
      ss << "\n";
    }
    ss << "Ok(result.into_iter().map(|res| (res";
    for (auto i = 1; i < input_size; i++) {
      ss << ", i" << i;
    }
    ss << ")))\n";
    ss << "})?\n";
    VLOG(10) << "path_expand finished write edge_expand";

    // get_v for path_expand
    std::stringstream getv_ss;
    getv_ss << ".flat_map(move |";
    write_arg_list(getv_ss, "i", input_size);
    getv_ss << "| {\n";
    bool filter_label = false;
    std::vector<int32_t> vertex_labels;
    for (auto vertex_label_pb : get_v_.params().tables()) {
      vertex_labels.push_back(vertex_label_pb.id());
    }
    if (!vertex_labels.empty()) {
      for (size_t i = 0; i < dst_vertex_labels_.size(); ++i) {
        if (std::find(vertex_labels.begin(), vertex_labels.end(),
                      dst_vertex_labels_[i]) == vertex_labels.end()) {
          filter_label = true;
          break;
        }
      }
    }
    if (filter_label) {
      ss << getv_ss.str();
    }
    ss << "})?\n";

    std::vector<int32_t> labels;
    for (auto label : dst_vertex_labels_) {
      labels.push_back(label);
    }
    ctx_.SetHeadType(0, labels);
    return ss.str();
  }

 private:
  void write_arg_list(std::stringstream& ss, std::string arg_name,
                      int32_t size) const {
    if (size > 1) {
      ss << "(";
    }
    for (auto i = 0; i < size; ++i) {
      ss << arg_name << i;
      if (i < size - 1) {
        ss << ", ";
      }
    }
    if (size > 1) {
      ss << ")";
    }
  }

  void write_result(std::stringstream& ss, std::string result_name,
                    int32_t alias_index) const {
    int32_t input_size = ctx_.InputSize();
    ss << "Ok((" << result_name;
    if (ctx_.ContainHead()) {
      for (auto i = 1; i < input_size; i++) {
        if (i == alias_index) {
          ss << ", " << result_name;
        } else {
          ss << ", i" << i;
        }
      }
      if (input_size == alias_index) {
        ss << ", " << result_name;
      }
    } else {
      for (auto i = 0; i < input_size; i++) {
        if (i == alias_index) {
          ss << ", " << result_name;
        } else {
          ss << ", i" << i;
        }
      }
      if (input_size == alias_index) {
        ss << ", " << result_name;
      }
    }
    ss << "))";
  }

  void write_edge_expand(std::stringstream& ss, LabelT src_label,
                         int32_t edge_label, LabelT dst_label) const {
    if (edge_expand_.direction() ==
        physical::EdgeExpand_Direction::EdgeExpand_Direction_IN) {
      ss << " if let Some(edges) = EDGE_" << src_label << "_" << edge_label
         << "_" << dst_label << "_IN.get_adj_list(vertex_id) {\n";
      ss << "for e in edges {\n";
      ss << "result.push(CSR.get_global_id(e.neighbor).unwrap() as u64)";
      ss << "}\n";
      ss << "}\n";
    } else if (edge_expand_.direction() ==
               physical::EdgeExpand_Direction::EdgeExpand_Direction_OUT) {
      ss << " if let Some(edges) = EDGE_" << src_label << "_" << edge_label
         << "_" << dst_label << "_OUT.get_adj_list(vertex_id) {\n";
      ss << "for e in edges {\n";
      ss << "result.push(CSR.get_global_id(e.neighbor).unwrap() as u64)";
      ss << "}\n";
      ss << "}\n";
    } else if (edge_expand_.direction() ==
               physical::EdgeExpand_Direction::EdgeExpand_Direction_BOTH) {
      ss << " if let Some(edges) = EDGE_" << src_label << "_" << edge_label
         << "_" << dst_label << "_IN.get_adj_list(vertex_id) {\n";
      ss << "for e in edges {\n";
      ss << "result.push(CSR.get_global_id(e.neighbor).unwrap() as u64)";
      ss << "}\n";
      ss << "}\n";
      ss << " if let Some(edges) = EDGE_" << src_label << "_" << edge_label
         << "_" << dst_label << "_OUT.get_adj_list(vertex_id) {\n";
      ss << "for e in edges {\n";
      ss << "result.push(CSR.get_global_id(e.neighbor).unwrap() as u64)";
      ss << "}\n";
      ss << "}\n";
    } else {
      LOG(FATAL) << "Unsupported direction";
    }
  }

  BuildingContext& ctx_;
  int32_t in_tag_id_, out_tag_id_;
  physical::EdgeExpand edge_expand_;
  physical::GetV get_v_;
  int32_t range_lower_, range_upper_;
  physical::PathExpand::PathOpt path_opt_;
  physical::PathExpand::ResultOpt result_opt_;
  std::vector<LabelT> src_vertex_labels_;
  std::vector<LabelT> dst_vertex_labels_;
  physical::PhysicalOpr::MetaData meta_data_;
};

// edge_expand_opt
// get_v_opt
// path_expand_opt
// op_code.
template <typename LabelT>
static std::string BuildPathExpandOp(
    BuildingContext& ctx, const physical::PathExpand& path_expand_pb,
    const physical::PhysicalOpr::MetaData& meta_data) {
  PathExpandOpBuilder<LabelT> builder(ctx);
  if (path_expand_pb.has_start_tag()) {
    builder.in_tag(path_expand_pb.start_tag().value());
  } else {
    builder.in_tag(-1);
  }

  if (path_expand_pb.has_alias()) {
    builder.out_tag(path_expand_pb.alias().value());
  } else {
    builder.out_tag(-1);
  }

  return builder
      .getv_opt(
          path_expand_pb.base().get_v())  // get_v_opt must be called first to
                                          // provide dst_label ids.
      .edge_expand_opt(path_expand_pb.base().edge_expand())
      .hop_range(path_expand_pb.hop_range())
      .path_opt(path_expand_pb.path_opt())
      .result_opt(path_expand_pb.result_opt())
      .condition(path_expand_pb.condition())
      .meta_data(meta_data)
      .Build();
}
}  // namespace pegasus
}  // namespace gs

#endif  // CODEGEN_SRC_PEGASUS_PEGASUS_PATH_EXPAND_BUILDER_H_
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
#ifndef CODEGEN_SRC_CODEGEN_UTILS_H_
#define CODEGEN_SRC_CODEGEN_UTILS_H_

#include <boost/format.hpp>
#include <sstream>
#include <string>

#include "flex/codegen/src/building_context.h"
#include "flex/codegen/src/graph_types.h"
#include "flex/codegen/src/string_utils.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "glog/logging.h"

#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {

// remote duplicate from vector
template <typename T>
std::vector<T> remove_duplicate(const std::vector<T>& labels) {
  std::vector<T> res;
  std::set<T> label_set(labels.begin(), labels.end());
  res.assign(label_set.begin(), label_set.end());
  return res;
}

std::string get_vertex_prop_column_name(std::string prop_name,
                                        int32_t label_id) {
  boost::format column_name_fmter("property_%1%_%2%");
  column_name_fmter % prop_name % label_id;
  return column_name_fmter.str();
}

std::string get_edge_prop_column_name(
    std::string prop_name, int32_t src_label, int32_t edge_label,
    int32_t dst_label, physical::EdgeExpand::Direction direction) {
  boost::format column_name_fmter("property_%1%_%2%_%3%_%4%_%5%");
  std::string edge_direction;
  if (direction == physical::EdgeExpand_Direction::EdgeExpand_Direction_OUT) {
    edge_direction = "out";
  } else if (direction ==
             physical::EdgeExpand_Direction::EdgeExpand_Direction_OUT) {
    edge_direction = "in";
  } else {
    LOG(FATAL) << "Unexpect direction";
  }
  column_name_fmter % prop_name % src_label % edge_label % dst_label %
      edge_direction;
  return column_name_fmter.str();
}

std::string get_subgraph_name(int32_t src_label, int32_t edge_label,
                              int32_t dst_label,
                              physical::EdgeExpand::Direction direction) {
  boost::format subgraph_name_fmter("subgraph_%1%_%2%_%3%_%4%");
  std::string edge_direction;
  if (direction == physical::EdgeExpand_Direction::EdgeExpand_Direction_OUT) {
    edge_direction = "out";
  } else if (direction ==
             physical::EdgeExpand_Direction::EdgeExpand_Direction_IN) {
    edge_direction = "in";
  } else {
    LOG(FATAL) << "Unexpect direction";
  }
  subgraph_name_fmter % src_label % edge_label % dst_label % edge_direction;
  return subgraph_name_fmter.str();
}

std::string generate_arg_list(std::string arg_name, int32_t size) {
  std::stringstream arg_ss;
  if (size > 1) {
    arg_ss << "(";
  }
  for (int32_t i = 0; i < size; i++) {
    arg_ss << arg_name << i;
    if (i < size - 1) {
      arg_ss << ", ";
    }
  }
  if (size > 1) {
    arg_ss << ")";
  }
  return arg_ss.str();
}

std::string generate_output_list(std::string input_name, int32_t input_size,
                                 std::string result_name, int32_t alias_index,
                                 bool contain_head) {
  std::stringstream result_ss;
  result_ss << "(" << result_name;
  if (contain_head) {
    for (int32_t i = 1; i < input_size; i++) {
      if (i == alias_index) {
        result_ss << ", " << result_name;
      } else {
        result_ss << ", " << input_name << i;
      }
    }
    if (input_size == alias_index) {
      result_ss << ", " << result_name;
    }
  } else {
    for (int32_t i = 0; i < input_size; i++) {
      if (i == alias_index) {
        result_ss << ", " << result_name;
      } else {
        result_ss << ", " << input_name << i;
      }
    }
    if (input_size == alias_index) {
      result_ss << ", " << result_name;
    }
  }
  result_ss << ")";
  return result_ss.str();
}

// check type consistent
bool data_type_consistent(const common::DataType& left,
                          const common::DataType& right) {
  if (left == common::DataType::NONE || right == common::DataType::NONE) {
    return true;
  }
  return left == right;
}

std::tuple<std::string, std::string> decode_param_from_decoder(
    const codegen::ParamConst& param_const, int32_t ind,
    const std::string& var_prefix, const std::string& decoder_name) {
  std::stringstream ss;
  std::string var_name = var_prefix + std::to_string(ind);
  ss << _4_SPACES;
  ss << data_type_2_string(param_const.type) << " " << var_name << " = ";
  ss << decoder_name << "." << decode_type_as_str(param_const.type) << ";";
  ss << std::endl;
  return std::tuple{var_name, ss.str()};
}

template <typename T>
void intersection(std::vector<T>& v1, const std::vector<T>& v2) {
  std::vector<T> res;
  for (auto num : v1) {
    for (size_t i = 0; i < v2.size(); i++) {
      if (num == v2[i]) {
        res.push_back(num);
        break;
      }
    }
  }
  res.swap(v1);
}

std::vector<std::string> add_quotes(const std::vector<std::string>& strs) {
  std::vector<std::string> res;
  for (auto& str : strs) {
    res.emplace_back("\"" + str + "\"");
  }
  return res;
}

std::string with_quote(std::string res) { return "\"" + res + "\""; }

std::string make_named_property(const std::vector<std::string>& prop_names,
                                const std::vector<std::string>& prop_types) {
  std::stringstream ss;
  auto quoted_prop_names = add_quotes(prop_names);
  std::string prop_names_str = gs::to_string(quoted_prop_names);
  std::string prop_types_str = gs::to_string(prop_types);
  ss << NAMED_PROPERTY_CLASS_NAME << "<" << prop_types_str << ">";
  ss << "(";
  // ss << "{" << prop_names_str << "}";
  ss << prop_names_str;
  ss << ")";
  return ss.str();
}

std::string make_inner_id_property(int tag_id, std::string prop_type) {
  std::stringstream ss;
  ss << INNER_ID_PROPERTY_NAME << "<" << tag_id << ">{}";
  return ss.str();
}

codegen::ParamConst variable_to_param_const(const common::Variable& var,
                                            BuildingContext& ctx) {
  codegen::ParamConst param_const;
  if (var.has_property()) {
    auto& var_property = var.property();
    if (var_property.has_label()) {
      param_const.var_name = "label";
      param_const.type = codegen::DataType::kLabelId;
    } else if (var_property.has_key()) {
      param_const.var_name = var.property().key().name();
      param_const.type =
          common_data_type_pb_2_data_type(var.node_type().data_type());
    } else {
      LOG(FATAL) << "Unexpected property type";
    }
  } else if (var.has_tag()) {
    // check is vertex or is edge from node_type
    if (var.has_node_type()) {
      auto node_type = var.node_type();
      param_const.var_name = ctx.GetNextVarName();
      if (node_type.type_case() == common::IrDataType::kDataType) {
        param_const.type =
            common_data_type_pb_2_data_type(node_type.data_type());
      } else {
        auto graph_type = node_type.graph_type();
        if (graph_type.element_opt() ==
            common::GraphDataType::GraphElementOpt::
                GraphDataType_GraphElementOpt_VERTEX) {
          param_const.type = codegen::DataType::kVertexId;
        } else if (graph_type.element_opt() ==
                   common::GraphDataType::GraphElementOpt::
                       GraphDataType_GraphElementOpt_EDGE) {
          param_const.type = codegen::DataType::kEdgeId;
        } else {
          LOG(FATAL) << "Unexpect graph type";
        }
      }
    } else {
      LOG(FATAL)
          << "Node type is not given when converting variable to param const";
    }
  }

  return param_const;
}

std::string interval_to_str(const common::Extract::Interval& interval) {
  switch (interval) {
  case common::Extract::Interval::Extract_Interval_YEAR:
    return "Interval::YEAR";
  case common::Extract::Interval::Extract_Interval_MONTH:
    return "Interval::MONTH";
  case common::Extract::Interval::Extract_Interval_DAY:
    return "Interval::DAY";
  case common::Extract::Interval::Extract_Interval_HOUR:
    return "Interval::HOUR";
  case common::Extract::Interval::Extract_Interval_MINUTE:
    return "Interval::MINUTE";
  case common::Extract::Interval::Extract_Interval_SECOND:
    return "Interval::SECOND";
  default:
    LOG(FATAL) << "Unexpected interval" << interval;
  }
}

}  // namespace gs

#endif  // CODEGEN_SRC_CODEGEN_UTILS_H_
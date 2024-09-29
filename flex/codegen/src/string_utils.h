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
#ifndef CODEGEN_SRC_STRING_UTILS_H_
#define CODEGEN_SRC_STRING_UTILS_H_

#include <sstream>
#include <string>

#include "flex/codegen/src/pb_parser/internal_struct.h"
#include "flex/proto_generated_gie/physical.pb.h"

#include "glog/logging.h"
namespace gs {

static constexpr const char* _4_SPACES = "    ";
static constexpr const char* _8_SPACES = "        ";
static constexpr const char* _ASSIGN_STR_ = " = ";

static constexpr const char* EDGE_EXPAND_V_METHOD_NAME = "EdgeExpandV";
static constexpr const char* EDGE_EXPAND_E_METHOD_NAME = "EdgeExpandE";
static constexpr const char* MAKE_GETV_OPT_NAME = "make_getv_opt";

static constexpr const char* NAMED_PROPERTY_CLASS_NAME = "gs::NamedProperty";
static constexpr const char* SORT_PROPER_PAIR_NAME = "gs::OrderingPropPair";
static constexpr const char* MAKE_PROJECT_OPT_NAME = "gs::make_project_opt";
static constexpr const char* PROJECT_SELF_STR = "gs::ProjectSelf";
static constexpr const char* PROJECT_PROPS_STR = "gs::AliasTagProp";
static constexpr const char* LABEL_ID_T = "label_id_t";
static constexpr const char* LABEL_ID_T_CASTER = "(label_id_t)";
static constexpr const char* EMPTY_TYPE = "grape::EmptyType";
static constexpr const char* INNER_ID_PROPERTY_NAME = "InnerIdProperty";
static constexpr const char* VERTEX_ID_T = "vertex_id_t";
static constexpr const char* GLOBAL_VERTEX_ID_T = "GlobalId";
static constexpr const char* EDGE_ID_T = "const DefaultEdge<vertex_id_t>&";
static constexpr const char* LENGTH_KEY_T = "LengthKey";
static constexpr const char* MAKE_PROJECT_EXPR = "make_project_expr";
static constexpr const char* APPEND_OPT_TEMP = "gs::AppendOpt::Temp";
static constexpr const char* APPEND_OPT_PERSIST = "gs::AppendOpt::Persist";
static constexpr const char* APPEND_OPT_REPLACE = "gs::AppendOpt::Replace";
static constexpr const char* GRAPE_EMPTY_TYPE = "grape::EmptyType";

static constexpr const char* NONE_LITERAL = "gs::NONE";
static constexpr const char* PROPERTY_SELECTOR =
    "gs::PropertySelector<%1%>(\"%2%\")";
static constexpr const char* PROP_NAME_ARRAY = "gs::PropNameArray<%1%>{%2%}";

std::string project_is_append_str(bool is_append, bool is_temp) {
  if (is_append) {
    if (is_temp) {
      return "PROJ_TO_APPEND_TEMP";
    } else {
      return "PROJ_TO_APPEND_PERSIST";
    }
  } else {
    return "PROJ_TO_NEW";
  }
}

std::string res_alias_to_append_opt(int res_alias) {
  return res_alias == -1 ? APPEND_OPT_TEMP : APPEND_OPT_PERSIST;
}

std::string res_alias_to_append_opt(int res_alias, int in_alias) {
  if (res_alias == -1) {
    return APPEND_OPT_TEMP;
  } else if (res_alias == in_alias) {
    return APPEND_OPT_REPLACE;
  } else {
    return APPEND_OPT_PERSIST;
  }
}

template <typename LabelIdT>
std::string ensure_label_id(LabelIdT label_id) {
  return std::string(LABEL_ID_T_CASTER) + std::string(" ") +
         std::to_string(label_id);
}

std::string make_move(int32_t i) {
  return "std::move(" + std::to_string(i) + ")";
}

std::string make_move(const std::string& param) {
  return "std::move(" + param + ")";
}

std::string format_input_col(const int32_t v_tag) {
  return "INPUT_COL_ID(" + std::to_string(v_tag) + ")";
}

std::string add_quote(const std::string& str) { return "\"" + str + "\""; }

std::string direction_pb_to_str(
    const physical::EdgeExpand::Direction& direction) {
  switch (direction) {
  case physical::EdgeExpand::Direction::EdgeExpand_Direction_IN:
    return "gs::Direction::In";
  case physical::EdgeExpand::Direction::EdgeExpand_Direction_OUT:
    return "gs::Direction::Out";
  case physical::EdgeExpand::Direction::EdgeExpand_Direction_BOTH:
    return "gs::Direction::Both";
  default:
    // LOG(FATAL) << "Unknown direction: " << direction;
    throw std::runtime_error("Unknown direction: ");
  }
}

std::string direction_pb_to_str(const gs::internal::Direction& direction) {
  switch (direction) {
  case gs::internal::Direction::kIn:
    return "gs::Direction::In";
  case gs::internal::Direction::kOut:
    return "gs::Direction::Out";
  case gs::internal::Direction::kBoth:
    return "gs::Direction::Both";
  default:
    throw std::runtime_error("Unknown direction: ");
  }
}

template <typename LabelT>
std::string label_ids_to_array_str(const std::vector<LabelT>& label_ids) {
  std::stringstream ss;
  ss << "std::array<label_id_t, " << label_ids.size() << ">{";
  for (size_t i = 0; i < label_ids.size(); ++i) {
    ss << ensure_label_id(label_ids[i]);
    if (i != label_ids.size() - 1) {
      ss << ", ";
    }
  }
  ss << "}";
  return ss.str();
}
}  // namespace gs

#endif  // CODEGEN_SRC_STRING_UTILS_H_
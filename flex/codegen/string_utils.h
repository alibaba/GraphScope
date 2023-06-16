#ifndef CODEGEN_SRC_STRING_UTILS_H
#define CODEGEN_SRC_STRING_UTILS_H

#include <string>

#include "flex/codegen/pb_parser/internal_struct.h"
#include "proto_generated_gie/physical.pb.h"
namespace gs {

static constexpr const char* _4_SPACES = "    ";
static constexpr const char* _8_SPACES = "        ";
static constexpr const char* _ASSIGN_STR_ = " = ";

static constexpr const char* EDGE_EXPAND_V_METHOD_NAME = "EdgeExpandV";
static constexpr const char* EDGE_EXPAND_V_MULTI_LABEL_METHOD_NAME =
    "EdgeExpandVMultiLabel";
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
static constexpr const char* group_key_class_name = "gs::AliasTagProp";
static constexpr const char* make_agg_prop_name = "gs::make_aggregate_prop";
static constexpr const char* INNER_ID_PROPERTY_NAME = "InnerIdProperty";
static constexpr const char* VERTEX_ID_T = "vertex_id_t";
static constexpr const char* MAKE_PROJECT_EXPR = "make_project_expr";

static constexpr const char* NONE_LITERAL = "gs::NONE";

template <typename LabelIdT>
static std::string ensure_label_id(LabelIdT label_id) {
  return std::string(LABEL_ID_T_CASTER) + std::string(" ") +
         std::to_string(label_id);
}

static std::string make_move(int32_t i) {
  return "std::move(" + std::to_string(i) + ")";
}

static std::string add_quote(const std::string& str) {
  return "\"" + str + "\"";
}

static std::string direction_pb_to_str(
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

static std::string direction_pb_to_str(
    const gs::internal::Direction& direction) {
  switch (direction) {
  case gs::internal::Direction::kIn:
    return "gs::Direction::In";
  case gs::internal::Direction::kOut:
    return "gs::Direction::Out";
  case gs::internal::Direction::kBoth:
    return "gs::Direction::Both";
  default:
    // LOG(FATAL) << "Unknown direction: ";
    throw std::runtime_error("Unknown direction: ");
  }
}
}  // namespace gs

#endif  // CODEGEN_SRC_STRING_UTILS_H
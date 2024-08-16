#include <sstream>

#include "flex/engines/graph_db/runtime/codegen/exprs/expr_utils.h"

namespace gs {
namespace runtime {
std::tuple<std::string, std::string, RTAnyType> value_pb_2_str(
    BuildingContext& context, const common::Value& value) {
  auto expr_name = context.GetNextExprName();
  std::stringstream ss;
  ss << "ConstAccessor<";
  switch (value.item_case()) {
  case common::Value::kI32: {
    ss << "int32_t> " << expr_name << "(" << value.i32() << ");\n";

    return std::make_tuple(ss.str(), expr_name, RTAnyType::kI32Value);
  }
  case common::Value::kI64: {
    ss << "int64_t> " << expr_name << "(" << value.i64() << ");\n";
    return std::make_tuple(ss.str(), expr_name, RTAnyType::kI64Value);
  }
  case common::Value::kF64: {
    ss << "double> " << expr_name << "(" << value.f64() << ");\n";
    return std::make_tuple(ss.str(), expr_name, RTAnyType::kF64Value);
  }
  case common::Value::kStr: {
    ss << "std::string> " << expr_name << "(\"" << value.str() << "\");\n";
    return std::make_tuple(ss.str(), expr_name, RTAnyType::kStringValue);
  }
  case common::Value::kBoolean: {
    ss << "bool> " << expr_name << "(" << (value.boolean() ? "true" : "false")
       << ");\n";
    return std::make_tuple(ss.str(), expr_name, RTAnyType::kBoolValue);
  }
  case common::Value::kDate: {
    ss << "Date> " << expr_name << "(" << value.date().DebugString() << ");\n";
    return std::make_tuple(ss.str(), expr_name, RTAnyType::kDate32);
  }
  case common::Value::kNone: {
    ss << "None> " << expr_name << ";\n";

    return std::make_tuple(ss.str(), expr_name, RTAnyType::kNull);
  }
  default: {
    LOG(FATAL) << "Unsupported value type: " << value.DebugString();
    break;
  }
  }
  return std::make_tuple("", expr_name, RTAnyType::kNull);
}

std::tuple<std::string, std::string, RTAnyType> param_pb_2_str(
    BuildingContext& context, const common::DynamicParam& param) {
  std::stringstream ss("ParamAccessor<");
  std::string expr_name = context.GetNextExprName();
  switch (param.data_type().data_type()) {
  case common::DataType::INT32: {
    ss << "int32_t> " << expr_name << "(params, \"" << param.name() << "\");\n";
    return std::make_tuple(ss.str(), expr_name, RTAnyType::kI32Value);
  }
  case common::DataType::INT64: {
    ss << "int64_t> " << expr_name << "(params, \"" << param.name() << "\");\n";
    return std::make_tuple(ss.str(), expr_name, RTAnyType::kI64Value);
  }
  case common::DataType::DOUBLE: {
    ss << "double> " << expr_name << "(params, \"" << param.name() << "\");\n";
    return std::make_tuple(ss.str(), expr_name, RTAnyType::kF64Value);
  }
  case common::DataType::STRING: {
    ss << "std::string> " << expr_name << "(params, \"" << param.name()
       << "\");\n";
    return std::make_tuple(ss.str(), expr_name, RTAnyType::kStringValue);
  }
  case common::DataType::BOOLEAN: {
    ss << "bool> " << expr_name << "(params, \"" << param.name() << "\");\n";
    return std::make_tuple(ss.str(), expr_name, RTAnyType::kBoolValue);
  }
  case common::DataType::DATE32: {
    ss << "Date> " << expr_name << "(params, \"" << param.name() << "\");\n";
    return std::make_tuple(ss.str(), expr_name, RTAnyType::kDate32);
  }
  default: {
    LOG(FATAL) << "Unsupported param type: " << param.DebugString();
    break;
  }
  }
  return std::make_tuple("", expr_name, RTAnyType::kNull);
}

std::string logical_2_str(const common::Logical& logi) {
  switch (logi) {
  case common::Logical::AND:
    return "AndOp";
  case common::Logical::OR:
    return "OROp";
  case common::Logical::NOT:
    return "NOT";
  case common::Logical::WITHIN:
    return "WITHIN";
  case common::Logical::WITHOUT:
    return "WITHOUT";
  case common::Logical::EQ:
    return "EQOp";
  case common::Logical::NE:
    return "NEOp";
  case common::Logical::GE:
    return "GEOp";
  case common::Logical::GT:
    return "GTOp";
  case common::Logical::LT:
    return "LTOp";
  case common::Logical::LE:
    return "LEOp";
  case common::Logical::REGEX:
    return "REGEX";
  default:
    LOG(FATAL) << "Unsupported logical operator: " << logi;
    break;
  }
  return "";
}

std::string arith_2_str(const common::Arithmetic& arith) {
  switch (arith) {
  case common::Arithmetic::ADD:
    return "AddOp";
  case common::Arithmetic::SUB:
    return "SubOp";
  case common::Arithmetic::MUL:
    return "MulOp";
  case common::Arithmetic::DIV:
    return "DivOp";
  case common::Arithmetic::MOD:
    return "ModOp";
  default:
    LOG(FATAL) << "Unsupported arithmetic operator: " << arith;
    break;
  }
  return "";
}

std::string type2str(const RTAnyType& type) {
  switch (type.type_enum_) {
  case RTAnyType::RTAnyTypeImpl::kI32Value:
    return "int32_t";
  case RTAnyType::RTAnyTypeImpl::kI64Value:
    return "int64_t";
  case RTAnyType::RTAnyTypeImpl::kF64Value:
    return "double";
  case RTAnyType::RTAnyTypeImpl::kStringValue:
    return "std::string";
  case RTAnyType::RTAnyTypeImpl::kBoolValue:
    return "bool";
  case RTAnyType::RTAnyTypeImpl::kDate32:
    return "Date";
  case RTAnyType::RTAnyTypeImpl::kNull:
    return "None";
  default:
    LOG(FATAL) << "Unsupported type: " << static_cast<int>(type.type_enum_);
    break;
  }
  return "";
}

std::tuple<std::string, std::string, RTAnyType> var_pb_2_str(
    BuildingContext& context, const common::Variable& var, VarType var_type) {
  int tag = -1;
  auto type = RTAnyType::kUnknown;
  if (var.has_node_type()) {
    type = parse_from_ir_data_type(var.node_type());
  }
  if (var.has_tag()) {
    tag = var.tag().id();
  }

  if (type == RTAnyType::kUnknown) {
    if (var.has_tag()) {
      type = context.get_elem_type(tag);
    } else if (var.has_property() && var.property().has_label()) {
      type = RTAnyType::kI64Value;
    } else {
      LOG(FATAL) << "not support";
    }
  }
  auto expr_name = context.GetNextExprName();
  auto ctx_name = context.GetCurCtxName();
  std::string ss;
  if (var.has_tag() || var_type == VarType::kPathVar) {
    if (context.get_column_type(tag) == ContextColumnType::kVertex) {
      if (var.has_property()) {
        auto pt = var.property();
        if (pt.has_id()) {
          ss += "VertexGIdPathAccessor ";
          ss +=
              expr_name + "(" + ctx_name + ", " + std::to_string(tag) + ");\n";
          return {ss, expr_name, RTAnyType::kI64Value};
        } else if (pt.has_key()) {
          if (pt.key().name() == "id") {
            ss += "VertexIdPathAccessor ";
            ss += expr_name + "(txn, " + ctx_name + ", " + std::to_string(tag) +
                  ");\n";
            return {ss, expr_name, RTAnyType::kI64Value};
          } else {
            ss += "VertexPropertyPathAccessor<";
            ss += type2str(type) + "> " + expr_name + "(txn, " + ctx_name +
                  ", " + std::to_string(tag) + ", \"" + pt.key().name() +
                  "\");\n";
            return {ss, expr_name, type};
          }
        } else if (pt.has_label()) {
          ss += "VertexLabelPathAccessor ";
          ss +=
              expr_name + "(" + ctx_name + ", " + std::to_string(tag) + ");\n";
          return {ss, expr_name, RTAnyType::kI32Value};
        } else {
          LOG(FATAL) << "not support" << var.DebugString();
        }
      } else {
        ss += "VertexPathAccessor ";
        ss += expr_name + "(" + ctx_name + ", " + std::to_string(tag) + ");\n";
        return {ss, expr_name, RTAnyType::kVertex};
      }
    } else if (context.get_column_type(tag) == ContextColumnType::kValue ||
               context.get_column_type(tag) ==
                   ContextColumnType::kOptionalValue) {
      auto elem_type = context.get_elem_type(tag);
      ss += "ContextValueAccessor<";
      ss += type2str(elem_type) + ">" + expr_name + "(" + ctx_name + ", " +
            std::to_string(tag) + ");\n";
    } else if (context.get_column_type(tag) == ContextColumnType::kEdge) {
      if (var.has_property()) {
        auto& pt = var.property();
        if (pt.has_key()) {
          auto name = pt.key().name();
          ss += "EdgePropertyPathAccessor<";
          ss += type2str(type) + "> " + expr_name + "(" + ctx_name + ", " +
                std::to_string(tag) + ", \"" + name + "\");\n";
          return {ss, expr_name, type};
        } else if (pt.has_label()) {
          ss += "EdgeLabelPathAccessor ";
          ss +=
              expr_name + "(" + ctx_name + ", " + std::to_string(tag) + ");\n";
          return {ss, expr_name, RTAnyType::kI32Value};
        } else {
          LOG(FATAL) << "not support" << var.DebugString();
        }
      } else {
        ss += "EdgeIdPathAccessor ";
        ss += expr_name + "(" + ctx_name + ", " + std::to_string(tag) + ");\n";
        return {ss, expr_name, RTAnyType::kEdge};
      }
    } else if (context.get_column_type(tag) == ContextColumnType::kPath) {
      if (var.has_property()) {
        auto& pt = var.property();
        if (pt.has_len()) {
          ss += "PathLenPathAccessor ";
          ss +=
              expr_name + "(" + ctx_name + ", " + std::to_string(tag) + ");\n";
          return {ss, expr_name, RTAnyType::kI32Value};
        } else {
          LOG(FATAL) << "not support" << var.DebugString();
        }
      } else {
        ss += "PathIdPathAccessor ";
        ss += expr_name + "(" + ctx_name + ", " + std::to_string(tag) + ");\n";
        return {ss, expr_name, RTAnyType::kPath};
      }
    } else {
      LOG(FATAL) << "not support" << var.DebugString();
    }
  } else {
    if (var_type == VarType::kVertexVar) {
      if (var.has_property()) {
        auto& pt = var.property();
        if (pt.has_id()) {
          ss += "VertexGIdVertexAccessor ";
          ss += expr_name + "();\n";
          return {ss, expr_name, RTAnyType::kI64Value};
        } else if (pt.has_key()) {
          if (pt.key().name() == "id") {
            ss += "VertexIdVertexAccessor ";
            ss += expr_name + "(txn);\n";
            return {ss, expr_name, RTAnyType::kI64Value};
          } else {
            ss += "VertexPropertyVertexAccessor<";
            ss += type2str(type) + "> " + expr_name + "(txn, \"" +
                  pt.key().name() + "\");\n";
            return {ss, expr_name, type};
          }
        } else if (pt.has_label()) {
          ss += "VertexLabelVertexAccessor ";
          ss += expr_name + "();\n";
          return {ss, expr_name, RTAnyType::kI32Value};
        } else {
          LOG(FATAL) << "not support" << var.DebugString();
        }
      }
    } else if (var_type == VarType::kEdgeVar) {
      if (var.has_property()) {
        auto& pt = var.property();
        if (pt.has_key()) {
          auto name = pt.key().name();
          ss += "EdgePropertyVertexAccessor<";

          ss +=
              type2str(type) + "> " + expr_name + "(txn, \"" + name + "\");\n";
          return {ss, expr_name, type};
        } else {
          LOG(FATAL) << "not support" << var.DebugString();
        }
      } else {
        LOG(FATAL) << "not support" << var.DebugString();
      }
    } else {
      LOG(FATAL) << "not support" << var.DebugString();
    }
  }
  return {ss, expr_name, RTAnyType::kNull};
}

}  // namespace runtime
}  // namespace gs
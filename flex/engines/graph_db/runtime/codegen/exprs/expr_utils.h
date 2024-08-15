#ifndef RUNTIME_CODEGEN_EXPRS_EXPR_UITLS_H
#define RUNTIME_CODEGEN_EXPRS_EXPR_UITLS_H
#include <string>
#include <tuple>

#include "flex/engines/graph_db/runtime/codegen/building_context.h"
#include "flex/engines/graph_db/runtime/common/utils.h"

namespace gs {
namespace runtime {
std::tuple<std::string, std::string, RTAnyType> value_pb_2_str(
    BuildingContext& context, const common::Value& value);

std::tuple<std::string, std::string, RTAnyType> param_pb_2_str(
    BuildingContext& context, const common::DynamicParam& param);

std::string logical_2_str(const common::Logical& logi);

std::string arith_2_str(const common::Arithmetic& arith);

std::string type2str(const RTAnyType& type);

std::tuple<std::string, std::string, RTAnyType> var_pb_2_str(
    BuildingContext& context, const common::Variable& var, VarType var_type);

}  // namespace runtime
}  // namespace gs
#endif
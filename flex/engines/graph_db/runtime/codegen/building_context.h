/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef RUNTIME_CODEGEN_BUILDING_CONTEXT_H_
#define RUNTIME_CODEGEN_BUILDING_CONTEXT_H_

#include "flex/engines/graph_db/runtime/common/types.h"

namespace gs {
namespace runtime {
class BuildingContext {
 public:
  BuildingContext() : ctx_id_(0), expr_id_(0) {}

  std::pair<std::string, std::string> GetCurAndNextCtxName() {
    size_t cur_ctx_id = ctx_id_++;
    return std::make_pair("ctx_" + std::to_string(cur_ctx_id),
                          "ctx_" + std::to_string(ctx_id_));
  }

  ContextColumnType get_column_type(int32_t idx) {
    return column_types_[idx + 1];
  }

  std::string GetCurCtxName() { return "ctx_" + std::to_string(ctx_id_); }

  std::string GetNextCtxName() { return "ctx_" + std::to_string(ctx_id_ + 1); }

  std::string GetNextExprName() { return "expr_" + std::to_string(expr_id_++); }

  void set_alias(int32_t alias, ContextColumnType type, RTAnyType elem_type) {
    if (column_types_.size() <= static_cast<size_t>(alias + 1)) {
      column_types_.resize(alias + 2);
      elem_types_.resize(alias + 2);
    }
    column_types_[alias + 1] = type;
    elem_types_[alias + 1] = elem_type;
  }

  RTAnyType get_elem_type(int32_t idx) { return elem_types_[idx + 1]; }

  void reset_columns() {
    column_types_.clear();
    elem_types_.clear();
  }

 private:
  int32_t ctx_id_;
  int32_t expr_id_;
  std::vector<ContextColumnType> column_types_;
  std::vector<RTAnyType> elem_types_;
};
}  // namespace runtime
}  // namespace gs
#endif  // RUNTIME_CODEGEN_BUILDING_CONTEXT_H_
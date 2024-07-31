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

#include "flex/engines/graph_db/runtime/adhoc/expr.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/utils.h"

namespace gs {

namespace runtime {

bool exchange_tag_alias(const physical::Project_ExprAlias& m, int& tag,
                        int& alias) {
  auto expr = m.expr();
  if (expr.operators().size() == 1 &&
      expr.operators(0).item_case() == common::ExprOpr::kVar) {
    auto var = expr.operators(0).var();
    tag = -1;
    if (var.has_tag()) {
      tag = var.tag().id();
    }
    alias = -1;
    if (m.has_alias()) {
      alias = m.alias().value();
    }
    if (tag == alias) {
      return true;
    }
  }
  return false;
}

Context eval_project(const physical::Project& opr, const ReadTransaction& txn,
                     Context&& ctx,
                     const std::map<std::string, std::string>& params,
                     const std::vector<common::IrDataType>& data_types) {
  // bool is_append = opr.is_append();
  int mappings_size = opr.mappings_size();
  Context ret;
  size_t row_num = ctx.row_num();
  LOG(INFO) << "row num: " << row_num << "\n";
  if (static_cast<size_t>(mappings_size) == data_types.size()) {
    for (int i = 0; i < mappings_size; ++i) {
      const physical::Project_ExprAlias& m = opr.mappings(i);
      {
        int tag, alias;
        if (exchange_tag_alias(m, tag, alias)) {
          ret.set(alias, ctx.get(tag));
          continue;
        }
      }
      LOG(INFO) << m.DebugString() << "\n";
      Expr expr(txn, ctx, params, m.expr(), VarType::kPathVar);
      int alias = -1;
      if (m.has_alias()) {
        alias = m.alias().value();
      }
      LOG(INFO) << m.DebugString() << "\n";
      auto col = build_column(data_types[i], expr, row_num);
      ret.set(alias, col);
    }
  } else {
    for (int i = 0; i < mappings_size; ++i) {
      const physical::Project_ExprAlias& m = opr.mappings(i);
      {
        int tag, alias;
        if (exchange_tag_alias(m, tag, alias)) {
          ret.set(alias, ctx.get(tag));
          continue;
        }
      }

      Expr expr(txn, ctx, params, m.expr(), VarType::kPathVar);
      int alias = -1;
      if (m.has_alias()) {
        alias = m.alias().value();
      }

      auto col = build_column_beta(expr, row_num);
      ret.set(alias, col);
    }
  }
  LOG(INFO) << ctx.row_num() << "row num\n";

  return ret;
}

}  // namespace runtime

}  // namespace gs
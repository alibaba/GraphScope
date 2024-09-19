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

namespace gs {

namespace runtime {

bl::result<Context> eval_select(
    const algebra::Select& opr, const ReadTransaction& txn, Context&& ctx,
    const std::map<std::string, std::string>& params) {
  Expr expr(txn, ctx, params, opr.predicate(), VarType::kPathVar);
  std::vector<size_t> offsets;
  size_t row_num = ctx.row_num();
  if (expr.is_optional()) {
    for (size_t i = 0; i < row_num; ++i) {
      if (expr.eval_path(i, 0).is_null()) {
        continue;
      } else if (expr.eval_path(i, 0).as_bool()) {
        offsets.push_back(i);
      }
    }
  } else {
    for (size_t i = 0; i < row_num; ++i) {
      if (expr.eval_path(i).as_bool()) {
        offsets.push_back(i);
      }
    }
  }

  ctx.reshuffle(offsets);
  return ctx;
}

}  // namespace runtime

}  // namespace gs
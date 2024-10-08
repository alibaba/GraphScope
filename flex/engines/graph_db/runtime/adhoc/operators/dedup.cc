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

#include "flex/engines/graph_db/runtime/common/operators/dedup.h"
#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/engines/graph_db/runtime/adhoc/var.h"

namespace gs {

namespace runtime {

bl::result<Context> eval_dedup(const algebra::Dedup& opr,
                               const ReadTransaction& txn, Context&& ctx) {
  std::vector<size_t> keys;
  std::vector<std::function<RTAny(size_t)>> vars;
  int keys_num = opr.keys_size();
  bool flag = false;
  for (int k_i = 0; k_i < keys_num; ++k_i) {
    const common::Variable& key = opr.keys(k_i);

    int tag = -1;
    if (key.has_tag()) {
      tag = key.tag().id();
    }
    if (key.has_property()) {
      Var var(txn, ctx, key, VarType::kPathVar);
      vars.emplace_back([var](size_t i) { return var.get(i); });
      flag = true;
    } else {
      keys.push_back(tag);
    }
  }
  if (!flag) {
    Dedup::dedup(txn, ctx, keys);
  } else {
    Dedup::dedup(txn, ctx, keys, vars);
  }
  return ctx;
}

}  // namespace runtime

}  // namespace gs

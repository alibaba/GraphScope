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

namespace gs {

namespace runtime {

Context eval_dedup(const algebra::Dedup& opr, const ReadTransaction& txn,
                   Context&& ctx) {
  std::vector<size_t> keys;
  int keys_num = opr.keys_size();
  for (int k_i = 0; k_i < keys_num; ++k_i) {
    const common::Variable& key = opr.keys(k_i);
    CHECK(!key.has_property());
    CHECK(key.has_tag());
    keys.push_back(key.tag().id());
  }

  Dedup::dedup(txn, ctx, keys);
  return ctx;
}

}  // namespace runtime

}  // namespace gs

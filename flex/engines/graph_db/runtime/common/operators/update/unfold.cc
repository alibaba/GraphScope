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

#include "flex/engines/graph_db/runtime/common/operators/update/unfold.h"

namespace gs {

namespace runtime {

WriteContext Unfold::unfold(WriteContext&& ctx, int key, int alias) {
  auto col = ctx.get(key);
  auto [new_col, offset] = col.unfold();
  ctx.set_with_reshuffle(alias, std::move(new_col), offset);
  return ctx;
}

}  // namespace runtime

}  // namespace gs
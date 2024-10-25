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

#ifndef RUNTIME_COMMON_OPERATORS_INTERSECT_H_
#define RUNTIME_COMMON_OPERATORS_INTERSECT_H_

#include <tuple>
#include <vector>

#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/leaf_utils.h"

namespace gs {

namespace runtime {

class Intersect {
 public:
  static bl::result<Context> intersect(
      Context&& ctx, std::vector<std::tuple<Context, int, int>>&& ctxs,
      int alias);

  static bl::result<Context> intersect(std::vector<Context>&& ctxs, int key);
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_INTERSECT_H_
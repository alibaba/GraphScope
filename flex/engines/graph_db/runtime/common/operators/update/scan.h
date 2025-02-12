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

#ifndef RUNTIME_COMMON_OPERATORS_UPDATE_SCAN_H_
#define RUNTIME_COMMON_OPERATORS_UPDATE_SCAN_H_
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/leaf_utils.h"

namespace gs {
namespace runtime {
namespace ops {

struct ScanParams {
  int alias;
  std::vector<label_t> tables;

  ScanParams() : alias(-1) {}
};
class UScan {
 public:
  template <typename PRED_T>
  static bl::result<Context> scan(const GraphUpdateInterface& graph,
                                  Context&& ctx, const ScanParams& params,
                                  const PRED_T& pred) {
    MLVertexColumnBuilder builder;
    for (auto& label : params.tables) {
      auto vit = graph.GetVertexIterator(label);
      for (; vit.IsValid(); vit.Next()) {
        if (pred(label, vit.GetIndex())) {
          builder.push_back_vertex(VertexRecord{label, vit.GetIndex()});
        }
      }
    }
    ctx.set(params.alias, builder.finish());
    return ctx;
  }
};

}  // namespace ops
}  // namespace runtime
}  // namespace gs
#endif
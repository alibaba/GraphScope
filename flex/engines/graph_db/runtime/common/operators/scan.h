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

#ifndef RUNTIME_COMMON_OPERATORS_SCAN_H_
#define RUNTIME_COMMON_OPERATORS_SCAN_H_

#include <vector>

#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace runtime {

struct ScanParams {
  int alias;
  std::vector<label_t> tables;
};

class Scan {
 public:
  template <typename PRED_T>
  static Context scan_vertex(const ReadTransaction& txn,
                             const ScanParams& params,
                             const PRED_T& predicate) {
    Context ctx;
    LOG(INFO) << params.tables.size() << " size\n";
    if (params.tables.size() == 1) {
      label_t label = params.tables[0];
      SLVertexColumnBuilder builder(label);
      vid_t vnum = txn.GetVertexNum(label);
      for (vid_t vid = 0; vid != vnum; ++vid) {
        if (predicate(label, vid)) {
          builder.push_back_opt(vid);
        }
      }
      ctx.set(params.alias, builder.finish());
    } else if (params.tables.size() > 1) {
      MLVertexColumnBuilder builder;

      for (auto label : params.tables) {
        vid_t vnum = txn.GetVertexNum(label);
        for (vid_t vid = 0; vid != vnum; ++vid) {
          if (predicate(label, vid)) {
            builder.push_back_vertex(std::make_pair(label, vid));
          }
        }
      }
      ctx.set(params.alias, builder.finish());
    }
    LOG(INFO) << ctx.row_num() << "row num\n";
    return ctx;
  }

  static Context find_vertex(const ReadTransaction& txn, label_t label,
                             const Any& pk, int alias);
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_SCAN_H_
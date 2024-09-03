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

#include "flex/engines/graph_db/runtime/adhoc/operators/operators.h"
#include "flex/proto_generated_gie/results.pb.h"

namespace gs {

namespace runtime {

void eval_sink(const Context& ctx, const ReadTransaction& txn,
               Encoder& output) {
  size_t row_num = ctx.row_num();
  results::CollectiveResults results;
  for (size_t i = 0; i < row_num; ++i) {
    auto result = results.add_results();
    for (size_t j : ctx.tag_ids) {
      auto col = ctx.get(j);
      if (col == nullptr) {
        continue;
      }
      auto column = result->mutable_record()->add_columns();
      auto val = col->get_elem(i);
      val.sink(txn, j, column);
    }
  }
  // LOG(INFO) << "sink: " << results.DebugString();
  auto res = results.SerializeAsString();
  output.put_bytes(res.data(), res.size());
}

}  // namespace runtime

}  // namespace gs
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

#include "flex/engines/graph_db/runtime/execute/ops/update/dedup.h"
#include "flex/engines/graph_db/runtime/common/operators/update/dedup.h"

namespace gs {
namespace runtime {
namespace ops {

class DedupInsertOpr : public IInsertOperator {
 public:
  DedupInsertOpr(const std::vector<size_t>& keys) : keys(keys) {}

  bl::result<gs::runtime::WriteContext> Eval(
      gs::runtime::GraphInsertInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::WriteContext&& ctx, gs::runtime::OprTimer& timer) override {
    return Dedup::dedup(graph, std::move(ctx), keys);
  }

 private:
  std::vector<size_t> keys;
};

std::unique_ptr<IInsertOperator> DedupInsertOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_id) {
  const auto& opr = plan.plan(op_id).opr().dedup();
  std::vector<size_t> keys;
  int keys_num = opr.keys_size();
  for (int k_i = 0; k_i < keys_num; ++k_i) {
    const common::Variable& key = opr.keys(k_i);
    int tag = -1;
    CHECK(key.has_tag());
    tag = key.tag().id();
    keys.emplace_back(tag);
    if (key.has_property()) {
      LOG(ERROR) << "dedup not support property";
      return nullptr;
    }
  }
  return std::make_unique<DedupInsertOpr>(keys);
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs
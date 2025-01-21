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

#include "flex/engines/graph_db/runtime/execute/ops/retrieve/dedup.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/dedup.h"
#include "flex/engines/graph_db/runtime/utils/var.h"

namespace gs {
namespace runtime {
namespace ops {
class DedupOpr : public IReadOperator {
 public:
  DedupOpr(const std::vector<size_t>& tag_ids) : tag_ids_(tag_ids) {}

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    return Dedup::dedup(graph, std::move(ctx), tag_ids_);
  }

  std::vector<size_t> tag_ids_;
};

class DedupWithPropertyOpr : public IReadOperator {
 public:
  DedupWithPropertyOpr(const algebra::Dedup& dedup_opr) : opr_(dedup_opr) {}

  bl::result<gs::runtime::Context> Eval(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    int keys_num = opr_.keys_size();
    std::vector<std::function<RTAny(size_t)>> keys;
    for (int k_i = 0; k_i < keys_num; ++k_i) {
      const auto& key = opr_.keys(k_i);
      int tag = key.has_tag() ? key.tag().id() : -1;
      if (key.has_property()) {
        Var var(graph, ctx, key, VarType::kPathVar);
        keys.emplace_back([var](size_t i) { return var.get(i); });
      } else {
        keys.emplace_back(
            [&ctx, tag](size_t i) { return ctx.get(tag)->get_elem(i); });
      }
    }
    return Dedup::dedup(graph, std::move(ctx), keys);
  }

  algebra::Dedup opr_;
};
std::pair<std::unique_ptr<IReadOperator>, ContextMeta> DedupOprBuilder::Build(
    const gs::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  const auto& dedup_opr = plan.plan(op_idx).opr().dedup();
  int keys_num = dedup_opr.keys_size();
  std::vector<size_t> keys;
  bool flag = true;
  for (int k_i = 0; k_i < keys_num; ++k_i) {
    const auto& key = dedup_opr.keys(k_i);
    int tag = key.has_tag() ? key.tag().id() : -1;
    keys.emplace_back(tag);
    if (key.has_property()) {
      flag = false;
      break;
    }
  }
  if (flag) {
    return std::make_pair(std::make_unique<DedupOpr>(keys), ctx_meta);
  } else {
    return std::make_pair(
        std::make_unique<DedupWithPropertyOpr>(plan.plan(op_idx).opr().dedup()),
        ctx_meta);
  }
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs
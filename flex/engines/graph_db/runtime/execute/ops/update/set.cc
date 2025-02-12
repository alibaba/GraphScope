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
#include "flex/engines/graph_db/runtime/execute/ops/update/set.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/utils/expr.h"

namespace gs {
namespace runtime {
namespace ops {

class SetOpr : public IUpdateOperator {
 public:
  SetOpr(std::vector<std::pair<int, std::string>> keys,
         std::vector<common::Expression> values)
      : keys_(std::move(keys)), values_(std::move(values)) {}

  std::string get_operator_name() const override { return "SetOpr"; }
  void set_vertex_property(GraphUpdateInterface& graph, label_t label,
                           vid_t vid, const std::string& key,
                           const RTAny& value) {}

  void set_edge_property(GraphUpdateInterface& graph, const LabelTriplet& label,
                         Direction dir, vid_t src, vid_t dst,
                         const std::string& key, const RTAny& value) {
    auto edge = graph.GetOutEdgeIterator(label.src_label, src, label.dst_label,
                                         label.edge_label);
  }

  bl::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer& timer) override {
    for (size_t i = 0; i < keys_.size(); ++i) {
      auto& key = keys_[i];
      auto& value = values_[i];
      const auto& prop = ctx.get(key.first);
      CHECK(prop->column_type() == ContextColumnType::kVertex ||
            prop->column_type() == ContextColumnType::kEdge);
      if (prop->column_type() == ContextColumnType::kVertex) {
        auto vertex_col = dynamic_cast<const IVertexColumn*>(prop.get());
        Expr expr(graph, ctx, params, value, VarType::kPathVar);
        for (size_t j = 0; j < ctx.row_num(); j++) {
          auto val = expr.eval_path(j);
          auto vertex = vertex_col->get_vertex(j);
          set_vertex_property(graph, vertex.label_, vertex.vid_, key.second,
                              val);
        }
      } else {
        auto edge_col = dynamic_cast<const IEdgeColumn*>(prop.get());
        Expr expr(graph, ctx, params, value, VarType::kPathVar);
        for (size_t j = 0; j < ctx.row_num(); j++) {
          auto val = expr.eval_path(j);
          auto edge = edge_col->get_edge(j);
          set_edge_property(graph, edge.label_triplet(), edge.dir_, edge.src_,
                            edge.dst_, key.second, val);
        }
      }
    }

    return ctx;
  }

 private:
  std::vector<std::pair<int, std::string>> keys_;
  std::vector<common::Expression> values_;
};
std::unique_ptr<IUpdateOperator> SetOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr().set();
  std::vector<std::pair<int, std::string>> keys;
  std::vector<common::Expression> values;
  for (int i = 0; i < opr.items_size(); ++i) {
    const auto& item = opr.items(i);
    // only support mutate property now
    CHECK(item.kind() == cypher::Set_Item_Kind::Set_Item_Kind_MUTATE_PROPERTY);
    CHECK(item.has_key() && item.has_value());
    int tag = item.key().tag().id();
    const std::string& property_name = item.key().property().key().name();
    keys.emplace_back(tag, property_name);
    values.emplace_back(item.value());
  }
  return std::make_unique<SetOpr>(std::move(keys), std::move(values));
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs
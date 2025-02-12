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

#include "flex/engines/graph_db/runtime/execute/ops/update/scan.h"
#include "flex/engines/graph_db/runtime/common/operators/update/scan.h"
#include "flex/engines/graph_db/runtime/execute/ops/retrieve/scan_utils.h"
#include "flex/engines/graph_db/runtime/utils/expr_impl.h"

namespace gs {
namespace runtime {
namespace ops {
class UScanOpr : public IUpdateOperator {
 public:
  UScanOpr(const ScanParams& params,
           const std::vector<std::function<std::vector<Any>(
               const std::map<std::string, std::string>&)>>& oids,
           const std::optional<common::Expression>& pred)
      : scan_params(params), oids(oids), pred(pred) {}

  bl::result<gs::runtime::Context> Eval(
      gs::runtime::GraphUpdateInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, gs::runtime::OprTimer& timer) override {
    std::vector<Any> oids_vec;
    for (auto& oid : oids) {
      auto oids = oid(params);
      oids_vec.insert(oids_vec.end(), oids.begin(), oids.end());
    }
    if (pred.has_value()) {
      auto expr = parse_expression<GraphUpdateInterface>(
          graph, ctx, params, pred.value(), VarType::kVertexVar);
      if (expr->is_optional()) {
        if (oids.empty()) {
          return UScan::scan(
              graph, std::move(ctx), scan_params,
              [&](label_t label, vid_t vid) {
                return expr->eval_vertex(label, vid, 0, 0).as_bool();
              });
        } else {
          return UScan::scan(
              graph, std::move(ctx), scan_params,
              [&](label_t label, vid_t vid) {
                for (auto& oid : oids_vec) {
                  if (graph.GetVertexId(label, vid) == oid) {
                    return expr->eval_vertex(label, vid, 0, 0).as_bool();
                  }
                }
                return false;
              });
        }
      } else {
        if (oids.empty()) {
          return UScan::scan(
              graph, std::move(ctx), scan_params,
              [&](label_t label, vid_t vid) {
                return expr->eval_vertex(label, vid, 0).as_bool();
              });
        } else {
          return UScan::scan(
              graph, std::move(ctx), scan_params,
              [&](label_t label, vid_t vid) {
                for (auto& oid : oids_vec) {
                  if (graph.GetVertexId(label, vid) == oid) {
                    return expr->eval_vertex(label, vid, 0).as_bool();
                  }
                }
                return false;
              });
        }
      }
    } else {
      if (oids.empty()) {
        return UScan::scan(graph, std::move(ctx), scan_params,
                           [](label_t label, vid_t vid) { return true; });
      } else {
        return UScan::scan(graph, std::move(ctx), scan_params,
                           [&](label_t label, vid_t vid) {
                             for (auto& oid : oids_vec) {
                               if (graph.GetVertexId(label, vid) == oid) {
                                 return true;
                               }
                             }
                             return false;
                           });
      }
    }
  }

  std::string get_operator_name() const override { return "ScanOpr"; }

 private:
  ScanParams scan_params;
  std::vector<std::function<std::vector<Any>(
      const std::map<std::string, std::string>&)>>
      oids;
  std::optional<common::Expression> pred;
};

std::unique_ptr<IUpdateOperator> UScanOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_id) {
  const auto& scan = plan.plan(op_id).opr().scan();
  int alias = scan.has_alias() ? scan.alias().value() : -1;
  if (!scan.has_params()) {
    LOG(ERROR) << "Scan operator should have params";
    return nullptr;
  }
  std::vector<label_t> tables;
  for (auto& label : scan.params().tables()) {
    if (label.id() >= schema.vertex_label_num()) {
      continue;
    }
    tables.emplace_back(label.id());
  }

  ScanParams params;
  params.alias = alias;
  params.tables = std::move(tables);
  std::vector<std::function<std::vector<Any>(
      const std::map<std::string, std::string>&)>>
      oids;

  if (scan.has_idx_predicate()) {
    bool scan_oid = false;
    if (!ScanUtils::check_idx_predicate(scan, scan_oid)) {
      LOG(ERROR) << "Index predicate is not supported" << scan.DebugString();
      return nullptr;
    }
    if (!scan_oid) {
      LOG(ERROR) << "Scan gid is not supported" << scan.DebugString();
    }

    std::set<int> types;
    for (auto& table : params.tables) {
      const auto& pks = schema.get_vertex_primary_key(table);
      const auto& [type, _, __] = pks[0];
      int type_impl = static_cast<int>(type.type_enum);
      if (types.find(type_impl) == types.end()) {
        auto oid = ScanUtils::parse_ids_with_type(type, scan.idx_predicate());
        types.insert(type_impl);
        oids.emplace_back(oid);
      }
    }
  }
  std::optional<common::Expression> pred(std::nullopt);
  if (scan.params().has_predicate()) {
    pred = scan.params().predicate();
  }

  return std::make_unique<UScanOpr>(params, oids, pred);
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs
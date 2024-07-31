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
#ifndef RUNTIME_ADHOC_PREDICATES_H_
#define RUNTIME_ADHOC_PREDICATES_H_

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/adhoc/expr.h"
#include "flex/engines/graph_db/runtime/adhoc/var.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/proto_generated_gie/expr.pb.h"

namespace gs {

namespace runtime {

struct GeneralPathPredicate {
  GeneralPathPredicate(const ReadTransaction& txn, const Context& ctx,
                       const std::map<std::string, std::string>& params,
                       const common::Expression& expr)
      : expr_(txn, ctx, params, expr, VarType::kPathVar) {}

  bool operator()(size_t idx) const {
    auto val = expr_.eval_path(idx);
    return val.as_bool();
  }

  Expr expr_;
};

struct GeneralVertexPredicate {
  GeneralVertexPredicate(const ReadTransaction& txn, const Context& ctx,
                         const std::map<std::string, std::string>& params,
                         const common::Expression& expr)
      : expr_(txn, ctx, params, expr, VarType::kVertexVar) {}

  bool operator()(label_t label, vid_t v, size_t path_idx) const {
    auto val = expr_.eval_vertex(label, v, path_idx);
    return val.as_bool();
  }

  Expr expr_;
};

struct GeneralEdgePredicate {
  GeneralEdgePredicate(const ReadTransaction& txn, const Context& ctx,
                       const std::map<std::string, std::string>& params,
                       const common::Expression& expr)
      : expr_(txn, ctx, params, expr, VarType::kEdgeVar) {}

  bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, Direction dir, size_t path_idx) const {
    auto val = expr_.eval_edge(label, src, dst, edata, path_idx);
    return val.as_bool();
  }

  Expr expr_;
};

struct DummyVertexPredicate {
  bool operator()(label_t label, vid_t v, size_t path_idx) const {
    return true;
  }
};

struct DummyEdgePredicate {
  bool operator()(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& edata, Direction dir, size_t path_idx) const {
    return true;
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_PREDICATES_H_
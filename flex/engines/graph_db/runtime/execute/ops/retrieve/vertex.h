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

#ifndef RUNTIME_EXECUTE_RETRIEVE_OPS_VERTEX_H_
#define RUNTIME_EXECUTE_RETRIEVE_OPS_VERTEX_H_

#include "flex/engines/graph_db/runtime/execute/operator.h"

#include "flex/engines/graph_db/runtime/adhoc/operators/special_predicates.h"
#include "flex/engines/graph_db/runtime/adhoc/predicates.h"
#include "flex/engines/graph_db/runtime/adhoc/utils.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/get_v.h"

namespace gs {
namespace runtime {
namespace ops {

class VertexOprBuilder : public IReadOperatorBuilder {
 public:
  VertexOprBuilder() = default;
  ~VertexOprBuilder() = default;

  std::pair<std::unique_ptr<IReadOperator>, ContextMeta> Build(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan, int op_idx) override;
  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kVertex};
  }
};

}  // namespace ops

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_RETRIEVE_OPS_VERTEX_H_
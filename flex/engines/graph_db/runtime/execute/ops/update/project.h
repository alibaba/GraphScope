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

#ifndef RUNTIME_EXECUTE_UPDATE_OPS_PROJECT_H_
#define RUNTIME_EXECUTE_UPDATE_OPS_PROJECT_H_

#include "flex/engines/graph_db/runtime/execute/operator.h"

namespace gs {
namespace runtime {
namespace ops {

class ProjectInsertOprBuilder : public IInsertOperatorBuilder {
 public:
  ProjectInsertOprBuilder() = default;
  ~ProjectInsertOprBuilder() = default;

  std::unique_ptr<IInsertOperator> Build(const Schema& schema,
                                         const physical::PhysicalPlan& plan,
                                         int op_id) override;

  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kProject;
  }
};

}  // namespace ops

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_UPDATE_OPS_VERTEX_H_
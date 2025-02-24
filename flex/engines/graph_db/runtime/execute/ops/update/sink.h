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

#ifndef RUNTIME_EXECUTE_UPDATE_OPS_SINK_H_
#define RUNTIME_EXECUTE_UPDATE_OPS_SINK_H_

#include "flex/engines/graph_db/runtime/execute/operator.h"

namespace gs {

namespace runtime {

namespace ops {

class SinkInsertOprBuilder : public IInsertOperatorBuilder {
 public:
  SinkInsertOprBuilder() = default;
  ~SinkInsertOprBuilder() = default;

  std::unique_ptr<IInsertOperator> Build(const Schema& schema,
                                         const physical::PhysicalPlan& plan,
                                         int op_idx) override;
  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kSink;
  }
};

class USinkOprBuilder : public IUpdateOperatorBuilder {
 public:
  USinkOprBuilder() = default;
  ~USinkOprBuilder() = default;

  std::unique_ptr<IUpdateOperator> Build(const Schema& schema,
                                         const physical::PhysicalPlan& plan,
                                         int op_idx) override;
  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kSink;
  }
};
}  // namespace ops

std::pair<std::unique_ptr<IInsertOperator>, int> create_sink_insert_operator(
    const physical::PhysicalPlan& plan, int op_id);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_UPDATE_OPS_SINK_H_
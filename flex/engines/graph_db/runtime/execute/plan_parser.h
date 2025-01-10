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

#ifndef RUNTIME_EXECUTE_PLAN_PARSER_H_
#define RUNTIME_EXECUTE_PLAN_PARSER_H_

#include "flex/engines/graph_db/runtime/execute/pipeline.h"

namespace gs {

namespace runtime {

class PlanParser {
 public:
  PlanParser() { read_op_builders_.resize(64); }
  ~PlanParser() = default;

  void init();

  static PlanParser& get();

  void register_read_operator_builder(
      std::unique_ptr<IReadOperatorBuilder>&& builder);

  void register_write_operator_builder(
      std::unique_ptr<IInsertOperatorBuilder>&& builder);

  std::pair<ReadPipeline, ContextMeta> parse_read_pipeline_with_meta(
      const gs::Schema& schema, const ContextMeta& ctx_meta,
      const physical::PhysicalPlan& plan);

  ReadPipeline parse_read_pipeline(const gs::Schema& schema,
                                   const ContextMeta& ctx_meta,
                                   const physical::PhysicalPlan& plan);

  InsertPipeline parse_write_pipeline(const gs::Schema& schema,
                                      const physical::PhysicalPlan& plan);

 private:
  std::vector<std::vector<
      std::pair<std::vector<physical::PhysicalOpr_Operator::OpKindCase>,
                std::unique_ptr<IReadOperatorBuilder>>>>
      read_op_builders_;

  std::map<physical::PhysicalOpr_Operator::OpKindCase,
           std::unique_ptr<IInsertOperatorBuilder>>
      write_op_builders_;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_PLAN_PARSER_H_
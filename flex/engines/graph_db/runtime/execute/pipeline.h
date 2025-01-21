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

#ifndef RUNTIME_EXECUTE_PIPELINE_H_
#define RUNTIME_EXECUTE_PIPELINE_H_

#include "flex/engines/graph_db/runtime/execute/operator.h"

namespace gs {

namespace runtime {

class ReadPipeline {
 public:
  ReadPipeline() {}
  ReadPipeline(ReadPipeline&& rhs) : operators_(std::move(rhs.operators_)) {}
  ReadPipeline(std::vector<std::unique_ptr<IReadOperator>>&& operators)
      : operators_(std::move(operators)) {}
  ~ReadPipeline() = default;

  Context Execute(const GraphReadInterface& graph, Context&& ctx,
                  const std::map<std::string, std::string>& params,
                  OprTimer& timer);

 private:
  std::vector<std::unique_ptr<IReadOperator>> operators_;
};

class InsertPipeline {
 public:
  InsertPipeline(InsertPipeline&& rhs)
      : operators_(std::move(rhs.operators_)) {}
  InsertPipeline(std::vector<std::unique_ptr<IInsertOperator>>&& operators)
      : operators_(std::move(operators)) {}
  ~InsertPipeline() = default;

  WriteContext Execute(GraphInsertInterface& graph, WriteContext&& ctx,
                       const std::map<std::string, std::string>& params,
                       OprTimer& timer);

 private:
  std::vector<std::unique_ptr<IInsertOperator>> operators_;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_PIPELINE_H_

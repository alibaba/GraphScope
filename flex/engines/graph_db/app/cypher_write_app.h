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

#ifndef ENGINES_GRAPH_DB_CYPHER_WRITE_APP_H_
#define ENGINES_GRAPH_DB_CYPHER_WRITE_APP_H_
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/runtime/execute/pipeline.h"

#include "flex/proto_generated_gie/physical.pb.h"

namespace gs {
class CypherWriteApp : public WriteAppBase {
 public:
  CypherWriteApp(const GraphDB& db) : db_(db) {}

  AppType type() const override { return AppType::kCypherAdhoc; }

  bool Query(GraphDBSession& graph, Decoder& input, Encoder& output) override;

  const runtime::OprTimer& timer() const { return timer_; }
  runtime::OprTimer& timer() { return timer_; }

 private:
  const GraphDB& db_;
  std::unordered_map<std::string, physical::PhysicalPlan> plan_cache_;
  std::unordered_map<std::string, runtime::InsertPipeline> pipeline_cache_;
  runtime::OprTimer timer_;
};

class CypherWriteAppFactory : public AppFactoryBase {
 public:
  CypherWriteAppFactory() = default;
  ~CypherWriteAppFactory() = default;

  AppWrapper CreateApp(const GraphDB& db) override;
};

}  // namespace gs
#endif  // ENGINES_GRAPH_DB_CYPHER_WRITE_APP_H_
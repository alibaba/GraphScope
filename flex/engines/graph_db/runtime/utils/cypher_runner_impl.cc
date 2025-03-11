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

#include "flex/engines/graph_db/runtime/utils/cypher_runner_impl.h"
#include "flex/engines/graph_db/app/cypher_app_utils.h"
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/database/insert_transaction.h"
#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/database/update_transaction.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/sink.h"
#include "flex/engines/graph_db/runtime/execute/plan_parser.h"

namespace gs {
namespace runtime {

bool CypherRunnerImpl::gen_plan(const GraphDB& db, const std::string& query,
                                std::string& plan_str) {
  auto& plan_cache = plan_cache_;
  const std::string statistics = db.work_dir() + "/statistics.json";
  const std::string& compiler_yaml = db.work_dir() + "/graph.yaml";
  const std::string& tmp_dir = db.work_dir() + "/runtime/tmp/";
  const auto& compiler_path = db.schema().get_compiler_path();

  if (plan_cache.get(query, plan_str)) {
    return true;
  }

  physical::PhysicalPlan plan;
  {
    // avoid multiple threads to generate plan for the same query
    std::unique_lock<std::mutex> lock(mutex_);
    if (plan_cache.get(query, plan_str)) {
      return true;
    }
    if (!generate_plan(query, statistics, compiler_path, compiler_yaml, tmp_dir,
                       plan)) {
      LOG(ERROR) << "Generate plan failed for query: " << query;
      return false;
    }
    plan_str = plan.SerializeAsString();
    plan_cache.put(query, plan_str);
  }

  return true;
}

std::string CypherRunnerImpl::run(
    gs::UpdateTransaction& tx, const std::string& cypher,
    const std::map<std::string, std::string>& params) {
  std::string plan_str;
  if (!gen_plan(tx.GetSession().db(), cypher, plan_str)) {
    std::string error = "    Generate plan failed: " + cypher;
    return "";
  }
  physical::PhysicalPlan plan;
  if (!plan.ParseFromString(plan_str)) {
    LOG(ERROR) << "Parse plan failed for query: " << cypher;
    std::string error = "    Parse plan failed: " + cypher;
    return "";
  }
  auto res =
      runtime::PlanParser::get().parse_update_pipeline(tx.schema(), plan);
  if (!res) {
    LOG(ERROR) << "Parse plan failed for query: " << cypher;
    std::string error = "    Parse plan failed: " + cypher;
    return "";
  }
  OprTimer timer;
  auto pipeline = std::move(res.value());
  GraphUpdateInterface graph(tx);
  if (pipeline.is_insert()) {
    auto ctx = pipeline.Execute(graph, WriteContext(), params, timer);
    if (!ctx) {
      LOG(ERROR) << "Execute pipeline failed for query: " << cypher;
      std::string error = "    Execute pipeline failed: " + cypher;
      return "";
    }
  } else {
    auto ctx = pipeline.Execute(graph, Context(), params, timer);
    if (!ctx) {
      LOG(ERROR) << "Execute pipeline failed for query: " << cypher;
      std::string error = "    Execute pipeline failed: " + cypher;
      return "";
    }
    std::vector<char> buffer;
    gs::Encoder encoder(buffer);
    Sink::sink_encoder(ctx.value(), graph, encoder);
    return std::string(buffer.begin(), buffer.end());
  }
  return "";
}

std::string CypherRunnerImpl::run(
    const gs::ReadTransaction& tx, const std::string& cypher,
    const std::map<std::string, std::string>& params) {
  std::string plan_str;
  if (!gen_plan(tx.GetSession().db(), cypher, plan_str)) {
    std::string error = "    Generate plan failed: " + cypher;
    return "";
  }
  physical::PhysicalPlan plan;
  if (!plan.ParseFromString(plan_str)) {
    LOG(ERROR) << "Parse plan failed for query: " << cypher;
    std::string error = "    Parse plan failed: " + cypher;
    return "";
  }
  ContextMeta ctx_meta;
  auto res = runtime::PlanParser::get().parse_read_pipeline(tx.schema(),
                                                            ctx_meta, plan);
  if (!res) {
    LOG(ERROR) << "Parse plan failed for query: " << cypher;
    std::string error = "    Parse plan failed: " + cypher;
    return "";
  }
  OprTimer timer;
  auto pipeline = std::move(res.value());
  GraphReadInterface graph(tx);

  auto ctx = pipeline.Execute(graph, Context(), params, timer);
  if (!ctx) {
    LOG(ERROR) << "Execute pipeline failed for query: " << cypher;
    std::string error = "    Execute pipeline failed: " + cypher;
    return "";
  }
  std::vector<char> buffer;
  gs::Encoder encoder(buffer);
  Sink::sink_encoder(ctx.value(), graph, encoder);
  return std::string(buffer.begin(), buffer.end());
}

std::string CypherRunnerImpl::run(
    InsertTransaction& tx, const std::string& cypher,
    const std::map<std::string, std::string>& params) {
  std::string plan_str;
  if (!gen_plan(tx.GetSession().db(), cypher, plan_str)) {
    std::string error = "    Generate plan failed: " + cypher;
    return "";
  }
  physical::PhysicalPlan plan;
  if (!plan.ParseFromString(plan_str)) {
    LOG(ERROR) << "Parse plan failed for query: " << cypher;
    std::string error = "    Parse plan failed: " + cypher;
    return "";
  }
  auto res = runtime::PlanParser::get().parse_write_pipeline(tx.schema(), plan);
  if (!res) {
    LOG(ERROR) << "Parse plan failed for query: " << cypher;
    std::string error = "    Parse plan failed: " + cypher;
    return "";
  }
  OprTimer timer;
  auto pipeline = std::move(res.value());
  GraphInsertInterface graph(tx);
  auto ctx = pipeline.Execute(graph, WriteContext(), params, timer);
  if (!ctx) {
    LOG(ERROR) << "Execute pipeline failed for query: " << cypher;
    std::string error = "    Execute pipeline failed: " + cypher;
    return "";
  }
  return "";
}

CypherRunnerImpl::CypherRunnerImpl() : plan_cache_() {}

CypherRunnerImpl& CypherRunnerImpl::get() {
  static CypherRunnerImpl runner;
  return runner;
}

const PlanCache& CypherRunnerImpl::get_plan_cache() const {
  return plan_cache_;
}

void CypherRunnerImpl::clear_cache() { plan_cache_.plan_cache.clear(); }

}  // namespace runtime
}  // namespace gs
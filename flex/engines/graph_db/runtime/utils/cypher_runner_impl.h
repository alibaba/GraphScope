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

#ifndef RUNTIME_UTILS_CYPHER_RUNNER_IMPL_H_
#define RUNTIME_UTILS_CYPHER_RUNNER_IMPL_H_
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace gs {

class ReadTransaction;
class UpdateTransaction;
class InsertTransaction;
class GraphDB;

namespace runtime {
struct PlanCache {
  bool get(const std::string& query, std::string& plan) const {
    std::shared_lock<std::shared_mutex> lock(mutex);
    if (plan_cache.count(query)) {
      plan = plan_cache.at(query);
      return true;
    }
    return false;
  }

  void put(const std::string& query, const std::string& plan) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    plan_cache[query] = plan;
  }
  mutable std::shared_mutex mutex;
  std::unordered_map<std::string, std::string> plan_cache;
};
class CypherRunnerImpl {
 public:
  std::string run(gs::UpdateTransaction& tx, const std::string& cypher,
                  const std::map<std::string, std::string>& params);
  std::string run(const gs::ReadTransaction& tx, const std::string& cypher,
                  const std::map<std::string, std::string>& params);

  std::string run(gs::InsertTransaction& tx, const std::string& cypher,
                  const std::map<std::string, std::string>& params);

  static CypherRunnerImpl& get();

  bool gen_plan(const GraphDB& db, const std::string& query, std::string& plan);

  const PlanCache& get_plan_cache() const;

  void clear_cache();

 private:
  CypherRunnerImpl();

  CypherRunnerImpl(const CypherRunnerImpl&) = delete;
  CypherRunnerImpl& operator=(const CypherRunnerImpl&) = delete;
  PlanCache plan_cache_;
  std::mutex mutex_;
};
}  // namespace runtime
}  // namespace gs
#endif  // RUNTIME_UTILS_CYPHER_RUNNER_IMPL_H_
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
#include <string>

namespace gs {

class ReadTransaction;
class UpdateTransaction;
class InsertTransaction;
namespace runtime {

struct PlanCache;
class CypherRunnerImpl {
 public:
  std::string run(gs::UpdateTransaction& tx, const std::string& cypher,
                  const std::map<std::string, std::string>& params);
  std::string run(const gs::ReadTransaction& tx, const std::string& cypher,
                  const std::map<std::string, std::string>& params);

  std::string run(gs::InsertTransaction& tx, const std::string& cypher,
                  const std::map<std::string, std::string>& params);

  static CypherRunnerImpl& get();

 private:
  CypherRunnerImpl();

  CypherRunnerImpl(const CypherRunnerImpl&) = delete;
  CypherRunnerImpl& operator=(const CypherRunnerImpl&) = delete;
  std::unique_ptr<PlanCache> plan_cache_;
};
}  // namespace runtime
}  // namespace gs
#endif  // RUNTIME_UTILS_CYPHER_RUNNER_IMPL_H_
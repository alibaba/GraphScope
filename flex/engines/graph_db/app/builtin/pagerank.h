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

#ifndef ENGINES_GRAPH_DB_APP_BUILDIN_PAGERANK_H_
#define ENGINES_GRAPH_DB_APP_BUILDIN_PAGERANK_H_
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/hqps_db/app/interactive_app_base.h"

namespace gs {
class PageRank
    : public CypherReadAppBase<std::string, std::string, double, int, double> {
 public:
  PageRank() {}
  results::CollectiveResults Query(const GraphDBSession& sess,
                                   std::string vertex_label,
                                   std::string edge_label,
                                   double damping_factor, int max_iterations,
                                   double epsilon);
};

class PageRankFactory : public AppFactoryBase {
 public:
  PageRankFactory() = default;
  ~PageRankFactory() = default;

  AppWrapper CreateApp(const GraphDB& db) override;
};

}  // namespace gs

#endif
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

#ifndef ENGINES_GRAPH_DB_APP_BUILDIN_K_HOP_NEIGHBORS_
#define ENGINES_GRAPH_DB_APP_BUILDIN_K_HOP_NEIGHBORS_
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/hqps_db/app/interactive_app_base.h"

namespace gs {
class KNeighbors : public CypherReadAppBase<std::string, int64_t, int32_t> {
 public:
  KNeighbors() {}
  results::CollectiveResults Query(const GraphDBSession& sess,
                                   std::string label_name, int64_t vertex_id,
                                   int32_t hop_range) override;
};

class KNeighborsFactory : public AppFactoryBase {
 public:
  KNeighborsFactory() = default;
  ~KNeighborsFactory() = default;

  AppWrapper CreateApp(const GraphDB& db) override;
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_APP_BUILDIN_K_HOP_NEIGHBORS_
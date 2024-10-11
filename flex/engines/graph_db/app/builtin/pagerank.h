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
class PageRank : public CypherInternalPbWriteAppBase {
 public:
  PageRank()
      : damping_factor_(0.85),
        max_iterations_(100),
        epsilon_(1e-6),
        vertex_label_id_(0),
        edge_label_id_(0) {}
  bool DoQuery(GraphDBSession& sess, Decoder& input, Encoder& output) override;

 private:
  double damping_factor_;
  int max_iterations_;
  double epsilon_;

  label_t vertex_label_id_;
  label_t edge_label_id_;
};

class PageRankFactory : public AppFactoryBase {
 public:
  PageRankFactory() = default;
  ~PageRankFactory() = default;

  AppWrapper CreateApp(const GraphDB& db) override;
};

}  // namespace gs

#endif
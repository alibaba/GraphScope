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

#ifndef ENGINES_GRAPH_DB_APP_BUILDIN_SHORTEST_PATH_AMONG_THREE_H_
#define ENGINES_GRAPH_DB_APP_BUILDIN_SHORTEST_PATH_AMONG_THREE_H_
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/hqps_db/app/interactive_app_base.h"

namespace gs {
class ShortestPathAmongThree
    : public CypherReadAppBase<std::string, std::string, std::string,
                               std::string, std::string, std::string> {
 public:
  ShortestPathAmongThree() {}
  results::CollectiveResults Query(const GraphDBSession& sess,
                                   std::string label_name1, std::string oid1,
                                   std::string label_name2, std::string oid2,
                                   std::string label_name3, std::string oid3);

 private:
  bool ShortestPath(const gs::ReadTransaction& txn, label_t v1_l,
                    vid_t v1_index, label_t v2_l, vid_t v2_index,
                    std::vector<std::pair<label_t, vid_t>>& result_);
  std::vector<std::pair<label_t, vid_t>> ConnectPath(
      const std::vector<std::pair<label_t, vid_t>>& path1,
      const std::vector<std::pair<label_t, vid_t>>& path2,
      const std::vector<std::pair<label_t, vid_t>>& path3);
};

class ShortestPathAmongThreeFactory : public AppFactoryBase {
 public:
  ShortestPathAmongThreeFactory() = default;
  ~ShortestPathAmongThreeFactory() = default;

  AppWrapper CreateApp(const GraphDB& db) override;
};

}  // namespace gs

#endif
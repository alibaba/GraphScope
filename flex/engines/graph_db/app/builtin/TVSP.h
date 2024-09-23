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

#ifndef ENGINES_GRAPH_DB_APP_BUILDIN_TVSP_H_
#define ENGINES_GRAPH_DB_APP_BUILDIN_TVSP_H_
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/hqps_db/app/interactive_app_base.h"

namespace gs {
// A simple app to count the number of vertices of a given label.
class TVSP : public CypherInternalPbWriteAppBase {
 private:
//  std::string label_name1;
//  int32_t vid1;
//  std::string label_name2;
//  int32_t vid2;
//  std::string label_name3;
//  int32_t vid3;

 public:
  TVSP() {}
  bool DoQuery(GraphDBSession& sess, Decoder& input, Encoder& output) override;
  bool ShortestPath(const gs::ReadTransaction& txn, label_t v1_l,vid_t v1_index,label_t v2_l,vid_t v2_index,std::vector<int64_t> &result_, uint vertex_num); 
  std::vector<int64_t> ConnectPath(std::vector<int64_t> &path1,std::vector<int64_t> &path2,std::vector<int64_t> &path3);

};

class TVSPFactory : public AppFactoryBase {
 public:
  TVSPFactory() = default;
  ~TVSPFactory() = default;

  AppWrapper CreateApp(const GraphDB& db) override;
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_APP_BUILDIN_TVSP_H_
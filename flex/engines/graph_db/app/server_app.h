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

#ifndef GRAPHSCOPE_SERVER_APP_H_
#define GRAPHSCOPE_SERVER_APP_H_

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"

namespace gs {

class ServerApp : public AppBase {
 public:
  ServerApp(GraphDBSession& graph) : graph_(graph) {}

  bool Query(Decoder& input, Encoder& output) override;

 private:
  struct vertex_range {
    vertex_range(uint32_t f, uint32_t t) : from(f), to(t) {}

    bool contains(uint32_t v) const { return v >= from && v < to; }

    bool empty() const { return (from >= to); }

    uint32_t from;
    uint32_t to;
  };

  GraphDBSession& graph_;
};

class ServerAppFactory : public AppFactoryBase {
 public:
  ServerAppFactory() {}
  ~ServerAppFactory() {}

  AppWrapper CreateApp(GraphDBSession& graph) override;
};

}  // namespace gs

#endif  // GRAPHSCOPE_SERVER_APP_H_

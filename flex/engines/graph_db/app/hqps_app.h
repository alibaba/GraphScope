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

#ifndef ENGINES_GRAPH_DB_APP_HQPS_APP_H_
#define ENGINES_GRAPH_DB_APP_HQPS_APP_H_

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"

namespace gs {

/**
 * @brief HQPSAdhocApp is a builtin, proxy app used to evaluate adhoc query.
 */
class HQPSAdhocApp : public AppBase {
 public:
  HQPSAdhocApp(GraphDBSession& graph);

  bool Query(Decoder& input, Encoder& output) override;

 private:
  GraphDBSession& graph_;
};

/**
 * @brief HQPSProcedureApp is a builtin, proxy app used to evaluate procedure
 * query.
 */
class HQPSProcedureApp : public AppBase {
 public:
  HQPSProcedureApp(GraphDBSession& graph);

  bool Query(Decoder& input, Encoder& output) override;

 private:
  GraphDBSession& graph_;
};

// Factory
class HQPSAdhocAppFactory : public AppFactoryBase {
 public:
  HQPSAdhocAppFactory();
  ~HQPSAdhocAppFactory();

  AppWrapper CreateApp(GraphDBSession& graph) override;
};

// Factory
class HQPSProcedureAppFactory : public AppFactoryBase {
 public:
  HQPSProcedureAppFactory();
  ~HQPSProcedureAppFactory();

  AppWrapper CreateApp(GraphDBSession& graph) override;
};
}  // namespace gs

#endif  // ENGINES_GRAPH_DB_APP_HQPS_APP_H_

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
class AbstractHQPSAdhocApp : public AppBase {
 public:
  AbstractHQPSAdhocApp(const GraphDB& graph) : graph_(graph) {}

  AppType type() const override { return AppType::kCypherAdhoc; }

  bool run(GraphDBSession& graph, Decoder& input, Encoder& output) override;

 private:
  const GraphDB& graph_;
};

class HqpsReadAdhocApp : public AbstractHQPSAdhocApp {
 public:
  HqpsReadAdhocApp(const GraphDB& graph) : AbstractHQPSAdhocApp(graph) {}

  AppMode mode() const override { return AppMode::kRead; }
};

class HqpsWriteAdhocApp : public AbstractHQPSAdhocApp {
 public:
  HqpsWriteAdhocApp(const GraphDB& graph) : AbstractHQPSAdhocApp(graph) {}

  AppMode mode() const override { return AppMode::kWrite; }
};

/**
 * @brief HQPSProcedureApp is a builtin, proxy app used to evaluate procedure
 * query.
 */
class AbstractHQPSProcedureApp : public AppBase {
 public:
  AbstractHQPSProcedureApp(const GraphDB& graph) {}

  AppType type() const override { return AppType::kCypherProcedure; }

  bool run(GraphDBSession& graph, Decoder& input, Encoder& output) override;
};

class HqpsReadProcedureApp : public AbstractHQPSProcedureApp {
 public:
  HqpsReadProcedureApp(const GraphDB& graph)
      : AbstractHQPSProcedureApp(graph) {}

  AppMode mode() const override { return AppMode::kRead; }
};

class HqpsWriteProcedureApp : public AbstractHQPSProcedureApp {
 public:
  HqpsWriteProcedureApp(const GraphDB& graph)
      : AbstractHQPSProcedureApp(graph) {}

  AppMode mode() const override { return AppMode::kWrite; }
};

// Factory
class HQPSReadAdhocAppFactory : public AppFactoryBase {
 public:
  HQPSReadAdhocAppFactory() = default;
  ~HQPSReadAdhocAppFactory() = default;

  AppWrapper CreateApp(const GraphDB& graph) override;
};

class HQPSWriteAdhocAppFactory : public AppFactoryBase {
 public:
  HQPSWriteAdhocAppFactory() = default;
  ~HQPSWriteAdhocAppFactory() = default;

  AppWrapper CreateApp(const GraphDB& graph) override;
};

// Factory
class HQPSReadProcedureAppFactory : public AppFactoryBase {
 public:
  HQPSReadProcedureAppFactory() = default;
  ~HQPSReadProcedureAppFactory() = default;

  AppWrapper CreateApp(const GraphDB& graph) override;
};

class HQPSWriteProcedureAppFactory : public AppFactoryBase {
 public:
  HQPSWriteProcedureAppFactory() = default;
  ~HQPSWriteProcedureAppFactory() = default;

  AppWrapper CreateApp(const GraphDB& graph) override;
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_APP_HQPS_APP_H_

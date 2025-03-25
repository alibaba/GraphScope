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

#ifndef ENGINES_KAFKA_WAL_INGESTER_APP_H_
#define ENGINES_KAFKA_WAL_INGESTER_APP_H_

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"

namespace gs {
#ifdef BUILD_KAFKA_WAL_WRITER_PARSER
// Ingest wal from kafka
class KafkaWalIngesterApp : public WriteAppBase {
 public:
  KafkaWalIngesterApp() {}

  AppType type() const override { return AppType::kBuiltIn; }

  bool Query(GraphDBSession& graph, Decoder& input, Encoder& output) override;
};

class KafkaWalIngesterAppFactory : public AppFactoryBase {
 public:
  KafkaWalIngesterAppFactory() = default;
  ~KafkaWalIngesterAppFactory() = default;

  AppWrapper CreateApp(const GraphDB& db) override;
};
#endif

}  // namespace gs

#endif  // ENGINES_KAFKA_WAL_INGESTER_APP_H_
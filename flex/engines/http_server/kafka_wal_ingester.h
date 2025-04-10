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
#include <cppkafka/cppkafka.h>
#include <thread>
#include <vector>

#ifdef BUILD_KAFKA_WAL_WRITER_PARSER
#include "flex/engines/graph_db/app/kafka_wal_ingester_app.h"
namespace server {
class KafkaWalIngester {
 public:
  KafkaWalIngester() : force_stop_(false), ingester_(nullptr) {}

  bool open(gs::GraphDB&, const std::string& uri);

  bool close();
  std::atomic<bool> force_stop_{false};
  std::unique_ptr<gs::KafkaWalIngesterApp> ingester_;
  std::thread ingester_thread_;
};
}  // namespace server
#endif
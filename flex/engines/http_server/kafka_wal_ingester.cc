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
#include "flex/engines/http_server/kafka_wal_ingester.h"
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/database/wal/kafka_wal_utils.h"

namespace server {
#ifdef BUILD_KAFKA_WAL_WRITER_PARSER

bool KafkaWalIngester::open(gs::GraphDB& db, const std::string& wal_uri) {
  ingester_ = std::make_unique<gs::KafkaWalIngesterApp>();
  ingester_thread_ = std::thread([&]() {
    auto res = gs::parse_uri(wal_uri);
    if (!res) {
      LOG(ERROR) << "Failed to parse uri: " << wal_uri;
      return;
    }
    gs::Decoder decoder(res.value().data(), res.value().size());
    std::vector<char> buf;
    gs::Encoder encoder(buf);
    ingester_->Query(db.GetSession(0), decoder, encoder);
    gs::Decoder output(buf.data(), buf.size());
    db.set_last_ingested_wal_ts(output.get_long());
  });
  return true;
}

bool KafkaWalIngester::close() {
  if (ingester_) {
    ingester_->terminal();
  }
  if (ingester_thread_.joinable()) {
    ingester_thread_.join();
  }
  return true;
}
#endif

}  // namespace server
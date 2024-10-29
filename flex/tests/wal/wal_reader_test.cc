
#include "cppkafka/cppkafka.h"
#include "librdkafka/rdkafka.h"

#include <iostream>
#include <string>
#include "flex/engines/graph_db/database/wal.h"
#include "grape/grape.h"

size_t visitWalRange(std::shared_ptr<gs::IWalsParser> parser, uint32_t from_ts,
                     uint32_t to_ts) {
  size_t insert_wals = 0;
  for (uint32_t ts = from_ts; ts < to_ts; ++ts) {
    auto& insert_wal = parser->get_insert_wal(ts);
    if (insert_wal.size != 0) {
      insert_wals++;
    }
  }
  return insert_wals;
}

int main(int argc, char** argv) {
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0]
              << "<kafka brokers> <local/kafka> <topic/directory>" << std::endl;
    return 1;
  }
  std::string brokers = argv[1];
  std::string type = argv[2];
  std::string topic_name = argv[3];

  double t = -grape::GetCurrentTime();

  std::shared_ptr<gs::IWalsParser> parser;
  if (type == "local") {
    std::cout << "Consuming message from directory " << topic_name;
    std::vector<std::string> wals;
    for (const auto& entry : std::filesystem::directory_iterator(topic_name)) {
      wals.push_back(entry.path().string());
    }
    parser = std::make_shared<gs::LocalWalsParser>(wals);
  } else {
    std::cout << "Consuming message from topic " << topic_name
              << " thread num ";
    cppkafka::Configuration config = {{"metadata.broker.list", brokers},
                                      {"group.id", "primary_group"},
                                      // Disable auto commit
                                      {"enable.auto.commit", false}};
    parser = std::make_shared<gs::KafkaWalsParser>(config, topic_name);
  }

  uint32_t from_ts = 1;
  size_t update_wals = 0;
  size_t insert_wals = 0;
  for (auto& update_wal : parser->update_wals()) {
    uint32_t to_ts = update_wal.timestamp;
    if (from_ts < to_ts) {
      insert_wals += visitWalRange(parser, from_ts, to_ts);
    }
    if (!update_wal.size == 0) {
      update_wals++;
    }
    from_ts = to_ts + 1;
  }
  if (from_ts <= parser->last_ts()) {
    insert_wals += visitWalRange(parser, from_ts, parser->last_ts() + 1);
  }

  t += grape::GetCurrentTime();
  std::cout << "Consuming message took " << t << " seconds, update wals"
            << update_wals << std::endl;

  return 0;
}
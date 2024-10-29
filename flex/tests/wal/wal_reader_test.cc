
#include "cppkafka/cppkafka.h"
#include "librdkafka/rdkafka.h"

#include <iostream>
#include <string>
#include "flex/engines/graph_db/database/wal.h"
#include "grape/grape.h"

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

  if (type == "local") {
    std::cout << "Consuming message from directory " << topic_name;
    std::vector<std::string> wals;
    for (const auto& entry : std::filesystem::directory_iterator(topic_name)) {
      wals.push_back(entry.path().string());
    }
    std::unique_ptr<gs::IWalsParser> parser =
        std::make_unique<gs::LocalWalsParser>(wals);
  } else {
    std::cout << "Consuming message from topic " << topic_name
              << " thread num ";
    cppkafka::Configuration config = {{"metadata.broker.list", brokers},
                                      {"group.id", "primary_group"},
                                      // Disable auto commit
                                      {"enable.auto.commit", false}};
    std::unique_ptr<gs::IWalsParser> parser =
        std::make_unique<gs::KafkaWalsParser>(config, topic_name);
  }

  t += grape::GetCurrentTime();
  std::cout << "Consuming message took " << t << " seconds" << std::endl;

  return 0;
}
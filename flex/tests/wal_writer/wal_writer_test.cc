#include "cppkafka/cppkafka.h"
#include "librdkafka/rdkafka.h"

#include <iostream>
#include <string>
#include "flex/engines/graph_db/database/wal.h"
#include "grape/grape.h"

using namespace std;
using namespace cppkafka;

const int times = 100000;

void run(std::vector<gs::IWalWriter*>& writers, const std::string& payload) {
  std::vector<std::thread> threads;
  double t = -grape::GetCurrentTime();
  for (size_t i = 0; i < writers.size(); ++i) {
    threads.emplace_back([&writers, i, payload]() {
      for (int j = 0; j < times; ++j) {
        if (!writers[i]->append(payload.c_str(), payload.size())) {
          std::cerr << "Failed to append message to kafka" << std::endl;
        }
        if (j % 10000 == 0) {
          std::cout << "Producing " << j << " messages" << std::endl;
        }
      }
    });
  }
  for (auto& thrd : threads) {
    thrd.join();
  }
  t += grape::GetCurrentTime();
  std::cout << "Producing " << writers.size() * 100
            << " messages to kafka took " << t << " seconds" << std::endl;
}

void test_local_wal_writer(const std::string& topic_name, int thread_num,
                           const std::string& payload) {
  std::vector<gs::IWalWriter*> kafka_writers;
  for (int i = 0; i < thread_num; ++i) {
    kafka_writers.emplace_back(new gs::LocalWalWriter());
  }
  for (int i = 0; i < thread_num; ++i) {
    std::string dst_path = "/tmp/";
    // check whether files exist
    kafka_writers[i]->open(dst_path, i);
  }
  run(kafka_writers, payload);
}

void test_kafka_wal_writer(const std::string& topic_name, int thread_num,
                           const std::string& brokers,
                           const std::string& payload) {
  // auto kafka_writer = std::make_unique<gs::KafkaWalWriter>(config);
  std::vector<gs::IWalWriter*> kafka_writers;
  for (int i = 0; i < thread_num; ++i) {
    kafka_writers.emplace_back(new gs::KafkaWalWriter(brokers));
  }
  for (int i = 0; i < thread_num; ++i) {
    kafka_writers[i]->open(topic_name, i);
  }
  run(kafka_writers, payload);
}

int main(int argc, char** argv) {
  if (argc != 4) {
    cerr << "Usage: " << argv[0] << "<local/kafka> <topic> <thread_num>"
         << endl;
    return 1;
  }
  std::string type = argv[1];
  std::string topic_name = argv[2];
  int thread_num = std::stoi(argv[3]);
  std::cout << "Producing message to topic " << topic_name << " thread num "
            << thread_num << std::endl;
  std::stringstream ss;
  for (size_t i = 0; i < 50; ++i) {
    ss << "hello world " << i << std::endl;
  }
  std::string payload = ss.str();
  if (type == "local") {
    test_local_wal_writer(topic_name, thread_num, payload);
  } else {
    test_kafka_wal_writer(topic_name, thread_num, "localhost:9092", payload);
  }

  return 0;
}
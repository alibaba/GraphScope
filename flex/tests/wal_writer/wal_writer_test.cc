#include "cppkafka/cppkafka.h"
#include "librdkafka/rdkafka.h"

#include <iostream>
#include <string>

using namespace std;
using namespace cppkafka;

int main(int argc, char** argv) {
  Configuration config = {{"metadata.broker.list", "127.0.0.1:9092"}};

  if (argc != 5) {
    cerr << "Usage: " << argv[0] << " <topic> <partition> <key> <value>"
         << endl;
    return 1;
  }
  std::string topic_name = argv[1];
  int partition = std::stoi(argv[2]);
  std::string key = argv[3];
  std::string value = argv[4];
  std::cout << "Producing message to topic " << topic_name << " partition "
            << partition << " key " << key << " value " << value << std::endl;

  // Create the producer
  Producer producer(config);

  // Produce a message!
  producer.produce(
      MessageBuilder(topic_name).partition(partition).key(key).payload(value));
  producer.flush();
  std::cout << "Message produced" << std::endl;
  return 0;
}
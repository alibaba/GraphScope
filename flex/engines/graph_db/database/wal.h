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

#ifndef GRAPHSCOPE_DATABASE_WAL_H_
#define GRAPHSCOPE_DATABASE_WAL_H_

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <queue>
#include <string>
#include <thread>

#include "flex/utils/result.h"

#ifdef BUILD_KAFKA_WAL_WRITER
#include "cppkafka/cppkafka.h"
#endif

#include "glog/logging.h"

namespace gs {

std::string generate_graph_wal_topic(const std::string& kafka_brokers,
                                     const std::string& graph_id,
                                     int partition_num = 4,
                                     int replication_factor = 1);

struct WalHeader {
  uint32_t timestamp;
  uint8_t type : 1;
  int32_t length : 31;
};

struct WalContentUnit {
  char* ptr{NULL};
  size_t size{0};
};

struct UpdateWalUnit {
  uint32_t timestamp{0};
  char* ptr{NULL};
  size_t size{0};
};

class IWalWriter {
 public:
  enum class WalWriterType : uint8_t {
    kLocal = 0,
    kKafka = 1,
  };
  static constexpr size_t MAX_WALS_NUM = 134217728;
  static inline WalWriterType parseWalWriterType(const std::string& type_str) {
    if (type_str == "local") {
      return WalWriterType::kLocal;
    } else if (type_str == "kafka") {
      return WalWriterType::kKafka;
    } else {
      LOG(FATAL) << "Unsupported wal writer type: " << type_str;
    }
    return WalWriterType::kLocal;
  }

  virtual ~IWalWriter() {}
  virtual void open(const std::string& path, int thread_id) = 0;
  virtual void close() = 0;
  virtual IWalWriter::WalWriterType type() const = 0;
  /**
   * @brief Append the data to the WAL.
   * The published messages contains these info.
   * 1. The thread id
   * 2. The timestamp of the message(could be deemed as the id of WAL).
   * 3. The data(The WAL content).
   *
   * @param data The data to be sent
   * @param length The length of the data
   *
   */
  virtual bool append(const char* data, size_t length) = 0;
};

class LocalWalWriter : public IWalWriter {
  static constexpr size_t TRUNC_SIZE = 1ul << 30;

 public:
  LocalWalWriter() : thread_id_(-1), fd_(-1), file_size_(0), file_used_(0) {}
  ~LocalWalWriter() { close(); }

  void open(const std::string& path, int thread_id) override;

  void close() override;

  bool append(const char* data, size_t length) override;

  IWalWriter::WalWriterType type() const override;

 private:
  int thread_id_;
  int fd_;
  size_t file_size_;
  size_t file_used_;
};

#ifdef BUILD_KAFKA_WAL_WRITER
class KafkaWalWriter : public IWalWriter {
 public:
  KafkaWalWriter(const std::string& kafka_brokers)
      : thread_id_(-1),
        kafka_brokers_(kafka_brokers),
        kafka_topic_(""),
        builder_("") {}
  ~KafkaWalWriter() { close(); }

  void open(const std::string& kafka_topic, int thread_id) override;

  void close() override;

  bool append(const char* data, size_t length) override;

  IWalWriter::WalWriterType type() const override;

 private:
  int thread_id_;
  std::string kafka_brokers_;
  std::string kafka_topic_;
  std::shared_ptr<cppkafka::BufferedProducer<std::string>> producer_;
  cppkafka::MessageBuilder builder_;
};
#endif

class IWalsParser {
 public:
  virtual ~IWalsParser() {}

  virtual uint32_t last_ts() const = 0;
  virtual const WalContentUnit& get_insert_wal(uint32_t ts) const = 0;
  virtual const std::vector<UpdateWalUnit>& update_wals() const = 0;
};

class LocalWalsParser : public IWalsParser {
 public:
  LocalWalsParser(const std::vector<std::string>& paths);
  ~LocalWalsParser();

  uint32_t last_ts() const override;
  const WalContentUnit& get_insert_wal(uint32_t ts) const override;
  const std::vector<UpdateWalUnit>& update_wals() const override;

 private:
  std::vector<int> fds_;
  std::vector<void*> mmapped_ptrs_;
  std::vector<size_t> mmapped_size_;
  WalContentUnit* insert_wal_list_;
  size_t insert_wal_list_size_;
  uint32_t last_ts_{0};

  std::vector<UpdateWalUnit> update_wal_list_;
};

#ifdef BUILD_KAFKA_WAL_WRITER

/**
 * @brief Parse all the WALs from kafka.
 */
class KafkaWalsParser : public IWalsParser {
 public:
  static constexpr const std::chrono::milliseconds POLL_TIMEOUT =
      std::chrono::milliseconds(100);
  static constexpr const size_t MAX_BATCH_SIZE = 1000;

  // always track all partitions and from begining
  KafkaWalsParser(cppkafka::Configuration config,
                  const std::string& topic_name);
  ~KafkaWalsParser();

  uint32_t last_ts() const override;
  const WalContentUnit& get_insert_wal(uint32_t ts) const override;
  const std::vector<UpdateWalUnit>& update_wals() const override;

 private:
  std::unique_ptr<cppkafka::Consumer> consumer_;
  WalContentUnit* insert_wal_list_;
  size_t insert_wal_list_size_;
  uint32_t last_ts_;

  std::vector<UpdateWalUnit> update_wal_list_;
  std::vector<std::string> message_vector_;  // used to hold the polled messages
};

/**
 * @brief The KafkaWalConsumer class is used to consume the WAL from kafka. The
 * topic could have multiple partitions, and we could use multiple thread to
 * consume the WAL from different partitions.(Not necessary to use the same
 * number of partitions)
 *
 * We assume that the messages in each partition are ordered by the timestamp.
 */
class KafkaWalConsumer {
 public:
  struct CustomComparator {
    inline bool operator()(const std::string& lhs, const std::string& rhs) {
      const WalHeader* header1 = reinterpret_cast<const WalHeader*>(lhs.data());
      const WalHeader* header2 = reinterpret_cast<const WalHeader*>(rhs.data());
      return header1->timestamp > header2->timestamp;
    }
  };
  static constexpr const std::chrono::milliseconds POLL_TIMEOUT =
      std::chrono::milliseconds(100);

  // always track all partitions and from begining
  KafkaWalConsumer(cppkafka::Configuration config,
                   const std::string& topic_name, int32_t thread_num);

  std::string poll();

 private:
  bool running;
  std::vector<std::unique_ptr<cppkafka::Consumer>> consumers_;
  std::priority_queue<std::string, std::vector<std::string>, CustomComparator>
      message_queue_;
};

#endif  // BUILD_KAFKA_WAL_WRITER

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_WAL_H_

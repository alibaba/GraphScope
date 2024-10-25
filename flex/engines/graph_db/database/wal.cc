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

#include "flex/engines/graph_db/database/wal.h"

#include <chrono>
#include <filesystem>

namespace gs {

int tmout_multip(int msecs) {
  int r;
  r = (int) (((double) (msecs)) * 1.0);
  return r;
}

void test_conf_init(rd_kafka_conf_t** conf, rd_kafka_topic_conf_t** topic_conf,
                    int timeout) {
  if (conf) {
    *conf = rd_kafka_conf_new();
  }

  if (topic_conf)
    *topic_conf = rd_kafka_topic_conf_new();
}

rd_kafka_t* test_create_handle(rd_kafka_type_t mode, rd_kafka_conf_t* conf) {
  rd_kafka_t* rk;
  char errstr[512];
  /* Creat kafka instance */
  rk = rd_kafka_new(mode, conf, errstr, sizeof(errstr));
  if (!rk) {
    LOG(ERROR) << "Failed to create new kafka instance: " << errstr;
    return NULL;
  }
  LOG(INFO) << "Created new kafka instance: " << rk;

  return rk;
}

rd_kafka_t* test_create_producer(const std::string& brokers) {
  rd_kafka_conf_t* conf;

  test_conf_init(&conf, NULL, 0);
  // set metadata.broker.list
  char errstr[512];
  if (rd_kafka_conf_set(conf, "metadata.broker.list", brokers.c_str(), errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    LOG(ERROR) << "Failed to set metadata.broker.list: " << errstr;
    return NULL;
  }

  return test_create_handle(RD_KAFKA_PRODUCER, conf);
}
std::string generate_graph_wal_topic(const std::string& kafka_brokers,
                                     const std::string& graph_id,
                                     int partition_num,
                                     int replication_factor) {
  auto topic_name = "graph_" + graph_id + "_wal";
  rd_kafka_t* rk;
  rd_kafka_NewTopic_t* newt[1];
  const size_t newt_cnt = 1;
  rd_kafka_AdminOptions_t* options;
  rd_kafka_queue_t* rkqu;
  rd_kafka_event_t* rkev;
  const rd_kafka_CreateTopics_result_t* res;
  const rd_kafka_topic_result_t** terr;
  int timeout_ms = tmout_multip(10000);
  size_t res_cnt;
  rd_kafka_resp_err_t err;
  char errstr[512];
  auto new_topic_ptr = rd_kafka_NewTopic_new(topic_name.c_str(), partition_num,
                                             1, errstr, sizeof(errstr));
  if (!new_topic_ptr) {
    LOG(FATAL) << "Failed to create new topic: " << errstr;
  }
  rk = test_create_producer(kafka_brokers);
  LOG(INFO) << "Create producer for topic " << topic_name;

  rkqu = rd_kafka_queue_new(rk);

  newt[0] = rd_kafka_NewTopic_new(topic_name.c_str(), partition_num,
                                  replication_factor, errstr, sizeof(errstr));
  if (!newt[0]) {
    LOG(FATAL) << "Failed to create new topic: " << errstr;
  }

  options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS);
  err = rd_kafka_AdminOptions_set_operation_timeout(options, timeout_ms, errstr,
                                                    sizeof(errstr));
  if (err) {
    LOG(FATAL) << "Failed to set operation timeout: " << errstr;
  }

  LOG(INFO) << "Creating topic " << topic_name << " with " << partition_num
            << " partitions and " << replication_factor
            << " replication factor";

  rd_kafka_CreateTopics(rk, newt, newt_cnt, options, rkqu);

  /* Wait for result */
  rkev = rd_kafka_queue_poll(rkqu, timeout_ms + 2000);
  if (!rkev) {
    LOG(FATAL) << "Failed to create topic " << topic_name
               << ": Timed out waiting for result";
  }

  if (rd_kafka_event_error(rkev)) {
    LOG(FATAL) << "CreateTopics failed: " << rd_kafka_event_error_string(rkev);
  }

  res = rd_kafka_event_CreateTopics_result(rkev);
  if (!res) {
    LOG(FATAL) << "CreateTopics failed: missing result";
  }

  terr = rd_kafka_CreateTopics_result_topics(res, &res_cnt);
  if (!terr) {
    LOG(FATAL) << "CreateTopics failed: missing topics";
  }
  if (res_cnt != 1) {
    LOG(FATAL) << "CreateTopics failed: expected 1 topic, not " << res_cnt;
  }

  if (rd_kafka_topic_result_error(terr[0]) ==
      RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS) {
    LOG(WARNING) << "Topic " << rd_kafka_topic_result_name(terr[0])
                 << " already exists";
  } else if (rd_kafka_topic_result_error(terr[0])) {
    LOG(FATAL) << "Topic " << rd_kafka_topic_result_name(terr[0])
               << " creation failed: "
               << rd_kafka_topic_result_error_string(terr[0]);
  } else {
    LOG(INFO) << "Topic " << rd_kafka_topic_result_name(terr[0])
              << " created successfully";
  }

  rd_kafka_event_destroy(rkev);

  rd_kafka_queue_destroy(rkqu);

  rd_kafka_AdminOptions_destroy(options);

  rd_kafka_NewTopic_destroy(newt[0]);

  rd_kafka_destroy(rk);
  LOG(INFO) << "Destroyed producer for topic " << topic_name;
  return topic_name;
}

void LocalWalWriter::open(const std::string& prefix, int thread_id) {
  if (fd_ != -1 || thread_id_ != -1) {
    LOG(FATAL) << "LocalWalWriter has been opened";
  }
  thread_id_ = thread_id;
  const int max_version = 65536;
  for (int version = 0; version != max_version; ++version) {
    std::string path = prefix + "/thread_" + std::to_string(thread_id_) + "_" +
                       std::to_string(version) + ".wal";
    if (std::filesystem::exists(path)) {
      continue;
    }
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    break;
  }
  if (fd_ == -1) {
    LOG(FATAL) << "Failed to open wal file " << strerror(errno);
  }
  if (ftruncate(fd_, TRUNC_SIZE) != 0) {
    LOG(FATAL) << "Failed to truncate wal file " << strerror(errno);
  }
  file_size_ = TRUNC_SIZE;
  file_used_ = 0;
}

void LocalWalWriter::close() {
  if (fd_ != -1) {
    if (::close(fd_) != 0) {
      LOG(FATAL) << "Failed to close file" << strerror(errno);
    }
    fd_ = -1;
    file_size_ = 0;
    file_used_ = 0;
  }
}

#define unlikely(x) __builtin_expect(!!(x), 0)

bool LocalWalWriter::append(uint32_t ts, const char* data, size_t length) {
  if (unlikely(fd_ == -1)) {
    return false;
  }
  size_t expected_size = file_used_ + length;
  if (expected_size > file_size_) {
    size_t new_file_size = (expected_size / TRUNC_SIZE + 1) * TRUNC_SIZE;
    if (ftruncate(fd_, new_file_size) != 0) {
      LOG(FATAL) << "Failed to truncate wal file " << strerror(errno);
    }
    file_size_ = new_file_size;
  }

  file_used_ += length;

  if (static_cast<size_t>(write(fd_, data, length)) != length) {
    LOG(ERROR) << "Failed to write wal file " << strerror(errno);
    return false;
  }

#if 1
#ifdef F_FULLFSYNC
  if (fcntl(fd_, F_FULLFSYNC) != 0) {
    LOG(ERROR) << "Failed to fcntl sync wal file " << strerrno(errno);
    return false;
  }
#else
  // if (fsync(fd_) != 0) {
  if (fdatasync(fd_) != 0) {
    LOG(ERROR) << "Failed to fsync wal file " << strerror(errno);
    return false;
  }
  return true;
#endif
#endif
}

#undef unlikely

IWalWriter::WalWriterType LocalWalWriter::type() const {
  return IWalWriter::WalWriterType::kLocal;
}

#ifdef ENABLE_KAFKA

void KafkaWalWriter::open(const std::string& topic, int thread_id) {
  if (thread_id_ != -1 || producer_) {
    LOG(FATAL) << "KafkaWalWriter has been opened";
  }
  thread_id_ = thread_id;
  if (!kafka_brokers_.empty()) {
    if (topic.empty()) {
      LOG(FATAL) << "Kafka topic is empty";
    }
    kafka_topic_ = topic;
    cppkafka::Configuration config = {{"metadata.broker.list", kafka_brokers_}};
    producer_ =
        std::make_shared<cppkafka::BufferedProducer<std::string>>(config);
    builder_.topic(kafka_topic_).partition(thread_id_);
  } else {
    LOG(FATAL) << "Kafka brokers is empty";
  }
}

void KafkaWalWriter::close() {
  if (producer_) {
    producer_->flush();
    producer_.reset();
    thread_id_ = -1;
    kafka_topic_.clear();
  }
}

bool KafkaWalWriter::append(uint32_t ts, const char* data, size_t length) {
  try {
    std::string key = std::to_string(ts);
    // cppkafka::Buffer key(reinterpret_cast<const char*>(&ts),
    // sizeof(uint32_t));
    producer_->sync_produce(builder_.key(key).payload({data, length}));
    producer_->flush(true);
  } catch (const cppkafka::HandleException& e) {
    LOG(ERROR) << "Failed to send to kafka: " << e.what();
    return false;
  }
  VLOG(10) << "Finished sending to kafka with message size: " << length
           << ", partition: " << thread_id_;
  return true;
}

IWalWriter::WalWriterType KafkaWalWriter::type() const {
  return IWalWriter::WalWriterType::kKafka;
}
#endif

WalsParser::WalsParser(const std::vector<std::string>& paths)
    : insert_wal_list_(NULL), insert_wal_list_size_(0) {
  for (auto path : paths) {
    LOG(INFO) << "Start to ingest WALs from file: " << path;
    size_t file_size = std::filesystem::file_size(path);
    if (file_size == 0) {
      continue;
    }
    int fd = open(path.c_str(), O_RDONLY);
    void* mmapped_buffer = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mmapped_buffer == MAP_FAILED) {
      LOG(FATAL) << "mmap failed...";
    }

    fds_.push_back(fd);
    mmapped_ptrs_.push_back(mmapped_buffer);
    mmapped_size_.push_back(file_size);
  }

  if (insert_wal_list_ != NULL) {
    munmap(insert_wal_list_, insert_wal_list_size_ * sizeof(WalContentUnit));
    insert_wal_list_ = NULL;
    insert_wal_list_size_ = 0;
  }
  insert_wal_list_ = static_cast<WalContentUnit*>(
      mmap(NULL, IWalWriter::MAX_WALS_NUM * sizeof(WalContentUnit),
           PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
           -1, 0));
  insert_wal_list_size_ = IWalWriter::MAX_WALS_NUM;
  for (size_t i = 0; i < mmapped_ptrs_.size(); ++i) {
    char* ptr = static_cast<char*>(mmapped_ptrs_[i]);
    while (true) {
      const WalHeader* header = reinterpret_cast<const WalHeader*>(ptr);
      ptr += sizeof(WalHeader);
      uint32_t ts = header->timestamp;
      if (ts == 0) {
        break;
      }
      int length = header->length;
      if (header->type) {
        UpdateWalUnit unit;
        unit.timestamp = ts;
        unit.ptr = ptr;
        unit.size = length;
        update_wal_list_.push_back(unit);
      } else {
        insert_wal_list_[ts].ptr = ptr;
        insert_wal_list_[ts].size = length;
      }
      ptr += length;
      last_ts_ = std::max(ts, last_ts_);
    }
  }

  if (!update_wal_list_.empty()) {
    std::sort(update_wal_list_.begin(), update_wal_list_.end(),
              [](const UpdateWalUnit& lhs, const UpdateWalUnit& rhs) {
                return lhs.timestamp < rhs.timestamp;
              });
  }
}

WalsParser::~WalsParser() {
  if (insert_wal_list_ != NULL) {
    munmap(insert_wal_list_, insert_wal_list_size_ * sizeof(WalContentUnit));
  }
  size_t ptr_num = mmapped_ptrs_.size();
  for (size_t i = 0; i < ptr_num; ++i) {
    munmap(mmapped_ptrs_[i], mmapped_size_[i]);
  }
  for (auto fd : fds_) {
    close(fd);
  }
}

uint32_t WalsParser::last_ts() const { return last_ts_; }

const WalContentUnit& WalsParser::get_insert_wal(uint32_t ts) const {
  return insert_wal_list_[ts];
}

const std::vector<UpdateWalUnit>& WalsParser::update_wals() const {
  return update_wal_list_;
}

const WalContentUnit* WalsParser::insert_wal_list() const {
  return insert_wal_list_;
}

// WalConsumer consumes from multiple partitions of a topic, and can start from
// the beginning or from the latest message.
WalConsumer::WalConsumer(cppkafka::Configuration config,
                         const std::string& topic_name, int32_t thread_num)
    : running(true), expect_timestamp_(1) {
  auto topic_partitions = get_all_topic_partitions(config, topic_name);
  consumers_.reserve(topic_partitions.size());
  for (size_t i = 0; i < topic_partitions.size(); ++i) {
    consumers_.emplace_back(std::make_unique<cppkafka::Consumer>(config));
    consumers_.back()->assign({topic_partitions[i]});
  }
}

std::pair<uint32_t, std::string> WalConsumer::poll() {
  for (auto& consumer : consumers_) {
    auto msg = consumer->poll();
    if (msg) {
      if (msg.get_error()) {
        if (!msg.is_eof()) {
          LOG(INFO) << "[+] Received error notification: " << msg.get_error();
        }
      } else {
        uint32_t key = atoi(static_cast<std::string>(msg.get_key()).c_str());
        std::string payload = msg.get_payload();
        LOG(INFO) << "receive from partition " << msg.get_partition()
                  << ", key: " << key << ", payload: " << payload
                  << " size: " << payload.size();
        message_queue_.push(std::make_pair(key, payload));
        // consumer->commit(msg);
      }
    }
  }
  // Check the message queue, if the top message is the expected message,
  // send it to the engine. Otherwise, wait for the expected message.
  if (!message_queue_.empty()) {
    while (!message_queue_.empty() &&
           message_queue_.top().first < expect_timestamp_) {
      LOG(WARNING) << "Drop message: <" << message_queue_.top().first << " -> "
                   << message_queue_.top().second << ">";
      message_queue_.pop();
    }
    while (!message_queue_.empty() &&
           message_queue_.top().first == expect_timestamp_) {
      expect_timestamp_++;
      auto ret = message_queue_.top();
      message_queue_.pop();
      return ret;
    }
    while (!message_queue_.empty() &&
           message_queue_.top().first < expect_timestamp_) {
      LOG(WARNING) << "Drop message: <" << message_queue_.top().first << " -> "
                   << message_queue_.top().second << ">";
      message_queue_.pop();
    }
    LOG(INFO) << "Expect timestamp: " << expect_timestamp_
              << ", but got: " << message_queue_.top().first;
  }
  return std::make_pair(std::numeric_limits<uint32_t>::max(), "");
}

std::vector<cppkafka::TopicPartition> get_all_topic_partitions(
    const cppkafka::Configuration& config, const std::string& topic_name) {
  std::vector<cppkafka::TopicPartition> partitions;
  cppkafka::Consumer consumer(config);  // tmp consumer
  auto metadata =
      consumer.get_metadata().get_topics({topic_name}).front().get_partitions();
  LOG(INFO) << "metadata: " << metadata.size();
  for (const auto& partition : metadata) {
    partitions.push_back(cppkafka::TopicPartition(
        topic_name, partition.get_id(), 0));  // from the beginning
  }
  return partitions;
}

KafkaWalParser::KafkaWalParser(cppkafka::Configuration config,
                               const std::string& topic_name)
    : insert_wal_list_(NULL), insert_wal_list_size_(0), last_ts_(0) {
  auto topic_partitions = get_all_topic_partitions(config, topic_name);
  // consumers_.reserve(topic_partitions.size());
  // for (size_t i = 0; i < topic_partitions.size(); ++i) {
  //   consumers_.emplace_back(std::make_unique<cppkafka::Consumer>(config));
  //   consumers_.back()->assign({topic_partitions[i]});
  // }
  consumer_ = std::make_unique<cppkafka::Consumer>(config);
  consumer_->assign(topic_partitions);

  insert_wal_list_ = static_cast<WalContentUnit*>(
      mmap(NULL, IWalWriter::MAX_WALS_NUM * sizeof(WalContentUnit),
           PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
           -1, 0));
  insert_wal_list_size_ = IWalWriter::MAX_WALS_NUM;

  while (true) {
    auto msgs = consumer_->poll_batch(MAX_BATCH_SIZE);
    if (msgs.empty() || msgs.empty()) {
      LOG(INFO) << "No message are polled, the topic has been all consumed.";
      break;
    }
    LOG(INFO) << "got messages of size: " << msgs.size();
    // message_vector_.emplace_back(std::move(msgs));
    for (auto& msg : msgs) {
      if (msg) {
        if (msg.get_error()) {
          if (!msg.is_eof()) {
            LOG(INFO) << "[+] Received error notification: " << msg.get_error();
          }
        } else {
          message_vector_.emplace_back(msg.get_payload());
          const char* payload = message_vector_.back().data();
          const WalHeader* header = reinterpret_cast<const WalHeader*>(payload);
          uint32_t cur_ts = header->timestamp;
          if (cur_ts == 0) {
            LOG(WARNING) << "Invalid timestamp 0, skip";
            continue;
          }
          int length = header->length;
          if (header->type) {
            UpdateWalUnit unit;
            unit.timestamp = cur_ts;
            unit.ptr = const_cast<char*>(payload + sizeof(WalHeader));
            unit.size = length;
            update_wal_list_.push_back(unit);
          } else {
            if (insert_wal_list_[cur_ts].ptr) {
              LOG(WARNING) << "Duplicated timestamp " << cur_ts << ", skip";
            }
            insert_wal_list_[cur_ts].ptr =
                const_cast<char*>(payload + sizeof(WalHeader));
            insert_wal_list_[cur_ts].size = length;
          }
          last_ts_ = std::max(cur_ts, last_ts_);
        }
      }
    }
  }
  LOG(INFO) << "last_ts: " << last_ts_;
  if (!update_wal_list_.empty()) {
    std::sort(update_wal_list_.begin(), update_wal_list_.end(),
              [](const UpdateWalUnit& lhs, const UpdateWalUnit& rhs) {
                return lhs.timestamp < rhs.timestamp;
              });
  }
}

KafkaWalParser::~KafkaWalParser() {
  if (insert_wal_list_ != NULL) {
    munmap(insert_wal_list_, insert_wal_list_size_ * sizeof(WalContentUnit));
  }
}

uint32_t KafkaWalParser::last_ts() const { return last_ts_; }
const WalContentUnit& KafkaWalParser::get_insert_wal(uint32_t ts) const {
  return insert_wal_list_[ts];
}
const std::vector<UpdateWalUnit>& KafkaWalParser::update_wals() const {
  return update_wal_list_;
}

const WalContentUnit* KafkaWalParser::insert_wal_list() const {
  return insert_wal_list_;
}

}  // namespace gs

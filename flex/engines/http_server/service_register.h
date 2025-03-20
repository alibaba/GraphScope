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
#ifdef ENABLE_SERVICE_REGISTER

#ifndef FLEX_ENGINES_HTTP_SERVICE_SERVICE_REGISTER_H_
#define FLEX_ENGINES_HTTP_SERVICE_SERVICE_REGISTER_H_

#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

#include "flex/engines/http_server/types.h"
#include "flex/third_party/etcd-cpp-apiv3/etcd/Client.hpp"
#include "flex/third_party/etcd-cpp-apiv3/etcd/KeepAlive.hpp"
#include "flex/third_party/etcd-cpp-apiv3/etcd/v3/Transaction.hpp"
#include "flex/utils/result.h"

#include "flex/third_party/etcd-cpp-apiv3/etcd/v3/V3Response.hpp"

#include <glog/logging.h>

namespace server {

struct ServiceMetrics {
  std::string snapshot_id;
  ServiceMetrics() = default;
  ServiceMetrics(const std::string& snapshot_id) : snapshot_id(snapshot_id) {}

  inline std::string to_string() const { return "\"snapshot_id\": \"" + snapshot_id + "\""; }
};

struct ServiceRegisterPayload {
  std::string endpoint;    // ip:port
  ServiceMetrics metrics;  // service metrics

  ServiceRegisterPayload() = default;
  ServiceRegisterPayload(const std::string& endpoint, const ServiceMetrics& metrics)
      : endpoint(endpoint), metrics(metrics) {}

  std::string to_string() const {
    return "{\"endpoint\": \"" + endpoint + "\", \"metrics\": {" + metrics.to_string() + "}}";
  }
};

struct AllServiceRegisterPayload {
  std::unordered_map<std::string, ServiceRegisterPayload>
      services;  // service name to service payload
  std::string graph_id;

  std::string to_string() const {
    std::string res = "{";
    for (const auto& [name, payload] : services) {
      res += "\"" + name + "\": " + payload.to_string() + ", ";
    }
    if (!services.empty()) {
      res.pop_back();
    }
    res += "}";
    return res;
  }
};

#define INSERT_OR_UPDATE_ETCD_KEY_VALUE(client, key, value, lease_id, retry)                     \
  {                                                                                              \
    int _retry = retry;                                                                          \
    while (_retry-- > 0) {                                                                       \
      auto _resp = client->put(key, value, lease_id);                                            \
      if (_resp.is_ok()) {                                                                       \
        return gs::Status::OK();                                                                 \
      } else {                                                                                   \
        continue;                                                                                \
      }                                                                                          \
    }                                                                                            \
    LOG(ERROR) << "Failed to insert or update key: " << key;                                     \
    return gs::Status(gs::StatusCode::INTERNAL_ERROR, "Failed to insert or update key: " + key); \
  }

#define INSERT_IF_ETCD_KEY_VALUE(client, key, value, lease_id, retry)                  \
  {                                                                                    \
    int _retry = retry;                                                                \
    while (_retry-- > 0) {                                                             \
      auto _resp = client->add(key, value, lease_id);                                  \
      if (_resp.is_ok()) {                                                             \
        return gs::Status::OK();                                                       \
      } else {                                                                         \
        continue;                                                                      \
      }                                                                                \
    }                                                                                  \
    LOG(ERROR) << "Failed to insert key: " << key;                                     \
    return gs::Status(gs::StatusCode::INTERNAL_ERROR, "Failed to insert key: " + key); \
  }

/**
 * A wapper of a thread that periodically register the service to master.
 */
class ServiceRegister {
 public:
  static constexpr const char* PRIMARY_SUFFIX = "primary";
  static constexpr const char* SERVICE_NAME = "service";
  static constexpr const int32_t MAX_RETRY = 5;
  ServiceRegister(const std::string& etcd_endpoint, const std::string& namespace_,
                  const std::string& instance_name,
                  std::function<std::pair<bool, AllServiceRegisterPayload>()> get_service_info,
                  int interval_seconds = 10)
      : etcd_endpoint_(etcd_endpoint),
        namespace_(namespace_),
        instance_name_(instance_name),
        interval_seconds_(interval_seconds),
        ttl_seconds_(interval_seconds_ + 1),
        get_service_info_(get_service_info),
        lease_id_(0) {}

  ~ServiceRegister() { Stop(); }

  /**
   * Start the service register thread.
   */
  void Start();

  void Stop();

 private:
  void register_service();

  // Should align with service_registry.py
  inline std::string get_service_instance_list_key(const std::string& service_name,
                                                   const std::string& endpoint,
                                                   const std::string& graph_id) {
    return "/" + namespace_ + "/" + instance_name_ + "/" + std::string(SERVICE_NAME) + "/" +
           graph_id + "/" + service_name + "/" + endpoint;
  }

  inline std::string get_service_primary_key(const std::string& service_name,
                                             const std::string& graph_id) {
    return "/" + namespace_ + "/" + instance_name_ + "/" + std::string(SERVICE_NAME) + "/" +
           graph_id + "/" + service_name + "/" + PRIMARY_SUFFIX;
  }

  gs::Status insert_to_instance_list(const std::string& key, const std::string& value);

  gs::Status insert_to_primary(const std::string& key, const std::string& value);

  std::string etcd_endpoint_;
  std::string namespace_;
  std::string instance_name_;
  int interval_seconds_;
  int ttl_seconds_;  // considering the network latency, the ttl should be a bit
                     // larger than the interval_seconds
  std::atomic<bool> running_{false};
  std::function<std::pair<bool, AllServiceRegisterPayload>()> get_service_info_;
  std::function<void(std::exception_ptr)> handler_;

  // A thread periodically wakeup and register the service itself to master.
  std::unique_ptr<std::thread> service_register_thread_;
  std::unique_ptr<etcd::SyncClient> client_;

  std::mutex mutex_;
  std::condition_variable cv_;
  int64_t lease_id_;
  std::unique_ptr<etcd::KeepAlive> keep_alive_;
};

}  // namespace server

#endif  // FLEX_ENGINES_HTTP_SERVICE_SERVICE_REGISTER_H_

#endif  // ENABLE_SERVICE_REGISTER
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

#include "flex/engines/http_server/service_register.h"

namespace server {

void ServiceRegister::Start() {
  if (service_register_thread_) {
    LOG(ERROR) << "ServiceRegister is already started";
    return;
  }
  // Expect the path is like http://ip:port
  VLOG(10) << "ETCD base URI: " << etcd_endpoint_;
  if (etcd_endpoint_.empty()) {
    LOG(FATAL) << "Invalid etcd endpoint: " << etcd_endpoint_;
  }
  client_ = std::make_unique<etcd::SyncClient>(etcd_endpoint_);
  running_.store(2);

  service_register_thread_ = std::make_unique<std::thread>([this]() {
    while (running_.load(std::memory_order_relaxed)) {
      {
        std::unique_lock<std::mutex> lock(mutex_);
        // TODO: consider cancel keepAlive when service is stopped
        cv_.wait_for(lock, std::chrono::seconds(interval_seconds_));
        register_service();
      }
    }
  });
  auto _resp = client_->leasegrant(ttl_seconds_);
  if (!_resp.is_ok()) {
    LOG(ERROR) << "Failed to grant lease: " << _resp.error_message();
    return;
  }
  lease_id_ = _resp.value().lease();
  handler_ = [](std::exception_ptr eptr) {
    try {
      if (eptr) {
        std::rethrow_exception(eptr);
      }
    } catch (const std::runtime_error& e) {
      LOG(ERROR) << "Keep alive error: " << e.what();
    } catch (const std::out_of_range& e) {
      LOG(ERROR) << "Lease expiry \"" << e.what();
    }
  };
  keep_alive_ = std::make_unique<etcd::KeepAlive>(client_.get(), handler_,
                                                  interval_seconds_, lease_id_);
  LOG(INFO) << "ServiceRegister started, lease id: " << lease_id_;
}

void ServiceRegister::Stop() {
  running_.store(0);
  // use condition variable to wake up the thread
  if (service_register_thread_) {
    cv_.notify_all();
    service_register_thread_->join();
    service_register_thread_.reset();
  }
}

void ServiceRegister::register_service() {
  auto service_info = get_service_info_();
  LOG(INFO) << "Start to register service: " << service_info.second.to_string();
  if (!service_info.first) {
    return;
  }
  for (auto& [service_name, service_payload] : service_info.second.services) {
    auto instance_key = get_service_instance_list_key(
        service_name, service_payload.endpoint, service_info.second.graph_id);
    auto service_payload_string = service_payload.to_string();
    auto primary_key =
        get_service_primary_key(service_name, service_info.second.graph_id);
    // For instance key-value, insert or update
    if (!insert_to_instance_list(instance_key, service_payload_string).ok()) {
      LOG(ERROR) << "Failed to insert to instance list: " << instance_key;
    }
    // For primary key-value, insert if it not exists
    if (!insert_to_primary(primary_key, service_payload_string).ok()) {
      LOG(ERROR) << "Failed to insert to primary: " << primary_key;
    }
  }
}

gs::Status ServiceRegister::insert_to_instance_list(const std::string& key,
                                                    const std::string& value) {
  LOG(INFO) << "Insert to instance list: " << key << ", value: " << value;
  INSERT_OR_UPDATE_ETCD_KEY_VALUE(client_, key, value, lease_id_, MAX_RETRY);
  return gs::Status::OK();
}

gs::Status ServiceRegister::insert_to_primary(const std::string& key,
                                              const std::string& value) {
  auto resp = client_->get(key);
  if (resp.is_ok() && !resp.value().as_string().empty()) {
    LOG(INFO) << "Primary key already exists: " << key << ", value: " << value;
    return gs::Status::OK();
  }
  LOG(INFO) << "Insert to primary: " << key << ", value: " << value;
  INSERT_IF_ETCD_KEY_VALUE(client_, key, value, lease_id_, MAX_RETRY);
  return gs::Status::OK();
}

}  // namespace server

#endif

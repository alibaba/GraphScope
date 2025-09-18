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
#include "flex/utils/service_utils.h"

namespace std {
std::string to_string(const etcd::Event::EventType event_type) {
  switch (event_type) {
  case etcd::Event::EventType::PUT:
    return "PUT";
  case etcd::Event::EventType::DELETE_:
    return "DELETE";
  case etcd::Event::EventType::INVALID:
    return "INVALID";
  default:
    return "INVALID";
  }
}

}  // namespace std

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

  init_lease();
  init_register_thread();
  init_election_thread();
}

void ServiceRegister::Stop() {
  if (running_.load() == 0) {
    return;
  }
  running_.store(0);
  // use condition variable to wake up the thread
  if (service_register_thread_) {
    cv_.notify_all();
    service_register_thread_->join();
    service_register_thread_.reset();
  }
  if (watcher_) {
    watcher_->Cancel();
    watcher_.reset();
  }
  if (election_thread_) {
    election_thread_->join();
    election_thread_.reset();
  }
  if (keep_alive_) {
    keep_alive_->Cancel();
    keep_alive_.reset();
  }
  if (client_) {
    client_.reset();
  }
  LOG(INFO) << "ServiceRegister stopped";
}

void ServiceRegister::init_lease() {
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

void ServiceRegister::init_register_thread() {
  service_register_thread_ = std::make_unique<std::thread>([this]() {
    while (running_.load(std::memory_order_relaxed)) {
      {
        std::unique_lock<std::mutex> lock(mutex_);
        // TODO: consider cancel keepAlive when service is stopped
        cv_.wait_for(lock, std::chrono::seconds(interval_seconds_));
        auto service_info = get_service_info_();
        if (!service_info.first) {
          continue;
        }
        LOG(INFO) << "Start to register service: "
                  << service_info.second.to_string();
        for (auto& [service_name, service_payload] :
             service_info.second.services) {
          auto instance_key = get_service_instance_list_key(
              service_name, service_payload.endpoint,
              service_info.second.graph_id);
          auto service_payload_string = service_payload.to_string();
          // For instance key-value, insert or update
          if (!insert_to_instance_list(instance_key, service_payload_string)
                   .ok()) {
            LOG(ERROR) << "Failed to insert to instance list: " << instance_key;
          }
        }
      }
    }
  });
}

void ServiceRegister::init_election_thread() {
  election_thread_ = std::make_unique<std::thread>([this]() {
    std::pair<bool, AllServiceRegisterPayload> service_info;
    while (true) {
      service_info = get_service_info_();
      if (service_info.first) {
        break;
      }
      LOG(INFO) << "In initial election thread, service info is not ready";
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (add_primary_until_success()) {
      is_primary_.store(true);
    }
    LOG(INFO) << "Start to watch primary key: "
              << get_service_primary_key(service_info.second.graph_id);

    watcher_ = std::make_unique<etcd::Watcher>(
        client_.get(), get_service_primary_key(service_info.second.graph_id),
        [&, graph_id = service_info.second.graph_id](etcd::Response resp) {
          if (!resp.is_ok()) {
            LOG(ERROR) << "Failed to watch primary key: "
                       << resp.error_message();
            std::this_thread::sleep_for(std::chrono::seconds(1));
          } else {
            if (resp.action() == "delete") {
              LOG(INFO) << "Got delete events size: " << resp.events().size();
              for (auto& event : resp.events()) {
                process_delete_events(event, graph_id);
              }
            } else {
              LOG(INFO) << "Got action: " << resp.action() << ", just skip";
            }
          }
        },
        false);
  });
}

void ServiceRegister::process_delete_events(const etcd::Event& event,
                                            const std::string graph_id) {
  if (event.event_type() != etcd::Event::EventType::DELETE_) {
    LOG(ERROR) << "Expect delete event, bot got: "
               << std::to_string(event.event_type());
    return;
  }
  auto primary_key = get_service_primary_key(graph_id);
  if (!event.has_kv()) {
    LOG(ERROR) << "Event has no kv: ";
    return;
  }
  auto kv = event.kv();
  if (kv.key() == primary_key) {
    // If somehow myself is primary, then try to add primary key again
    if (is_primary_.load()) {
      is_primary_.store(false);
    }
    if (add_primary_until_success()) {
      LOG(INFO) << "Successfully add primary key after delete event: "
                << kv.key();
      is_primary_.store(true);
    } else {
      LOG(INFO) << "Failed to add primary key after delete event: " << kv.key()
                << ", maybe other node is primary";
    }
  } else {
    LOG(INFO) << "Unknown delete event, key: " << kv.key()
              << ", primary key: " << primary_key;
  }
}

bool ServiceRegister::add_primary_until_success() {
  auto ip = gs::get_local_ip();
  auto service_info = get_service_info_();
  if (!service_info.first) {
    LOG(INFO) << "Service info is not ready, skip add primary";
    return false;
  }
  auto primary_key = get_service_primary_key(service_info.second.graph_id);
  auto retry = MAX_RETRY;
  LOG(INFO) << "Try to add primary key for service: " << primary_key;
  while (retry > 0) {
    auto get_resp = client_->get(primary_key);
    if (get_resp.is_ok() && !get_resp.value().as_string().empty()) {
      LOG(INFO) << "Primary key already exists: " << primary_key;
      return false;
    }
    LOG(INFO) << "Try lock: " << primary_key;
    auto lock = client_->lock_with_lease(primary_key, lease_id_);
    if (lock.is_ok()) {
      auto add_resp = client_->add(primary_key, ip, lease_id_);
      if (add_resp.is_ok()) {
        LOG(INFO) << "Add primary key success: " << primary_key;
        return true;
      }
      LOG(ERROR) << "Failed to add primary key: " << primary_key;
    } else {
      LOG(ERROR) << "Failed to lock primary key: " << primary_key;
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
    retry--;
  }
  LOG(ERROR) << "Max retry reached, failed to add primary key: " << primary_key;
  return false;
}

gs::Status ServiceRegister::insert_to_instance_list(const std::string& key,
                                                    const std::string& value) {
  LOG(INFO) << "Insert to instance list: " << key << ", value: " << value;
  INSERT_OR_UPDATE_ETCD_KEY_VALUE(client_, key, value, lease_id_, MAX_RETRY);
  return gs::Status::OK();
}

}  // namespace server

#endif

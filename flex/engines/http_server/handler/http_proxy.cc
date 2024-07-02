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

#include "flex/engines/http_server/handler/http_proxy.h"
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"

#include <seastar/core/when_all.hh>

namespace server {

HeartBeatChecker::HeartBeatChecker(
    std::vector<httplib::Client>& clients,
    const std::vector<std::pair<std::string, uint16_t>>& endpoints,
    int32_t heart_beat_interval)
    : running_(false),
      heart_beat_interval_(DEFAULT_HEART_BEAT_INTERVAL),
      clients_(clients),
      endpoints_(endpoints) {
  endpoint_status_.resize(clients.size(), true);
}

HeartBeatChecker::~HeartBeatChecker() {
  if (running_) {
    stop();
  }
}

gs::Status HeartBeatChecker::start() {
  running_ = true;
  heartbeat_thread_ = std::thread(&HeartBeatChecker::check_heartbeat, this);
  VLOG(10) << "HeartBeatChecker started";
  return gs::Status::OK();
}

gs::Status HeartBeatChecker::stop() {
  running_ = false;
  VLOG(10) << "Stopping HeartBeatChecker";
  while (!heartbeat_thread_.joinable()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  heartbeat_thread_.join();
  VLOG(10) << "HeartBeatChecker stopped";
  return gs::Status::OK();
}

void HeartBeatChecker::check_heartbeat() {
  while (running_) {
    for (size_t i = 0; i < clients_.size(); ++i) {
      httplib::Client client(endpoints_[i].first, endpoints_[i].second);
      auto res = client.Get("/");
      if (!res) {
        LOG(ERROR) << "Failed to connect to endpoint at index: " << i;
        endpoint_status_[i] = false;
      } else {
        VLOG(10) << "Heartbeat check to " << i << " is OK";
        endpoint_status_[i] = true;
      }
    }
    std::this_thread::sleep_for(std::chrono::seconds(heart_beat_interval_));
  }
}

const std::vector<bool>& HeartBeatChecker::get_endpoint_status() const {
  return endpoint_status_;
}

// Utils functions

HttpForwardingResponse to_response(const httplib::Result& res) {
  if (res.error() != httplib::Error::Success) {
    LOG(ERROR) << "Failed to send request: " << res.error();
    return std::make_pair(-1, httplib::to_string(res.error()));
  }
  return std::make_pair(res->status, res->body);
}

// std::multimap<std::string, std::string, httplib::detail::ci>;
httplib::Headers to_httplib_headers(const seastar_http_headers_t& headers) {
  httplib::Headers httplib_headers;
  for (auto& header : headers) {
    // Those headers should not be forwarded, otherwise will cause error.
    if (header.first == "Host" || header.first == "User-Agent" ||
        header.first == "Content-Length") {
      continue;
    }
    httplib_headers.emplace(std::string(header.first.c_str()),
                            std::string(header.second.c_str()));
  }
  return httplib_headers;
}

HttpProxy::HttpProxy() : initialized_(false), enable_heart_beat_check_(false) {}

HttpProxy::~HttpProxy() { close(); }

void HttpProxy::close() {
  if (initialized_) {
    if (heartbeat_checker_) {
      heartbeat_checker_->stop();
    }
    for (auto& client : clients_) {
      client.stop();
    }
    initialized_ = false;
  }
}

gs::Status HttpProxy::init(
    const std::vector<std::pair<std::string, uint16_t>>& endpoints,
    bool enable_heart_beat_check, int32_t heart_beat_interval,
    bool hang_until_success) {
  enable_heart_beat_check_ = enable_heart_beat_check;
  hang_until_success_ = hang_until_success;
  endpoints_ = endpoints;
  if (endpoints_.empty()) {
    return gs::Status(gs::StatusCode::InValidArgument, "No endpoint provided");
  }
  // TODO: check connection to endpoint, if not connected, return error
  clients_.reserve(endpoints_.size());
  for (auto& endpoint : endpoints_) {
    httplib::Client client(endpoint.first, endpoint.second);
    client.set_connection_timeout(CONNECTION_TIMEOUT, 0);  // 5s
    client.set_read_timeout(READ_TIMEOUT, 0);              // 10s
    client.set_write_timeout(WRITE_TIMEOUT, 0);            // 10s
    clients_.emplace_back(std::move(client));
  }
  // test connection
  for (auto& client : clients_) {
    auto res = client.Get("/heartbeat");
    if (!res) {
      return gs::Status(gs::StatusCode::InternalError,
                        "Failed to connect to endpoint");
    }
  }
  // start heart beat check
  if (enable_heart_beat_check_) {
    heartbeat_checker_ =
        std::make_unique<HeartBeatChecker>(clients_, endpoints_);
    RETURN_IF_NOT_OK(heartbeat_checker_->start());
  }
  initialized_ = true;
  return gs::Status::OK();
}

seastar::future<gs::Result<HttpForwardingResponses>> HttpProxy::forward_request(
    const std::string& path, const std::string& method, const std::string& body,
    const seastar_http_headers_t& headers) {
  LOG(INFO) << "Forwarding request to " << path << ", method: " << method
            << ", body: " << body << ", headers: " << headers.size();
  if (!initialized_) {
    return seastar::make_ready_future<gs::Result<HttpForwardingResponses>>(
        HttpForwardingResponses{});
  }
  // std::vector<seastar::future<HttpForwardingResponse>> reply_futs;
  // Get the status of the endpoints from last heartbeat check
  {
    bool all_endpoints_ready = true;
    if (heartbeat_checker_) {
      const auto& endpoint_status = heartbeat_checker_->get_endpoint_status();
      // First check if all the endpoints
      for (size_t i = 0; i < clients_.size(); ++i) {
        if (!endpoint_status[i]) {
          LOG(WARNING) << "Endpoint at index " << i << " is not available";
          all_endpoints_ready = false;
        }
      }
    }
    if (!all_endpoints_ready) {
      // TODO: add results to indicate the endpoint is not available
      return seastar::make_ready_future<gs::Result<HttpForwardingResponses>>(
          HttpForwardingResponses{});
    }
  }
  // HttpForwardingResponses replies;
  // First send to client 0 and then send to client 1
  return do_send_requests(path, method, body, headers, clients_)
      .then_wrapped([](seastar::future<HttpForwardingResponses>&& fut) {
        try {
          auto responses = fut.get();
          return gs::Result<HttpForwardingResponses>(std::move(responses));
        } catch (const std::exception& e) {
          return gs::Result<HttpForwardingResponses>(
              gs::Status(gs::StatusCode::InternalError, e.what()));
        }
      });
}

seastar::future<HttpForwardingResponses> HttpProxy::do_send_request(
    const std::string& path, const std::string& method, const std::string& body,
    const seastar_http_headers_t& headers,
    std::vector<httplib::Client>& clients, size_t ind,
    HttpForwardingResponses&& responses) {
  if (ind >= clients.size()) {
    return seastar::make_ready_future<HttpForwardingResponses>(
        std::move(responses));
  }

  if (method != "GET" && method != "POST" && method != "DELETE" &&
      method != "PUT") {
    LOG(ERROR) << "Unsupported method: " << method;
    return seastar::make_exception_future<HttpForwardingResponses>(
        std::runtime_error("Unsupported method: " + method));
  }

  HttpForwardingResponse response;

  auto lambda = [this, &path, &method, &body, &headers, &clients, ind,
                 &responses]() {
    if (method == "GET") {
      VLOG(10) << "Forwarding GET request to " << path;
      return to_response(
          clients[ind].Get(path.c_str(), to_httplib_headers(headers)));
    } else if (method == "POST") {
      return to_response(clients[ind].Post(
          path.c_str(), to_httplib_headers(headers), body, "application/json"));
    } else if (method == "DELETE") {
      return to_response(
          clients[ind].Delete(path.c_str(), to_httplib_headers(headers)));
    } else {  // must be put
      return to_response(clients[ind].Put(
          path.c_str(), to_httplib_headers(headers), body, "application/json"));
    }
  };

  if (hang_until_success_) {
    while (true) {
      response = lambda();
      if (response.first == 200) {
        responses.emplace_back(std::move(response));
        break;
      } else {
        LOG(ERROR) << "Failed to send request to endpoint at index " << ind
                   << ", status: " << response.first
                   << ", msg: " << response.second;
        if (response.first == 404) {
          LOG(ERROR) << "Endpoint not found, skip it";
          responses.emplace_back(std::move(response));
          break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(3));
      }
    }
  } else {
    response = lambda();
    responses.emplace_back(std::move(response));
  }
  return do_send_request(path, method, body, headers, clients, ind + 1,
                         std::move(responses));
}
seastar::future<HttpForwardingResponses> HttpProxy::do_send_requests(
    const std::string& path, const std::string& method, const std::string& body,
    const seastar_http_headers_t& headers,
    std::vector<httplib::Client>& clients) {
  HttpForwardingResponses responses;
  return do_send_request(path, method, body, headers, clients, 0,
                         std::move(responses));
}

}  // namespace server
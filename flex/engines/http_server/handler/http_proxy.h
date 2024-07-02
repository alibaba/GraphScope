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
#ifndef ENGINES_HTTP_SERVER_HANDLER_FORWARD_HTTP_CLIENT_H_
#define ENGINES_HTTP_SERVER_HANDLER_FORWARD_HTTP_CLIENT_H_

#include <thread>

#include "flex/third_party/httplib.h"
#include "flex/utils/result.h"

#include <seastar/http/common.hh>
#include <seastar/http/httpd.hh>

namespace server {

class HeartBeatChecker {
 public:
  static constexpr int32_t DEFAULT_HEART_BEAT_INTERVAL = 2;  // 2s
  HeartBeatChecker(
      std::vector<httplib::Client>& clients,
      const std::vector<std::pair<std::string, uint16_t>>& endpoints,
      int32_t heart_beat_interval = DEFAULT_HEART_BEAT_INTERVAL);
  ~HeartBeatChecker();

  gs::Status start();

  gs::Status stop();

  const std::vector<bool>& get_endpoint_status() const;

 private:
  void check_heartbeat();

  std::atomic<bool> running_;
  int32_t heart_beat_interval_;
  std::vector<httplib::Client>& clients_;
  const std::vector<std::pair<std::string, uint16_t>>& endpoints_;
  std::vector<bool> endpoint_status_;  // to mark whether the endpoint is alive
  std::thread heartbeat_thread_;
};

using HttpForwardingResponse = std::pair<int32_t, std::string>;
using HttpForwardingResponses = std::vector<HttpForwardingResponse>;
using seastar_http_headers_t =
    std::unordered_map<seastar::sstring, seastar::sstring,
                       seastar::httpd::request::case_insensitive_hash,
                       seastar::httpd::request::case_insensitive_cmp>;

// A wrapped http client which will send request to multiple endpoints and
// return the summary of the responses.
// It will do heartbeat check to the endpoints to make sure the endpoints are
// available.
// Currently, we don't distinguish the read/write requests, we just
// send the request to all the endpoints.
class HttpProxy {
 public:
  static constexpr int32_t CONNECTION_TIMEOUT = 5;  // 5s
  static constexpr int32_t READ_TIMEOUT = 30;       // 5s
  static constexpr int32_t WRITE_TIMEOUT = 30;      // 10s
  HttpProxy();
  ~HttpProxy();

  gs::Status init(
      const std::vector<std::pair<std::string, uint16_t>>& endpoints,
      bool enable_heart_beat_check = false,
      int32_t heart_beat_interval =
          HeartBeatChecker::DEFAULT_HEART_BEAT_INTERVAL,
      bool hang_until_success = true);

  void close();

  seastar::future<gs::Result<HttpForwardingResponses>> forward_request(
      const std::string& path, const std::string& method,
      const std::string& body, const seastar_http_headers_t& headers);

 private:
  seastar::future<HttpForwardingResponses> do_send_request(
      const std::string& path, const std::string& method,
      const std::string& body, const seastar_http_headers_t& headers,
      std::vector<httplib::Client>& clients, size_t ind,
      HttpForwardingResponses&& responses);

  seastar::future<HttpForwardingResponses> do_send_requests(
      const std::string& path, const std::string& method,
      const std::string& body, const seastar_http_headers_t& headers,
      std::vector<httplib::Client>& clients);

  std::atomic<bool> initialized_;
  bool enable_heart_beat_check_;
  bool hang_until_success_;
  std::vector<std::pair<std::string, uint16_t>> endpoints_;  // ip and ports

  std::vector<httplib::Client> clients_;

  std::unique_ptr<HeartBeatChecker> heartbeat_checker_;
};

}  // namespace server

#endif  // ENGINES_HTTP_SERVER_HANDLER_FORWARD_HTTP_CLIENT_H_

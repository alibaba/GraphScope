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

#include "flex/engines/http_server/actor/proxy_actor.act.h"
#include "flex/engines/http_server/service/proxy_service.h"

#include "nlohmann/json.hpp"

#include <seastar/core/print.hh>

namespace server {

proxy_actor::~proxy_actor() {
  // finalization
  // ...
}

proxy_actor::proxy_actor(hiactor::actor_base* exec_ctx,
                         const hiactor::byte_t* addr)
    : hiactor::actor(exec_ctx, addr) {
  set_max_concurrency(1);  // set max concurrency for task reentrancy (stateful)
  // initialization
  // ...
}

seastar::future<proxy_query_result> proxy_actor::do_query(
    proxy_request&& request_payload) {
  auto& request = request_payload.content;
  VLOG(10) << "proxy_actor::forward_request, method: " << request->_method
           << ", path: " << request->_url << ", query: " << request->content;

  // recover the old url with paramters in request
  auto& proxy_service = ProxyService::get();
  auto& client = proxy_service.get_client();
  return client
      .forward_request(request->_url, request->_method, request->content,
                       request->_headers)
      .then([&proxy_service](gs::Result<HttpForwardingResponses>&& result) {
        if (!result.ok()) {
          return seastar::make_ready_future<proxy_query_result>(
              proxy_query_result{(result.status())});
        }
        auto& content = result.value();
        if (content.size() == 0) {
          return seastar::make_exception_future<proxy_query_result>(
              std::runtime_error("Got no responses when forwarding request "
                                 "to interactive servers."));
        }
        // Check all responses are ok, if not ok, return error
        seastar::sstring res_string;
        size_t error_count = 0;
        for (size_t i = 0; i < content.size(); ++i) {
          auto& response = content[i];
          if (response.first != 200) {
            error_count++;
          }
        }
        if (error_count == 0) {
          res_string = content[0].second;
          return seastar::make_ready_future<proxy_query_result>(
              proxy_query_result{std::move(res_string)});
        } else {
          res_string =
              "Got error response when forwarding request "
              "to interactive servers, error count: " +
              std::to_string(error_count) + "\n";
          for (size_t i = 0; i < content.size(); ++i) {
            auto& response = content[i];
            if (response.first != 200) {
              LOG(ERROR) << "Got error response when forwarding request "
                            "to interactive servers at index: "
                         << std::to_string(i) << ", endpoint: "
                         << proxy_service.get_endpoints()[i].first + ":"
                         << std::to_string(
                                proxy_service.get_endpoints()[i].second)
                         << std::to_string(response.first) + ", msg:"
                         << response.second;
              std::string tmp =
                  "Got error response when forwarding request "
                  "to interactive servers at index: " +
                  std::to_string(i) +
                  ", endpoint: " + proxy_service.get_endpoints()[i].first +
                  ":" +
                  std::to_string(proxy_service.get_endpoints()[i].second) +
                  ", code: " + std::to_string(response.first) +
                  ", msg: " + response.second + "\n";
              res_string += tmp;
            }
          }
          return seastar::make_ready_future<proxy_query_result>(
              proxy_query_result{gs::Result<seastar::sstring>(gs::Status(
                  gs::StatusCode::QueryFailed, std::move(res_string)))});
        }
      });
}

}  // namespace server

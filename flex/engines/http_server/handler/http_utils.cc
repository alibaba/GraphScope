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

#include "flex/engines/http_server/handler/http_utils.h"

namespace server {

seastar::future<std::unique_ptr<seastar::httpd::reply>> new_bad_request_reply(
    std::unique_ptr<seastar::httpd::reply> rep, const std::string& msg) {
  rep->set_status(seastar::httpd::reply::status_type::bad_request);
  rep->set_content_type("application/json");
  gs::Status status = gs::Status(gs::StatusCode::BAD_REQUEST, msg);
  rep->write_body("json", seastar::sstring(msg));
  rep->done();
  return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(
      std::move(rep));
}

seastar::future<std::unique_ptr<seastar::httpd::reply>>
new_internal_error_reply(std::unique_ptr<seastar::httpd::reply> rep,
                         const std::string& msg) {
  rep->set_status(seastar::httpd::reply::status_type::bad_request);
  rep->set_content_type("application/json");
  gs::Status status = gs::Status(gs::StatusCode::INTERNAL_ERROR, msg);
  rep->write_body("json", seastar::sstring(status.ToString()));
  rep->done();
  return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(
      std::move(rep));
}

seastar::httpd::reply::status_type status_code_to_http_code(
    gs::StatusCode code) {
  switch (code) {
  case gs::StatusCode::OK:
    return seastar::httpd::reply::status_type::ok;
  case gs::StatusCode::INVALID_ARGUMENT:
    return seastar::httpd::reply::status_type::bad_request;
  case gs::StatusCode::ALREADY_EXISTS:
    return seastar::httpd::reply::status_type::conflict;
  case gs::StatusCode::ALREADY_LOCKED:
    return seastar::httpd::reply::status_type::bad_request;
  case gs::StatusCode::NOT_FOUND:
    return seastar::httpd::reply::status_type::not_found;
  case gs::StatusCode::CODEGEN_ERROR:
    return seastar::httpd::reply::status_type::internal_server_error;
  case gs::StatusCode::INVALID_SCHEMA:
    return seastar::httpd::reply::status_type::bad_request;
  case gs::StatusCode::PERMISSION_DENIED:
    return seastar::httpd::reply::status_type::forbidden;
  case gs::StatusCode::ILLEGAL_OPERATION:
    return seastar::httpd::reply::status_type::bad_request;
  case gs::StatusCode::INTERNAL_ERROR:
    return seastar::httpd::reply::status_type::internal_server_error;
  case gs::StatusCode::INVALID_IMPORT_FILE:
    return seastar::httpd::reply::status_type::bad_request;
  case gs::StatusCode::IO_ERROR:
    return seastar::httpd::reply::status_type::internal_server_error;
  case gs::StatusCode::QUERY_FAILED:
    return seastar::httpd::reply::status_type::internal_server_error;
  default:
    return seastar::httpd::reply::status_type::internal_server_error;
  }
}

seastar::future<std::unique_ptr<seastar::httpd::reply>>
catch_exception_and_return_reply(std::unique_ptr<seastar::httpd::reply> rep,
                                 std::exception_ptr ex) {
  try {
    std::rethrow_exception(ex);
  } catch (std::exception& e) {
    LOG(ERROR) << "Exception: " << e.what();
    seastar::sstring what = e.what();
    rep->set_content_type("application/json");
    // for the exception, we are not sure whether it is a bad request or
    // internal server error
    gs::Status status = gs::Status(gs::StatusCode::UNKNOWN, what);
    rep->write_body("json", seastar::sstring(status.ToString()));
    rep->set_status(seastar::httpd::reply::status_type::internal_server_error);
    rep->done();
    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(
        std::move(rep));
  }
}

seastar::future<std::unique_ptr<seastar::httpd::reply>>
return_reply_with_result(std::unique_ptr<seastar::httpd::reply> rep,
                         seastar::future<admin_query_result>&& fut) {
  if (__builtin_expect(fut.failed(), false)) {
    return catch_exception_and_return_reply(std::move(rep),
                                            fut.get_exception());
  }
  auto&& result = fut.get0();
  auto status_code =
      status_code_to_http_code(result.content.status().error_code());
  rep->set_status(status_code);
  rep->set_content_type("application/json");
  if (status_code == seastar::httpd::reply::status_type::ok) {
    rep->write_body("json", std::move(result.content.value()));
  } else {
    // Expect a json like "{"code": 400, "message": "Bad Request"}"
    rep->write_body("json",
                    seastar::sstring(result.content.status().ToString()));
  }
  rep->done();
  return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(
      std::move(rep));
}

std::string trim_slash(const std::string& origin) {
  std::string res = origin;
  if (res.front() == '/') {
    res.erase(res.begin());
  }
  if (res.back() == '/') {
    res.pop_back();
  }
  return res;
}

}  // namespace server
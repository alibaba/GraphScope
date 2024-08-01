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
#include "flex/engines/http_server/types.h"
#include "flex/utils/result.h"
#include "seastar/http/common.hh"
#include "seastar/http/reply.hh"

#ifndef ENGINES_HTTP_SERVER_HANDLER_HTTP_UTILS_H_
#define ENGINES_HTTP_SERVER_HANDLER_HTTP_UTILS_H_

namespace server {

seastar::future<std::unique_ptr<seastar::httpd::reply>> new_bad_request_reply(
    std::unique_ptr<seastar::httpd::reply> rep, const std::string& msg);

seastar::httpd::reply::status_type status_code_to_http_code(
    gs::StatusCode code);

seastar::future<std::unique_ptr<seastar::httpd::reply>>
catch_exception_and_return_reply(std::unique_ptr<seastar::httpd::reply> rep,
                                 std::exception_ptr ex);

seastar::future<std::unique_ptr<seastar::httpd::reply>>
return_reply_with_result(std::unique_ptr<seastar::httpd::reply> rep,
                         seastar::future<admin_query_result>&& fut);

// To avoid macro conflict between /usr/include/arpa/nameser_compact.h#120(which
// is included by httplib.h) and seastar/http/common.hh#61
static constexpr seastar::httpd::operation_type SEASTAR_DELETE =
    seastar::httpd::operation_type::DELETE;

std::string trim_slash(const std::string& origin);

}  // namespace server

namespace gs {

template <typename T>
struct to_string_impl;

template <>
struct to_string_impl<seastar::sstring> {
  static std::string to_string(const seastar::sstring& t) { return t.c_str(); }
};
}  // namespace gs

#endif  // ENGINES_HTTP_SERVER_HANDLER_HTTP_UTILS_H_

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
#include "seastar/http/reply.hh"

#ifndef ENGINES_HTTP_SERVER_SERVER_UTILS_H_
#define ENGINES_HTTP_SERVER_SERVER_UTILS_H_

namespace server {
seastar::httpd::reply::status_type status_code_to_http_code(
    gs::StatusCode code);

seastar::future<std::unique_ptr<seastar::httpd::reply>>
catch_exception_and_return_reply(std::unique_ptr<seastar::httpd::reply> rep,
                                 std::exception_ptr ex);

seastar::future<std::unique_ptr<seastar::httpd::reply>>
return_reply_with_result(std::unique_ptr<seastar::httpd::reply> rep,
                         admin_query_result&& result);

}  // namespace server

#endif  // ENGINES_HTTP_SERVER_SERVER_UTILS_H_
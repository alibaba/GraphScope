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

#ifndef ENGINES_HTTP_SERVER_ACTOR_PROXY_ACTOR_H_
#define ENGINES_HTTP_SERVER_ACTOR_PROXY_ACTOR_H_


#include "flex/engines/http_server/types.h"

#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>
#include <seastar/http/httpd.hh>

namespace server {

class ANNOTATION(actor:impl) proxy_actor : public hiactor::actor {
 public:
  proxy_actor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr);
  ~proxy_actor() override;

  seastar::future<proxy_query_result> ANNOTATION(actor:method) do_query(proxy_request&& param);

  // DECLARE_RUN_QUERIES;
  /// Declare `do_work` func here, no need to implement.
  ACTOR_DO_WORK()

 private:
  int32_t your_private_members_ = 0;
};
}

#endif  // ENGINES_HTTP_SERVER_ACTOR_PROXY_ACTOR_H_
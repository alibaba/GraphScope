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

#ifndef ENGINES_HTTP_SERVER_HANDLER_GRAPH_DB_HTTP_HANDLER_H_
#define ENGINES_HTTP_SERVER_HANDLER_GRAPH_DB_HTTP_HANDLER_H_

#include "flex/engines/http_server/executor_group.actg.h"
#include "flex/engines/http_server/generated/actor/codegen_actor_ref.act.autogen.h"
#include "flex/engines/http_server/generated/actor/executor_ref.act.autogen.h"

#include <seastar/http/httpd.hh>

namespace server {

class StoppableHandler : public seastar::httpd::handler_base {
 public:
  virtual seastar::future<> stop() = 0;

  virtual bool is_stopped() const = 0;

  virtual bool start() = 0;
};

class graph_db_http_handler {
 public:
  graph_db_http_handler(uint16_t http_port, int32_t shard_num,
                        bool enable_hqps_handlers = false);

  ~graph_db_http_handler();

  void start();
  void stop();

  uint16_t get_port() const;

  bool is_running() const;

  bool is_actors_running() const;

  seastar::future<> stop_query_actors();

  void start_query_actors();

 private:
  seastar::future<> set_routes();

 private:
  const uint16_t http_port_;
  seastar::httpd::http_server_control server_;

  std::atomic<bool> enable_hqps_handlers_{false}, running_{false},
      actors_running_{false};

  std::vector<StoppableHandler*> graph_db_handlers_;
  std::vector<StoppableHandler*> ic_handlers_;
  std::vector<StoppableHandler*> adhoc_query_handlers_;
};

}  // namespace server

#endif  // ENGINES_HTTP_SERVER_HANDLER_GRAPH_DB_HTTP_HANDLER_H_

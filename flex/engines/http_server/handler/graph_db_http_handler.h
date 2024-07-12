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

#define RANDOM_DISPATCHER 1
// when RANDOM_DISPATCHER is false, the dispatcher will use round-robin
// algorithm to dispatch the query to different executors

#include "flex/engines/http_server/executor_group.actg.h"
#include "flex/engines/http_server/generated/actor/codegen_actor_ref.act.autogen.h"
#include "flex/engines/http_server/generated/actor/executor_ref.act.autogen.h"

#if RANDOM_DISPATCHER
#include <random>
#endif

#include <seastar/http/httpd.hh>

class query_dispatcher {
 public:
  query_dispatcher(uint32_t shard_concurrency)
      :
#if RANDOM_DISPATCHER
        rd_(),
        gen_(rd_()),
        dis_(0, shard_concurrency - 1)
#else
        shard_concurrency_(shard_concurrency),
        executor_idx_(0)
#endif  // RANDOM_DISPATCHER
  {
  }

  inline int get_executor_idx() {
#if RANDOM_DISPATCHER
    return dis_(gen_);
#else
    auto idx = executor_idx_;
    executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;
    return idx;
#endif  // RANDOM_DISPATCHER
  }

 private:
#if RANDOM_DISPATCHER
  std::random_device rd_;
  std::mt19937 gen_;
  std::uniform_int_distribution<> dis_;
#else
  int shard_concurrency_;
  int executor_idx_;
#endif
};

#undef RANDOM_DISPATCHER

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

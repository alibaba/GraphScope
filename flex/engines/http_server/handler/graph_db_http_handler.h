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

#include <array>

#include <seastar/http/httpd.hh>

namespace server {

class StoppableHandler : public seastar::httpd::handler_base {
 public:
  static constexpr const char* ACTOR_SCOPE_CANCEL_MESSAGE =
      "Unable to send message";
  StoppableHandler(uint32_t init_group_id, uint32_t max_group_id,
                   uint32_t group_inc_step, uint32_t shard_concurrency)
      : is_cancelled_(false),
        cur_group_id_(init_group_id),
        max_group_id_(max_group_id),
        group_inc_step_(group_inc_step),
        shard_concurrency_(shard_concurrency),
        shard_id_(hiactor::local_shard_id()) {}

  inline bool is_stopped() const { return is_cancelled_; }

  inline uint32_t shard_id() const { return shard_id_; }

  virtual seastar::future<> stop() = 0;
  virtual bool start() = 0;

 protected:
  template <typename FuncT>
  seastar::future<> cancel_scope(FuncT func) {
    if (is_cancelled_) {
      LOG(INFO) << "The current scope has been already cancelled!";
      return seastar::make_ready_future<>();
    }
    hiactor::scope_builder builder;
    builder.set_shard(shard_id_)
        .enter_sub_scope(hiactor::scope<executor_group>(0))
        .enter_sub_scope(hiactor::scope<hiactor::actor_group>(cur_group_id_));
    return hiactor::actor_engine()
        .cancel_scope_request(builder, false)
        .then_wrapped([this, func](auto&& fut) {
          try {
            fut.get();
            LOG(INFO) << "Cancel IC scope successfully!";
            // clear the actor refs
            // executor_refs_.clear();
            is_cancelled_ = true;
          } catch (const std::exception& e) {
            // In case the scope is already cancelled, we should ignore the
            // exception.
            LOG(INFO) << "Failed to cancel IC scope: " << e.what();
          }
          func();
          return seastar::make_ready_future<>();
        });
  }

  template <typename FuncT>
  bool start_scope(FuncT func) {
    VLOG(10) << "Create actors with a different sub scope id: "
             << cur_group_id_;
    if (cur_group_id_ + group_inc_step_ > max_group_id_) {
      LOG(ERROR) << "The max group id is reached, cannot create more actors!";
      return false;
    }
    if (cur_group_id_ + group_inc_step_ < cur_group_id_) {
      LOG(ERROR) << "overflow detected!";
      return false;
    }
    cur_group_id_ += group_inc_step_;
    hiactor::scope_builder builder;
    builder.set_shard(shard_id_)
        .enter_sub_scope(hiactor::scope<executor_group>(0))
        .enter_sub_scope(hiactor::scope<hiactor::actor_group>(cur_group_id_));
    func(builder);
    is_cancelled_ = false;  // locked outside
    return true;
  }

  bool is_cancelled_;
  uint32_t cur_group_id_;
  const uint32_t max_group_id_, group_inc_step_;
  const uint32_t shard_concurrency_;
  const uint32_t shard_id_;
};

class graph_db_http_handler {
 public:
  static constexpr int NUM_OPERATION = 4;  // (PUT/GET/POST/DELETE)
  static constexpr seastar::httpd::operation_type OPERATIONS[NUM_OPERATION] = {
      seastar::httpd::operation_type::PUT, seastar::httpd::operation_type::GET,
      seastar::httpd::operation_type::POST,
      seastar::httpd::operation_type::DELETE};
  graph_db_http_handler(uint16_t http_port, int32_t shard_num,
                        bool enable_adhoc_handlers = false);

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
  seastar::future<> stop_query_actors(size_t index);

 private:
  const uint16_t http_port_;
  seastar::httpd::http_server_control server_;

  std::atomic<bool> enable_adhoc_handlers_{false}, running_{false},
      actors_running_{false};

  // Handles graph queries submitted to /v1/graph/current/query
  std::vector<StoppableHandler*> current_graph_query_handlers_;
  std::vector<StoppableHandler*> all_graph_query_handlers_;
  std::vector<StoppableHandler*> adhoc_query_handlers_;
  // shard_num * operation time(PUT/GET/POST/DELETE)
  std::vector<std::array<StoppableHandler*, NUM_OPERATION>> vertex_handlers_;
  std::vector<std::array<StoppableHandler*, NUM_OPERATION>> edge_handlers_;
};

}  // namespace server

#endif  // ENGINES_HTTP_SERVER_HANDLER_GRAPH_DB_HTTP_HANDLER_H_

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
#endif
  {
  }

  inline int get_executor_idx() {
#if RANDOM_DISPATCHER
    return dis_(gen_);
#else
    auto idx = executor_idx_;
    executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;
    return idx;
#endif
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

class graph_db_ic_handler : public seastar::httpd::handler_base {
 public:
  graph_db_ic_handler(uint32_t group_id, uint32_t shard_concurrency);

  ~graph_db_ic_handler() override = default;

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override;

 private:
  const uint32_t shard_concurrency_;
  query_dispatcher dispatcher_;
  std::vector<executor_ref> executor_refs_;
};

class hqps_ic_handler : public seastar::httpd::handler_base {
 public:
  // extra headers
  static constexpr const char* INTERACTIVE_REQUEST_FORMAT =
      "X-Interactive-Request-Format";
  static constexpr const char* PROTOCOL_FORMAT = "proto";
  static constexpr const char* JSON_FORMAT = "json";
  static constexpr const char* ENCODER_FORMAT = "encoder";
  hqps_ic_handler(uint32_t init_group_id, uint32_t max_group_id,
                  uint32_t group_inc_step, uint32_t shard_concurrency);
  ~hqps_ic_handler() override;

  bool create_actors();

  seastar::future<> cancel_current_scope();

  bool is_current_scope_cancelled() const;

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override;

 private:
  bool is_running_graph(const seastar::sstring& graph_id) const;

  uint32_t cur_group_id_;
  const uint32_t max_group_id_, group_inc_step_;
  const uint32_t shard_concurrency_;
  uint32_t executor_idx_;
  std::vector<executor_ref> executor_refs_;
  bool is_cancelled_;
#ifdef HAVE_OPENTELEMETRY_CPP
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Counter<uint64_t>>
      total_counter_;
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Histogram<double>>
      latency_histogram_;
#endif
};

class hqps_adhoc_query_handler : public seastar::httpd::handler_base {
 public:
  hqps_adhoc_query_handler(uint32_t init_adhoc_group_id,
                           uint32_t init_codegen_group_id,
                           uint32_t max_group_id, uint32_t group_inc_step,
                           uint32_t shard_concurrency);

  ~hqps_adhoc_query_handler() override;

  seastar::future<> cancel_current_scope();

  bool is_current_scope_cancelled() const;

  bool create_actors();

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override;

 private:
  uint32_t cur_adhoc_group_id_, cur_codegen_group_id_;
  const uint32_t max_group_id_, group_inc_step_;
  const uint32_t shard_concurrency_;
  uint32_t executor_idx_;
  std::vector<executor_ref> executor_refs_;
  std::vector<codegen_actor_ref> codegen_actor_refs_;
  bool is_cancelled_;
#ifdef HAVE_OPENTELEMETRY_CPP
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Counter<uint64_t>>
      total_counter_;
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Histogram<double>>
      latency_histogram_;
#endif
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

  std::vector<graph_db_ic_handler*> graph_db_handlers_;
  std::vector<hqps_ic_handler*> ic_handlers_;
  std::vector<hqps_adhoc_query_handler*> adhoc_query_handlers_;
};

}  // namespace server

#endif  // ENGINES_HTTP_SERVER_HANDLER_GRAPH_DB_HTTP_HANDLER_H_

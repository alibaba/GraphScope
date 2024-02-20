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
#include "flex/engines/http_server/executor_group.actg.h"
#include "flex/engines/http_server/options.h"
#include "flex/engines/http_server/service/hqps_service.h"
#include "flex/engines/http_server/types.h"

namespace server {

hqps_ic_handler::hqps_ic_handler(uint32_t init_group_id, uint32_t max_group_id,
                                 uint32_t group_inc_step,
                                 uint32_t shard_concurrency)
    : cur_group_id_(init_group_id),
      max_group_id_(max_group_id),
      group_inc_step_(group_inc_step),
      shard_concurrency_(shard_concurrency),
      executor_idx_(0) {
  executor_refs_.reserve(shard_concurrency_);
  hiactor::scope_builder builder;
  builder.set_shard(hiactor::local_shard_id())
      .enter_sub_scope(hiactor::scope<executor_group>(0))
      .enter_sub_scope(hiactor::scope<hiactor::actor_group>(cur_group_id_));
  for (unsigned i = 0; i < shard_concurrency_; ++i) {
    executor_refs_.emplace_back(builder.build_ref<executor_ref>(i));
  }
}

hqps_ic_handler::~hqps_ic_handler() = default;

seastar::future<> hqps_ic_handler::cancel_current_scope() {
  hiactor::scope_builder builder;
  builder.set_shard(hiactor::local_shard_id())
      .enter_sub_scope(hiactor::scope<executor_group>(0))
      .enter_sub_scope(hiactor::scope<hiactor::actor_group>(cur_group_id_));
  return hiactor::actor_engine()
      .cancel_scope_request(builder, false)
      .then([this] {
        LOG(INFO) << "Cancel IC scope successfully!";
        // clear the actor refs
        executor_refs_.clear();
        return seastar::make_ready_future<>();
      });
}

bool hqps_ic_handler::create_actors() {
  if (executor_refs_.size() > 0) {
    LOG(ERROR) << "The actors have been already created!";
    return false;
  }

  VLOG(10) << "Create actors with a different sub scope id: " << cur_group_id_;
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
  builder.set_shard(hiactor::local_shard_id())
      .enter_sub_scope(hiactor::scope<executor_group>(0))
      .enter_sub_scope(hiactor::scope<hiactor::actor_group>(cur_group_id_));
  for (unsigned i = 0; i < shard_concurrency_; ++i) {
    executor_refs_.emplace_back(builder.build_ref<executor_ref>(i));
  }
  return true;
}

seastar::future<std::unique_ptr<seastar::httpd::reply>> hqps_ic_handler::handle(
    const seastar::sstring& path, std::unique_ptr<seastar::httpd::request> req,
    std::unique_ptr<seastar::httpd::reply> rep) {
  auto dst_executor = executor_idx_;
  executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;
  req->content.append(gs::Schema::HQPS_PROCEDURE_PLUGIN_ID_STR, 1);

  return executor_refs_[dst_executor]
      .run_graph_db_query(query_param{std::move(req->content)})
      .then([](auto&& output) {
        if (output.content.size() < 4) {
          LOG(ERROR) << "Invalid output size: " << output.content.size();
          return seastar::make_ready_future<query_param>(std::move(output));
        }
        return seastar::make_ready_future<query_param>(
            std::move(output.content.substr(4)));
      })
      .then_wrapped(
          [rep = std::move(rep)](seastar::future<query_result>&& fut) mutable {
            if (__builtin_expect(fut.failed(), false)) {
              rep->set_status(
                  seastar::httpd::reply::status_type::internal_server_error);
              try {
                std::rethrow_exception(fut.get_exception());
              } catch (std::exception& e) {
                rep->write_body("bin", seastar::sstring(e.what()));
              }
              rep->done();
              return seastar::make_ready_future<
                  std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
            }
            auto result = fut.get0();
            rep->write_body("bin", std::move(result.content));
            rep->done();
            return seastar::make_ready_future<
                std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
          });
}

// a handler to handle adhoc query.

hqps_adhoc_query_handler::hqps_adhoc_query_handler(
    uint32_t init_adhoc_group_id, uint32_t init_codegen_group_id,
    uint32_t max_group_id, uint32_t group_inc_step, uint32_t shard_concurrency)
    : cur_adhoc_group_id_(init_adhoc_group_id),
      cur_codegen_group_id_(init_codegen_group_id),
      max_group_id_(max_group_id),
      group_inc_step_(group_inc_step),
      shard_concurrency_(shard_concurrency),
      executor_idx_(0) {
  executor_refs_.reserve(shard_concurrency_);
  {
    hiactor::scope_builder builder;
    builder.set_shard(hiactor::local_shard_id())
        .enter_sub_scope(hiactor::scope<executor_group>(0))
        .enter_sub_scope(
            hiactor::scope<hiactor::actor_group>(cur_adhoc_group_id_));
    for (unsigned i = 0; i < shard_concurrency_; ++i) {
      executor_refs_.emplace_back(builder.build_ref<executor_ref>(i));
    }
  }
  {
    hiactor::scope_builder builder;
    builder.set_shard(hiactor::local_shard_id())
        .enter_sub_scope(hiactor::scope<executor_group>(0))
        .enter_sub_scope(
            hiactor::scope<hiactor::actor_group>(cur_codegen_group_id_));
    codegen_actor_refs_.emplace_back(builder.build_ref<codegen_actor_ref>(0));
  }
}
hqps_adhoc_query_handler::~hqps_adhoc_query_handler() = default;

seastar::future<> hqps_adhoc_query_handler::cancel_current_scope() {
  hiactor::scope_builder adhoc_builder;
  adhoc_builder.set_shard(hiactor::local_shard_id())
      .enter_sub_scope(hiactor::scope<executor_group>(0))
      .enter_sub_scope(
          hiactor::scope<hiactor::actor_group>(cur_adhoc_group_id_));
  hiactor::scope_builder codegen_builder;
  codegen_builder.set_shard(hiactor::local_shard_id())
      .enter_sub_scope(hiactor::scope<executor_group>(0))
      .enter_sub_scope(
          hiactor::scope<hiactor::actor_group>(cur_codegen_group_id_));
  return hiactor::actor_engine()
      .cancel_scope_request(adhoc_builder, false)
      .then([codegen_builder] {
        LOG(INFO) << "Cancel adhoc scope successfully!";
        return hiactor::actor_engine().cancel_scope_request(codegen_builder,
                                                            false);
      })
      .then([this] {
        LOG(INFO) << "Cancel codegen scope successfully!";
        // clear the actor refs
        executor_refs_.clear();
        codegen_actor_refs_.clear();
        LOG(INFO) << "Clear actor refs successfully!";
        return seastar::make_ready_future<>();
      });
}

bool hqps_adhoc_query_handler::create_actors() {
  if (executor_refs_.size() > 0 || codegen_actor_refs_.size() > 0) {
    LOG(ERROR) << "The actors have been already created!";
    return false;
  }
  // Check whether cur_adhoc_group_id + group_inc_step_ is larger than
  // max_group_id_, considering overflow
  if (cur_adhoc_group_id_ + group_inc_step_ > max_group_id_ ||
      cur_codegen_group_id_ + group_inc_step_ > max_group_id_) {
    LOG(ERROR) << "The max group id is reached, cannot create more actors!";
    return false;
  }
  if (cur_adhoc_group_id_ + group_inc_step_ < cur_adhoc_group_id_ ||
      cur_codegen_group_id_ + group_inc_step_ < cur_codegen_group_id_) {
    LOG(ERROR) << "overflow detected!";
    return false;
  }

  {
    cur_adhoc_group_id_ += group_inc_step_;
    hiactor::scope_builder builder;
    builder.set_shard(hiactor::local_shard_id())
        .enter_sub_scope(hiactor::scope<executor_group>(0))
        .enter_sub_scope(
            hiactor::scope<hiactor::actor_group>(cur_adhoc_group_id_));
    for (unsigned i = 0; i < shard_concurrency_; ++i) {
      executor_refs_.emplace_back(builder.build_ref<executor_ref>(i));
    }
  }
  {
    cur_codegen_group_id_ += group_inc_step_;
    hiactor::scope_builder builder;
    builder.set_shard(hiactor::local_shard_id())
        .enter_sub_scope(hiactor::scope<executor_group>(0))
        .enter_sub_scope(
            hiactor::scope<hiactor::actor_group>(cur_codegen_group_id_));
    codegen_actor_refs_.emplace_back(builder.build_ref<codegen_actor_ref>(0));
  }
  return true;
}

seastar::future<std::unique_ptr<seastar::httpd::reply>>
hqps_adhoc_query_handler::handle(const seastar::sstring& path,
                                 std::unique_ptr<seastar::httpd::request> req,
                                 std::unique_ptr<seastar::httpd::reply> rep) {
  auto dst_executor = executor_idx_;
  executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;

  return codegen_actor_refs_[0]
      .do_codegen(query_param{std::move(req->content)})
      .then([this, dst_executor](auto&& param) {
        param.content.append(gs::Schema::HQPS_ADHOC_PLUGIN_ID_STR, 1);
        return executor_refs_[dst_executor].run_graph_db_query(
            query_param{std::move(param.content)});
      })
      .then([](auto&& output) {
        if (output.content.size() < 4) {
          LOG(ERROR) << "Invalid output size: " << output.content.size();
          return seastar::make_ready_future<query_param>(std::move(output));
        }
        return seastar::make_ready_future<query_param>(
            std::move(output.content.substr(4)));
      })
      .then_wrapped(
          [rep = std::move(rep)](seastar::future<query_result>&& fut) mutable {
            if (__builtin_expect(fut.failed(), false)) {
              rep->set_status(
                  seastar::httpd::reply::status_type::internal_server_error);
              try {
                std::rethrow_exception(fut.get_exception());
              } catch (std::exception& e) {
                rep->write_body("bin", seastar::sstring(e.what()));
              }
              rep->done();
              return seastar::make_ready_future<
                  std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
            }
            auto result = fut.get0();
            rep->write_body("bin", std::move(result.content));
            rep->done();
            return seastar::make_ready_future<
                std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
          });
}

seastar::future<std::unique_ptr<seastar::httpd::reply>>
hqps_exit_handler::handle(const seastar::sstring& path,
                          std::unique_ptr<seastar::httpd::request> req,
                          std::unique_ptr<seastar::httpd::reply> rep) {
  HQPSService::get().set_exit_state();
  rep->write_body("bin", seastar::sstring{"HQPS service is exiting ..."});
  return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(
      std::move(rep));
}

hqps_http_handler::hqps_http_handler(uint16_t http_port)
    : http_port_(http_port) {
  ic_handler_ = new hqps_ic_handler(ic_query_group_id, max_group_id,
                                    group_inc_step, shard_query_concurrency);
  adhoc_query_handler_ = new hqps_adhoc_query_handler(
      ic_adhoc_group_id, codegen_group_id, max_group_id, group_inc_step,
      shard_adhoc_concurrency);
  exit_handler_ = new hqps_exit_handler();
}

hqps_http_handler::~hqps_http_handler() {
  if (is_running()) {
    stop();
  }
  delete ic_handler_;
  delete adhoc_query_handler_;
  delete exit_handler_;
}

uint16_t hqps_http_handler::get_port() const { return http_port_; }

bool hqps_http_handler::is_running() const { return running_.load(); }

void hqps_http_handler::start() {
  auto fut = seastar::alien::submit_to(
      *seastar::alien::internal::default_instance, 0, [this] {
        return server_.start()
            .then([this] { return set_routes(); })
            .then([this] { return server_.listen(http_port_); })
            .then([this] {
              fmt::print(
                  "HQPS Query http handler is listening on port {} "
                  "...\n",
                  http_port_);
            });
      });
  fut.wait();
  // update running state
  running_.store(true);
}

void hqps_http_handler::stop() {
  auto fut = seastar::alien::submit_to(
      *seastar::alien::internal::default_instance, 0, [this] {
        LOG(INFO) << "Stopping HQPS http handler ...";
        return server_.stop();
      });
  fut.wait();
  // update running state
  running_.store(false);
}

seastar::future<> hqps_http_handler::stop_query_actors() {
  // First cancel the scope.
  return ic_handler_->cancel_current_scope()
      .then([this] {
        LOG(INFO) << "Cancel ic scope";
        return adhoc_query_handler_->cancel_current_scope();
      })
      .then([] {
        LOG(INFO) << "Cancel adhoc scope";
        return seastar::make_ready_future<>();
      });
}

void hqps_http_handler::start_query_actors() {
  ic_handler_->create_actors();
  adhoc_query_handler_->create_actors();
  LOG(INFO) << "Restart all actors";
}

seastar::future<> hqps_http_handler::set_routes() {
  return server_.set_routes([this](seastar::httpd::routes& r) {
    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/interactive/query"), ic_handler_);
    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/v1/query"), ic_handler_);
    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/interactive/adhoc_query"),
          adhoc_query_handler_);
    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/interactive/exit"), exit_handler_);
    return seastar::make_ready_future<>();
  });
}

}  // namespace server

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
#include "flex/engines/http_server/hqps_service.h"
#include "flex/engines/http_server/options.h"

#include <seastar/core/alien.hh>
#include <seastar/core/print.hh>
#include <seastar/http/handlers.hh>
#include "flex/engines/http_server/generated/codegen_actor_ref.act.autogen.h"
#include "flex/engines/http_server/generated/executor_ref.act.autogen.h"
#include "flex/engines/http_server/types.h"

namespace server {

class hqps_ic_handler : public seastar::httpd::handler_base {
 public:
  hqps_ic_handler(uint32_t group_id, uint32_t shard_concurrency)
      : shard_concurrency_(shard_concurrency), executor_idx_(0) {
    executor_refs_.reserve(shard_concurrency_);
    hiactor::scope_builder builder;
    builder.set_shard(hiactor::local_shard_id())
        .enter_sub_scope(hiactor::scope<executor_group>(0))
        .enter_sub_scope(hiactor::scope<hiactor::actor_group>(group_id));
    for (unsigned i = 0; i < shard_concurrency_; ++i) {
      executor_refs_.emplace_back(builder.build_ref<executor_ref>(i));
    }
  }
  ~hqps_ic_handler() override = default;

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    auto dst_executor = executor_idx_;
    executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;

    return executor_refs_[dst_executor]
        .run_hqps_procedure_query(query_param{std::move(req->content)})
        .then_wrapped([rep = std::move(rep)](
                          seastar::future<query_result>&& fut) mutable {
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

 private:
  const uint32_t shard_concurrency_;
  uint32_t executor_idx_;
  std::vector<executor_ref> executor_refs_;
};

// a handler for handl adhoc query.
class hqps_adhoc_query_handler : public seastar::httpd::handler_base {
 public:
  hqps_adhoc_query_handler(uint32_t group_id, uint32_t codegen_actor_group_id,
                           uint32_t shard_concurrency)
      : shard_concurrency_(shard_concurrency), executor_idx_(0) {
    executor_refs_.reserve(shard_concurrency_);
    {
      hiactor::scope_builder builder;
      builder.set_shard(hiactor::local_shard_id())
          .enter_sub_scope(hiactor::scope<executor_group>(0))
          .enter_sub_scope(hiactor::scope<hiactor::actor_group>(group_id));
      for (unsigned i = 0; i < shard_concurrency_; ++i) {
        executor_refs_.emplace_back(builder.build_ref<executor_ref>(i));
      }
    }
    {
      hiactor::scope_builder builder;
      builder.set_shard(hiactor::local_shard_id())
          .enter_sub_scope(hiactor::scope<executor_group>(0))
          .enter_sub_scope(
              hiactor::scope<hiactor::actor_group>(codegen_actor_group_id));
      codegen_actor_ref_ = builder.build_ref<codegen_actor_ref>(0);
    }
  }
  ~hqps_adhoc_query_handler() override = default;

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    auto dst_executor = executor_idx_;
    executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;

    return codegen_actor_ref_.do_codegen(query_param{std::move(req->content)})
        .then([this, dst_executor](auto&& param) {
          return executor_refs_[dst_executor].run_hqps_adhoc_query(
              std::move(param));
        })
        .then_wrapped([rep = std::move(rep)](
                          seastar::future<query_result>&& fut) mutable {
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

 private:
  const uint32_t shard_concurrency_;
  uint32_t executor_idx_;
  std::vector<executor_ref> executor_refs_;
  codegen_actor_ref codegen_actor_ref_;
};

class hqps_exit_handler : public seastar::httpd::handler_base {
 public:
  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    HQPSService::get().set_exit_state();
    rep->write_body(
        "bin",
        seastar::sstring{"The ldbc snb interactive service is exiting ..."});
    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(
        std::move(rep));
  }
};

hqps_http_handler::hqps_http_handler(uint16_t http_port)
    : http_port_(http_port) {}

void hqps_http_handler::start() {
  auto fut = seastar::alien::submit_to(
      *seastar::alien::internal::default_instance, 0, [this] {
        return server_.start()
            .then([this] { return set_routes(); })
            .then([this] { return server_.listen(http_port_); })
            .then([this] {
              fmt::print(
                  "Ldbc snb interactive http handler is listening on port {} "
                  "...\n",
                  http_port_);
            });
      });
  fut.wait();
}

void hqps_http_handler::stop() {
  auto fut =
      seastar::alien::submit_to(*seastar::alien::internal::default_instance, 0,
                                [this] { return server_.stop(); });
  fut.wait();
}

seastar::future<> hqps_http_handler::set_routes() {
  return server_.set_routes([this](seastar::httpd::routes& r) {
    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/interactive/query"),
          new hqps_ic_handler(ic_query_group_id, shard_query_concurrency));
    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/interactive/adhoc_query"),
          new hqps_adhoc_query_handler(ic_adhoc_group_id, codegen_group_id,
                                       shard_adhoc_concurrency));
    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/interactive/exit"), new hqps_exit_handler());
    return seastar::make_ready_future<>();
  });
}

}  // namespace server

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

#include "flex/engines/graph_db/server/executor_group.actg.h"
#include "flex/engines/graph_db/server/options.h"
#include "flex/engines/graph_db/server/service.h"

#include <seastar/core/alien.hh>
#include <seastar/core/print.hh>
#include <seastar/http/handlers.hh>
#include "flex/engines/graph_db/server/generated/executor_ref.act.autogen.h"
#include "flex/engines/graph_db/server/types.h"

namespace server {

class ic_handler : public seastar::httpd::handler_base {
 public:
  ic_handler(uint32_t group_id, uint32_t shard_concurrency)
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
  ~ic_handler() override = default;

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    auto dst_executor = executor_idx_;
    executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;

    return executor_refs_[dst_executor]
        .run_query(query_param{std::move(req->content)})
        .then_wrapped([rep = std::move(rep)](
                          seastar::future<query_result>&& fut) mutable {
          if (__builtin_expect(fut.failed(), false)) {
            return seastar::make_exception_future<
                std::unique_ptr<seastar::httpd::reply>>(fut.get_exception());
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

class exit_handler : public seastar::httpd::handler_base {
 public:
  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    service::get().set_exit_state();
    rep->write_body("bin",
                    seastar::sstring{"The graph_db server is exiting ..."});
    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(
        std::move(rep));
  }
};

http_handler::http_handler(uint16_t http_port) : http_port_(http_port) {}

void http_handler::start() {
  auto fut = seastar::alien::submit_to(
      *seastar::alien::internal::default_instance, 0, [this] {
        return server_.start()
            .then([this] { return set_routes(); })
            .then([this] { return server_.listen(http_port_); })
            .then([this] {
              fmt::print("Http handler is listening on port {} ...\n",
                         http_port_);
            });
      });
  fut.wait();
}

void http_handler::stop() {
  auto fut =
      seastar::alien::submit_to(*seastar::alien::internal::default_instance, 0,
                                [this] { return server_.stop(); });
  fut.wait();
}

seastar::future<> http_handler::set_routes() {
  return server_.set_routes([this](seastar::httpd::routes& r) {
    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/interactive/query"),
          new ic_handler(ic_query_group_id, shard_query_concurrency));
    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/interactive/update"),
          new ic_handler(ic_update_group_id, shard_update_concurrency));
    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/interactive/app"),
          new ic_handler(ic_update_group_id, shard_update_concurrency));
    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/interactive/exit"), new exit_handler());
    return seastar::make_ready_future<>();
  });
}

}  // namespace server

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

#include "flex/engines/http_server/handler/proxy_http_handler.h"

#include "flex/engines/http_server/executor_group.actg.h"
#include "flex/engines/http_server/handler/http_utils.h"
#include "flex/engines/http_server/options.h"

#include "flex/engines/http_server/types.h"

namespace server {

proxy_http_forward_handler::proxy_http_forward_handler(
    uint32_t group_id, uint32_t shard_concurrency)
    : executor_idx_(0), shard_concurrency_(shard_concurrency) {
  executor_refs_.reserve(shard_concurrency_);
  hiactor::scope_builder builder;
  builder.set_shard(hiactor::local_shard_id())
      .enter_sub_scope(hiactor::scope<executor_group>(0))
      .enter_sub_scope(hiactor::scope<hiactor::actor_group>(group_id));
  for (unsigned i = 0; i < shard_concurrency_; ++i) {
    executor_refs_.emplace_back(builder.build_ref<proxy_actor_ref>(i));
  }
}

seastar::future<std::unique_ptr<seastar::httpd::reply>>
proxy_http_forward_handler::handle(const seastar::sstring& path,
                                   std::unique_ptr<seastar::httpd::request> req,
                                   std::unique_ptr<seastar::httpd::reply> rep) {
  auto dst_executor = executor_idx_;
  executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;

  return executor_refs_[dst_executor]
      .do_query(proxy_request{std::move(req)})
      .then_wrapped([rep = std::move(rep)](
                        seastar::future<proxy_query_result>&& fut) mutable {
        return return_reply_with_result(std::move(rep), std::move(fut));
        // if (__builtin_expect(fut.failed(), false)) {
        //   return seastar::make_exception_future<
        //       std::unique_ptr<seastar::httpd::reply>>(fut.get_exception());
        // }
        // auto result = fut.get0();
        // rep->write_body("bin", std::move(result.content));
        // rep->done();
        // return seastar::make_ready_future<
        //     std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
      });
}

proxy_http_handler::proxy_http_handler(uint16_t http_port)
    : http_port_(http_port) {}

void proxy_http_handler::start() {
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

void proxy_http_handler::stop() {
  auto fut =
      seastar::alien::submit_to(*seastar::alien::internal::default_instance, 0,
                                [this] { return server_.stop(); });
  fut.wait();
}

seastar::future<> proxy_http_handler::set_routes() {
  return server_.set_routes([](seastar::httpd::routes& r) {
    r.add_default_handler(new proxy_http_forward_handler(
        proxy_group_id, shard_proxy_concurrency));
    return seastar::make_ready_future<>();
  });
}

}  // namespace server
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

#include "flex/engines/http_server/handler/admin_http_handler.h"
#include "flex/engines/http_server/executor_group.actg.h"
#include "flex/engines/http_server/options.h"

#include <seastar/core/alien.hh>
#include <seastar/core/print.hh>
#include <seastar/http/handlers.hh>
#include "flex/engines/http_server/generated/actor/admin_actor_ref.act.autogen.h"
#include "flex/engines/http_server/types.h"
#include "flex/engines/http_server/workdir_manipulator.h"

#include <glog/logging.h>

namespace server {

/**
 * Handle all request for graph management.
 */
class admin_http_graph_handler_impl : public seastar::httpd::handler_base {
 public:
  admin_http_graph_handler_impl(uint32_t group_id, uint32_t shard_concurrency)
      : shard_concurrency_(shard_concurrency), executor_idx_(0) {
    admin_actor_refs_.reserve(shard_concurrency_);
    hiactor::scope_builder builder;
    builder.set_shard(hiactor::local_shard_id())
        .enter_sub_scope(hiactor::scope<executor_group>(0))
        .enter_sub_scope(hiactor::scope<hiactor::actor_group>(group_id));
    for (unsigned i = 0; i < shard_concurrency_; ++i) {
      admin_actor_refs_.emplace_back(builder.build_ref<admin_actor_ref>(i));
    }
  }
  ~admin_http_graph_handler_impl() override = default;

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    auto dst_executor = executor_idx_;

    executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;
    LOG(INFO) << "Handling path:" << path << ", method: " << req->_method;
    auto& method = req->_method;
    if (method == "POST") {
      if (path.find("dataloading") != seastar::sstring::npos) {
        LOG(INFO) << "Route to loading graph";
        if (!req->param.exists("graph_name")) {
          return seastar::make_exception_future<
              std::unique_ptr<seastar::httpd::reply>>(
              std::runtime_error("graph_name not exists"));
        } else {
          auto graph_name = req->param.at("graph_name");
          LOG(INFO) << "Graph name: " << graph_name;
          auto pair = std::make_pair(graph_name, std::move(req->content));
          return admin_actor_refs_[dst_executor]
              .run_graph_loading(graph_management_param{std::move(pair)})
              .then_wrapped([rep = std::move(rep)](
                                seastar::future<query_result>&& fut) mutable {
                if (__builtin_expect(fut.failed(), false)) {
                  return seastar::make_exception_future<
                      std::unique_ptr<seastar::httpd::reply>>(
                      fut.get_exception());
                }
                auto result = fut.get0();
                rep->write_body("application/json", std::move(result.content));
                rep->done();
                return seastar::make_ready_future<
                    std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
              });
        }
      } else {
        LOG(INFO) << "Route to creating graph";
        return admin_actor_refs_[dst_executor]
            .run_create_graph(query_param{std::move(req->content)})
            .then_wrapped([rep = std::move(rep)](
                              seastar::future<query_result>&& fut) mutable {
              if (__builtin_expect(fut.failed(), false)) {
                return seastar::make_exception_future<
                    std::unique_ptr<seastar::httpd::reply>>(
                    fut.get_exception());
              }
              auto result = fut.get0();
              rep->write_body("application/json", std::move(result.content));
              rep->done();
              return seastar::make_ready_future<
                  std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
            });
      }
    } else if (method == "GET") {
      if (req->param.exists("graph_name") &&
          path.find("schema") != seastar::sstring::npos) {
        auto graph_name = req->param.at("graph_name");
        return admin_actor_refs_[dst_executor]
            .run_get_graph_schema(query_param{std::move(graph_name)})
            .then_wrapped([rep = std::move(rep)](
                              seastar::future<query_result>&& fut) mutable {
              if (__builtin_expect(fut.failed(), false)) {
                return seastar::make_exception_future<
                    std::unique_ptr<seastar::httpd::reply>>(
                    fut.get_exception());
              }
              auto result = fut.get0();
              rep->write_body("application/json", std::move(result.content));
              rep->done();
              return seastar::make_ready_future<
                  std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
            });
      } else {
        return admin_actor_refs_[dst_executor]
            .run_list_graphs(query_param{std::move(req->content)})
            .then_wrapped([rep = std::move(rep)](
                              seastar::future<query_result>&& fut) mutable {
              if (__builtin_expect(fut.failed(), false)) {
                return seastar::make_exception_future<
                    std::unique_ptr<seastar::httpd::reply>>(
                    fut.get_exception());
              }
              auto result = fut.get0();
              rep->write_body("application/json", std::move(result.content));
              rep->done();
              return seastar::make_ready_future<
                  std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
            });
      }
    } else if (method == "DELETE") {
      if (!req->param.exists("graph_name")) {
        return seastar::make_exception_future<
            std::unique_ptr<seastar::httpd::reply>>(
            std::runtime_error("graph_name not given"));
      }
      auto graph_name = req->param.at("graph_name");
      return admin_actor_refs_[dst_executor]
          .run_delete_graph(query_param{std::move(graph_name)})
          .then_wrapped([rep = std::move(rep)](
                            seastar::future<query_result>&& fut) mutable {
            if (__builtin_expect(fut.failed(), false)) {
              return seastar::make_exception_future<
                  std::unique_ptr<seastar::httpd::reply>>(fut.get_exception());
            }
            auto result = fut.get0();
            rep->write_body("application/json", std::move(result.content));
            rep->done();
            return seastar::make_ready_future<
                std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
          });
    } else {
      return seastar::make_exception_future<
          std::unique_ptr<seastar::httpd::reply>>(
          std::runtime_error("Unsupported method" + method));
    }
  }

 private:
  const uint32_t shard_concurrency_;
  uint32_t executor_idx_;
  std::vector<admin_actor_ref> admin_actor_refs_;
};

class admin_http_procedure_handler_impl : public seastar::httpd::handler_base {
 public:
  admin_http_procedure_handler_impl(uint32_t group_id,
                                    uint32_t shard_concurrency)
      : shard_concurrency_(shard_concurrency), executor_idx_(0) {
    admin_actor_refs_.reserve(shard_concurrency_);
    hiactor::scope_builder builder;
    builder.set_shard(hiactor::local_shard_id())
        .enter_sub_scope(hiactor::scope<executor_group>(0))
        .enter_sub_scope(hiactor::scope<hiactor::actor_group>(group_id));
    for (unsigned i = 0; i < shard_concurrency_; ++i) {
      admin_actor_refs_.emplace_back(builder.build_ref<admin_actor_ref>(i));
    }
  }
  ~admin_http_procedure_handler_impl() override = default;

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    auto dst_executor = executor_idx_;

    executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;
    LOG(INFO) << "Handling path:" << path << ", method: " << req->_method;
    // LOG(INFO) << "Graph_name:" << req->param.at("graph_name");
    if (req->_method == "GET") {
      // get graph_name param
      if (!req->param.exists("graph_name")) {
        return seastar::make_exception_future<
            std::unique_ptr<seastar::httpd::reply>>(
            std::runtime_error("graph_name not exists"));
      }
      auto graph_name = req->param.at("graph_name");
      // remove / from the graph_name
      graph_name.erase(std::remove(graph_name.begin(), graph_name.end(), '/'),
                       graph_name.end());
      if (req->param.exists("procedure_name")) {
        // Get the procedures
        auto procedure_name = req->param.at("procedure_name");
        // remove / from the procedure_name
        procedure_name.erase(
            std::remove(procedure_name.begin(), procedure_name.end(), '/'),
            procedure_name.end());

        LOG(INFO) << "Get procedure for: " << graph_name << ", "
                  << procedure_name;
        auto pair = std::make_pair(graph_name, procedure_name);
        return admin_actor_refs_[dst_executor]
            .get_procedure_by_procedure_name(
                procedure_query_param{std::move(pair)})
            .then_wrapped([rep = std::move(rep)](
                              seastar::future<query_result>&& fut) mutable {
              if (__builtin_expect(fut.failed(), false)) {
                return seastar::make_exception_future<
                    std::unique_ptr<seastar::httpd::reply>>(
                    fut.get_exception());
              }
              auto result = fut.get0();
              rep->write_body("application/json", std::move(result.content));
              rep->done();
              return seastar::make_ready_future<
                  std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
            });
      } else {
        // get all procedures.
        LOG(INFO) << "Get all procedures for: " << graph_name;
        return admin_actor_refs_[dst_executor]
            .get_procedures_by_graph_name(query_param{std::move(graph_name)})
            .then_wrapped([rep = std::move(rep)](
                              seastar::future<query_result>&& fut) mutable {
              if (__builtin_expect(fut.failed(), false)) {
                return seastar::make_exception_future<
                    std::unique_ptr<seastar::httpd::reply>>(
                    fut.get_exception());
              }
              auto result = fut.get0();
              rep->write_body("application/json", std::move(result.content));
              rep->done();
              return seastar::make_ready_future<
                  std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
            });
      }
    } else if (req->_method == "POST") {
      if (!req->param.exists("graph_name")) {
        return seastar::make_exception_future<
            std::unique_ptr<seastar::httpd::reply>>(
            std::runtime_error("graph_name not given"));
      }
      auto graph_name = req->param.at("graph_name");
      // remove / from the graph_name
      graph_name.erase(std::remove(graph_name.begin(), graph_name.end(), '/'),
                       graph_name.end());
      LOG(INFO) << "Creating procedure for: " << graph_name;
      return admin_actor_refs_[dst_executor]
          .create_procedure(create_procedure_query_param{
              std::make_pair(graph_name, std::move(req->content))})
          .then_wrapped([rep = std::move(rep)](
                            seastar::future<query_result>&& fut) mutable {
            if (__builtin_expect(fut.failed(), false)) {
              return seastar::make_exception_future<
                  std::unique_ptr<seastar::httpd::reply>>(fut.get_exception());
            }
            auto result = fut.get0();
            rep->write_body("application/json", std::move(result.content));
            rep->done();
            return seastar::make_ready_future<
                std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
          });
    } else if (req->_method == "DELETE") {
      // delete must give graph_name and procedure_name
      if (!req->param.exists("graph_name") ||
          !req->param.exists("procedure_name")) {
        return seastar::make_exception_future<
            std::unique_ptr<seastar::httpd::reply>>(
            std::runtime_error("graph_name or procedure_name not given: "));
      }
      auto graph_name = req->param.at("graph_name");
      graph_name.erase(std::remove(graph_name.begin(), graph_name.end(), '/'),
                       graph_name.end());
      auto procedure_name = req->param.at("procedure_name");
      procedure_name.erase(
          std::remove(procedure_name.begin(), procedure_name.end(), '/'),
          procedure_name.end());
      LOG(INFO) << "Deleting procedure for: " << graph_name << ", "
                << procedure_name;
      return admin_actor_refs_[dst_executor]
          .delete_procedure(
              procedure_query_param{std::make_pair(graph_name, procedure_name)})
          .then_wrapped([rep = std::move(rep)](
                            seastar::future<query_result>&& fut) mutable {
            if (__builtin_expect(fut.failed(), false)) {
              return seastar::make_exception_future<
                  std::unique_ptr<seastar::httpd::reply>>(fut.get_exception());
            }
            auto result = fut.get0();
            rep->write_body("application/json", std::move(result.content));
            rep->done();
            return seastar::make_ready_future<
                std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
          });
    } else if (req->_method == "PUT") {
      if (!req->param.exists("graph_name") ||
          !req->param.exists("procedure_name")) {
        return seastar::make_exception_future<
            std::unique_ptr<seastar::httpd::reply>>(
            std::runtime_error("graph_name or procedure_name not given: "));
      }
      auto graph_name = req->param.at("graph_name");
      graph_name.erase(std::remove(graph_name.begin(), graph_name.end(), '/'),
                       graph_name.end());
      auto procedure_name = req->param.at("procedure_name");
      procedure_name.erase(
          std::remove(procedure_name.begin(), procedure_name.end(), '/'),
          procedure_name.end());
      LOG(INFO) << "Update procedure for: " << graph_name << ", "
                << procedure_name;
      return admin_actor_refs_[dst_executor]
          .update_procedure(update_procedure_query_param{
              std::make_tuple(graph_name, procedure_name, req->content)})
          .then_wrapped([rep = std::move(rep)](
                            seastar::future<query_result>&& fut) mutable {
            if (__builtin_expect(fut.failed(), false)) {
              return seastar::make_exception_future<
                  std::unique_ptr<seastar::httpd::reply>>(fut.get_exception());
            }
            auto result = fut.get0();
            rep->write_body("application/json", std::move(result.content));
            rep->done();
            return seastar::make_ready_future<
                std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
          });
    } else {
      return seastar::make_exception_future<
          std::unique_ptr<seastar::httpd::reply>>(
          std::runtime_error("Unsupported method" + req->_method));
    }
  }

 private:
  const uint32_t shard_concurrency_;
  uint32_t executor_idx_;
  std::vector<admin_actor_ref> admin_actor_refs_;
};

// Handling request for node and service management
class admin_http_service_handler_impl : public seastar::httpd::handler_base {
 public:
  admin_http_service_handler_impl(uint32_t group_id, uint32_t shard_concurrency)
      : shard_concurrency_(shard_concurrency), executor_idx_(0) {
    admin_actor_refs_.reserve(shard_concurrency_);
    hiactor::scope_builder builder;
    builder.set_shard(hiactor::local_shard_id())
        .enter_sub_scope(hiactor::scope<executor_group>(0))
        .enter_sub_scope(hiactor::scope<hiactor::actor_group>(group_id));
    for (unsigned i = 0; i < shard_concurrency_; ++i) {
      admin_actor_refs_.emplace_back(builder.build_ref<admin_actor_ref>(i));
    }
  }
  ~admin_http_service_handler_impl() override = default;

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    auto dst_executor = executor_idx_;

    executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;
    LOG(INFO) << "Handling path:" << path << ", method: " << req->_method;
    auto& method = req->_method;
    if (method == "POST") {
      // Then param[action] should exists
      if (!req->param.exists("action")) {
        return seastar::make_exception_future<
            std::unique_ptr<seastar::httpd::reply>>(
            std::runtime_error("action is expected for /v1/service/"));
      }
      auto action = req->param.at("action");
      LOG(INFO) << "POST with action: " << action;
      // Remove / from the action
      action.erase(std::remove(action.begin(), action.end(), '/'),
                   action.end());

      if (action == "start" || action == "restart") {
        return admin_actor_refs_[dst_executor]
            .start_service(query_param{std::move(req->content)})
            .then_wrapped([rep = std::move(rep)](
                              seastar::future<query_result>&& fut) mutable {
              if (__builtin_expect(fut.failed(), false)) {
                return seastar::make_exception_future<
                    std::unique_ptr<seastar::httpd::reply>>(
                    fut.get_exception());
              }
              auto result = fut.get0();
              rep->write_body("application/json", std::move(result.content));
              rep->done();
              return seastar::make_ready_future<
                  std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
            });
      } else if (action == "stop") {
        return seastar::make_exception_future<
            std::unique_ptr<seastar::httpd::reply>>(
            std::runtime_error("Stopping service not supported."));
      } else {
        return seastar::make_exception_future<
            std::unique_ptr<seastar::httpd::reply>>(
            std::runtime_error("Unsupported action: " + action));
      }
    } else {
      // get status
      LOG(INFO) << "GET with action: status";
      return admin_actor_refs_[dst_executor]
          .service_status(query_param{std::move(req->content)})
          .then_wrapped([rep = std::move(rep)](
                            seastar::future<query_result>&& fut) mutable {
            if (__builtin_expect(fut.failed(), false)) {
              return seastar::make_exception_future<
                  std::unique_ptr<seastar::httpd::reply>>(fut.get_exception());
            }
            auto result = fut.get0();
            LOG(INFO) << "Service status: " << result.content;
            rep->write_body("application/json", std::move(result.content));
            rep->done();
            return seastar::make_ready_future<
                std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
          });
    }
  }

 private:
  const uint32_t shard_concurrency_;
  uint32_t executor_idx_;
  std::vector<admin_actor_ref> admin_actor_refs_;
};

class admin_http_node_handler_impl : public seastar::httpd::handler_base {
 public:
  admin_http_node_handler_impl(uint32_t group_id, uint32_t shard_concurrency)
      : shard_concurrency_(shard_concurrency), executor_idx_(0) {
    admin_actor_refs_.reserve(shard_concurrency_);
    hiactor::scope_builder builder;
    builder.set_shard(hiactor::local_shard_id())
        .enter_sub_scope(hiactor::scope<executor_group>(0))
        .enter_sub_scope(hiactor::scope<hiactor::actor_group>(group_id));
    for (unsigned i = 0; i < shard_concurrency_; ++i) {
      admin_actor_refs_.emplace_back(builder.build_ref<admin_actor_ref>(i));
    }
  }
  ~admin_http_node_handler_impl() override = default;

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    auto dst_executor = executor_idx_;

    executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;
    LOG(INFO) << "Handling path:" << path << ", method: " << req->_method;
    auto& method = req->_method;
    if (method == "GET") {
      LOG(INFO) << "GET with action: status";
      return admin_actor_refs_[dst_executor]
          .node_status(query_param{std::move(req->content)})
          .then_wrapped([rep = std::move(rep)](
                            seastar::future<query_result>&& fut) mutable {
            if (__builtin_expect(fut.failed(), false)) {
              return seastar::make_exception_future<
                  std::unique_ptr<seastar::httpd::reply>>(fut.get_exception());
            }
            auto result = fut.get0();
            LOG(INFO) << "Node status: " << result.content;
            rep->write_body("application/json", std::move(result.content));
            rep->done();
            return seastar::make_ready_future<
                std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
          });
    } else {
      return seastar::make_exception_future<
          std::unique_ptr<seastar::httpd::reply>>(
          std::runtime_error("Unsupported method" + method));
    }
  }

 private:
  const uint32_t shard_concurrency_;
  uint32_t executor_idx_;
  std::vector<admin_actor_ref> admin_actor_refs_;
};

admin_http_handler::admin_http_handler(uint16_t http_port)
    : http_port_(http_port) {}

void admin_http_handler::start() {
  auto fut = seastar::alien::submit_to(
      *seastar::alien::internal::default_instance, 0, [this] {
        return server_.start()
            .then([this] { return set_routes(); })
            .then([this] { return server_.listen(http_port_); })
            .then([this] {
              fmt::print(
                  "HQPS admin http handler is listening on port {} ...\n",
                  http_port_);
            });
      });
  fut.wait();
}

void admin_http_handler::stop() {
  auto fut =
      seastar::alien::submit_to(*seastar::alien::internal::default_instance, 0,
                                [this] { return server_.stop(); });
  fut.wait();
}

seastar::future<> admin_http_handler::set_routes() {
  return server_.set_routes([](seastar::httpd::routes& r) {
    auto admin_graph_handler = new admin_http_graph_handler_impl(
        interactive_admin_group_id, shard_admin_graph_concurrency);

    auto procedures_handler = new admin_http_procedure_handler_impl(
        interactive_admin_group_id, shard_admin_procedure_concurrency);

    auto service_handler = new admin_http_service_handler_impl(
        interactive_admin_group_id, shard_admin_service_concurrency);

    auto node_handler = new admin_http_node_handler_impl(
        interactive_admin_group_id, shard_admin_node_concurrency);

    ////Procedure management ///
    {
      auto match_rule = new seastar::httpd::match_rule(procedures_handler);
      match_rule->add_str("/v1/graph")
          .add_param("graph_name")
          .add_str("/procedure");
      // Get All procedures
      r.add(match_rule, seastar::httpd::operation_type::GET);
      // Create a new procedure
      r.add(match_rule, seastar::httpd::operation_type::POST);
    }
    {
      // Each procedure's handling
      auto match_rule = new seastar::httpd::match_rule(procedures_handler);
      match_rule->add_str("/v1/graph")
          .add_param("graph_name")
          .add_str("/procedure")
          .add_param("procedure_name");
      // Get a procedure
      r.add(match_rule, seastar::httpd::operation_type::GET);
      // Delete a procedure
      r.add(match_rule, seastar::httpd::operation_type::DELETE);
      // Update a procedure
      r.add(match_rule, seastar::httpd::operation_type::PUT);
    }

    // List all graphs.
    r.add(seastar::httpd::operation_type::GET, seastar::httpd::url("/v1/graph"),
          admin_graph_handler);
    // Create a new Graph
    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/v1/graph"), admin_graph_handler);

    // Delete a graph
    r.add(seastar::httpd::operation_type::DELETE,
          seastar::httpd::url("/v1/graph").remainder("graph_name"),
          admin_graph_handler);

    {  // load data to graph
      auto match_rule = new seastar::httpd::match_rule(admin_graph_handler);
      match_rule->add_str("/v1/graph")
          .add_param("graph_name")
          .add_str("/dataloading");
      r.add(match_rule, seastar::httpd::operation_type::POST);
    }
    {  // Get Graph Schema
      auto match_rule = new seastar::httpd::match_rule(admin_graph_handler);
      match_rule->add_str("/v1/graph")
          .add_param("graph_name")
          .add_str("/schema");
      r.add(match_rule, seastar::httpd::operation_type::GET);
    }

    {
      // Node and service management
      r.add(seastar::httpd::operation_type::GET,
            seastar::httpd::url("/v1/node/status"), node_handler);

      auto match_rule = new seastar::httpd::match_rule(service_handler);
      match_rule->add_str("/v1/service").add_param("action");
      r.add(match_rule, seastar::httpd::operation_type::POST);

      r.add(seastar::httpd::operation_type::GET,
            seastar::httpd::url("/v1/service/status"), service_handler);
    }

    {
      seastar::httpd::parameters params;
      auto test_handler = r.get_handler(seastar::httpd::operation_type::POST,
                                        "/v1/graph/abc/dataloading", params);
      CHECK(test_handler);
      CHECK(params.exists("graph_name"));
      CHECK(params.at("graph_name") == "/abc") << params.at("graph_name");
    }

    {
      seastar::httpd::parameters params;
      auto test_handler = r.get_handler(seastar::httpd::operation_type::GET,
                                        "/v1/graph/abc/schema", params);
      CHECK(test_handler);
      CHECK(params.exists("graph_name"));
      CHECK(params.at("graph_name") == "/abc") << params.at("graph_name");
    }

    {
      seastar::httpd::parameters params;
      auto test_handler = r.get_handler(seastar::httpd::operation_type::GET,
                                        "/v1/graph/abc/procedure", params);
      CHECK(test_handler);
      CHECK(params.exists("graph_name"));
      CHECK(params.at("graph_name") == "/abc") << params.at("graph_name");
    }

    {
      seastar::httpd::parameters params;
      auto test_handler = r.get_handler(seastar::httpd::operation_type::POST,
                                        "/v1/graph/abc/procedure", params);
      CHECK(test_handler);
      CHECK(params.exists("graph_name"));
      CHECK(params.at("graph_name") == "/abc") << params.at("graph_name");
    }

    {
      seastar::httpd::parameters params;
      auto test_handler =
          r.get_handler(seastar::httpd::operation_type::GET,
                        "/v1/graph/abc/procedure/proce1", params);
      CHECK(test_handler);
      CHECK(params.exists("graph_name"));
      CHECK(params.at("graph_name") == "/abc") << params.at("graph_name");
      CHECK(params.exists("procedure_name"));
      CHECK(params.at("procedure_name") == "/proce1")
          << params.at("procedure_name");
      params.clear();
      test_handler = r.get_handler(seastar::httpd::operation_type::DELETE,
                                   "/v1/graph/abc/procedure/proce1", params);
      CHECK(test_handler);
      test_handler = r.get_handler(seastar::httpd::operation_type::PUT,
                                   "/v1/graph/abc/procedure/proce1", params);
      CHECK(test_handler);
    }

    return seastar::make_ready_future<>();
  });
}

}  // namespace server

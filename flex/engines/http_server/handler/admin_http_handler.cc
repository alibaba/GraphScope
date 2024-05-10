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

std::string trim_slash(const std::string& origin) {
  std::string res = origin;
  if (res.front() == '/') {
    res.erase(res.begin());
  }
  if (res.back() == '/') {
    res.pop_back();
  }
  return res;
}

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
        if (!req->param.exists("graph_id")) {
          return seastar::make_exception_future<
              std::unique_ptr<seastar::httpd::reply>>(
              std::runtime_error("graph_id not exists"));
        } else {
          auto graph_id = trim_slash(req->param.at("graph_id"));
          LOG(INFO) << "Graph id: " << graph_id;
          auto pair = std::make_pair(graph_id, std::move(req->content));
          return admin_actor_refs_[dst_executor]
              .run_graph_loading(graph_management_param{std::move(pair)})
              .then_wrapped(
                  [rep = std::move(rep)](
                      seastar::future<admin_query_result>&& fut) mutable {
                    return return_reply_with_result(std::move(rep),
                                                    std::move(fut));
                  });
        }
      } else {
        LOG(INFO) << "Route to creating graph";
        return admin_actor_refs_[dst_executor]
            .run_create_graph(query_param{std::move(req->content)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      }
    } else if (method == "GET") {
      if (req->param.exists("graph_id")) {
        auto graph_id = trim_slash(req->param.at("graph_id"));
        if (path.find("schema") != seastar::sstring::npos) {
          // get graph schema
          return admin_actor_refs_[dst_executor]
              .run_get_graph_schema(query_param{std::move(graph_id)})
              .then_wrapped(
                  [rep = std::move(rep)](
                      seastar::future<admin_query_result>&& fut) mutable {
                    return return_reply_with_result(std::move(rep),
                                                    std::move(fut));
                  });
        } else {
          // Get the metadata of graph.
          return admin_actor_refs_[dst_executor]
              .run_get_graph_meta(query_param{std::move(graph_id)})
              .then_wrapped(
                  [rep = std::move(rep)](
                      seastar::future<admin_query_result>&& fut) mutable {
                    return return_reply_with_result(std::move(rep),
                                                    std::move(fut));
                  });
        }
      } else {
        return admin_actor_refs_[dst_executor]
            .run_list_graphs(query_param{std::move(req->content)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      }
    } else if (method == "DELETE") {
      if (!req->param.exists("graph_id")) {
        return seastar::make_exception_future<
            std::unique_ptr<seastar::httpd::reply>>(
            std::runtime_error("graph_id not given"));
      }
      auto graph_id = trim_slash(req->param.at("graph_id"));
      return admin_actor_refs_[dst_executor]
          .run_delete_graph(query_param{std::move(graph_id)})
          .then_wrapped([rep = std::move(rep)](
                            seastar::future<admin_query_result>&& fut) mutable {
            return return_reply_with_result(std::move(rep), std::move(fut));
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
    if (req->_method == "GET") {
      // get graph_id param
      if (!req->param.exists("graph_id")) {
        return seastar::make_exception_future<
            std::unique_ptr<seastar::httpd::reply>>(
            std::runtime_error("graph_id not exists"));
      }
      auto graph_id = trim_slash(req->param.at("graph_id"));
      if (req->param.exists("procedure_id")) {
        // Get the procedures
        auto procedure_id = trim_slash(req->param.at("procedure_id"));

        LOG(INFO) << "Get procedure for: " << graph_id << ", " << procedure_id;
        auto pair = std::make_pair(graph_id, procedure_id);
        return admin_actor_refs_[dst_executor]
            .get_procedure_by_procedure_name(
                procedure_query_param{std::move(pair)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      } else {
        // get all procedures.
        LOG(INFO) << "Get all procedures for: " << graph_id;
        return admin_actor_refs_[dst_executor]
            .get_procedures_by_graph_name(query_param{std::move(graph_id)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      }
    } else if (req->_method == "POST") {
      if (!req->param.exists("graph_id")) {
        return seastar::make_exception_future<
            std::unique_ptr<seastar::httpd::reply>>(
            std::runtime_error("graph_id not given"));
      }
      auto graph_id = trim_slash(req->param.at("graph_id"));
      LOG(INFO) << "Creating procedure for: " << graph_id;
      return admin_actor_refs_[dst_executor]
          .create_procedure(create_procedure_query_param{
              std::make_pair(graph_id, std::move(req->content))})
          .then_wrapped([rep = std::move(rep)](
                            seastar::future<admin_query_result>&& fut) mutable {
            return return_reply_with_result(std::move(rep), std::move(fut));
          });
    } else if (req->_method == "DELETE") {
      // delete must give graph_id and procedure_id
      if (!req->param.exists("graph_id") ||
          !req->param.exists("procedure_id")) {
        return seastar::make_exception_future<
            std::unique_ptr<seastar::httpd::reply>>(
            std::runtime_error("graph_id or procedure_id not given: "));
      }
      auto graph_id = trim_slash(req->param.at("graph_id"));
      auto procedure_id = trim_slash(req->param.at("procedure_id"));
      LOG(INFO) << "Deleting procedure for: " << graph_id << ", "
                << procedure_id;
      return admin_actor_refs_[dst_executor]
          .delete_procedure(
              procedure_query_param{std::make_pair(graph_id, procedure_id)})
          .then_wrapped([rep = std::move(rep)](
                            seastar::future<admin_query_result>&& fut) mutable {
            return return_reply_with_result(std::move(rep), std::move(fut));
          });
    } else if (req->_method == "PUT") {
      if (!req->param.exists("graph_id") ||
          !req->param.exists("procedure_id")) {
        return seastar::make_exception_future<
            std::unique_ptr<seastar::httpd::reply>>(
            std::runtime_error("graph_id or procedure_id not given: "));
      }
      auto graph_id = trim_slash(req->param.at("graph_id"));
      auto procedure_id = trim_slash(req->param.at("procedure_id"));
      LOG(INFO) << "Update procedure for: " << graph_id << ", " << procedure_id;
      return admin_actor_refs_[dst_executor]
          .update_procedure(update_procedure_query_param{
              std::make_tuple(graph_id, procedure_id, req->content)})
          .then_wrapped([rep = std::move(rep)](
                            seastar::future<admin_query_result>&& fut) mutable {
            return return_reply_with_result(std::move(rep), std::move(fut));
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
      auto action = trim_slash(req->param.at("action"));
      LOG(INFO) << "POST with action: " << action;

      if (action == "start" || action == "restart") {
        return admin_actor_refs_[dst_executor]
            .start_service(query_param{std::move(req->content)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      } else if (action == "stop") {
        return admin_actor_refs_[dst_executor]
            .stop_service(query_param{std::move(req->content)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
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
                            seastar::future<admin_query_result>&& fut) mutable {
            return return_reply_with_result(std::move(rep), std::move(fut));
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
                            seastar::future<admin_query_result>&& fut) mutable {
            return return_reply_with_result(std::move(rep), std::move(fut));
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

class admin_http_job_handler_impl : public seastar::httpd::handler_base {
 public:
  admin_http_job_handler_impl(uint32_t group_id, uint32_t shard_concurrency)
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
  ~admin_http_job_handler_impl() override = default;

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    auto dst_executor = executor_idx_;

    executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;
    auto& method = req->_method;
    if (method == "GET") {
      if (req->param.exists("job_id")) {
        auto job_id = trim_slash(req->param.at("job_id"));
        return admin_actor_refs_[dst_executor]
            .get_job(query_param{std::move(job_id)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      } else {
        return admin_actor_refs_[dst_executor]
            .list_jobs(query_param{std::move(req->content)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      }
    } else if (method == "DELETE") {
      if (!req->param.exists("job_id")) {
        rep->set_status(seastar::httpd::reply::status_type::bad_request);
        rep->set_content_type("application/json");
        rep->write_body("json",
                        seastar::sstring("expect field 'job_id' in request"));
        rep->done();
        return seastar::make_ready_future<
            std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
      }
      auto job_id = trim_slash(req->param.at("job_id"));
      return admin_actor_refs_[dst_executor]
          .cancel_job(query_param{std::move(job_id)})
          .then_wrapped([rep = std::move(rep)](
                            seastar::future<admin_query_result>&& fut) mutable {
            return return_reply_with_result(std::move(rep), std::move(fut));
          });
    } else {
      rep->set_status(seastar::httpd::reply::status_type::bad_request);
      rep->set_content_type("application/json");
      rep->write_body("json",
                      seastar::sstring("Unsupported method: ") + method);
      rep->done();
      return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(
          std::move(rep));
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
    ////Procedure management ///
    {
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_procedure_handler_impl(
              interactive_admin_group_id, shard_admin_procedure_concurrency));
      match_rule->add_str("/v1/graph")
          .add_param("graph_id")
          .add_str("/procedure");
      // Get All procedures
      r.add(match_rule, seastar::httpd::operation_type::GET);
    }
    {
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_procedure_handler_impl(
              interactive_admin_group_id, shard_admin_procedure_concurrency));
      match_rule->add_str("/v1/graph")
          .add_param("graph_id")
          .add_str("/procedure");
      // Create a new procedure
      r.add(match_rule, seastar::httpd::operation_type::POST);
    }
    {
      // Each procedure's handling
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_procedure_handler_impl(
              interactive_admin_group_id, shard_admin_procedure_concurrency));
      match_rule->add_str("/v1/graph")
          .add_param("graph_id")
          .add_str("/procedure")
          .add_param("procedure_id");
      // Get a procedure
      r.add(new seastar::httpd::match_rule(*match_rule),
            seastar::httpd::operation_type::GET);
    }
    {
      // Each procedure's handling
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_procedure_handler_impl(
              interactive_admin_group_id, shard_admin_procedure_concurrency));
      match_rule->add_str("/v1/graph")
          .add_param("graph_id")
          .add_str("/procedure")
          .add_param("procedure_id");
      // Delete a procedure
      r.add(new seastar::httpd::match_rule(*match_rule),
            seastar::httpd::operation_type::DELETE);
    }
    {
      // Each procedure's handling
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_procedure_handler_impl(
              interactive_admin_group_id, shard_admin_procedure_concurrency));
      match_rule->add_str("/v1/graph")
          .add_param("graph_id")
          .add_str("/procedure")
          .add_param("procedure_id");
      // Update a procedure
      r.add(new seastar::httpd::match_rule(*match_rule),
            seastar::httpd::operation_type::PUT);
    }

    // List all graphs.
    r.add(seastar::httpd::operation_type::GET, seastar::httpd::url("/v1/graph"),
          new admin_http_graph_handler_impl(interactive_admin_group_id,
                                            shard_admin_graph_concurrency));
    // Create a new Graph
    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/v1/graph"),
          new admin_http_graph_handler_impl(interactive_admin_group_id,
                                            shard_admin_graph_concurrency));

    // Delete a graph
    r.add(seastar::httpd::operation_type::DELETE,
          seastar::httpd::url("/v1/graph").remainder("graph_id"),
          new admin_http_graph_handler_impl(interactive_admin_group_id,
                                            shard_admin_graph_concurrency));

    // Get graph metadata
    {
      // by setting full_path = false, we can match /v1/graph/{graph_id}/ and
      // /v1/graph/{graph_id}/schema
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_graph_handler_impl(
              interactive_admin_group_id, shard_admin_graph_concurrency));
      match_rule->add_str("/v1/graph").add_param("graph_id", false);
      // Get graph schema
      r.add(match_rule, seastar::httpd::operation_type::GET);
    }

    {  // load data to graph
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_graph_handler_impl(
              interactive_admin_group_id, shard_admin_graph_concurrency));
      match_rule->add_str("/v1/graph")
          .add_param("graph_id")
          .add_str("/dataloading");
      r.add(match_rule, seastar::httpd::operation_type::POST);
    }
    {  // Get Graph Schema
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_graph_handler_impl(
              interactive_admin_group_id, shard_admin_graph_concurrency));
      match_rule->add_str("/v1/graph").add_param("graph_id").add_str("/schema");
      r.add(match_rule, seastar::httpd::operation_type::GET);
    }

    {
      // Node and service management
      r.add(seastar::httpd::operation_type::GET,
            seastar::httpd::url("/v1/node/status"),
            new admin_http_node_handler_impl(interactive_admin_group_id,
                                             shard_admin_node_concurrency));

      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_service_handler_impl(
              interactive_admin_group_id, shard_admin_service_concurrency));
      match_rule->add_str("/v1/service").add_param("action");
      r.add(match_rule, seastar::httpd::operation_type::POST);

      r.add(seastar::httpd::operation_type::GET,
            seastar::httpd::url("/v1/service/status"),
            new admin_http_service_handler_impl(
                interactive_admin_group_id, shard_admin_service_concurrency));
    }

    {
      seastar::httpd::parameters params;
      auto test_handler = r.get_handler(seastar::httpd::operation_type::POST,
                                        "/v1/graph/abc/dataloading", params);
      CHECK(test_handler);
      CHECK(params.exists("graph_id"));
      CHECK(params.at("graph_id") == "/abc") << params.at("graph_id");
    }

    {
      seastar::httpd::parameters params;
      auto test_handler = r.get_handler(seastar::httpd::operation_type::GET,
                                        "/v1/graph/abc/schema", params);
      CHECK(test_handler);
      CHECK(params.exists("graph_id"));
      CHECK(params.at("graph_id") == "/abc") << params.at("graph_id");
    }

    {
      seastar::httpd::parameters params;
      auto test_handler = r.get_handler(seastar::httpd::operation_type::GET,
                                        "/v1/graph/abc", params);
      CHECK(test_handler);
      CHECK(params.exists("graph_id"));
      CHECK(params.at("graph_id") == "/abc") << params.at("graph_id");
    }

    {
      seastar::httpd::parameters params;
      auto test_handler = r.get_handler(seastar::httpd::operation_type::GET,
                                        "/v1/graph/abc/procedure", params);
      CHECK(test_handler);
      CHECK(params.exists("graph_id"));
      CHECK(params.at("graph_id") == "/abc") << params.at("graph_id");
    }

    {
      seastar::httpd::parameters params;
      auto test_handler = r.get_handler(seastar::httpd::operation_type::POST,
                                        "/v1/graph/abc/procedure", params);
      CHECK(test_handler);
      CHECK(params.exists("graph_id"));
      CHECK(params.at("graph_id") == "/abc") << params.at("graph_id");
    }

    {
      seastar::httpd::parameters params;
      auto test_handler =
          r.get_handler(seastar::httpd::operation_type::GET,
                        "/v1/graph/abc/procedure/proce1", params);
      CHECK(test_handler);
      CHECK(params.exists("graph_id"));
      CHECK(params.at("graph_id") == "/abc") << params.at("graph_id");
      CHECK(params.exists("procedure_id"));
      CHECK(params.at("procedure_id") == "/proce1")
          << params.at("procedure_id");
      params.clear();
      test_handler = r.get_handler(seastar::httpd::operation_type::DELETE,
                                   "/v1/graph/abc/procedure/proce1", params);
      CHECK(test_handler);
      test_handler = r.get_handler(seastar::httpd::operation_type::PUT,
                                   "/v1/graph/abc/procedure/proce1", params);
      CHECK(test_handler);
    }

    {
      // job request handling.
      r.add(seastar::httpd::operation_type::GET, seastar::httpd::url("/v1/job"),
            new admin_http_job_handler_impl(interactive_admin_group_id,
                                            shard_admin_job_concurrency));
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_job_handler_impl(
              interactive_admin_group_id, shard_admin_job_concurrency));

      match_rule->add_str("/v1/job").add_param("job_id");
      r.add(match_rule, seastar::httpd::operation_type::GET);

      r.add(seastar::httpd::operation_type::DELETE,
            seastar::httpd::url("/v1/job").remainder("job_id"),
            new admin_http_job_handler_impl(interactive_admin_group_id,
                                            shard_admin_job_concurrency));
    }

    return seastar::make_ready_future<>();
  });
}

}  // namespace server

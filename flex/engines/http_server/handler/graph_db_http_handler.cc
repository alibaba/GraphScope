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

#include "flex/engines/http_server/handler/graph_db_http_handler.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/http_server/executor_group.actg.h"
#include "flex/engines/http_server/graph_db_service.h"
#include "flex/engines/http_server/options.h"
#include "flex/engines/http_server/types.h"
#include "flex/otel/otel.h"

#include <seastar/core/alien.hh>
#include <seastar/core/print.hh>
#include <seastar/core/when_all.hh>
#include <seastar/http/handlers.hh>

#ifdef HAVE_OPENTELEMETRY_CPP
#include "opentelemetry/context/context.h"
#include "opentelemetry/trace/span_metadata.h"
#include "opentelemetry/trace/span_startoptions.h"
#endif  // HAVE_OPENTELEMETRY_CPP

#define RANDOM_DISPATCHER 1
// when RANDOM_DISPATCHER is false, the dispatcher will use round-robin
// algorithm to dispatch the query to different executors

#if RANDOM_DISPATCHER
#include <random>
#endif

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

//////////////////////////////////////////////////////////////////////////
namespace seastar {
namespace httpd {
// The seastar::httpd::param_matcher will fail to match if param is not
// specified.
class optional_param_matcher : public matcher {
 public:
  /**
   * Constructor
   * @param name the name of the parameter, will be used as the key
   * in the parameters object
   * @param entire_path when set to true, the matched parameters will
   * include all the remaining url until the end of it.
   * when set to false the match will terminate at the next slash
   */
  explicit optional_param_matcher(const sstring& name) : _name(name) {}

  size_t match(const sstring& url, size_t ind, parameters& param) override {
    size_t last = find_end_param(url, ind);
    if (last == url.size()) {
      // Means we didn't find the parameter, but we still return true,
      // and set the value to empty string.
      param.set(_name, "");
      return ind;
    }
    param.set(_name, url.substr(ind, last - ind));
    return last;
  }

 private:
  size_t find_end_param(const sstring& url, size_t ind) {
    size_t pos = url.find('/', ind + 1);
    if (pos == sstring::npos) {
      return url.length();
    }
    return pos;
  }
  sstring _name;
};
}  // namespace httpd
}  // namespace seastar

namespace server {

bool is_running_graph(const seastar::sstring& graph_id) {
  std::string graph_id_str(graph_id.data(), graph_id.size());
  auto running_graph_res =
      GraphDBService::get().get_metadata_store()->GetRunningGraph();
  if (!running_graph_res.ok()) {
    LOG(ERROR) << "Failed to get running graph: "
               << running_graph_res.status().error_message();
    return false;
  }
  return running_graph_res.value() == graph_id_str;
}

////////////////////////////stored_proc_handler////////////////////////////
class stored_proc_handler : public StoppableHandler {
 public:
  static std::vector<std::vector<executor_ref>>& get_executors() {
    static std::vector<std::vector<executor_ref>> executor_refs;
    return executor_refs;
  }

  stored_proc_handler(uint32_t init_group_id, uint32_t max_group_id,
                      uint32_t group_inc_step, uint32_t shard_concurrency)
      : StoppableHandler(init_group_id, max_group_id, group_inc_step,
                         shard_concurrency),
        dispatcher_(shard_concurrency) {
    auto& executors = get_executors();
    CHECK(executors.size() >= StoppableHandler::shard_id());
    executors[StoppableHandler::shard_id()].reserve(shard_concurrency);
    hiactor::scope_builder builder;
    LOG(INFO) << "Creating stored proc handler on shard id: "
              << StoppableHandler::shard_id();
    builder.set_shard(StoppableHandler::shard_id())
        .enter_sub_scope(hiactor::scope<executor_group>(0))
        .enter_sub_scope(hiactor::scope<hiactor::actor_group>(
            StoppableHandler::cur_group_id_));
    for (unsigned i = 0; i < StoppableHandler::shard_concurrency_; ++i) {
      executors[StoppableHandler::shard_id()].emplace_back(
          builder.build_ref<executor_ref>(i));
    }
#ifdef HAVE_OPENTELEMETRY_CPP
    total_counter_ = otel::create_int_counter("hqps_procedure_query_total");
    latency_histogram_ =
        otel::create_double_histogram("hqps_procedure_query_latency");
#endif
  }
  ~stored_proc_handler() override = default;

  seastar::future<> stop() override {
    return StoppableHandler::cancel_scope(
        [this] { get_executors()[StoppableHandler::shard_id()].clear(); });
  }

  bool start() override {
    if (get_executors()[StoppableHandler::shard_id()].size() > 0) {
      LOG(ERROR) << "The actors have been already created!";
      return false;
    }
    return StoppableHandler::start_scope(
        [this](hiactor::scope_builder& builder) {
          for (unsigned i = 0; i < StoppableHandler::shard_concurrency_; ++i) {
            get_executors()[StoppableHandler::shard_id()].emplace_back(
                builder.build_ref<executor_ref>(i));
          }
        });
  }

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    auto dst_executor = dispatcher_.get_executor_idx();
    // TODO(zhanglei): choose read or write based on the request, after the
    // read/write info is supported in physical plan
    if (req->param.exists("graph_id") && req->param["graph_id"] != "current") {
      // TODO(zhanglei): get from graph_db.
      if (!is_running_graph(req->param["graph_id"])) {
        rep->set_status(
            seastar::httpd::reply::status_type::internal_server_error);
        rep->write_body("bin",
                        seastar::sstring("The querying query is not running:" +
                                         req->param["graph_id"]));
        rep->done();
        return seastar::make_ready_future<
            std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
      }
    }
    auto& method = req->_method;
    if (method == "POST") {
      if (path.find("vertex") != seastar::sstring::npos) {
        return get_executors()[StoppableHandler::shard_id()][dst_executor]
            .create_vertex(query_param{std::move(req->content)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      } else if (path.find("edge") != seastar::sstring::npos) {
        return get_executors()[StoppableHandler::shard_id()][dst_executor]
            .create_edge(query_param{std::move(req->content)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      }
    } else if (method == "GET") {
      if (path.find("vertex") != seastar::sstring::npos) {
        return get_executors()[StoppableHandler::shard_id()][dst_executor]
            .get_vertex(
                graph_management_query_param{std::move(req->query_parameters)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      } else if (path.find("edge") != seastar::sstring::npos) {
        return get_executors()[StoppableHandler::shard_id()][dst_executor]
            .get_edge(
                graph_management_query_param{std::move(req->query_parameters)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      }
    } else if (method == "DELETE") {
      if (path.find("vertex") != seastar::sstring::npos) {
        return get_executors()[StoppableHandler::shard_id()][dst_executor]
            .delete_vertex(query_param{std::move(req->content)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      } else if (path.find("edge") != seastar::sstring::npos) {
        return get_executors()[StoppableHandler::shard_id()][dst_executor]
            .delete_edge(query_param{std::move(req->content)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      }
    } else if (method == "PUT") {
      if (path.find("vertex") != seastar::sstring::npos) {
        return get_executors()[StoppableHandler::shard_id()][dst_executor]
            .update_vertex(query_param{std::move(req->content)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      } else if (path.find("edge") != seastar::sstring::npos) {
        return get_executors()[StoppableHandler::shard_id()][dst_executor]
            .update_edge(query_param{std::move(req->content)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      }
    }
    uint8_t last_byte;
    if (req->content.size() > 0) {
      // read last byte and get the format info from the byte.
      last_byte = req->content.back();
      if (last_byte >
          static_cast<uint8_t>(
              gs::GraphDBSession::InputFormat::kCypherProtoProcedure)) {
        LOG(ERROR) << "Unsupported request format: " << (int) last_byte;
        rep->set_status(
            seastar::httpd::reply::status_type::internal_server_error);
        rep->write_body("bin", seastar::sstring("Unsupported request format!"));
        rep->done();
        return seastar::make_ready_future<
            std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
      }
    } else {
      LOG(ERROR) << "Empty request content!";
      rep->set_status(
          seastar::httpd::reply::status_type::internal_server_error);
      rep->write_body("bin", seastar::sstring("Empty request content!"));
      rep->done();
      return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(
          std::move(rep));
    }

#ifdef HAVE_OPENTELEMETRY_CPP
    auto tracer = otel::get_tracer("hqps_procedure_query_handler");
    // Extract context from headers. This copy is necessary to avoid access
    // after header content been freed
    std::map<std::string, std::string> headers(req->_headers.begin(),
                                               req->_headers.end());
    auto current_ctx = opentelemetry::context::RuntimeContext::GetCurrent();
    auto options = otel::get_parent_ctx(current_ctx, headers);
    auto outer_span = tracer->StartSpan("procedure_query_handling", options);
    auto scope = tracer->WithActiveSpan(outer_span);
    auto start_ts = gs::GetCurrentTimeStamp();
#endif  // HAVE_OPENTELEMETRY_CPP

    return get_executors()[StoppableHandler::shard_id()][dst_executor]
        .run_graph_db_query(query_param{std::move(req->content)})
        .then([last_byte
#ifdef HAVE_OPENTELEMETRY_CPP
               ,
               this, outer_span = outer_span
#endif  // HAVE_OPENTELEMETRY_CPP
    ](auto&& output) {
          if (last_byte == static_cast<uint8_t>(
                               gs::GraphDBSession::InputFormat::kCppEncoder)) {
            return seastar::make_ready_future<query_param>(
                std::move(output.content));
          } else {
            // For cypher input format, the results are written with
            // output.put_string(), which will add extra 4 bytes. So we need
            // to remove the first 4 bytes here.
            if (output.content.size() < 4) {
              LOG(ERROR) << "Invalid output size: " << output.content.size();
#ifdef HAVE_OPENTELEMETRY_CPP
              outer_span->SetStatus(opentelemetry::trace::StatusCode::kError,
                                    "Invalid output size");
              outer_span->End();
              std::map<std::string, std::string> labels = {
                  { "status",
                    "fail" }};
              total_counter_->Add(1, labels);
#endif  // HAVE_OPENTELEMETRY_CPP
              return seastar::make_ready_future<query_param>(std::move(output));
            }
            return seastar::make_ready_future<query_param>(
                std::move(output.content.substr(4)));
          }
        })
        .then_wrapped([rep = std::move(rep)
#ifdef HAVE_OPENTELEMETRY_CPP
                           ,
                       this, outer_span, start_ts
#endif  // HAVE_OPENTELEMETRY_CPP
    ](seastar::future<query_result>&& fut) mutable {
          if (__builtin_expect(fut.failed(), false)) {
            try {
              std::rethrow_exception(fut.get_exception());
            } catch (std::exception& e) {
              // if the exception's message contains "Unable to send message",
              // then set the status to 503, otherwise set the status to 500
              if (std::string(e.what()).find(
                      StoppableHandler::ACTOR_SCOPE_CANCEL_MESSAGE) !=
                  std::string::npos) {
                rep->set_status(
                    seastar::httpd::reply::status_type::service_unavailable);
              } else {
                rep->set_status(
                    seastar::httpd::reply::status_type::internal_server_error);
              }
              rep->write_body("bin", seastar::sstring(e.what()));
            }
#ifdef HAVE_OPENTELEMETRY_CPP
            outer_span->SetStatus(opentelemetry::trace::StatusCode::kError,
                                  "Internal Server Error");
            outer_span->End();
            std::map<std::string, std::string> labels = {{ "status", "fail" }};
            total_counter_->Add(1, labels);
#endif  // HAVE_OPENTELEMETRY_CPP
            rep->done();
            return seastar::make_ready_future<
                std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
          }
          auto result = fut.get0();
          rep->write_body("bin", std::move(result.content));
#ifdef HAVE_OPENTELEMETRY_CPP
          outer_span->End();
          std::map<std::string, std::string> labels = {{ "status", "success" }};
          total_counter_->Add(1, labels);
          auto end_ts = gs::GetCurrentTimeStamp();
#if OPENTELEMETRY_ABI_VERSION_NO >= 2
          latency_histogram_->Record(end_ts - start_ts);
#else
          latency_histogram_->Record(end_ts - start_ts,
                                     opentelemetry::context::Context{});
#endif
#endif  // HAVE_OPENTELEMETRY_CPP
          rep->done();
          return seastar::make_ready_future<
              std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
        });
  }

 private:
  query_dispatcher dispatcher_;
#ifdef HAVE_OPENTELEMETRY_CPP
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Counter<uint64_t>>
      total_counter_;
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Histogram<double>>
      latency_histogram_;
#endif
};  // namespace server

class adhoc_runtime_query_handler : public StoppableHandler {
 public:
  static std::vector<std::vector<executor_ref>>& get_executors() {
    static std::vector<std::vector<executor_ref>> executor_refs;
    return executor_refs;
  }

  adhoc_runtime_query_handler(uint32_t init_group_id, uint32_t max_group_id,
                              uint32_t group_inc_step,
                              uint32_t shard_concurrency)
      : StoppableHandler(init_group_id, max_group_id, group_inc_step,
                         shard_concurrency),
        executor_idx_(0) {
    auto& executor_refs = get_executors();
    CHECK(executor_refs.size() >= StoppableHandler::shard_id());
    executor_refs[StoppableHandler::shard_id()].reserve(shard_concurrency_);
    {
      hiactor::scope_builder builder;
      builder.set_shard(StoppableHandler::shard_id())
          .enter_sub_scope(hiactor::scope<executor_group>(0))
          .enter_sub_scope(hiactor::scope<hiactor::actor_group>(init_group_id));
      for (unsigned i = 0; i < shard_concurrency_; ++i) {
        executor_refs[StoppableHandler::shard_id()].emplace_back(
            builder.build_ref<executor_ref>(i));
      }
    }
#ifdef HAVE_OPENTELEMETRY_CPP
    total_counter_ = otel::create_int_counter("hqps_adhoc_query_total");
    latency_histogram_ =
        otel::create_double_histogram("hqps_adhoc_query_latency");
#endif  // HAVE_OPENTELEMETRY_CPP
  }

  ~adhoc_runtime_query_handler() override = default;

  seastar::future<> stop() override {
    return StoppableHandler::cancel_scope([this] {
      LOG(INFO) << "Stopping adhoc actors on shard id: "
                << StoppableHandler::shard_id();
      get_executors()[StoppableHandler::shard_id()].clear();
    });
  }

  bool start() override {
    if (get_executors()[StoppableHandler::shard_id()].size() > 0) {
      LOG(ERROR) << "The actors have been already created!";
      return false;
    }
    return StoppableHandler::start_scope(
        [this](hiactor::scope_builder& builder) {
          for (unsigned i = 0; i < StoppableHandler::shard_concurrency_; ++i) {
            get_executors()[StoppableHandler::shard_id()].emplace_back(
                builder.build_ref<executor_ref>(i));
          }
        });
  }

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    auto dst_executor = executor_idx_;
    executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;

    if (path != "/v1/graph/current/adhoc_query" &&
        req->param.exists("graph_id")) {
      // TODO(zhanglei): get from graph_db.
      if (!is_running_graph(req->param["graph_id"])) {
        rep->set_status(
            seastar::httpd::reply::status_type::internal_server_error);
        rep->write_body("bin",
                        seastar::sstring("The querying query is not running:" +
                                         req->param["graph_id"]));
        rep->done();
        return seastar::make_ready_future<
            std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
      }
    }

#ifdef HAVE_OPENTELEMETRY_CPP
    auto tracer = otel::get_tracer("adhoc_runtime_query_handler");
    // Extract context from headers. This copy is necessary to avoid access
    // after header content been freed
    std::map<std::string, std::string> headers(req->_headers.begin(),
                                               req->_headers.end());
    auto current_ctx = opentelemetry::context::RuntimeContext::GetCurrent();
    auto options = otel::get_parent_ctx(current_ctx, headers);
    auto outer_span = tracer->StartSpan("adhoc_query_handling", options);
    auto scope = tracer->WithActiveSpan(outer_span);
    // create a new span for query execution, not started.
    auto start_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
#endif  // HAVE_OPENTELEMETRY_CPP

#ifdef HAVE_OPENTELEMETRY_CPP
    options.parent = outer_span->GetContext();
    auto query_span = tracer->StartSpan("adhoc_query_execution", options);
    auto query_scope = tracer->WithActiveSpan(query_span);
#endif  // HAVE_OPENTELEMETRY_CPP
        // TODO(zhanglei): choose read or write based on the request, after the
        //  read/write info is supported in physical plan
        // The content contains the path to dynamic library
    req->content.append(gs::Schema::ADHOC_READ_PLUGIN_ID_STR, 1);
    req->content.append(gs::GraphDBSession::kCypherProtoAdhocStr, 1);
    return get_executors()[StoppableHandler::shard_id()][dst_executor]
        .run_graph_db_query(query_param{std::move(req->content)})
        .then([
#ifdef HAVE_OPENTELEMETRY_CPP
                  query_span = query_span, query_scope = std::move(query_scope)
#endif  // HAVE_OPENTELEMETRY_CPP
    ](auto&& output) {
          return seastar::make_ready_future<query_param>(
              std::move(output.content));
        })
        .then_wrapped([rep = std::move(rep)
#ifdef HAVE_OPENTELEMETRY_CPP
                           ,
                       this, outer_span, start_ts
#endif  // HAVE_OPENTELEMETRY_CPP
    ](seastar::future<query_result>&& fut) mutable {
          if (__builtin_expect(fut.failed(), false)) {
            rep->set_status(
                seastar::httpd::reply::status_type::internal_server_error);
            try {
              std::rethrow_exception(fut.get_exception());
            } catch (std::exception& e) {
              rep->write_body("bin", seastar::sstring(e.what()));
            }
#ifdef HAVE_OPENTELEMETRY_CPP
            outer_span->SetStatus(opentelemetry::trace::StatusCode::kError,
                                  "Internal Server Error");
            outer_span->End();
            std::map<std::string, std::string> labels = {{ "status", "fail" }};
            total_counter_->Add(1, labels);
#endif  // HAVE_OPENTELEMETRY_CPP
            rep->done();
            return seastar::make_ready_future<
                std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
          }
          auto result = fut.get0();
          rep->write_body("bin", std::move(result.content));
#ifdef HAVE_OPENTELEMETRY_CPP
          outer_span->End();
          std::map<std::string, std::string> labels = {{ "status", "success" }};
          total_counter_->Add(1, labels);
          auto end_ts = gs::GetCurrentTimeStamp();
#if OPENTELEMETRY_ABI_VERSION_NO >= 2
          latency_histogram_->Record(end_ts - start_ts);
#else
          latency_histogram_->Record(end_ts - start_ts,
                                     opentelemetry::context::Context{});
#endif
#endif  // HAVE_OPENTELEMETRY_CPP
          rep->done();
          return seastar::make_ready_future<
              std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
        });
  }

 private:
  uint32_t executor_idx_;

#ifdef HAVE_OPENTELEMETRY_CPP
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Counter<uint64_t>>
      total_counter_;
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Histogram<double>>
      latency_histogram_;
#endif
};

class adhoc_query_handler : public StoppableHandler {
 public:
  static std::vector<std::vector<executor_ref>>& get_executors() {
    static std::vector<std::vector<executor_ref>> executor_refs;
    return executor_refs;
  }

  static std::vector<std::vector<codegen_actor_ref>>& get_codegen_actors() {
    static std::vector<std::vector<codegen_actor_ref>> codegen_actor_refs;
    return codegen_actor_refs;
  }

  adhoc_query_handler(uint32_t init_group_id, uint32_t max_group_id,
                      uint32_t group_inc_step, uint32_t shard_concurrency)
      : StoppableHandler(init_group_id, max_group_id, group_inc_step,
                         shard_concurrency),
        executor_idx_(0) {
    auto& executor_refs = get_executors();
    CHECK(executor_refs.size() >= StoppableHandler::shard_id());
    executor_refs[StoppableHandler::shard_id()].reserve(shard_concurrency_);
    {
      hiactor::scope_builder builder;
      builder.set_shard(StoppableHandler::shard_id())
          .enter_sub_scope(hiactor::scope<executor_group>(0))
          .enter_sub_scope(hiactor::scope<hiactor::actor_group>(init_group_id));
      for (unsigned i = 0; i < shard_concurrency_; ++i) {
        executor_refs[StoppableHandler::shard_id()].emplace_back(
            builder.build_ref<executor_ref>(i));
      }
    }
    auto& codegen_actor_refs = get_codegen_actors();
    CHECK(codegen_actor_refs.size() >= StoppableHandler::shard_id());
    {
      hiactor::scope_builder builder;
      builder.set_shard(StoppableHandler::shard_id())
          .enter_sub_scope(hiactor::scope<executor_group>(0))
          .enter_sub_scope(hiactor::scope<hiactor::actor_group>(init_group_id));
      codegen_actor_refs[StoppableHandler::shard_id()].emplace_back(
          builder.build_ref<codegen_actor_ref>(0));
    }
#ifdef HAVE_OPENTELEMETRY_CPP
    total_counter_ = otel::create_int_counter("hqps_adhoc_query_total");
    latency_histogram_ =
        otel::create_double_histogram("hqps_adhoc_query_latency");
#endif  // HAVE_OPENTELEMETRY_CPP
  }

  ~adhoc_query_handler() override = default;

  seastar::future<> stop() override {
    return StoppableHandler::cancel_scope([this] {
      LOG(INFO) << "Stopping adhoc actors on shard id: "
                << StoppableHandler::shard_id();
      get_executors()[StoppableHandler::shard_id()].clear();
      get_codegen_actors()[StoppableHandler::shard_id()].clear();
    });
  }

  bool start() override {
    if (get_executors()[StoppableHandler::shard_id()].size() > 0 ||
        get_codegen_actors()[StoppableHandler::shard_id()].size() > 0) {
      LOG(ERROR) << "The actors have been already created!";
      return false;
    }
    return StoppableHandler::start_scope(
        [this](hiactor::scope_builder& builder) {
          for (unsigned i = 0; i < StoppableHandler::shard_concurrency_; ++i) {
            get_executors()[StoppableHandler::shard_id()].emplace_back(
                builder.build_ref<executor_ref>(i));
          }
          get_codegen_actors()[StoppableHandler::shard_id()].emplace_back(
              builder.build_ref<codegen_actor_ref>(0));
        });
  }

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    auto dst_executor = executor_idx_;
    executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;
    if (path != "/v1/graph/current/adhoc_query" &&
        req->param.exists("graph_id")) {
      // TODO(zhanglei): get from graph_db.
      if (!is_running_graph(req->param["graph_id"])) {
        rep->set_status(
            seastar::httpd::reply::status_type::internal_server_error);
        rep->write_body("bin",
                        seastar::sstring("The querying query is not running:" +
                                         req->param["graph_id"]));
        rep->done();
        return seastar::make_ready_future<
            std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
      }
    }

#ifdef HAVE_OPENTELEMETRY_CPP
    auto tracer = otel::get_tracer("adhoc_query_handler");
    // Extract context from headers. This copy is necessary to avoid access
    // after header content been freed
    std::map<std::string, std::string> headers(req->_headers.begin(),
                                               req->_headers.end());
    auto current_ctx = opentelemetry::context::RuntimeContext::GetCurrent();
    auto options = otel::get_parent_ctx(current_ctx, headers);
    auto outer_span = tracer->StartSpan("adhoc_query_handling", options);
    auto scope = tracer->WithActiveSpan(outer_span);
    // Start a new span for codegen
    auto codegen_span = tracer->StartSpan("adhoc_codegen", options);
    auto codegen_scope = tracer->WithActiveSpan(codegen_span);
    // create a new span for query execution, not started.
    auto start_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
#endif  // HAVE_OPENTELEMETRY_CPP
    return get_codegen_actors()[StoppableHandler::shard_id()][0]
        .do_codegen(query_param{std::move(req->content)})
        .then([this, dst_executor
#ifdef HAVE_OPENTELEMETRY_CPP
               ,
               codegen_span = codegen_span, tracer = tracer, options = options,
               codegen_scope = std::move(codegen_scope), outer_span = outer_span
#endif  // HAVE_OPENTELEMETRY_CPP
    ](auto&& param) mutable {
#ifdef HAVE_OPENTELEMETRY_CPP
          codegen_span->End();
          options.parent = outer_span->GetContext();
          auto query_span = tracer->StartSpan("adhoc_query_execution", options);
          auto query_scope = tracer->WithActiveSpan(query_span);
#endif  // HAVE_OPENTELEMETRY_CPP
        // TODO(zhanglei): choose read or write based on the request, after the
        //  read/write info is supported in physical plan
        // The content contains the path to dynamic library
          param.content.append(gs::Schema::HQPS_ADHOC_READ_PLUGIN_ID_STR, 1);
          param.content.append(gs::GraphDBSession::kCypherProtoAdhocStr, 1);
          return get_executors()[StoppableHandler::shard_id()][dst_executor]
              .run_graph_db_query(query_param{std::move(param.content)})
              .then([
#ifdef HAVE_OPENTELEMETRY_CPP
                        query_span = query_span,
                        query_scope = std::move(query_scope)
#endif  // HAVE_OPENTELEMETRY_CPP
          ](auto&& output) {
#ifdef HAVE_OPENTELEMETRY_CPP
                query_span->End();
#endif  // HAVE_OPENTELEMETRY_CPP
                return seastar::make_ready_future<query_param>(
                    std::move(output.content));
              });
        })
        .then([
#ifdef HAVE_OPENTELEMETRY_CPP
                  this, outer_span = outer_span
#endif  // HAVE_OPENTELEMETRY_CPP
    ](auto&& output) {
          if (output.content.size() < 4) {
            LOG(ERROR) << "Invalid output size: " << output.content.size();
#ifdef HAVE_OPENTELEMETRY_CPP
            std::map<std::string, std::string> labels = {{ "status", "fail" }};
            total_counter_->Add(1, labels);
            outer_span->SetStatus(opentelemetry::trace::StatusCode::kError,
                                  "Internal output size");
            outer_span->End();
#endif  // HAVE_OPENTELEMETRY_CPP
            return seastar::make_ready_future<query_param>(std::move(output));
          }
          return seastar::make_ready_future<query_param>(
              std::move(output.content.substr(4)));
        })
        .then_wrapped([rep = std::move(rep)
#ifdef HAVE_OPENTELEMETRY_CPP
                           ,
                       this, outer_span, start_ts
#endif  // HAVE_OPENTELEMETRY_CPP
    ](seastar::future<query_result>&& fut) mutable {
          if (__builtin_expect(fut.failed(), false)) {
            try {
              std::rethrow_exception(fut.get_exception());
            } catch (std::exception& e) {
              // if the exception's message contains "Unable to send message",
              // then set the status to 503, otherwise set the status to 500
              if (std::string(e.what()).find(
                      StoppableHandler::ACTOR_SCOPE_CANCEL_MESSAGE) !=
                  std::string::npos) {
                rep->set_status(
                    seastar::httpd::reply::status_type::service_unavailable);
              } else {
                rep->set_status(
                    seastar::httpd::reply::status_type::internal_server_error);
              }
              rep->write_body("bin", seastar::sstring(e.what()));
#ifdef HAVE_OPENTELEMETRY_CPP
              std::map<std::string, std::string> labels = {
                  { "status",
                    "fail" }};
              total_counter_->Add(1, labels);
              outer_span->SetStatus(opentelemetry::trace::StatusCode::kError,
                                    "Internal Server Error");
              outer_span->SetAttribute(
                  "exception", opentelemetry::common::AttributeValue(e.what()));
              outer_span->End();
#endif  // HAVE_OPENTELEMETRY_CPP
            }
            rep->done();
            return seastar::make_ready_future<
                std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
          }
          auto result = fut.get0();
          rep->write_body("bin", std::move(result.content));
#ifdef HAVE_OPENTELEMETRY_CPP
          std::map<std::string, std::string> labels = {{ "status", "success" }};
          total_counter_->Add(1, labels);
          outer_span->End();
          auto end_ts = gs::GetCurrentTimeStamp();
#if OPENTELEMETRY_ABI_VERSION_NO >= 2
          latency_histogram_->Record(end_ts - start_ts);
#else
          latency_histogram_->Record(end_ts - start_ts,
                                     opentelemetry::context::Context{});
#endif
#endif  // HAVE_OPENTELEMETRY_CPP
          rep->done();
          return seastar::make_ready_future<
              std::unique_ptr<seastar::httpd::reply>>(std::move(rep));
        });
  }

 private:
  uint32_t executor_idx_;

#ifdef HAVE_OPENTELEMETRY_CPP
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Counter<uint64_t>>
      total_counter_;
  opentelemetry::nostd::unique_ptr<opentelemetry::metrics::Histogram<double>>
      latency_histogram_;
#endif
};

///////////////////////////graph_db_http_handler/////////////////////////////

graph_db_http_handler::graph_db_http_handler(uint16_t http_port,
                                             int32_t shard_num,
                                             bool enable_adhoc_handlers)
    : http_port_(http_port),
      enable_adhoc_handlers_(enable_adhoc_handlers),
      running_(false),
      actors_running_(true) {
  current_graph_query_handlers_.resize(shard_num);
  all_graph_query_handlers_.resize(shard_num);
  adhoc_query_handlers_.resize(shard_num);
  vertex_handlers_.resize(shard_num);
  edge_handlers_.resize(shard_num);
  if (enable_adhoc_handlers_) {
    adhoc_query_handler::get_executors().resize(shard_num);
    adhoc_query_handler::get_codegen_actors().resize(shard_num);
  } else {
    adhoc_runtime_query_handler::get_executors().resize(shard_num);
  }
  stored_proc_handler::get_executors().resize(shard_num);
}

graph_db_http_handler::~graph_db_http_handler() {
  if (is_running()) {
    stop();
  }
  // DO NOT DELETE the handler pointers, they will be deleted by
  // seastar::httpd::match_rule
}

uint16_t graph_db_http_handler::get_port() const { return http_port_; }

bool graph_db_http_handler::is_running() const { return running_.load(); }

bool graph_db_http_handler::is_actors_running() const {
  return actors_running_.load();
}

seastar::future<> graph_db_http_handler::stop_query_actors(size_t index) {
  if (index >= current_graph_query_handlers_.size()) {
    return seastar::make_ready_future<>();
  }
  return current_graph_query_handlers_[index]
      ->stop()
      .then([this, index] {
        LOG(INFO) << "Stopped current query actors on shard id: " << index;
        return all_graph_query_handlers_[index]->stop();
      })
      .then([this, index] {
        LOG(INFO) << "Stopped all query actors on shard id: " << index;
        if (enable_adhoc_handlers_.load()) {
          return adhoc_query_handlers_[index]->stop();
        }
        return seastar::make_ready_future<>();
      })
      .then([this, index] {
        // wait for vertex_handlers_ and edge_handlers_ to stop and ready
        std::vector<seastar::future<>> futures;
        for (size_t i = 0; i < vertex_handlers_[index].size(); ++i) {
          futures.push_back(vertex_handlers_[index][i]->stop());
          futures.push_back(edge_handlers_[index][i]->stop());
        }
        return seastar::when_all_succeed(futures.begin(), futures.end());
      })
      .then([this, index] {
        if (index + 1 == current_graph_query_handlers_.size()) {
          actors_running_.store(false);
          return seastar::make_ready_future<>();
        } else {
          return stop_query_actors(index + 1);
        }
      });
}

seastar::future<> graph_db_http_handler::stop_query_actors() {
  return stop_query_actors(0);
}

void graph_db_http_handler::start_query_actors() {
  // to start actors, call method on each handler
  for (size_t i = 0; i < current_graph_query_handlers_.size(); ++i) {
    current_graph_query_handlers_[i]->start();
    all_graph_query_handlers_[i]->start();
    for (size_t j = 0; j < vertex_handlers_[i].size(); ++j) {
      vertex_handlers_[i][j]->start();
      edge_handlers_[i][j]->start();
    }
    if (enable_adhoc_handlers_.load()) {
      adhoc_query_handlers_[i]->start();
    }
  }

  actors_running_.store(true);
}

void graph_db_http_handler::start() {
  auto fut = seastar::alien::submit_to(
      *seastar::alien::internal::default_instance, 0, [this] {
        return server_.start()
            .then([this] { return set_routes(); })
            .then([this] { return server_.listen(http_port_); })
            .then([this] {
              fmt::print("Http handler is listening on port {} ...\n",
                         http_port_);
              running_.store(true);
            });
      });
  fut.wait();
}

void graph_db_http_handler::stop() {
  auto fut = seastar::alien::submit_to(
      *seastar::alien::internal::default_instance, 0, [this] {
        LOG(INFO) << "Stopping http handler ...";
        return server_.stop();
      });
  fut.wait();
  // update running state
  running_.store(false);
}

seastar::future<> graph_db_http_handler::set_routes() {
  return server_.set_routes([this](seastar::httpd::routes& r) {
    // matches /v1/graph/current/query
    current_graph_query_handlers_[hiactor::local_shard_id()] =
        new stored_proc_handler(ic_query_group_id, max_group_id, group_inc_step,
                                shard_query_concurrency);
    r.put(seastar::httpd::operation_type::POST, "/v1/graph/current/query",
          current_graph_query_handlers_[hiactor::local_shard_id()]);

    // matches /v1/graph/{graph_id}/query
    all_graph_query_handlers_[hiactor::local_shard_id()] =
        new stored_proc_handler(ic_query_group_id, max_group_id, group_inc_step,
                                shard_query_concurrency);
    auto rule_proc = new seastar::httpd::match_rule(
        all_graph_query_handlers_[hiactor::local_shard_id()]);
    rule_proc->add_str("/v1/graph")
        .add_matcher(new seastar::httpd::optional_param_matcher("graph_id"))
        .add_str("/query");
    r.add(rule_proc, seastar::httpd::operation_type::POST);
    if (enable_adhoc_handlers_.load()) {
      auto adhoc_query_handler_ =
          new adhoc_query_handler(ic_adhoc_group_id, max_group_id,
                                  group_inc_step, shard_adhoc_concurrency);
      adhoc_query_handlers_[hiactor::local_shard_id()] = adhoc_query_handler_;
      // Add routes
      auto rule_adhoc = new seastar::httpd::match_rule(adhoc_query_handler_);
      rule_adhoc->add_str("/v1/graph")
          .add_matcher(new seastar::httpd::optional_param_matcher("graph_id"))
          .add_str("/adhoc_query");
      r.add(rule_adhoc, seastar::httpd::operation_type::POST);
    } else {
      auto adhoc_runtime_query_handler_ = new adhoc_runtime_query_handler(
          ic_adhoc_group_id, max_group_id, group_inc_step,
          shard_adhoc_concurrency);
      adhoc_query_handlers_[hiactor::local_shard_id()] =
          adhoc_runtime_query_handler_;
      // Add routes
      auto rule_adhoc =
          new seastar::httpd::match_rule(adhoc_runtime_query_handler_);
      rule_adhoc->add_str("/v1/graph")
          .add_matcher(new seastar::httpd::optional_param_matcher("graph_id"))
          .add_str("/adhoc_query");
      r.add(rule_adhoc, seastar::httpd::operation_type::POST);
    }
    for (size_t i = 0; i < NUM_OPERATION; ++i) {
      vertex_handlers_[hiactor::local_shard_id()][i] =
          new stored_proc_handler(ic_query_group_id, max_group_id,
                                  group_inc_step, shard_query_concurrency);
      edge_handlers_[hiactor::local_shard_id()][i] =
          new stored_proc_handler(ic_query_group_id, max_group_id,
                                  group_inc_step, shard_query_concurrency);
    }

    for (size_t i = 0; i < NUM_OPERATION; ++i) {
      // Add routes
      auto match_rule = new seastar::httpd::match_rule(
          vertex_handlers_[hiactor::local_shard_id()][i]);
      match_rule->add_str("/v1/graph")
          .add_matcher(new seastar::httpd::optional_param_matcher("graph_id"))
          .add_str("/vertex");
      r.add(match_rule, OPERATIONS[i]);
    }

    for (size_t i = 0; i < NUM_OPERATION; ++i) {
      // Add routes
      auto match_rule = new seastar::httpd::match_rule(
          edge_handlers_[hiactor::local_shard_id()][i]);
      match_rule->add_str("/v1/graph")
          .add_matcher(new seastar::httpd::optional_param_matcher("graph_id"))
          .add_str("/edge");
      r.add(match_rule, OPERATIONS[i]);
    }

    return seastar::make_ready_future<>();
  });
}

}  // namespace server

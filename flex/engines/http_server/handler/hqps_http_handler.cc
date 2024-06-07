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
#include "flex/engines/http_server/handler/hqps_http_handler.h"

#ifdef HAVE_OPENTELEMETRY_CPP
#include "opentelemetry/context/context.h"
#include "opentelemetry/trace/span_metadata.h"
#include "opentelemetry/trace/span_startoptions.h"
#endif  // HAVE_OPENTELEMETRY_CPP

#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/http_server/executor_group.actg.h"
#include "flex/engines/http_server/options.h"
#include "flex/engines/http_server/service/hqps_service.h"
#include "flex/engines/http_server/types.h"
#include "flex/otel/otel.h"

#include <seastar/core/when_all.hh>

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

hqps_ic_handler::hqps_ic_handler(uint32_t init_group_id, uint32_t max_group_id,
                                 uint32_t group_inc_step,
                                 uint32_t shard_concurrency)
    : cur_group_id_(init_group_id),
      max_group_id_(max_group_id),
      group_inc_step_(group_inc_step),
      shard_concurrency_(shard_concurrency),
      executor_idx_(0),
      is_cancelled_(false) {
  executor_refs_.reserve(shard_concurrency_);
  hiactor::scope_builder builder;
  builder.set_shard(hiactor::local_shard_id())
      .enter_sub_scope(hiactor::scope<executor_group>(0))
      .enter_sub_scope(hiactor::scope<hiactor::actor_group>(cur_group_id_));
  for (unsigned i = 0; i < shard_concurrency_; ++i) {
    executor_refs_.emplace_back(builder.build_ref<executor_ref>(i));
  }
#ifdef HAVE_OPENTELEMETRY_CPP
  total_counter_ = otel::create_int_counter("hqps_procedure_query_total");
  latency_histogram_ =
      otel::create_double_histogram("hqps_procedure_query_latency");
#endif
}

hqps_ic_handler::~hqps_ic_handler() = default;

seastar::future<> hqps_ic_handler::cancel_current_scope() {
  if (is_cancelled_) {
    LOG(INFO) << "The current scope has been already cancelled!";
    return seastar::make_ready_future<>();
  }
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
        is_cancelled_ = true;
        return seastar::make_ready_future<>();
      });
}

bool hqps_ic_handler::is_current_scope_cancelled() const {
  return is_cancelled_;
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
  is_cancelled_ = false;  // locked outside
  return true;
}

bool hqps_ic_handler::is_running_graph(const seastar::sstring& graph_id) const {
  std::string graph_id_str(graph_id.data(), graph_id.size());
  auto running_graph_res =
      HQPSService::get().get_metadata_store()->GetRunningGraph();
  if (!running_graph_res.ok()) {
    LOG(ERROR) << "Failed to get running graph: "
               << running_graph_res.status().error_message();
    return false;
  }
  return running_graph_res.value() == graph_id_str;
}

// Handles both /v1/graph/{graph_id}/query and /v1/graph/current/query/
seastar::future<std::unique_ptr<seastar::httpd::reply>> hqps_ic_handler::handle(
    const seastar::sstring& path, std::unique_ptr<seastar::httpd::request> req,
    std::unique_ptr<seastar::httpd::reply> rep) {
  auto dst_executor = executor_idx_;
  executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;
  // TODO(zhanglei): choose read or write based on the request, after the
  // read/write info is supported in physical plan
  auto request_format = req->get_header(INTERACTIVE_REQUEST_FORMAT);
  if (request_format.empty()) {
    // If no format specfied, we use default format: proto
    request_format = PROTOCOL_FORMAT;
  }
  if (request_format == JSON_FORMAT) {
    req->content.append(gs::GraphDBSession::kCypherJson, 1);
  } else if (request_format == PROTOCOL_FORMAT) {
    req->content.append(gs::GraphDBSession::kCypherInternalProcedure, 1);
  } else if (request_format == ENCODER_FORMAT) {
    req->content.append(gs::GraphDBSession::kCppEncoder, 1);
  } else {
    LOG(ERROR) << "Unsupported request format: " << request_format;
    rep->set_status(seastar::httpd::reply::status_type::internal_server_error);
    rep->write_body("bin", seastar::sstring("Unsupported request format!"));
    rep->done();
    return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(
        std::move(rep));
  }
  if (path != "/v1/graph/current/query" && req->param.exists("graph_id")) {
    // TODO(zhanglei): get from graph_db.
    if (!is_running_graph(req->param["graph_id"])) {
      rep->set_status(
          seastar::httpd::reply::status_type::internal_server_error);
      rep->write_body("bin",
                      seastar::sstring("The querying query is not running:" +
                                       req->param["graph_id"]));
      rep->done();
      return seastar::make_ready_future<std::unique_ptr<seastar::httpd::reply>>(
          std::move(rep));
    }
  }
#ifdef HAVE_OPENTELEMETRY_CPP
  auto tracer = otel::get_tracer("hqps_procedure_query_handler");
  // Extract context from headers. This copy is necessary to avoid access after
  // header content been freed
  std::map<std::string, std::string> headers(req->_headers.begin(),
                                             req->_headers.end());
  auto current_ctx = opentelemetry::context::RuntimeContext::GetCurrent();
  auto options = otel::get_parent_ctx(current_ctx, headers);
  auto outer_span = tracer->StartSpan("procedure_query_handling", options);
  auto scope = tracer->WithActiveSpan(outer_span);
  auto start_ts = gs::GetCurrentTimeStamp();
#endif  // HAVE_OPENTELEMETRY_CPP

  return executor_refs_[dst_executor]
      .run_graph_db_query(query_param{std::move(req->content)})
      .then([request_format
#ifdef HAVE_OPENTELEMETRY_CPP
             ,
             this, outer_span = outer_span
#endif  // HAVE_OPENTELEMETRY_CPP
  ](auto&& output) {
        if (request_format == ENCODER_FORMAT) {
          return seastar::make_ready_future<query_param>(
              std::move(output.content));
        } else {
          // For cypher input format, the results are written with
          // output.put_string(), which will add extra 4 bytes. So we need to
          // remove the first 4 bytes here.
          if (output.content.size() < 4) {
            LOG(ERROR) << "Invalid output size: " << output.content.size();
#ifdef HAVE_OPENTELEMETRY_CPP
            outer_span->SetStatus(opentelemetry::trace::StatusCode::kError,
                                  "Invalid output size");
            outer_span->End();
            std::map<std::string, std::string> labels = {{"status", "fail"}};
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
          std::map<std::string, std::string> labels = {{"status", "fail"}};
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
        std::map<std::string, std::string> labels = {{"status", "success"}};
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

// a handler to handle adhoc query.

hqps_adhoc_query_handler::hqps_adhoc_query_handler(
    uint32_t init_adhoc_group_id, uint32_t init_codegen_group_id,
    uint32_t max_group_id, uint32_t group_inc_step, uint32_t shard_concurrency)
    : cur_adhoc_group_id_(init_adhoc_group_id),
      cur_codegen_group_id_(init_codegen_group_id),
      max_group_id_(max_group_id),
      group_inc_step_(group_inc_step),
      shard_concurrency_(shard_concurrency),
      executor_idx_(0),
      is_cancelled_(false) {
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
#ifdef HAVE_OPENTELEMETRY_CPP
  total_counter_ = otel::create_int_counter("hqps_adhoc_query_total");
  latency_histogram_ =
      otel::create_double_histogram("hqps_adhoc_query_latency");
#endif  // HAVE_OPENTELEMETRY_CPP
}
hqps_adhoc_query_handler::~hqps_adhoc_query_handler() = default;

seastar::future<> hqps_adhoc_query_handler::cancel_current_scope() {
  if (is_cancelled_) {
    LOG(INFO) << "The current scope has been already cancelled!";
    return seastar::make_ready_future<>();
  }
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
        is_cancelled_ = true;
        return seastar::make_ready_future<>();
      });
}

bool hqps_adhoc_query_handler::is_current_scope_cancelled() const {
  return is_cancelled_;
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
  is_cancelled_ = false;
  return true;
}

seastar::future<std::unique_ptr<seastar::httpd::reply>>
hqps_adhoc_query_handler::handle(const seastar::sstring& path,
                                 std::unique_ptr<seastar::httpd::request> req,
                                 std::unique_ptr<seastar::httpd::reply> rep) {
  auto dst_executor = executor_idx_;
  executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;

#ifdef HAVE_OPENTELEMETRY_CPP
  auto tracer = otel::get_tracer("hqps_adhoc_query_handler");
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
  return codegen_actor_refs_[0]
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
        param.content.append(gs::Schema::HQPS_ADHOC_WRITE_PLUGIN_ID_STR, 1);
        param.content.append(gs::GraphDBSession::kCypherInternalAdhoc, 1);
        return executor_refs_[dst_executor]
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
          std::map<std::string, std::string> labels = {{"status", "fail"}};
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
          rep->set_status(
              seastar::httpd::reply::status_type::internal_server_error);
          try {
            std::rethrow_exception(fut.get_exception());
          } catch (std::exception& e) {
            rep->write_body("bin", seastar::sstring(e.what()));
#ifdef HAVE_OPENTELEMETRY_CPP
            std::map<std::string, std::string> labels = {{"status", "fail"}};
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
        std::map<std::string, std::string> labels = {{"status", "success"}};
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

hqps_http_handler::hqps_http_handler(uint16_t http_port, int32_t shard_num)
    : http_port_(http_port), actors_running_(true) {
  ic_handlers_.resize(shard_num);
  adhoc_query_handlers_.resize(shard_num);
}

hqps_http_handler::~hqps_http_handler() {
  if (is_running()) {
    stop();
  }
  // DO NOT DELETE the handler pointers, they will be deleted by
  // seastar::httpd::match_rule
}

uint16_t hqps_http_handler::get_port() const { return http_port_; }

bool hqps_http_handler::is_running() const { return running_.load(); }

bool hqps_http_handler::is_actors_running() const {
  return actors_running_.load();
}

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
  return ic_handlers_[hiactor::local_shard_id()]
      ->cancel_current_scope()
      .then([this] {
        LOG(INFO) << "Cancelled ic scope";
        return adhoc_query_handlers_[hiactor::local_shard_id()]
            ->cancel_current_scope();
      })
      .then([this] {
        LOG(INFO) << "Cancelled proc scope";
        actors_running_.store(false);
        return seastar::make_ready_future<>();
      });
}

void hqps_http_handler::start_query_actors() {
  ic_handlers_[hiactor::local_shard_id()]->create_actors();
  adhoc_query_handlers_[hiactor::local_shard_id()]->create_actors();
  actors_running_.store(true);
}

seastar::future<> hqps_http_handler::set_routes() {
  return server_.set_routes([this](seastar::httpd::routes& r) {
    auto ic_handler =
        new hqps_ic_handler(ic_query_group_id, max_group_id, group_inc_step,
                            shard_query_concurrency);
    auto adhoc_query_handler = new hqps_adhoc_query_handler(
        ic_adhoc_group_id, codegen_group_id, max_group_id, group_inc_step,
        shard_adhoc_concurrency);

    auto rule_proc = new seastar::httpd::match_rule(ic_handler);
    rule_proc->add_str("/v1/graph")
        .add_matcher(new seastar::httpd::optional_param_matcher("graph_id"))
        .add_str("/query");

    r.add(rule_proc, seastar::httpd::operation_type::POST);

    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/interactive/adhoc_query"), adhoc_query_handler);

    ic_handlers_[hiactor::local_shard_id()] = ic_handler;
    adhoc_query_handlers_[hiactor::local_shard_id()] = adhoc_query_handler;

    return seastar::make_ready_future<>();
  });
}

}  // namespace server

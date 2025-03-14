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
#include "flex/third_party/httplib.h"

#include <glog/logging.h>
#include <rapidjson/document.h>

namespace server {

// Only returns success if all results are success
// But currently, only one file uploading is supported.
admin_query_result generate_final_result(
    server::payload<std::vector<gs::Result<seastar::sstring>>>& result) {
  auto result_val = result.content;
  rapidjson::Document json_res(rapidjson::kObjectType);
  if (result_val.size() != 1) {
    LOG(INFO) << "Only one file uploading is supported";
    return admin_query_result{gs::Result<seastar::sstring>(
        gs::Status(gs::StatusCode::INTERNAL_ERROR,
                   "Only one file uploading is supported"))};
  }
  for (auto& res : result_val) {
    if (res.ok()) {
      json_res.AddMember("file_path", std::string(res.value().c_str()),
                         json_res.GetAllocator());
    } else {
      return admin_query_result{std::move(res)};
    }
  }
  return admin_query_result{
      gs::Result<seastar::sstring>(gs::rapidjson_stringify(json_res))};
}

inline bool parse_multipart_boundary(const seastar::sstring& content_type,
                                     seastar::sstring& boundary) {
  auto pos = content_type.find("boundary=");
  if (pos == std::string::npos) {
    return false;
  }
  boundary = content_type.substr(pos + 9);
  if (boundary.length() >= 2 && boundary[0] == '"' && boundary.back() == '"') {
    boundary = boundary.substr(1, boundary.size() - 2);
  }
  return !boundary.empty();
}

class admin_file_upload_handler_impl : public seastar::httpd::handler_base {
 public:
  admin_file_upload_handler_impl(uint32_t group_id, uint32_t shard_concurrency,
                                 int32_t exclusive_shard_id)
      : shard_concurrency_(shard_concurrency), executor_idx_(0) {
    admin_actor_refs_.reserve(shard_concurrency_);
    hiactor::scope_builder builder;
    builder
        .set_shard(exclusive_shard_id >= 0 ? exclusive_shard_id
                                           : hiactor::local_shard_id())
        .enter_sub_scope(hiactor::scope<executor_group>(0))
        .enter_sub_scope(hiactor::scope<hiactor::actor_group>(group_id));
    for (unsigned i = 0; i < shard_concurrency_; ++i) {
      admin_actor_refs_.emplace_back(builder.build_ref<admin_actor_ref>(i));
    }
  }
  ~admin_file_upload_handler_impl() override = default;

  seastar::future<server::payload<std::vector<gs::Result<seastar::sstring>>>>
  upload_file(std::vector<std::pair<seastar::sstring, seastar::sstring>>&&
                  file_name_and_contents,
              size_t cur_ind, uint32_t dst_executor,
              std::vector<gs::Result<seastar::sstring>>&& results) {
    if (cur_ind >= file_name_and_contents.size()) {
      VLOG(10) << "Successfully uploaded " << file_name_and_contents.size()
               << " files.";
      return seastar::make_ready_future<
          server::payload<std::vector<gs::Result<seastar::sstring>>>>(
          std::move(results));
    } else {
      return admin_actor_refs_[dst_executor]
          .upload_file(graph_management_param{
              std::move(file_name_and_contents[cur_ind])})
          .then_wrapped([this, dst_executor, cur_ind,
                         file_name_and_contents =
                             std::move(file_name_and_contents),
                         results =
                             std::move(results)](auto&& result_fut) mutable {
            auto result = result_fut.get0();
            auto result_val = result.content;
            if (result_val.ok()) {
              VLOG(10) << "Upload file success: "
                       << file_name_and_contents[cur_ind].first << ", "
                       << result_val.value();
            } else {
              LOG(ERROR) << "Upload file failed";
              return seastar::make_exception_future<
                  server::payload<std::vector<gs::Result<seastar::sstring>>>>(
                  std::runtime_error("Upload file failed: " +
                                     result_val.status().error_message()));
            }
            results.emplace_back(result_val);
            return upload_file(std::move(file_name_and_contents), cur_ind + 1,
                               dst_executor, std::move(results));
          });
    }
  }

  seastar::future<server::payload<gs::Result<seastar::sstring>>> upload_files(
      std::vector<std::pair<seastar::sstring, seastar::sstring>>&&
          file_name_and_contents,
      uint32_t dst_executor) {
    // upload each file in chain
    std::vector<gs::Result<seastar::sstring>> results;
    return upload_file(std::move(file_name_and_contents), 0, dst_executor,
                       std::move(results))
        .then([](auto&& results) {
          auto final_res = generate_final_result(results);
          return seastar::make_ready_future<admin_query_result>(
              std::move(final_res));
        });
  }

  std::vector<std::pair<seastar::sstring, seastar::sstring>>
  parse_multipart_form_data(const seastar::sstring& content,
                            const seastar::sstring& boundary) {
    std::vector<seastar::sstring> names, filenames, content_types, contents;
    httplib::detail::MultipartFormDataParser parser;
    parser.set_boundary(boundary);
    httplib::MultipartContentHeader header_callback =
        [&names, &filenames,
         &content_types](const httplib::MultipartFormData& header) {
          names.push_back(header.name);
          filenames.push_back(header.filename);
          content_types.push_back(header.content_type);
          return true;
        };
    httplib::ContentReceiver content_callback =
        [&contents](const char* data, size_t data_length) {
          contents.emplace_back(data, data_length);
          return true;
        };
    parser.parse(content.data(), content.size(), content_callback,
                 header_callback);
    VLOG(10) << "filestorage names:" << gs::to_string(names);
    VLOG(10) << "filenames: " << gs::to_string(filenames);
    VLOG(10) << "content types" << gs::to_string(content_types);
    std::vector<std::pair<seastar::sstring, seastar::sstring>> res;
    for (size_t i = 0; i < filenames.size(); ++i) {
      res.emplace_back(filenames[i], contents[i]);
    }
    return res;
  }

  seastar::future<std::unique_ptr<seastar::httpd::reply>> handle(
      const seastar::sstring& path,
      std::unique_ptr<seastar::httpd::request> req,
      std::unique_ptr<seastar::httpd::reply> rep) override {
    auto dst_executor = executor_idx_;

    executor_idx_ = (executor_idx_ + 1) % shard_concurrency_;
    LOG(INFO) << "Handling path:" << path << ", method: " << req->_method;
    auto& method = req->_method;
    if (method == "POST") {
      seastar::sstring boundary;
      if (!parse_multipart_boundary(req->_headers["Content-Type"], boundary)) {
        LOG(ERROR) << "Failed to parse boundary";
        return new_bad_request_reply(std::move(rep),
                                     "Failed to parse boundary");
      }
      std::vector<std::pair<seastar::sstring, seastar::sstring>>
          file_name_and_contents =
              parse_multipart_form_data(req->content, boundary);
      // upload for each file
      return upload_files(std::move(file_name_and_contents), dst_executor)
          .then_wrapped([rep = std::move(rep)](
                            seastar::future<admin_query_result>&& fut) mutable {
            return return_reply_with_result(std::move(rep), std::move(fut));
          });
    } else {
      return new_bad_request_reply(std::move(rep),
                                   "Unsupported method: " + method);
    }
  }

 private:
  const uint32_t shard_concurrency_;
  uint32_t executor_idx_;
  std::vector<admin_actor_ref> admin_actor_refs_;
};

/**
 * Handle all request for graph management.
 */
class admin_http_graph_handler_impl : public seastar::httpd::handler_base {
 public:
  admin_http_graph_handler_impl(uint32_t group_id, uint32_t shard_concurrency,
                                int32_t exclusive_shard_id)
      : shard_concurrency_(shard_concurrency), executor_idx_(0) {
    admin_actor_refs_.reserve(shard_concurrency_);
    hiactor::scope_builder builder;
    builder
        .set_shard(exclusive_shard_id >= 0 ? exclusive_shard_id
                                           : hiactor::local_shard_id())
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
    auto& method = req->_method;
    if (method == "POST") {
      if (path.find("dataloading") != seastar::sstring::npos) {
        LOG(INFO) << "Route to loading graph";
        if (!req->param.exists("graph_id")) {
          return new_bad_request_reply(std::move(rep), "graph_id not given");
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
        } else if (path.find("statistics") != seastar::sstring::npos) {
          return admin_actor_refs_[dst_executor]
              .run_get_graph_statistic(query_param{std::move(graph_id)})
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
        return new_bad_request_reply(std::move(rep), "graph_id not given");
      }
      auto graph_id = trim_slash(req->param.at("graph_id"));
      return admin_actor_refs_[dst_executor]
          .run_delete_graph(query_param{std::move(graph_id)})
          .then_wrapped([rep = std::move(rep)](
                            seastar::future<admin_query_result>&& fut) mutable {
            return return_reply_with_result(std::move(rep), std::move(fut));
          });
    } else {
      return new_bad_request_reply(std::move(rep),
                                   "Unsupported method: " + method);
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
                                    uint32_t shard_concurrency,
                                    int32_t exclusive_shard_id)
      : shard_concurrency_(shard_concurrency), executor_idx_(0) {
    admin_actor_refs_.reserve(shard_concurrency_);
    hiactor::scope_builder builder;
    builder
        .set_shard(exclusive_shard_id >= 0 ? exclusive_shard_id
                                           : hiactor::local_shard_id())
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
        return new_bad_request_reply(std::move(rep), "graph_id not given");
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
        return new_bad_request_reply(std::move(rep), "graph_id not given");
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
        return new_bad_request_reply(std::move(rep),
                                     "graph_id or procedure_id not given");
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
        return new_bad_request_reply(std::move(rep),
                                     "graph_id or procedure_id not given");
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
      return new_bad_request_reply(std::move(rep),
                                   "Unsupported method: " + req->_method);
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
  admin_http_service_handler_impl(uint32_t group_id, uint32_t shard_concurrency,
                                  int32_t exclusive_shard_id)
      : shard_concurrency_(shard_concurrency), executor_idx_(0) {
    admin_actor_refs_.reserve(shard_concurrency_);
    hiactor::scope_builder builder;
    builder
        .set_shard(exclusive_shard_id >= 0 ? exclusive_shard_id
                                           : hiactor::local_shard_id())
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
    auto& method = req->_method;
    if (method == "POST") {
      // Then param[action] should exists
      if (!req->param.exists("action")) {
        return new_bad_request_reply(std::move(rep), "action not given");
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
        return new_bad_request_reply(
            std::move(rep), std::string("Unsupported action: ") + action);
      }
    } else {
      // v1/service/ready or v1/service/status
      if (path.find("ready") != seastar::sstring::npos) {
        return admin_actor_refs_[dst_executor]
            .service_ready(query_param{std::move(req->content)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      } else {
        return admin_actor_refs_[dst_executor]
            .service_status(query_param{std::move(req->content)})
            .then_wrapped(
                [rep = std::move(rep)](
                    seastar::future<admin_query_result>&& fut) mutable {
                  return return_reply_with_result(std::move(rep),
                                                  std::move(fut));
                });
      }
    }
  }

 private:
  const uint32_t shard_concurrency_;
  uint32_t executor_idx_;
  std::vector<admin_actor_ref> admin_actor_refs_;
};

class admin_http_node_handler_impl : public seastar::httpd::handler_base {
 public:
  admin_http_node_handler_impl(uint32_t group_id, uint32_t shard_concurrency,
                               int32_t exclusive_shard_id)
      : shard_concurrency_(shard_concurrency), executor_idx_(0) {
    admin_actor_refs_.reserve(shard_concurrency_);
    hiactor::scope_builder builder;
    builder
        .set_shard(exclusive_shard_id >= 0 ? exclusive_shard_id
                                           : hiactor::local_shard_id())
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
      return new_bad_request_reply(std::move(rep),
                                   "Unsupported method: " + method);
    }
  }

 private:
  const uint32_t shard_concurrency_;
  uint32_t executor_idx_;
  std::vector<admin_actor_ref> admin_actor_refs_;
};

class admin_http_job_handler_impl : public seastar::httpd::handler_base {
 public:
  admin_http_job_handler_impl(uint32_t group_id, uint32_t shard_concurrency,
                              int32_t exclusive_shard_id)
      : shard_concurrency_(shard_concurrency), executor_idx_(0) {
    admin_actor_refs_.reserve(shard_concurrency_);
    hiactor::scope_builder builder;
    builder
        .set_shard(exclusive_shard_id >= 0 ? exclusive_shard_id
                                           : hiactor::local_shard_id())
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
        return new_bad_request_reply(std::move(rep), "job_id not given");
      }
      auto job_id = trim_slash(req->param.at("job_id"));
      return admin_actor_refs_[dst_executor]
          .cancel_job(query_param{std::move(job_id)})
          .then_wrapped([rep = std::move(rep)](
                            seastar::future<admin_query_result>&& fut) mutable {
            return return_reply_with_result(std::move(rep), std::move(fut));
          });
    } else {
      return new_bad_request_reply(std::move(rep),
                                   "Unsupported method: " + method);
    }
  }

 private:
  const uint32_t shard_concurrency_;
  uint32_t executor_idx_;
  std::vector<admin_actor_ref> admin_actor_refs_;
};

admin_http_handler::admin_http_handler(uint16_t http_port,
                                       int32_t exclusive_shard_id,
                                       size_t max_content_length)
    : http_port_(http_port),
      exclusive_shard_id_(exclusive_shard_id),
      max_content_length_(max_content_length) {}

void admin_http_handler::start() {
  auto fut = seastar::alien::submit_to(
      *seastar::alien::internal::default_instance, 0, [this] {
        return server_.start()
            .then([this] { return set_routes(); })
            .then([this] {
              server_.server().local().set_content_length_limit(
                  max_content_length_);
              return server_.listen(http_port_);
            })
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
  return server_.set_routes([&](seastar::httpd::routes& r) {
    ////Procedure management ///
    {
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_procedure_handler_impl(
              interactive_admin_group_id, shard_admin_concurrency,
              exclusive_shard_id_));
      match_rule->add_str("/v1/graph")
          .add_param("graph_id")
          .add_str("/procedure");
      // Get All procedures
      r.add(match_rule, seastar::httpd::operation_type::GET);
    }
    {
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_procedure_handler_impl(
              interactive_admin_group_id, shard_admin_concurrency,
              exclusive_shard_id_));
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
              interactive_admin_group_id, shard_admin_concurrency,
              exclusive_shard_id_));
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
              interactive_admin_group_id, shard_admin_concurrency,
              exclusive_shard_id_));
      match_rule->add_str("/v1/graph")
          .add_param("graph_id")
          .add_str("/procedure")
          .add_param("procedure_id");
      // Delete a procedure
      r.add(new seastar::httpd::match_rule(*match_rule), SEASTAR_DELETE);
    }
    {
      // Each procedure's handling
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_procedure_handler_impl(
              interactive_admin_group_id, shard_admin_concurrency,
              exclusive_shard_id_));
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
                                            shard_admin_concurrency,
                                            exclusive_shard_id_));
    // Create a new Graph
    r.add(seastar::httpd::operation_type::POST,
          seastar::httpd::url("/v1/graph"),
          new admin_http_graph_handler_impl(interactive_admin_group_id,
                                            shard_admin_concurrency,
                                            exclusive_shard_id_));

    // Delete a graph
    r.add(SEASTAR_DELETE,
          seastar::httpd::url("/v1/graph").remainder("graph_id"),
          new admin_http_graph_handler_impl(interactive_admin_group_id,
                                            shard_admin_concurrency,
                                            exclusive_shard_id_));
    {
      // uploading file to server
      r.add(seastar::httpd::operation_type::POST,
            seastar::httpd::url("/v1/file/upload"),
            new admin_file_upload_handler_impl(interactive_admin_group_id,
                                               shard_admin_concurrency,
                                               exclusive_shard_id_));
    }

    // Get graph metadata
    {
      // by setting full_path = false, we can match /v1/graph/{graph_id}/ and
      // /v1/graph/{graph_id}/schema
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_graph_handler_impl(
              interactive_admin_group_id, shard_admin_concurrency,
              exclusive_shard_id_));
      match_rule->add_str("/v1/graph").add_param("graph_id", false);
      // Get graph schema
      r.add(match_rule, seastar::httpd::operation_type::GET);
    }

    {  // load data to graph
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_graph_handler_impl(
              interactive_admin_group_id, shard_admin_concurrency,
              exclusive_shard_id_));
      match_rule->add_str("/v1/graph")
          .add_param("graph_id")
          .add_str("/dataloading");
      r.add(match_rule, seastar::httpd::operation_type::POST);
    }
    {  // Get Graph Schema
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_graph_handler_impl(
              interactive_admin_group_id, shard_admin_concurrency,
              exclusive_shard_id_));
      match_rule->add_str("/v1/graph").add_param("graph_id").add_str("/schema");
      r.add(match_rule, seastar::httpd::operation_type::GET);
    }
    {
      // Get running graph statistics
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_graph_handler_impl(
              interactive_admin_group_id, shard_admin_concurrency,
              exclusive_shard_id_));
      match_rule->add_str("/v1/graph")
          .add_param("graph_id")
          .add_str("/statistics");
      r.add(match_rule, seastar::httpd::operation_type::GET);
    }

    {
      // Node and service management
      r.add(seastar::httpd::operation_type::GET,
            seastar::httpd::url("/v1/node/status"),
            new admin_http_node_handler_impl(interactive_admin_group_id,
                                             shard_admin_concurrency,
                                             exclusive_shard_id_));

      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_service_handler_impl(
              interactive_admin_group_id, shard_admin_concurrency,
              exclusive_shard_id_));
      match_rule->add_str("/v1/service").add_param("action");
      r.add(match_rule, seastar::httpd::operation_type::POST);

      r.add(seastar::httpd::operation_type::GET,
            seastar::httpd::url("/v1/service/status"),
            new admin_http_service_handler_impl(interactive_admin_group_id,
                                                shard_admin_concurrency,
                                                exclusive_shard_id_));

      r.add(seastar::httpd::operation_type::GET,
            seastar::httpd::url("/v1/service/ready"),
            new admin_http_service_handler_impl(interactive_admin_group_id,
                                                shard_admin_concurrency,
                                                exclusive_shard_id_));
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
      test_handler = r.get_handler(SEASTAR_DELETE,
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
                                            shard_admin_concurrency,
                                            exclusive_shard_id_));
      auto match_rule =
          new seastar::httpd::match_rule(new admin_http_job_handler_impl(
              interactive_admin_group_id, shard_admin_concurrency,
              exclusive_shard_id_));

      match_rule->add_str("/v1/job").add_param("job_id");
      r.add(match_rule, seastar::httpd::operation_type::GET);

      r.add(SEASTAR_DELETE, seastar::httpd::url("/v1/job").remainder("job_id"),
            new admin_http_job_handler_impl(interactive_admin_group_id,
                                            shard_admin_concurrency,
                                            exclusive_shard_id_));
    }

    return seastar::make_ready_future<>();
  });
}

}  // namespace server

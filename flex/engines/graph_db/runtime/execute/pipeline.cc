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

#include "flex/engines/graph_db/runtime/execute/pipeline.h"

namespace gs {
namespace runtime {

bl::result<Context> ReadPipeline::Execute(
    const GraphReadInterface& graph, Context&& ctx,
    const std::map<std::string, std::string>& params, OprTimer& timer) {
  for (auto& opr : operators_) {
    gs::Status status = gs::Status::OK();
    auto ret = bl::try_handle_all(
        [&]() -> bl::result<Context> {
          return opr->Eval(graph, params, std::move(ctx), timer);
        },
        [&status](const gs::Status& err) {
          status = err;
          return Context();
        },
        [&](const bl::error_info& err) {
          status = gs::Status(gs::StatusCode::INTERNAL_ERROR,
                              "Error: " + std::to_string(err.error().value()) +
                                  ", Exception: " + err.exception()->what());
          return Context();
        },
        [&]() {
          status = gs::Status(gs::StatusCode::UNKNOWN, "Unknown error");
          return Context();
        });

    if (!status.ok()) {
      std::stringstream ss;
      ss << "[Execute Failed] " << opr->get_operator_name()
         << " execute failed: " << status.ToString();
      auto err = gs::Status(gs::StatusCode::INTERNAL_ERROR, ss.str());
      return bl::new_error(err);
    }
    ctx = std::move(ret);
  }
  return ctx;
}

bl::result<WriteContext> InsertPipeline::Execute(
    GraphInsertInterface& graph, WriteContext&& ctx,
    const std::map<std::string, std::string>& params, OprTimer& timer) {
  for (auto& opr : operators_) {
    gs::Status status = gs::Status::OK();
    auto ret = bl::try_handle_all(
        [&]() -> bl::result<WriteContext> {
          return opr->Eval(graph, params, std::move(ctx), timer);
        },
        [&status](const gs::Status& err) {
          status = err;
          return WriteContext();
        },
        [&](const bl::error_info& err) {
          status = gs::Status(gs::StatusCode::INTERNAL_ERROR,
                              "Error: " + std::to_string(err.error().value()) +
                                  ", Exception: " + err.exception()->what());
          return WriteContext();
        },
        [&]() {
          status = gs::Status(gs::StatusCode::UNKNOWN, "Unknown error");
          return WriteContext();
        });

    if (!status.ok()) {
      std::stringstream ss;
      ss << "[Execute Failed] " << opr->get_operator_name()
         << " execute failed: " << status.ToString();
      auto err = gs::Status(gs::StatusCode::INTERNAL_ERROR, ss.str());
      return bl::new_error(err);
    }

    ctx = std::move(ret);
  }
  return ctx;
}

bl::result<Context> UpdatePipeline::Execute(
    GraphUpdateInterface& graph, Context&& ctx,
    const std::map<std::string, std::string>& params, OprTimer& timer) {
  for (auto& opr : operators_) {
    gs::Status status = gs::Status::OK();
    auto ret = bl::try_handle_all(
        [&]() -> bl::result<Context> {
          return opr->Eval(graph, params, std::move(ctx), timer);
        },
        [&status](const gs::Status& err) {
          status = err;
          return Context();
        },
        [&](const bl::error_info& err) {
          status = gs::Status(gs::StatusCode::INTERNAL_ERROR,
                              "Error: " + std::to_string(err.error().value()) +
                                  ", Exception: " + err.exception()->what());
          return Context();
        },
        [&]() {
          status = gs::Status(gs::StatusCode::UNKNOWN, "Unknown error");
          return Context();
        });

    if (!status.ok()) {
      std::stringstream ss;
      ss << "[Execute Failed] " << opr->get_operator_name()
         << " execute failed: " << status.ToString();
      auto err = gs::Status(gs::StatusCode::INTERNAL_ERROR, ss.str());
      return bl::new_error(err);
    }
    ctx = std::move(ret);
  }
  return ctx;
}

bl::result<WriteContext> UpdatePipeline::Execute(
    GraphInsertInterface& graph, WriteContext&& ctx,
    const std::map<std::string, std::string>& params, OprTimer& timer) {
  for (auto& opr : insert_oprs_) {
    gs::Status status = gs::Status::OK();
    auto ret = bl::try_handle_all(
        [&]() -> bl::result<WriteContext> {
          return opr->Eval(graph, params, std::move(ctx), timer);
        },
        [&status](const gs::Status& err) {
          status = err;
          return WriteContext();
        },
        [&](const bl::error_info& err) {
          status = gs::Status(gs::StatusCode::INTERNAL_ERROR,
                              "Error: " + std::to_string(err.error().value()) +
                                  ", Exception: " + err.exception()->what());
          return WriteContext();
        },
        [&]() {
          status = gs::Status(gs::StatusCode::UNKNOWN, "Unknown error");
          return WriteContext();
        });

    if (!status.ok()) {
      std::stringstream ss;
      ss << "[Execute Failed] " << opr->get_operator_name()
         << " execute failed: " << status.ToString();
      auto err = gs::Status(gs::StatusCode::INTERNAL_ERROR, ss.str());
      return bl::new_error(err);
    }

    ctx = std::move(ret);
  }
  return ctx;
}

}  // namespace runtime

}  // namespace gs

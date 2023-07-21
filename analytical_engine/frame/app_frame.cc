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

// Include _GRAPH_HEADER at begin
#define DO_QUOTE(X) #X
#define QUOTE(X) DO_QUOTE(X)
#if defined(_GRAPH_TYPE) && defined(_GRAPH_HEADER)
#include QUOTE(_GRAPH_HEADER)
#else
#error "Missing macro _GRAPH_TYPE or _GRAPH_HEADER"
#endif

#include <memory>
#include <utility>

#include "arrow/api.h"

#include "grape/app/context_base.h"

#if defined __has_include
#if __has_include("vineyard/graph/fragment/arrow_fragment_modifier.h")
#include "vineyard/graph/fragment/arrow_fragment_modifier.h"
#endif
#endif

#include "core/app/app_invoker.h"
#include "core/error.h"
#include "frame/ctx_wrapper_builder.h"

#if defined(_APP_TYPE) && defined(_APP_HEADER)
#include QUOTE(_APP_HEADER)
#else
#error "Missing macro _APP_TYPE or _APP_HEADER"
#endif

namespace bl = boost::leaf;

/**
 * app_frame.cc is designed to serve for building apps as a library. The library
 * provides CreateWorker, Query, and DeleteWorker functions to be invoked by the
 * grape instance. The library will be loaded when a CREATE_APP request arrived
 * on the analytical engine. Then multiple query requests can be emitted based
 * on worker instance. Finally, a UNLOAD_APP request should be submitted to
 * release the resources.
 */
typedef struct worker_handler {
  std::shared_ptr<typename _APP_TYPE::worker_t> worker;
} worker_handler_t;

namespace detail {
__attribute__((visibility("hidden"))) void* CreateWorker(
    const std::shared_ptr<void>& fragment, const grape::CommSpec& comm_spec,
    const grape::ParallelEngineSpec& spec) {
  auto app = std::make_shared<_APP_TYPE>();
  auto* worker_handler = static_cast<worker_handler_t*>(new worker_handler_t);
  worker_handler->worker = _APP_TYPE::CreateWorker(
      app, std::static_pointer_cast<_APP_TYPE::fragment_t>(fragment));
  worker_handler->worker->Init(comm_spec, spec);
  return worker_handler;
}

__attribute__((visibility("hidden"))) std::nullptr_t DeleteWorker(
    void* worker_handler) {
  auto* handler = static_cast<worker_handler_t*>(worker_handler);

  handler->worker->Finalize();
  handler->worker.reset();
  delete handler;

  return nullptr;
}

__attribute__((visibility("hidden"))) bl::result<std::nullptr_t> Query(
    void* worker_handler, const gs::rpc::QueryArgs& query_args,
    const std::string& context_key,
    std::shared_ptr<gs::IFragmentWrapper> frag_wrapper,
    std::shared_ptr<gs::IContextWrapper>& ctx_wrapper) {
  auto worker = static_cast<worker_handler_t*>(worker_handler)->worker;
  auto result = gs::AppInvoker<_APP_TYPE>::Query(worker, query_args);
  if (result) {
    if (!context_key.empty()) {
      auto ctx = worker->GetContext();
      ctx_wrapper = gs::CtxWrapperBuilder<typename _APP_TYPE::context_t>::build(
          context_key, frag_wrapper, ctx);
    }
  }
  return result;
}

}  // namespace detail

extern "C" {
void* CreateWorker(const std::shared_ptr<void>& fragment,
                   const grape::CommSpec& comm_spec,
                   const grape::ParallelEngineSpec& spec) {
  void* worker_handler = nullptr;
  __FRAME_CATCH_AND_LOG_GS_ERROR(
      worker_handler, detail::CreateWorker(fragment, comm_spec, spec));
  return worker_handler;
}

void DeleteWorker(void* worker_handler) {
  std::nullptr_t result = nullptr;
  __FRAME_CATCH_AND_LOG_GS_ERROR(result, detail::DeleteWorker(worker_handler));
}

void Query(void* worker_handler, const gs::rpc::QueryArgs& query_args,
           const std::string& context_key,
           std::shared_ptr<gs::IFragmentWrapper> frag_wrapper,
           std::shared_ptr<gs::IContextWrapper>& ctx_wrapper,
           bl::result<nullptr_t>& wrapper_error) {
  __FRAME_CATCH_AND_ASSIGN_GS_ERROR(
      wrapper_error, detail::Query(worker_handler, query_args, context_key,
                                   frag_wrapper, ctx_wrapper));
}
}  // extern "C"

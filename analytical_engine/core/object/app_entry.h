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

#ifndef ANALYTICAL_ENGINE_CORE_OBJECT_APP_ENTRY_H_
#define ANALYTICAL_ENGINE_CORE_OBJECT_APP_ENTRY_H_

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "boost/leaf/error.hpp"
#include "boost/leaf/result.hpp"

#include "core/object/gs_object.h"
#include "core/utils/lib_utils.h"

namespace bl = boost::leaf;

namespace grape {
class CommSpec;
struct ParallelEngineSpec;
}  // namespace grape

namespace gs {
class IContextWrapper;
class IFragmentWrapper;
namespace rpc {
class QueryArgs;
}

typedef void* CreateWorkerT(const std::shared_ptr<void>& fragment,
                            const grape::CommSpec& comm_spec,
                            const grape::ParallelEngineSpec& spec);

typedef void DeleteWorkerT(void* worker_handler);

typedef void QueryT(void* worker_handler, const rpc::QueryArgs& query_args,
                    const std::string& context_key,
                    std::shared_ptr<IFragmentWrapper> frag_wrapper,
                    std::shared_ptr<IContextWrapper>& ctx_wrapper,
                    bl::result<std::nullptr_t>& wrapper_error);

/**
 * @brief AppEntry is a class manages an application.
 *
 * AppEntry holds the a group of function pointers to manipulate the
 * AppFrame, such as gs::CreateWorker, DeleteWorker and Query. The method Init
 * must be called to load the library before use.
 */
class AppEntry : public GSObject {
 public:
  AppEntry(std::string id, std::string lib_path)
      : GSObject(std::move(id), ObjectType::kAppEntry),
        lib_path_(std::move(lib_path)),
        dl_handle_(nullptr),
        create_worker_(nullptr),
        delete_worker_(nullptr),
        query_(nullptr) {}

  bl::result<void> Init() {
    { BOOST_LEAF_ASSIGN(dl_handle_, open_lib(lib_path_.c_str())); }
    {
      BOOST_LEAF_AUTO(p_fun,
                      get_func_ptr(lib_path_, dl_handle_, "CreateWorker"));
      create_worker_ = reinterpret_cast<CreateWorkerT*>(p_fun);
    }
    {
      BOOST_LEAF_AUTO(p_fun,
                      get_func_ptr(lib_path_, dl_handle_, "DeleteWorker"));
      delete_worker_ = reinterpret_cast<DeleteWorkerT*>(p_fun);
    }
    {
      BOOST_LEAF_AUTO(p_fun, get_func_ptr(lib_path_, dl_handle_, "Query"));
      query_ = reinterpret_cast<QueryT*>(p_fun);
    }
    return {};
  }

  bl::result<std::shared_ptr<void>> CreateWorker(
      const std::shared_ptr<void>& fragment, const grape::CommSpec& comm_spec,
      const grape::ParallelEngineSpec& spec) {
    return std::shared_ptr<void>(create_worker_(fragment, comm_spec, spec),
                                 delete_worker_);
  }

  bl::result<std::shared_ptr<IContextWrapper>> Query(
      void* worker_handler, const rpc::QueryArgs& query_args,
      const std::string& context_key,
      std::shared_ptr<IFragmentWrapper>& frag_wrapper) {
    std::shared_ptr<IContextWrapper> ctx_wrapper;
    bl::result<std::nullptr_t> wrapper_error;
    query_(worker_handler, query_args, context_key, frag_wrapper, ctx_wrapper,
           wrapper_error);
    if (!wrapper_error) {
      return std::move(wrapper_error);
    }
    return ctx_wrapper;
  }

 private:
  std::string lib_path_;
  void* dl_handle_;
  CreateWorkerT* create_worker_;
  DeleteWorkerT* delete_worker_;
  QueryT* query_;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_OBJECT_APP_ENTRY_H_

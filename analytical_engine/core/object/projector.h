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

#ifndef ANALYTICAL_ENGINE_CORE_OBJECT_PROJECTOR_H_
#define ANALYTICAL_ENGINE_CORE_OBJECT_PROJECTOR_H_

#include <iosfwd>
#include <memory>
#include <string>
#include <utility>

#include "boost/leaf/error.hpp"
#include "boost/leaf/result.hpp"

#include "core/object/gs_object.h"
#include "core/server/rpc_utils.h"
#include "core/utils/lib_utils.h"

namespace bl = boost::leaf;

namespace gs {
class IFragmentWrapper;

typedef void ProjectT(
    std::shared_ptr<IFragmentWrapper>& wrapper_in,
    const std::string& projected_graph_name, const rpc::GSParams& params,
    bl::result<std::shared_ptr<IFragmentWrapper>>& wrapper_out);

/**
 * @brief Projector is a invoker of the project_frame library. A method
 * called "Project" can project a property (with/without label) fragment to a
 * simple fragment.
 */
class Projector : public GSObject {
 public:
  explicit Projector(std::string id, std::string lib_path)
      : GSObject(std::move(id), ObjectType::kProjectUtils),
        lib_path_(std::move(lib_path)),
        dl_handle_(nullptr),
        project_func_(nullptr) {}

  bl::result<void> Init() {
    { BOOST_LEAF_ASSIGN(dl_handle_, open_lib(lib_path_.c_str())); }
    {
      BOOST_LEAF_AUTO(project_fun,
                      get_func_ptr(lib_path_, dl_handle_, "Project"));
      project_func_ = reinterpret_cast<ProjectT*>(project_fun);
    }
    return {};
  }

  bl::result<std::shared_ptr<IFragmentWrapper>> Project(
      std::shared_ptr<IFragmentWrapper>& wrapper_in,
      const std::string& projected_graph_name, const rpc::GSParams& params) {
    bl::result<std::shared_ptr<IFragmentWrapper>> wrapper_out;
    project_func_(wrapper_in, projected_graph_name, params, wrapper_out);
    return wrapper_out;
  }

 private:
  std::string lib_path_;
  void* dl_handle_;
  ProjectT* project_func_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_OBJECT_PROJECTOR_H_

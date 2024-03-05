/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_FLASH_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_FLASH_CONTEXT_H_

#include <memory>
#include <vector>

#include "flash/flash_ware.h"
#include "grape/grape.h"

namespace gs {

/**
 * @brief The context that manages the result for each vertex of Flash apps.
 *
 * @tparam FRAG_T
 * @tparam VALUE_T
 * @tparam RESULT_T
 */
template <typename FRAG_T, typename VALUE_T, typename RESULT_T>
class FlashVertexDataContext
    : public grape::VertexDataContext<FRAG_T, RESULT_T> {
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using result_t = RESULT_T;
  using fw_t = FlashWare<FRAG_T, VALUE_T>;

 public:
  explicit FlashVertexDataContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, RESULT_T>(fragment, true),
        result(this->data()) {}

  template <typename APP_T>
  void SetResult(const std::shared_ptr<fw_t> fw, std::shared_ptr<APP_T> app) {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      vid_t id = fw->Lid2Key(v.GetValue());
      this->result[v] = *(app->Res(fw->Get(id)));
    }
  }

  void Output(std::ostream& os) {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      os << frag.GetId(v) << " " << this->result[v] << std::endl;
    }
  }

  typename FRAG_T::template vertex_array_t<result_t>& result;
};

/**
 * @brief The context that manages the global result of FLASH apps.
 *
 * @tparam FRAG_T
 * @tparam VALUE_T
 * @tparam RESULT_T
 */
template <typename FRAG_T, typename VALUE_T, typename RESULT_T>
class FlashGlobalDataContext : public TensorContext<FRAG_T, RESULT_T> {
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using result_t = RESULT_T;
  using fw_t = FlashWare<FRAG_T, VALUE_T>;

 public:
  explicit FlashGlobalDataContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, RESULT_T>(fragment) {}

  template <typename APP_T>
  void SetResult(const std::shared_ptr<fw_t> fw, std::shared_ptr<APP_T> app) {
    this->result = app->GlobalRes();
    std::vector<size_t> shape{1};
    this->set_shape(shape);
    this->assign(this->result);
  }

  void Output(std::ostream& os) {
    auto& frag = this->fragment();
    if (frag.fid() == 0) {
      os << this->result << std::endl;
    }
  }

  result_t result;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_FLASH_CONTEXT_H_

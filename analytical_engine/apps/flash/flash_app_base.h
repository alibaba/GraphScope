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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_FLASH_APP_BASE_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_FLASH_APP_BASE_H_

#include <memory>

#include "flash/api.h"
#include "grape/types.h"

namespace gs {

/**
 * @brief FlashAppBase is a base class for Flash apps.
 *
 * @tparam FRAG_T
 * @tparam VALUE_T
 */
template <typename FRAG_T, typename VALUE_T>
class FlashAppBase {
 public:
  static constexpr bool need_split_edges = false;
  static constexpr bool need_split_edges_by_fragment = false;
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongEdgeToOuterVertex;
  using fw_t = FlashWare<FRAG_T, VALUE_T>;
  bool sync_all_ = false;

  FlashAppBase() = default;
  virtual ~FlashAppBase() = default;

  void Res(VALUE_T* v) { LOG(ERROR) << "Vertex result not defined.\n"; }

  void GlobalRes() { LOG(ERROR) << "Global result not defined.\n"; }

  template <class... Args>
  void Run(const FRAG_T& graph, const std::shared_ptr<fw_t> fw,
           Args&&... args) {}
};

#define INSTALL_FLASH_WORKER(APP_T, VALUE_T, FRAG_T)              \
 public:                                                          \
  using fragment_t = FRAG_T;                                      \
  using value_t = VALUE_T;                                        \
  using worker_t = FlashWorker<APP_T>;                            \
  using vid_t = typename fragment_t::vid_t;                       \
  using oid_t = typename fragment_t::oid_t;                       \
  using vertex_t = typename fragment_t::vertex_t;                 \
  using vdata_t = typename fragment_t::vdata_t;                   \
  using edata_t = typename fragment_t::edata_t;                   \
  using adj_list_t = typename fragment_t::adj_list_t;             \
  using vset_t = VertexSubset<fragment_t, value_t>;               \
  using fw_t = FlashWare<fragment_t, value_t>;                    \
  virtual ~APP_T() {}                                             \
  static std::shared_ptr<worker_t> CreateWorker(                  \
      std::shared_ptr<APP_T> app, std::shared_ptr<FRAG_T> frag) { \
    return std::shared_ptr<worker_t>(new worker_t(app, frag));    \
  }

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_FLASH_APP_BASE_H_

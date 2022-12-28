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

#ifndef ANALYTICAL_ENGINE_CORE_APP_PARALLEL_PROPERTY_APP_BASE_H_
#define ANALYTICAL_ENGINE_CORE_APP_PARALLEL_PROPERTY_APP_BASE_H_

#include <memory>

#include "grape/types.h"

#include "core/worker/parallel_property_worker.h"  // IWYU pragma: export

namespace gs {

class ParallelPropertyMessageManager;

/**
 * @brief ParallelPropertyAppBase is a base class for apps on property graph,
 * with a ParallelPropertyMessageManager. Users can process messages in a more
 * flexible way in this kind of app. The ParallelPropertyMessageManager enables
 * send/receive messages during computation. This strategy improves performance
 * by overlapping the communication time and the evaluation time.
 *
 * @tparam FRAG_T
 * @tparam CONTEXT_T
 */
template <typename FRAG_T, typename CONTEXT_T>
class ParallelPropertyAppBase {
 public:
  static constexpr bool need_split_edges = false;
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kSyncOnOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kOnlyOut;

  using message_manager_t = ParallelPropertyMessageManager;

  ParallelPropertyAppBase() = default;
  virtual ~ParallelPropertyAppBase() = default;

  /**
   * @brief Partial evaluation to implement.
   * @note: This pure virtual function works as an interface, instructing users
   * to implement in the specific app. The PEval in the inherited apps would be
   * invoked directly, not via virtual functions.
   *
   * @param graph
   * @param context
   * @param messages
   */
  virtual void PEval(const FRAG_T& graph, CONTEXT_T& context,
                     message_manager_t& messages) = 0;

  /**
   * @brief Incremental evaluation to implement.
   *
   * @note: This pure virtual function works as an interface, instructing users
   * to implement in the specific app. The IncEval in the inherited apps would
   * be invoked directly, not via virtual functions.
   *
   * @param graph
   * @param context
   * @param messages
   */
  virtual void IncEval(const FRAG_T& graph, CONTEXT_T& context,
                       message_manager_t& messages) = 0;
};

#define INSTALL_PARALLEL_PROPERTY_WORKER(APP_T, CONTEXT_T, FRAG_T) \
 public:                                                           \
  using fragment_t = FRAG_T;                                       \
  using context_t = CONTEXT_T;                                     \
  using message_manager_t = gs::ParallelPropertyMessageManager;    \
  using worker_t = gs::ParallelPropertyWorker<APP_T>;              \
  virtual ~APP_T() {}                                              \
  static std::shared_ptr<worker_t> CreateWorker(                   \
      std::shared_ptr<APP_T> app, std::shared_ptr<FRAG_T> frag) {  \
    return std::shared_ptr<worker_t>(new worker_t(app, frag));     \
  }

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_APP_PARALLEL_PROPERTY_APP_BASE_H_

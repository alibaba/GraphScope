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

#ifndef ANALYTICAL_ENGINE_CORE_WORKER_PROPERTY_AUTO_WORKER_H_
#define ANALYTICAL_ENGINE_CORE_WORKER_PROPERTY_AUTO_WORKER_H_

#include <mpi.h>

#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>

#include "grape/communication/communicator.h"
#include "grape/config.h"
#include "grape/parallel/parallel_engine.h"
#include "grape/util.h"
#include "grape/worker/comm_spec.h"

#include "core/parallel/property_auto_message_manager.h"

namespace gs {

template <typename FRAG_T, typename CONTEXT_T>
class PropertyAutoAppBase;

/**
 * @brief A Worker manages the computation cycle. PropertyAutoWorker is a kind
 * of worker for apps derived from PropertyAutoAppBase.
 *
 * @tparam APP_T
 */
template <typename APP_T>
class PropertyAutoWorker {
  static_assert(std::is_base_of<PropertyAutoAppBase<typename APP_T::fragment_t,
                                                    typename APP_T::context_t>,
                                APP_T>::value,
                "PropertyAutoWorker should work with AutoApp");

 public:
  using fragment_t = typename APP_T::fragment_t;
  using context_t = typename APP_T::context_t;

  using message_manager_t =
      PropertyAutoMessageManager<typename APP_T::fragment_t>;

  PropertyAutoWorker(std::shared_ptr<APP_T> app,
                     std::shared_ptr<fragment_t> graph)
      : app_(app), context_(std::make_shared<context_t>(*graph)) {}
  ~PropertyAutoWorker() = default;

  void Init(const grape::CommSpec& comm_spec,
            const grape::ParallelEngineSpec& pe_spec =
                grape::DefaultParallelEngineSpec()) {
    // auto& graph = const_cast<fragment_t&>(context_->fragment());
    // prepare for the query
    // graph_.PrepareToRunApp(APP_T::message_strategy,
    // APP_T::need_split_edges);

    comm_spec_ = comm_spec;
    MPI_Barrier(comm_spec_.comm());

    messages_.Init(comm_spec_.comm());

    grape::InitParallelEngine(app_, pe_spec);
    grape::InitCommunicator(app_, comm_spec_.comm());
  }

  void Finalize() {}

  template <class... Args>
  void Query(Args&&... args) {
    double t = grape::GetCurrentTime();

    auto& graph = context_->fragment();

    MPI_Barrier(comm_spec_.comm());

    context_->Init(messages_, std::forward<Args>(args)...);

    int round = 0;

    messages_.Start();

    messages_.StartARound();

    app_->PEval(graph, *context_);

    messages_.FinishARound();

    if (comm_spec_.worker_id() == grape::kCoordinatorRank) {
      VLOG(1) << "[Coordinator]: Finished PEval, time: "
              << grape::GetCurrentTime() - t << " sec";
    }

    int step = 1;

    while (!messages_.ToTerminate()) {
      t = grape::GetCurrentTime();
      round++;
      messages_.StartARound();

      app_->IncEval(graph, *context_);

      messages_.FinishARound();

      if (comm_spec_.worker_id() == grape::kCoordinatorRank) {
        VLOG(1) << "[Coordinator]: Finished IncEval - " << step
                << ", time: " << grape::GetCurrentTime() - t << " sec";
      }
      ++step;
    }

    MPI_Barrier(comm_spec_.comm());

    messages_.Finalize();
  }

  std::shared_ptr<context_t> GetContext() { return context_; }

  void Output(std::ostream& os) { context_->Output(os); }

 private:
  std::shared_ptr<APP_T> app_;
  std::shared_ptr<context_t> context_;
  message_manager_t messages_;

  grape::CommSpec comm_spec_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_WORKER_PROPERTY_AUTO_WORKER_H_

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

#ifndef ANALYTICAL_ENGINE_CORE_WORKER_DEFAULT_WORKER_H_
#define ANALYTICAL_ENGINE_CORE_WORKER_DEFAULT_WORKER_H_

#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "grape/communication/communicator.h"
#include "grape/communication/sync_comm.h"
#include "grape/parallel/default_message_manager.h"
#include "grape/parallel/parallel_engine.h"
#include "grape/util.h"

namespace gs {

template <typename FRAG_T, typename CONTEXT_T>
class AppBase;

template <typename FRAG_T>
class JavaContextBase;

/**
 * @brief DefaultWorker manages the computation cycle. DefaultWorker is a kind
 * of serial worker for apps derived from AppBase.
 *
 * @tparam APP_T
 */
template <typename APP_T>
class DefaultWorker {
  static_assert(std::is_base_of<AppBase<typename APP_T::fragment_t,
                                        typename APP_T::context_t>,
                                APP_T>::value,
                "DefaultWorker should work with App");

 public:
  using fragment_t = typename APP_T::fragment_t;
  using context_t = typename APP_T::context_t;

  using message_manager_t = grape::DefaultMessageManager;

  DefaultWorker(std::shared_ptr<APP_T> app, std::shared_ptr<fragment_t> graph)
      : app_(app), context_(std::make_shared<context_t>(*graph)) {}

  ~DefaultWorker() = default;

  void Init(const grape::CommSpec& comm_spec,
            const grape::ParallelEngineSpec& pe_spec =
                grape::DefaultParallelEngineSpec()) {
    auto& graph = const_cast<fragment_t&>(context_->fragment());

    // prepare for the query
    grape::PrepareConf conf;
    conf.message_strategy = APP_T::message_strategy;
    conf.need_split_edges = APP_T::need_split_edges;
    conf.need_mirror_info = false;
    graph.PrepareToRunApp(comm_spec, conf);

    comm_spec_ = comm_spec;

    MPI_Barrier(comm_spec_.comm());

    messages_.Init(comm_spec_.comm());

    InitParallelEngine(app_, pe_spec);
    grape::InitCommunicator(app_, comm_spec.comm());
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

    app_->PEval(graph, *context_, messages_);

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

      app_->IncEval(graph, *context_, messages_);

      messages_.FinishARound();

      if (comm_spec_.worker_id() == grape::kCoordinatorRank) {
        VLOG(1) << "[Coordinator]: Finished IncEval - " << step
                << ", time: " << grape::GetCurrentTime() - t << " sec";
      }
      ++step;
    }

    MPI_Barrier(comm_spec_.comm());

    messages_.Finalize();
    finishQuery();
  }

  std::shared_ptr<context_t> GetContext() { return context_; }

  void Output(std::ostream& os) { context_->Output(os); }

 private:
  template <typename T = context_t>
  typename std::enable_if<
      std::is_base_of<JavaContextBase<fragment_t>, T>::value>::type
  finishQuery() {
    auto java_context =
        std::dynamic_pointer_cast<JavaContextBase<fragment_t>>(context_);
    if (java_context) {
      VLOG(1) << "Write java heap data back to cpp context since it is java "
                 "context";
      java_context->WriteBackJVMHeapToCppContext();
    }
  }

  template <typename T = context_t>
  typename std::enable_if<
      !std::is_base_of<JavaContextBase<fragment_t>, T>::value>::type
  finishQuery() {}

  std::shared_ptr<APP_T> app_;
  std::shared_ptr<context_t> context_;
  message_manager_t messages_;

  grape::CommSpec comm_spec_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_WORKER_DEFAULT_WORKER_H_

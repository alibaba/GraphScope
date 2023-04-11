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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_FLASH_WORKER_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_FLASH_WORKER_H_

#include <mpi.h>

#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>

#include "flash/flash_app_base.h"
#include "flash/flash_ware.h"
#include "flash/vertex_subset.h"

namespace gs {

/**
 * @brief A worker manages the computation flow of Flash.
 *
 * @tparam APP_T
 */
template <typename APP_T>
class FlashWorker {
  static_assert(std::is_base_of<FlashAppBase<typename APP_T::fragment_t,
                                             typename APP_T::value_t>,
                                APP_T>::value,
                "FlashWorker should work with App");

 public:
  using fragment_t = typename APP_T::fragment_t;
  using value_t = typename APP_T::value_t;
  using vertex_t = typename APP_T::vertex_t;
  using vid_t = typename APP_T::vid_t;
  using context_t = typename APP_T::context_t;
  using fw_t = FlashWare<fragment_t, value_t>;

  FlashWorker(std::shared_ptr<APP_T> app, std::shared_ptr<fragment_t> graph)
      : app_(app),
        context_(std::make_shared<context_t>(*graph)),
        graph_(graph) {
    prepare_conf_.message_strategy = APP_T::message_strategy;
    prepare_conf_.need_split_edges = APP_T::need_split_edges;
    prepare_conf_.need_split_edges_by_fragment =
        APP_T::need_split_edges_by_fragment;
  }
  ~FlashWorker() = default;

  void Init(const grape::CommSpec& comm_spec,
            const grape::ParallelEngineSpec& pe_spec =
                grape::DefaultParallelEngineSpec()) {
    graph_->PrepareToRunApp(comm_spec_, prepare_conf_);
    comm_spec_ = comm_spec;
    MPI_Barrier(comm_spec_.comm());

    fw_ = std::make_shared<fw_t>();
    fw_->InitFlashWare(comm_spec_, app_->sync_all_, graph_);
  }

  void Finalize() {}

  template <class... Args>
  void Query(Args&&... args) {
    fw_->Start();
    app_->Run(*graph_, fw_, std::forward<Args>(args)...);
    context_->template SetResult<APP_T>(fw_, app_);
    fw_->Terminate();
  }

  std::shared_ptr<context_t> GetContext() { return context_; }

  const grape::TerminateInfo& GetTerminateInfo() const {
    return fw_->messages_.GetTerminateInfo();
  }

 private:
  std::shared_ptr<APP_T> app_;
  std::shared_ptr<context_t> context_;
  std::shared_ptr<fragment_t> graph_;
  std::shared_ptr<fw_t> fw_;
  grape::CommSpec comm_spec_;
  grape::PrepareConf prepare_conf_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_FLASH_WORKER_H_

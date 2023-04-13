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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_MEASUREMENT_MSF_BLOCK_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_MEASUREMENT_MSF_BLOCK_H_

#include <memory>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class MSFBlockFlash : public FlashAppBase<FRAG_T, EMPTY_TYPE> {
 public:
  INSTALL_FLASH_WORKER(MSFBlockFlash<FRAG_T>, EMPTY_TYPE, FRAG_T)
  using context_t = FlashGlobalDataContext<FRAG_T, EMPTY_TYPE, double>;
  using E = std::pair<edata_t, std::pair<vid_t, vid_t>>;

  bool sync_all_ = false;
  double wt = 0;

  double GlobalRes() { return wt; }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run MSF with Flash, total vertices: " << n_vertex
              << std::endl;

    std::vector<E> edges, mst0(n_vertex - 1), mst;
    TraverseLocal(for_out(edges.push_back(
        std::make_pair(weight, std::make_pair(id, nb_id)));););

    Block(kruskal(edges, mst0.data(), n_vertex); Reduce(
              mst0, mst, std::vector<E> edges; edges.assign(mst0, mst0 + len);
              edges.insert(edges.end(), mst, mst + len);
              kruskal(edges, mst, len + 1)));

    for (auto& e : mst)
      wt += e.first;
    LOG(INFO) << "mst weight " << wt << std::endl;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_MEASUREMENT_MSF_BLOCK_H_

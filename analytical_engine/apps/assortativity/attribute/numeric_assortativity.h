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

Author: Ning Xin
*/

#ifndef ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_ATTRIBUTE_NUMERIC_ASSORTATIVITY_H_
#define ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_ATTRIBUTE_NUMERIC_ASSORTATIVITY_H_

#include <numeric>
#include <unordered_map>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "apps/assortativity/attribute/attribute_assortativity_context.h"
#include "apps/assortativity/attribute/attribute_common.h"
#include "apps/assortativity/utils.h"
#include "core/app/app_base.h"
#include "core/worker/default_worker.h"

namespace gs {
/**
 * @brief Compute the attribute assortativity for graph.
 *  Assortativity measures the similarity of connections in the graph with
 * respect to the attribute.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class NumericAssortativity
    : public AppBase<FRAG_T, AttributeAssortativityContext<FRAG_T>>,
      public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(NumericAssortativity<FRAG_T>,
                         AttributeAssortativityContext<FRAG_T>, FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  using oid_t = typename fragment_t::oid_t;
  using vdata_t = typename fragment_t::vdata_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      ProcessVertex<vdata_t>(v, frag, ctx, messages);
    }
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    if (!ctx.merge_stage) {
      UpdateAttributeMixingMap<vertex_t, vdata_t>(frag, ctx, messages);
    } else {
      // merge in worker 0
      if (frag.fid() == 0) {
        MergeAttributeMixingMap<vdata_t>(ctx, messages);
        std::vector<std::vector<double>> attribute_mixing_matrix;
        std::unordered_map<int, double> map;
        getAttributeMixingMatrix(ctx, attribute_mixing_matrix, map);
        // compute numeric assortativity
        ctx.attribute_assortativity =
            ProcessMatrix(attribute_mixing_matrix, map);

        std::vector<size_t> shape{1};
        ctx.set_shape(shape);
        // write result to ctx
        ctx.assign(ctx.attribute_assortativity);
        VLOG(0) << "attribute assortatity: " << ctx.attribute_assortativity
                << std::endl;
      }
    }
  }

 private:
  /**
   * @brief get attribute mixing matrix by attribute mixing map
   *
   * @param ctx
   * @param[out] attribute_mixing_matrix
   * @param[out] map index -> data of a node
   */
  void getAttributeMixingMatrix(
      context_t& ctx, std::vector<std::vector<double>>& attribute_mixing_matrix,
      std::unordered_map<int, double>& map) {
    int total_edge_num = 0;
    // <data, index> pair, index:{0, 1, ..., n}
    std::unordered_map<vdata_t, int> index_map;
    int count = 0;
    for (auto& pair1 : ctx.attribute_mixing_map) {
      for (auto& pair2 : pair1.second) {
        if (index_map.count(pair1.first) == 0) {
          index_map[pair1.first] = count;
          map[count] = static_cast<double>(pair1.first);
          count++;
        }
        if (index_map.count(pair2.first) == 0) {
          index_map[pair2.first] = count;
          map[count] = static_cast<double>(pair2.first);
          count++;
        }
        total_edge_num += pair2.second;
      }
    }
    int n = index_map.size();
    std::vector<std::vector<double>> tmp(n, std::vector<double>(n, 0.0));
    attribute_mixing_matrix = move(tmp);
    for (auto& pair1 : ctx.attribute_mixing_map) {
      for (auto& pair2 : pair1.second) {
        int row = index_map[pair1.first];
        int column = index_map[pair2.first];
        attribute_mixing_matrix[row][column] =
            pair2.second / static_cast<double>(total_edge_num);
      }
    }
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_ATTRIBUTE_NUMERIC_ASSORTATIVITY_H_

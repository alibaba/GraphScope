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

#ifndef ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_ATTRIBUTE_ATTRIBUTE_ASSORTATIVITY_H_
#define ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_ATTRIBUTE_ATTRIBUTE_ASSORTATIVITY_H_

#include <unordered_map>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "apps/assortativity/attribute/attribute_assortativity_context.h"
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
class AttributeAssortativity
    : public AppBase<FRAG_T, AttributeAssortativityContext<FRAG_T>>,
      public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(AttributeAssortativity<FRAG_T>,
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
      processVertex(v, frag, ctx, messages);
    }
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    if (!ctx.merge_stage) {
      vdata_t source_data;
      vertex_t u;
      while (messages.GetMessage(frag, u, source_data)) {
        vdata_t target_data = frag.GetData(u);
        attributeMixingCount(source_data, target_data, ctx);
      }
      ctx.merge_stage = true;
      // send message to work 0
      if (frag.fid() != 0) {
        messages.SendToFragment(0, ctx.attribute_mixing_map);
      }
      messages.ForceContinue();
    } else {
      // merge in work 0
      if (frag.fid() == 0) {
        std::unordered_map<vdata_t, std::unordered_map<vdata_t, int>> msg;
        while (messages.GetMessage(msg)) {
          for (auto& pair1 : msg) {
            for (auto& pair2 : pair1.second) {
              // merge
              if (ctx.attribute_mixing_map.count(pair1.first) == 0 ||
                  ctx.attribute_mixing_map[pair1.first].count(pair2.first) ==
                      0) {
                ctx.attribute_mixing_map[pair1.first][pair2.first] =
                    pair2.second;
              } else {
                ctx.attribute_mixing_map[pair1.first][pair2.first] +=
                    pair2.second;
              }
            }
          }
        }
        std::vector<std::vector<double>> attribute_mixing_matrix;
        getAttributeMixingMatrix(ctx, attribute_mixing_matrix);
        ctx.attribute_assortativity =
            processAttributeMatrix(attribute_mixing_matrix);
        // write result to ctx
        std::vector<size_t> shape{1};
        ctx.set_shape(shape);
        ctx.assign(ctx.attribute_assortativity);
        VLOG(0) << "attribute assortatity: " << ctx.attribute_assortativity
                << std::endl;
      }
    }
  }

 private:
  /**
   * @brief traverse the outgoing neighbors of vertex v and update the
   * attribute-attribute pairs.
   *
   * @param v
   * @param frag
   * @param ctx
   * @param messages
   */
  void processVertex(const vertex_t& v, const fragment_t& frag, context_t& ctx,
                     message_manager_t& messages) {
    vdata_t source_data = frag.GetData(v);
    // get all neighbors of vertex v
    auto oes = frag.GetOutgoingAdjList(v);
    for (auto& e : oes) {
      vertex_t neighbor = e.get_neighbor();
      if (frag.IsOuterVertex(neighbor)) {
        messages.SyncStateOnOuterVertex(frag, neighbor, source_data);
      } else {
        vdata_t target_data = frag.GetData(neighbor);
        attributeMixingCount(source_data, target_data, ctx);
      }
    }
  }

  /**
   * @brief Compute assortativity for attribute matrix attribute_mixing_matrix
   *
   * @param attribute_mixing_matrix n x n matrix
   * @return attribute assortativity
   */
  double processAttributeMatrix(
      std::vector<std::vector<double>>& attribute_mixing_matrix) {
    int n = attribute_mixing_matrix.size();
    std::vector<double> a;
    // sum of column
    for (auto& row : attribute_mixing_matrix) {
      a.emplace_back(accumulate(row.begin(), row.end(), 0.0));
    }
    std::vector<double> b;
    // sum of row
    for (int i = 0; i < n; i++) {
      double sum = 0.0;
      for (int j = 0; j < n; j++) {
        sum += attribute_mixing_matrix[j][i];
      }
      b.emplace_back(sum);
    }
    double sum_eii = 0.0, sum_ai_bi = 0.0;
    for (int i = 0; i < n; i++) {
      sum_eii += attribute_mixing_matrix[i][i];
      sum_ai_bi += a[i] * b[i];
    }
    return (sum_eii - sum_ai_bi) / (1 - sum_ai_bi);
  }

  /**
   * @brief count the attribute-attribute pairs
   *
   * @param source_data the data of source node
   * @param target_data the data of target node
   * @param ctx
   */
  inline void attributeMixingCount(vdata_t source_data, vdata_t target_data,
                                   context_t& ctx) {
    if (ctx.attribute_mixing_map.count(source_data) == 0 ||
        ctx.attribute_mixing_map[source_data].count(target_data) == 0) {
      ctx.attribute_mixing_map[source_data][target_data] = 1;
    } else {
      ctx.attribute_mixing_map[source_data][target_data] += 1;
    }
  }

  /**
   * @brief get attribute mixing matrix by attribute mixing map
   *
   * @param ctx
   * @param[out] attribute_mixing_matrix
   */
  void getAttributeMixingMatrix(
      context_t& ctx,
      std::vector<std::vector<double>>& attribute_mixing_matrix) {
    int total_edge_num = 0;
    // data-index map, index:{0, 1, ..., n}
    std::unordered_map<vdata_t, int> property_map;
    int count = 0;
    for (auto& pair1 : ctx.attribute_mixing_map) {
      for (auto& pair2 : pair1.second) {
        if (property_map.count(pair1.first) == 0) {
          property_map[pair1.first] = count;
          count++;
        }
        if (property_map.count(pair2.first) == 0) {
          property_map[pair2.first] = count;
          count++;
        }
        total_edge_num += pair2.second;
      }
    }
    int n = property_map.size();
    std::vector<std::vector<double>> tmp(n, std::vector<double>(n, 0.0));
    attribute_mixing_matrix.swap(tmp);
    for (auto& pair1 : ctx.attribute_mixing_map) {
      for (auto& pair2 : pair1.second) {
        int row = property_map[pair1.first];
        int column = property_map[pair2.first];
        attribute_mixing_matrix[row][column] =
            pair2.second / static_cast<double>(total_edge_num);
      }
    }
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_ASSORTATIVITY_ATTRIBUTE_ATTRIBUTE_ASSORTATIVITY_H_

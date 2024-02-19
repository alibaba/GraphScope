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

#ifndef ANALYTICAL_ENGINE_APPS_PREGEL_LOUVAIN_LOUVAIN_H_
#define ANALYTICAL_ENGINE_APPS_PREGEL_LOUVAIN_LOUVAIN_H_

#include <map>
#include <set>
#include <utility>
#include <vector>

#include "apps/pregel/louvain/auxiliary.h"
#include "apps/pregel/louvain/louvain_vertex.h"
#include "core/app/pregel/i_vertex_program.h"
#include "core/app/pregel/pregel_compute_context.h"

namespace gs {

/*
 * Distribute-louvain algorithm description:
 * phase-1
 * 0. Each vertex receives community values from its community hub
 *    and sends its own community to its neighbors.
 * 1. Each vertex determines if it should move to a neighboring community or not
 *    and sends its information to its community hub.
 * 2. Each community hub re-calculates community totals and sends the updates
 *    to each community member.
 * repeat phase 1 process until a local maxima of the modularity attained.
 * phase-2
 *  -2 Community hub calls its member to gather the community sigma tot.
 *  -1 Compress each community such that they are represented by one node.
 *
 * reapply the phase-1 process to the new graph.
 *
 * The passes are iterated until there are no more changes and a maximum of
 * modularity is attained.
 *
 * References:
 * https://sotera.github.io/distributed-graph-analytics/louvain/
 * https://github.com/Sotera/distributed-graph-analytics
 */

template <typename FRAG_T>
class PregelLouvain
    : public IPregelProgram<
          LouvainVertex<FRAG_T, typename FRAG_T::oid_t,
                        LouvainMessage<typename FRAG_T::vid_t>>,
          PregelComputeContext<FRAG_T, typename FRAG_T::oid_t,
                               LouvainMessage<typename FRAG_T::vid_t>>> {
 public:
  using fragment_t = FRAG_T;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using vd_t = oid_t;
  using md_t = LouvainMessage<vid_t>;
  using compute_context_t = PregelComputeContext<fragment_t, vd_t, md_t>;
  using pregel_vertex_t = LouvainVertex<fragment_t, vd_t, md_t>;
  using state_t = LouvainNodeState<vid_t>;
  using edata_t = typename pregel_vertex_t::edata_t;

 public:
  void Init(pregel_vertex_t& v, compute_context_t& context) override {
    state_t& state = v.state();
    edata_t sigma_total = 0.0;
    for (auto& e : v.outgoing_edges()) {
      sigma_total += static_cast<edata_t>(e.get_data());
    }

    state.community = v.get_gid();
    state.community_sigma_total = sigma_total + state.internal_weight;
    state.node_weight = sigma_total;
    state.is_from_louvain_vertex_reader = true;
    state.nodes_in_community.push_back(state.community);
  }

  void Compute(grape::IteratorPair<md_t*> messages, pregel_vertex_t& v,
               compute_context_t& context) override {
    state_t& state = v.state();

    int current_super_step = context.superstep();
    // the minor step in phase 1
    int current_minor_step = current_super_step % 3;
    // the current iteration, two iterations make a full pass.
    int current_iteration = current_super_step / 3;

    if (current_super_step == phase_two_start_step) {
      sendCommunitiesInfo(v);
      return;
    } else if (current_super_step == compress_community_step) {
      compressCommunities(v, messages);
      return;
    }

    // count the total edge weight of the graph on the phase-1 start only
    if (current_super_step == phase_one_start_step) {
      if (!state.is_from_louvain_vertex_reader) {
        // not from the disk but from the previous round's result
        state.community = v.get_gid();
        state.node_weight = 0;
        // It must use fake edges since we already set them last round.
        for (auto& e : v.fake_edges()) {
          state.node_weight += e.second;
        }
      }
      state.reset_total_edge_weight = true;
      v.context()->local_total_edge_weight()[v.tid()] +=
          state.node_weight + state.internal_weight;
    }

    if (current_super_step == phase_one_start_step && v.edge_size() == 0) {
      // isolated nodes send themselves a message on the phase_1 start step
      md_t message;
      message.dst_id = v.get_gid();
      v.send_by_gid(v.get_gid(), message);
      v.vote_to_halt();
      return;
    } else if (current_super_step == 1 && v.edge_size() == 0) {
      // isolated node aggregate their quality value and exit computation on
      // step 1
      grape::IteratorPair<md_t*> msgs(NULL, NULL);
      auto q = calculateActualQuality(v, context, msgs);
      v.context()->local_actual_quality()[v.tid()] += q;
      v.vote_to_halt();
      return;
    }
    // at the start of each full pass check to see whether progress is still
    // being made, if not halt
    if (current_minor_step == phase_one_minor_step_1 && current_iteration > 0 &&
        current_iteration % 2 == 0) {
      state.changed = 0;  // reset changed.
      if (v.context()->halt()) {
        // phase-1 halt, calculate current actual quality and return.
        auto q = calculateActualQuality(v, context, messages);
        replaceNodeEdgesWithCommunityEdges(v, messages);
        v.context()->local_actual_quality()[v.tid()] += q;
        return;
      }
    }

    switch (current_minor_step) {
    case phase_one_minor_step_0:
      getAndSendCommunityInfo(v, context, messages);

      // next step will require a progress check, aggregate the number of
      // nodes who have changed community.
      if (current_iteration > 0 && current_iteration % 2 == 0) {
        v.context()->local_change_num()[v.tid()] += state.changed;
      }
      break;
    case phase_one_minor_step_1:
      calculateBestCommunity(v, context, messages, current_iteration);
      break;
    case phase_one_minor_step_2:
      updateCommunities(v, messages);
      break;
    default:
      LOG(ERROR) << "Invalid minor step: " << current_minor_step;
    }
    v.vote_to_halt();
  }

 private:
  void aggregateQuality(compute_context_t& context, double quality) {
    context.aggregate(actual_quality_aggregator, quality);
  }

  edata_t getTotalEdgeWeight(compute_context_t& context, pregel_vertex_t& v) {
    auto& state = v.state();
    if (state.reset_total_edge_weight) {
      // we just aggregate the total edge weight in previous step.
      state.total_edge_weight =
          context.template get_aggregated_value<double>(edge_weight_aggregator);
      state.reset_total_edge_weight = false;
    }
    return state.total_edge_weight;
  }

  /**
   * Each vertex will receive its own communities sigma_total (if updated),
   * and then send its current community info to its neighbors.
   */
  void getAndSendCommunityInfo(pregel_vertex_t& vertex,
                               compute_context_t& context,
                               const grape::IteratorPair<md_t*>& messages) {
    state_t& state = vertex.state();
    // set new community information.
    if (context.superstep() > 0) {
      assert(messages.size() == 1);
      state.community = messages.begin()->community_id;
      state.community_sigma_total = messages.begin()->community_sigma_total;
    }
    md_t out_message(state.community, state.community_sigma_total, 0.0,
                     vertex.get_gid(), 0);
    if (vertex.use_fake_edges()) {
      for (const auto& edge : vertex.fake_edges()) {
        out_message.edge_weight = edge.second;
        out_message.dst_id = edge.first;
        vertex.send_by_gid(edge.first, out_message);
      }
    } else {
      for (auto& edge : vertex.outgoing_edges()) {
        auto neighbor_gid = vertex.fragment()->Vertex2Gid(edge.get_neighbor());
        out_message.edge_weight = static_cast<edata_t>(edge.get_data());
        out_message.dst_id = neighbor_gid;
        vertex.send_by_gid(neighbor_gid, out_message);
      }
    }
  }

  /**
   * Based on community of each of its neighbors, each vertex determines if
   * it should retain its current community or switch to a neighboring
   * community.
   * At the end of this step a message is sent to the nodes community hub so a
   * new community sigma_total can be calculated.
   */
  void calculateBestCommunity(pregel_vertex_t& vertex,
                              compute_context_t& context,
                              const grape::IteratorPair<md_t*>& messages,
                              int iteration) {
    // group messages by communities.
    std::map<vid_t, md_t> community_map;
    for (auto& message : messages) {
      vid_t community_id = message.community_id;
      if (community_map.find(community_id) != community_map.end()) {
        community_map[community_id].edge_weight += message.edge_weight;
      } else {
        community_map[community_id] = message;
      }
    }
    // calculate change in quality for each potential community
    auto& state = vertex.state();
    vid_t best_community_id = state.community;
    vid_t starting_community_id = best_community_id;
    double max_delta_q = 0.0;
    for (auto& entry : community_map) {
      double delta_q = calculateQualityDelta(
          context, vertex, starting_community_id, entry.second.community_id,
          entry.second.community_sigma_total, entry.second.edge_weight,
          state.node_weight, state.internal_weight);
      if (delta_q > max_delta_q ||
          (delta_q == max_delta_q &&
           entry.second.community_id < best_community_id)) {
        best_community_id = entry.second.community_id;
        max_delta_q = delta_q;
      }
    }

    // ignore switches based on iteration (prevent certain cycles)
    if ((state.community > best_community_id && iteration % 2 == 0) ||
        (state.community < best_community_id && iteration % 2 != 0)) {
      best_community_id = state.community;
    }

    // update community
    if (state.community != best_community_id) {
      md_t c = community_map[best_community_id];
      assert(best_community_id == c.community_id);
      state.community = c.community_id;
      state.community_sigma_total = c.community_sigma_total;
      state.changed = 1;  // community changed.
    }

    // send node weight to the community hub to sum in next super step
    md_t message(state.community, state.node_weight + state.internal_weight, 0,
                 vertex.get_gid(), state.community);
    vertex.send_by_gid(state.community, message);
  }

  /**
   * determine the change in quality if a node were to move to
   * the given community.
   */
  double calculateQualityDelta(compute_context_t& context, pregel_vertex_t& v,
                               const vid_t& curr_community_id,
                               const vid_t& test_community_id,
                               edata_t test_sigma_total,
                               edata_t edge_weight_in_community,
                               edata_t node_weight, edata_t internal_weight) {
    bool is_current_community = (curr_community_id == test_community_id);
    auto m2 = getTotalEdgeWeight(context, v);

    edata_t k_i_in_L = is_current_community
                           ? edge_weight_in_community + internal_weight
                           : edge_weight_in_community;
    edata_t k_i_in = k_i_in_L;
    edata_t k_i = node_weight + internal_weight;
    edata_t sigma_tot = test_sigma_total;

    if (is_current_community) {
      sigma_tot -= k_i;
    }

    double delta_q = 0.0;
    if (!(is_current_community && sigma_tot == delta_q)) {
      double dividend = k_i * sigma_tot;
      delta_q = k_i_in - dividend / m2;
    }

    return delta_q;
  }

  /**
   * Each community hub aggregates the values from each of its members to
   * update the node's sigma total, and then sends this back to each of its
   * members.
   */
  void updateCommunities(pregel_vertex_t& vertex,
                         const grape::IteratorPair<md_t*>& messages) {
    // sum all community contributions
    md_t sum;
    sum.community_id = vertex.get_gid();
    for (auto& m : messages) {
      sum.community_sigma_total += m.community_sigma_total;
    }

    for (auto& m : messages) {
      sum.dst_id = m.source_id;
      vertex.send_by_gid(m.source_id, sum);
    }
  }

  /**
   * Calculate this nodes contribution for the actual quality value
   * of the graph.
   */
  double calculateActualQuality(pregel_vertex_t& vertex,
                                compute_context_t& context,
                                const grape::IteratorPair<md_t*>& messages) {
    auto& state = vertex.state();
    edata_t k_i_in = state.internal_weight;
    std::set<vid_t> source_ids;
    for (auto& m : messages) {
      if (m.community_id == state.community) {
        source_ids.insert(m.source_id);
      }
    }
    k_i_in += vertex.get_edge_values(source_ids);
    edata_t sigma_tot = state.community_sigma_total;
    auto m2 = getTotalEdgeWeight(context, vertex);
    edata_t k_i = state.node_weight + state.internal_weight;

    double q = k_i_in / m2 - (sigma_tot * k_i) / pow(m2, 2);
    q = q < 0 ? 0 : q;
    return q;
  }

  /**
   * Replace each edge to a neighbor with an edge to that neighbors community
   * instead. Done just before exiting computation. In the next state of the
   * pipe line this edges are aggregated and all communities are represented
   * as single nodes. Edges from the community to itself are tracked be the
   * nodes internal weight.
   */
  void replaceNodeEdgesWithCommunityEdges(
      pregel_vertex_t& vertex, grape::IteratorPair<md_t*>& messages) {
    std::map<vid_t, edata_t> community_map;
    for (auto& message : messages) {
      const auto& community_id = message.community_id;
      community_map[community_id] += message.edge_weight;
    }
    std::vector<std::pair<vid_t, edata_t>> edges(community_map.begin(),
                                                 community_map.end());
    vertex.set_fake_edges(std::move(edges));
  }

  void sendCommunitiesInfo(pregel_vertex_t& vertex) {
    state_t& state = vertex.state();
    md_t message;
    message.internal_weight = state.internal_weight;
    assert((vertex.edge_size() == 0) || vertex.use_fake_edges());
    message.edges = std::move(vertex.move_fake_edges());
    vertex.set_fake_edges(std::vector<std::pair<vid_t, edata_t>>());
    if (vertex.get_gid() != state.community) {
      message.nodes_in_self_community.swap(vertex.nodes_in_self_community());
    }
    message.dst_id = state.community;
    vertex.send_by_gid(state.community, message);
    vertex.vote_to_halt();
  }

  void compressCommunities(pregel_vertex_t& vertex,
                           grape::IteratorPair<md_t*>& messages) {
    auto community_id = vertex.get_gid();
    edata_t weight = 0;
    std::map<vid_t, edata_t> edge_map;
    auto& nodes_in_self_community = vertex.nodes_in_self_community();
    for (auto& m : messages) {
      weight += m.internal_weight;
      for (auto& entry : m.edges) {
        if (entry.first == community_id) {
          weight += entry.second;
        } else {
          edge_map[entry.first] += entry.second;
        }
      }
      nodes_in_self_community.insert(nodes_in_self_community.end(),
                                     m.nodes_in_self_community.begin(),
                                     m.nodes_in_self_community.end());
    }
    vertex.state().internal_weight = weight;
    std::vector<std::pair<vid_t, edata_t>> edges(edge_map.begin(),
                                                 edge_map.end());
    vertex.set_fake_edges(std::move(edges));
    vertex.state().is_from_louvain_vertex_reader = false;

    // send self fake message to activate next round.
    md_t fake_message;
    fake_message.dst_id = community_id;
    vertex.send_by_gid(community_id, fake_message);
    // do not vote to halt since next round those new vertex need to be active.
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PREGEL_LOUVAIN_LOUVAIN_H_

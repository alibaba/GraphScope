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

#ifndef ANALYTICAL_ENGINE_APPS_PREGEL_LOUVAIN_AUXILIARY_H_
#define ANALYTICAL_ENGINE_APPS_PREGEL_LOUVAIN_AUXILIARY_H_

#include <map>
#include <utility>
#include <vector>

#include "grape/grape.h"

namespace gs {

// aggregators define
constexpr char change_aggregator[] = "change_aggregator";
constexpr char edge_weight_aggregator[] = "total_edge_weight_aggregator";
constexpr char actual_quality_aggregator[] = "actual_quality_aggregator";

// major phase of louvain
constexpr int phase_one_start_step = 0;
constexpr int phase_two_start_step = -2;
constexpr int compress_community_step = -1;
constexpr int sync_result_step = -10;
constexpr int terminate_step = -9;

// minor step of phase 1
constexpr int phase_one_minor_step_0 = 0;
constexpr int phase_one_minor_step_1 = 1;
constexpr int phase_one_minor_step_2 = 2;

constexpr double min_quality_improvement = 0.001;

/**
 * The state of a vertex.
 */
template <typename VID_T>
struct LouvainNodeState {
  using vid_t = VID_T;
  using edata_t = float;

  vid_t community = 0;
  edata_t community_sigma_total;

  // the internal edge weight of a node
  edata_t internal_weight;

  // degree of the node
  edata_t node_weight;

  // 1 if the node has changed communities this cycle, otherwise 0
  int64_t changed;

  bool reset_total_edge_weight;
  bool is_from_louvain_vertex_reader = false;
  bool use_fake_edges = false;
  bool is_alived_community = true;

  std::vector<std::pair<vid_t, edata_t>> fake_edges;
  std::vector<vid_t> nodes_in_community;
  edata_t total_edge_weight;

  LouvainNodeState()
      : community(0),
        community_sigma_total(0.0),
        internal_weight(0.0),
        node_weight(0.0),
        changed(0),
        reset_total_edge_weight(false),
        is_from_louvain_vertex_reader(false),
        use_fake_edges(false),
        is_alived_community(true) {}

  ~LouvainNodeState() = default;
};

/**
 * Message type of louvain.
 */
template <typename VID_T>
struct LouvainMessage {
  using vid_t = VID_T;
  using edata_t = float;

  vid_t community_id;
  edata_t community_sigma_total;
  edata_t edge_weight;
  vid_t source_id;
  vid_t dst_id;

  // For reconstruct graph info.
  // Each vertex send self's meta info to its community and silence itself,
  // the community compress its member's data and make self a new vertex for
  // next phase.
  edata_t internal_weight = 0;
  std::vector<std::pair<vid_t, edata_t>> edges;
  std::vector<vid_t> nodes_in_self_community;

  LouvainMessage()
      : community_id(0),
        community_sigma_total(0.0),
        edge_weight(0.0),
        source_id(0),
        dst_id(0) {}

  LouvainMessage(const vid_t& community_id, edata_t community_sigma_total,
                 edata_t edge_weight, const vid_t& source_id,
                 const vid_t& dst_id)
      : community_id(community_id),
        community_sigma_total(community_sigma_total),
        edge_weight(edge_weight),
        source_id(source_id),
        dst_id(dst_id) {}

  ~LouvainMessage() = default;

  // for message manager to serialize and deserialize LouvainMessage
  friend grape::InArchive& operator<<(grape::InArchive& in_archive,
                                      const LouvainMessage& u) {
    in_archive << u.community_id;
    in_archive << u.community_sigma_total;
    in_archive << u.edge_weight;
    in_archive << u.source_id;
    in_archive << u.dst_id;
    in_archive << u.internal_weight;
    in_archive << u.edges;
    in_archive << u.nodes_in_self_community;
    return in_archive;
  }
  friend grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                       LouvainMessage& val) {
    out_archive >> val.community_id;
    out_archive >> val.community_sigma_total;
    out_archive >> val.edge_weight;
    out_archive >> val.source_id;
    out_archive >> val.dst_id;
    out_archive >> val.internal_weight;
    out_archive >> val.edges;
    out_archive >> val.nodes_in_self_community;
    return out_archive;
  }
};

/**
 * Determine if progress is still being made or if the computation should halt.
 *
 * @param history change history of the pass.
 * @param min_progress The minimum delta X required to be considered progress.
 * where X is the number of nodes that have changed their community on a
 * particular pass.
 * @param progress_tries Number of times the minimum.progress setting is not met
 * before exiting form the current level and compressing the graph
 * @return true
 * @return false
 */
bool decide_to_halt(const std::vector<int64_t>& history, int min_progress,
                    int progress_tries) {
  // halt if the most recent change was 0
  if (0 == history.back()) {
    return true;
  }
  // halt if the change count has increased progress_tries times
  int64_t previous = history.front();
  int count = 0;
  for (const auto& cur : history) {
    if (previous - cur <= min_progress) {
      count++;
    }
    previous = cur;
  }
  return (count > progress_tries);
}

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PREGEL_LOUVAIN_AUXILIARY_H_

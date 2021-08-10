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

#ifndef ANALYTICAL_ENGINE_APPS_PREGEL_LOUVAIN_LOUVAIN_VERTEX_H_
#define ANALYTICAL_ENGINE_APPS_PREGEL_LOUVAIN_LOUVAIN_VERTEX_H_

#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "core/app/pregel/pregel_vertex.h"

#include "apps/pregel/louvain/louvain_context.h"

namespace gs {

/**
 * @brief LouvainVertex is a specific PregelVertex for louvain alorithm.
 * LouvainVertex provides communication-related method to send messages to
 * certain fragment and also access to context of louvain.
 * @tparam FRAG_T
 * @tparam VD_T
 * @tparam MD_T
 */
template <typename FRAG_T, typename VD_T, typename MD_T>
class LouvainVertex : public PregelVertex<FRAG_T, VD_T, MD_T> {
  using fragment_t = FRAG_T;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using edata_t = double;
  using vertex_t = typename fragment_t::vertex_t;
  using adj_list_t = typename fragment_t::const_adj_list_t;
  using compute_context_t = PregelComputeContext<fragment_t, VD_T, MD_T>;
  using context_t = LouvainContext<FRAG_T, compute_context_t>;
  using state_t = LouvainNodeState<vid_t>;

 public:
  using vd_t = VD_T;
  using md_t = MD_T;

  state_t& state() { return context_->GetVertexState(this->vertex_); }

  void send_by_gid(vid_t dst_gid, const md_t& md) {
    this->compute_context_->send_p2p_message(dst_gid, md, tid_);
  }

  void set_context(context_t* context) { context_ = context; }

  vid_t get_gid() { return this->fragment_->Vertex2Gid(this->vertex_); }

  vid_t get_vertex_gid(const vertex_t& v) {
    return this->fragment_->Vertex2Gid(v);
  }

  size_t edge_size() {
    if (!this->use_fake_edges()) {
      return this->incoming_edges().Size() + this->outgoing_edges().Size();
    } else {
      return this->fake_edges().size();
    }
  }

  bool use_fake_edges() const {
    return context_->GetVertexState(this->vertex_).use_fake_edges;
  }

  const std::map<vid_t, edata_t>& fake_edges() const {
    return context_->GetVertexState(this->vertex_).fake_edges;
  }

  edata_t get_edge_value(const vid_t& dst_id) {
    if (!this->use_fake_edges()) {
      for (auto& edge : this->incoming_edges()) {
        if (this->fragment_->Vertex2Gid(edge.get_neighbor()) == dst_id) {
          return static_cast<edata_t>(edge.get_data());
        }
      }
      for (auto& edge : this->outgoing_edges()) {
        if (this->fragment_->Vertex2Gid(edge.get_neighbor()) == dst_id) {
          return static_cast<edata_t>(edge.get_data());
        }
      }
    } else {
      return this->fake_edges().at(dst_id);
    }
    return edata_t();
  }

  edata_t get_edge_values(const std::set<vid_t>& dst_ids) {
    edata_t ret = 0;
    if (!this->use_fake_edges()) {
      for (auto& edge : this->incoming_edges()) {
        auto gid = this->fragment_->Vertex2Gid(edge.get_neighbor());
        if (dst_ids.find(gid) != dst_ids.end()) {
          ret += static_cast<edata_t>(edge.get_data());
        }
      }
      for (auto& edge : this->outgoing_edges()) {
        auto gid = this->fragment_->Vertex2Gid(edge.get_neighbor());
        if (dst_ids.find(gid) != dst_ids.end()) {
          ret += static_cast<edata_t>(edge.get_data());
        }
      }
    } else {
      auto edges = this->fake_edges();
      for (auto gid : dst_ids) {
        if (edges.find(gid) != edges.end()) {
          ret += edges.at(gid);
        } else {
          LOG(ERROR) << "Warning: Cannot find a edge from " << this->id()
                     << " to " << gid;
        }
      }
    }
    return ret;
  }

  void set_fake_edges(std::map<vid_t, edata_t>&& edges) {
    state_t& ref_state = this->state();
    ref_state.fake_edges = std::move(edges);
    ref_state.use_fake_edges = true;
  }

  std::vector<vid_t>& nodes_in_self_community() {
    return context_->GetVertexState(this->vertex_).nodes_in_community;
  }

  int tid() { return tid_; }
  void set_tid(int id) { tid_ = id; }

  PregelComputeContext<fragment_t, VD_T, MD_T>* compute_context() {
    return this->compute_context_;
  }

  const fragment_t* fragment() { return this->fragment_; }
  context_t* context() { return context_; }

 private:
  int tid_;
  context_t* context_;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PREGEL_LOUVAIN_LOUVAIN_VERTEX_H_

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

#ifndef ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_COMPUTE_CONTEXT_H_
#define ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_COMPUTE_CONTEXT_H_

#include <stdint.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "grape/utils/atomic_ops.h"
#include "grape/utils/iterator_pair.h"

#include "core/app/pregel/aggregators/aggregator.h"
#include "core/app/pregel/aggregators/aggregator_factory.h"
#include "core/app/pregel/pregel_vertex.h"
#include "core/config.h"

namespace gs {
/**
 * @brief PregelComputeContext holds the properties of the graph and
 * messages during the computation.
 * @tparam FRAG_T
 * @tparam VD_T
 * @tparam MD_T
 */
template <typename FRAG_T, typename VD_T, typename MD_T>
class PregelComputeContext {
  using fragment_t = FRAG_T;
  using pregel_vertex_t = PregelVertex<fragment_t, VD_T, MD_T>;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;

 public:
  // exposes to PregelContext
  using vd_t = VD_T;
  explicit PregelComputeContext(
      typename FRAG_T::template vertex_array_t<VD_T>& vertex_data)
      : vertex_data_(vertex_data) {}

  void init(const fragment_t& frag) {
    auto vertices = frag.Vertices();
    auto inner_vertices = frag.InnerVertices();

    messages_out_.Init(vertices, {});
    total_vertex_num_ = vertices.size();

    messages_in_.Init(inner_vertices, {});
    halted_.Init(inner_vertices, false);
    vid_parser_.Init(frag.fnum(), 1);
    inner_vertex_num_ = inner_vertices.size();

    step_ = 0;
    voted_to_halt_num_ = 0;
    enable_combine_ = false;
  }

  void inc_step() { step_++; }

  int superstep() const { return step_; }

  void set_superstep(int step) { step_ = step; }

  void set_vertex_value(const pregel_vertex_t& vertex, const VD_T& value) {
    vertex_data_[vertex.vertex()] = value;
  }

  void set_vertex_value(const pregel_vertex_t& vertex, const VD_T&& value) {
    vertex_data_[vertex.vertex()] = std::move(value);
  }

  const VD_T& get_vertex_value(const pregel_vertex_t& v) const {
    return vertex_data_[v.vertex()];
  }

  void send_message(const vertex_t& v, const MD_T& value) {
    if (enable_combine_) {
      messages_out_[v].emplace_back(value);
    } else {
      if (fragment_->IsOuterVertex(v)) {
        message_manager_->SyncStateOnOuterVertex<fragment_t, MD_T>(*fragment_,
                                                                   v, value);
      } else {
        messages_out_[v].emplace_back(value);
      }
    }
  }

  void send_message(const vertex_t& v, MD_T&& value) {
    if (enable_combine_) {
      messages_out_[v].emplace_back(std::move(value));
    } else {
      if (fragment_->IsOuterVertex(v)) {
        message_manager_->SyncStateOnOuterVertex<fragment_t, MD_T>(*fragment_,
                                                                   v, value);
      } else {
        messages_out_[v].emplace_back(std::move(value));
      }
    }
  }

  void send_p2p_message(const vid_t& v_gid, const MD_T& value, int tid = 0) {
    auto fid = vid_parser_.GetFid(v_gid);
    parallel_message_manager_->Channels()[tid].SendToFragment(fid, value);
  }

  void send_p2p_message(const vid_t v_gid, MD_T&& value, int tid = 0) {
    auto fid = vid_parser_.GetFid(v_gid);
    parallel_message_manager_->Channels()[tid].SendToFragment(fid, value);
  }

  template <typename COMBINATOR_T>
  void apply_combine(COMBINATOR_T& cb) {
    auto vertices = fragment_->Vertices();
    for (auto v : vertices) {
      auto& msgs = messages_out_[v];
      if (!msgs.empty()) {
        MD_T ret = cb.CombineMessages(grape::IteratorPair<MD_T*>(
            &msgs[0], &msgs[0] + static_cast<ptrdiff_t>(msgs.size())));
        msgs.clear();
        msgs.emplace_back(std::move(ret));
      }
    }
  }

  void before_comm() {
    auto inner_vertices = fragment_->InnerVertices();
    for (auto v : inner_vertices) {
      messages_in_[v].clear();
      messages_in_[v].swap(messages_out_[v]);
      if (!messages_in_[v].empty()) {
        activate(v);
      }
    }
  }

  bool active(const vertex_t& v) { return !halted_[v]; }

  void activate(const vertex_t& v) {
    if (halted_[v] == true) {
      halted_[v] = false;
    }
  }

  void vote_to_halt(const pregel_vertex_t& vertex) {
    if (halted_[vertex.vertex()] == false) {
      halted_[vertex.vertex()] = true;
    }
  }

  bool all_halted() {
    auto inner_vertices = fragment_->InnerVertices();
    for (auto& v : inner_vertices) {
      if (!halted_[v]) {
        return false;
      }
    }
    return true;
  }

  typename FRAG_T::template vertex_array_t<std::vector<MD_T>>& messages_in() {
    return messages_in_;
  }

  typename FRAG_T::template vertex_array_t<std::vector<MD_T>>& messages_out() {
    return messages_out_;
  }

  typename FRAG_T::template vertex_array_t<VD_T>& vertex_data() {
    return vertex_data_;
  }
  void clear_for_next_round() {
    if (!enable_combine_) {
      auto inner_vertices = fragment_->InnerVertices();
      for (auto v : inner_vertices) {
        messages_in_[v].clear();
        messages_in_[v].swap(messages_out_[v]);
        if (!messages_in_[v].empty()) {
          activate(v);
        }
      }
    }
  }

  void enable_combine() { enable_combine_ = true; }
  void set_fragment(const fragment_t* fragment) { fragment_ = fragment; }
  void set_message_manager(grape::DefaultMessageManager* message_manager) {
    message_manager_ = message_manager;
  }

  void set_parallel_message_manager(
      grape::ParallelMessageManager* message_manager) {
    parallel_message_manager_ = message_manager;
  }

  oid_t GetId(const vertex_t& v) { return fragment_->GetId(v); }

  void set_config(const std::string& key, const std::string& value) {
    config_.emplace(key, value);
  }

  std::string get_config(const std::string& key) {
    auto iter = config_.find(key);
    if (iter != config_.end()) {
      return iter->second;
    } else {
      return "";
    }
  }

  const vineyard::IdParser<vid_t>& vid_parser() const { return vid_parser_; }

  std::unordered_map<std::string, std::shared_ptr<IAggregator>>& aggregators() {
    return aggregators_;
  }

  void register_aggregator(const std::string& name, PregelAggregatorType type) {
    if (aggregators_.find(name) == aggregators_.end()) {
      aggregators_.emplace(name, AggregatorFactory::CreateAggregator(type));
      aggregators_.at(name)->Init();
    }
  }

  template <typename AGGR_TYPE>
  void aggregate(const std::string& name, AGGR_TYPE value) {
    if (aggregators_.find(name) != aggregators_.end()) {
      std::dynamic_pointer_cast<Aggregator<AGGR_TYPE>>(aggregators_.at(name))
          ->Aggregate(value);
    }
  }

  template <typename AGGR_TYPE>
  AGGR_TYPE get_aggregated_value(const std::string& name) {
    return std::dynamic_pointer_cast<Aggregator<AGGR_TYPE>>(
               aggregators_.at(name))
        ->GetAggregatedValue();
  }

 private:
  const fragment_t* fragment_;
  grape::DefaultMessageManager* message_manager_;
  grape::ParallelMessageManager* parallel_message_manager_;

  typename FRAG_T::template vertex_array_t<VD_T>& vertex_data_;

  size_t voted_to_halt_num_;
  typename FRAG_T::template vertex_array_t<bool> halted_;

  typename FRAG_T::template vertex_array_t<std::vector<MD_T>> messages_out_;
  typename FRAG_T::template vertex_array_t<std::vector<MD_T>> messages_in_;

  size_t inner_vertex_num_;
  size_t total_vertex_num_;

  bool enable_combine_;

  int step_;
  std::unordered_map<std::string, std::string> config_;
  std::unordered_map<std::string, std::shared_ptr<IAggregator>> aggregators_;
  vineyard::IdParser<vid_t> vid_parser_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_COMPUTE_CONTEXT_H_

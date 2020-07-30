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

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "grape/utils/iterator_pair.h"

#include "core/app/pregel/pregel_vertex.h"

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
    inner_vertex_num_ = inner_vertices.size();

    step_ = 0;
    voted_to_halt_num_ = 0;
    enable_combine_ = false;
    has_messages_ = false;
  }

  void inc_step() { step_++; }

  int superstep() const { return step_; }

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
        messages_in_[v].emplace_back(value);
        has_messages_ = true;
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
        messages_in_[v].emplace_back(std::move(value));
        has_messages_ = true;
      }
    }
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
        has_messages_ = true;
      }
    }
  }

  bool active(const vertex_t& v) { return !messages_in_[v].empty(); }

  void activate(const vertex_t& v) {
    halted_[v] = false;
    --voted_to_halt_num_;
  }

  void vote_to_halt(const pregel_vertex_t& vertex) {
    // halted_[vertex.vertex()] = true;
    // ++voted_to_halt_num_;
  }

  bool all_halted() { return voted_to_halt_num_ == inner_vertex_num_; }

  bool has_messages() { return has_messages_; }

  typename FRAG_T::template vertex_array_t<std::vector<MD_T>>& messages_in() {
    return messages_in_;
  }

  typename FRAG_T::template vertex_array_t<std::vector<MD_T>>& messages_out() {
    return messages_out_;
  }

  typename FRAG_T::template vertex_array_t<VD_T>& vertex_data() {
    return vertex_data_;
  }
  void clear_for_next_round() { has_messages_ = false; }

  void enable_combine() { enable_combine_ = true; }
  void set_fragment(const fragment_t* fragment) { fragment_ = fragment; }
  void set_message_manager(grape::DefaultMessageManager* message_manager) {
    message_manager_ = message_manager;
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

 private:
  const fragment_t* fragment_;
  grape::DefaultMessageManager* message_manager_;

  typename FRAG_T::template vertex_array_t<VD_T>& vertex_data_;

  size_t voted_to_halt_num_;
  typename FRAG_T::template vertex_array_t<bool> halted_;

  typename FRAG_T::template vertex_array_t<std::vector<MD_T>> messages_out_;
  typename FRAG_T::template vertex_array_t<std::vector<MD_T>> messages_in_;

  bool has_messages_;

  size_t inner_vertex_num_;
  size_t total_vertex_num_;

  bool enable_combine_;

  int step_;
  std::unordered_map<std::string, std::string> config_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_COMPUTE_CONTEXT_H_

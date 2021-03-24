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

#ifndef ANALYTICAL_ENGINE_APPS_PREGEL_LOUVAIN_LOUVAIN_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_PREGEL_LOUVAIN_LOUVAIN_CONTEXT_H_

#include <vector>

#include "grape/grape.h"

#include "core/context/vertex_data_context.h"

namespace gs {

/**
 * @brief Context of louvain that holds the computation result with
 * grape::VertexDataContext and some state of louvain process.
 * @tparam FRAG_T
 * @tparam COMPUTE_CONTEXT_T
 */
template <typename FRAG_T, typename COMPUTE_CONTEXT_T>
class LouvainContext
    : public grape::VertexDataContext<FRAG_T,
                                      typename COMPUTE_CONTEXT_T::vd_t> {
  using fragment_t = FRAG_T;
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using edata_t = double;
  using vertex_t = typename FRAG_T::vertex_t;
  using state_t = LouvainNodeState<vid_t>;
  using vertex_state_array_t =
      typename fragment_t::template vertex_array_t<state_t>;

 public:
  explicit LouvainContext(const FRAG_T& fragment)
      : grape::VertexDataContext<FRAG_T, typename COMPUTE_CONTEXT_T::vd_t>(
            fragment),
        compute_context_(this->data()) {}

  void Init(grape::ParallelMessageManager& messages, int min_progress,
            int progress_tries) {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();

    compute_context_.init(frag);
    compute_context_.set_fragment(&frag);
    compute_context_.set_parallel_message_manager(&messages);

    this->min_progress_ = min_progress;
    this->progress_tries_ = progress_tries;

    vertex_state_.Init(inner_vertices);
    halt_ = false;
    prev_quality_ = 0.0;
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto& result = compute_context_.vertex_data();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      os << frag.GetId(v) << " " << result[v] << std::endl;
    }
  }

  state_t& GetVertexState(const vertex_t& v) { return vertex_state_[v]; }

  void ClearLocalAggregateValues(uint32_t thread_num) {
    local_change_num_.clear();
    local_total_edge_weight_.clear();
    local_actual_quality_.clear();
    local_change_num_.resize(thread_num, 0);
    local_total_edge_weight_.resize(thread_num, 0.0);
    local_actual_quality_.resize(thread_num, 0.0);
  }

  int64_t GetLocalChangeSum() {
    int64_t sum = 0;
    for (const auto& val : local_change_num_) {
      sum += val;
    }
    return sum;
  }

  edata_t GetLocalEdgeWeightSum() {
    edata_t sum = 0;
    for (const auto& val : local_total_edge_weight_) {
      sum += val;
    }
    return sum;
  }

  double GetLocalQualitySum() {
    double sum = 0;
    for (const auto& val : local_actual_quality_) {
      sum += val;
    }
    return sum;
  }

  COMPUTE_CONTEXT_T& compute_context() { return compute_context_; }

  std::vector<int64_t>& change_history() { return change_history_; }

  vertex_state_array_t& vertex_state() { return vertex_state_; }

  std::vector<int64_t>& local_change_num() { return local_change_num_; }

  std::vector<edata_t>& local_total_edge_weight() {
    return local_total_edge_weight_;
  }
  std::vector<double>& local_actual_quality() { return local_actual_quality_; }

  bool halt() { return halt_; }
  void set_halt(bool halt) { halt_ = halt; }

  double prev_quality() { return prev_quality_; }
  void set_prev_quality(double value) { prev_quality_ = value; }

  int min_progress() { return min_progress_; }
  int progress_tries() { return progress_tries_; }

 private:
  std::vector<int64_t> change_history_;
  COMPUTE_CONTEXT_T compute_context_;
  vertex_state_array_t vertex_state_;

  // members to store local aggrated value
  std::vector<int64_t> local_change_num_;
  std::vector<edata_t> local_total_edge_weight_;
  std::vector<double> local_actual_quality_;

  bool halt_;  // phase-1 halt
  double prev_quality_;
  int min_progress_;
  int progress_tries_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PREGEL_LOUVAIN_LOUVAIN_CONTEXT_H_

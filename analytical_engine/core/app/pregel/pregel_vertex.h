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

#ifndef ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_VERTEX_H_
#define ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_VERTEX_H_

#include <string>
#include <utility>

#include "boost/lexical_cast.hpp"

#include "grape/grape.h"

namespace gs {

template <typename FRAG_T, typename VD_T, typename MD_T>
class PregelComputeContext;

/**
 * @brief PregelVertex provides methods to access the edges attached to it. The
 * communication-related methods also are provided to send messages to its
 * neighbors.
 * @tparam FRAG_T
 * @tparam VD_T
 * @tparam MD_T
 */
template <typename FRAG_T, typename VD_T, typename MD_T>
class PregelVertex {
  using fragment_t = FRAG_T;
  using vertex_t = typename fragment_t::vertex_t;
  using adj_list_t = typename fragment_t::const_adj_list_t;

 public:
  using vd_t = VD_T;
  using md_t = MD_T;

  PregelVertex() = default;

  std::string id() {
    return boost::lexical_cast<std::string>(fragment_->GetId(vertex_));
  }

  void set_value(const VD_T& value) {
    compute_context_->set_vertex_value(*this, value);
  }
  void set_value(const VD_T&& value) {
    compute_context_->set_vertex_value(*this, std::move(value));
  }

  const VD_T& value() { return compute_context_->get_vertex_value(*this); }

  vertex_t vertex() const { return vertex_; }

  adj_list_t outgoing_edges() { return fragment_->GetOutgoingAdjList(vertex_); }

  adj_list_t incoming_edges() { return fragment_->GetIncomingAdjList(vertex_); }

  void send(const vertex_t& v, const MD_T& value) {
    compute_context_->send_message(v, value);
  }

  void send(const vertex_t& v, MD_T&& value) {
    compute_context_->send_message(v, std::move(value));
  }

  void vote_to_halt() { compute_context_->vote_to_halt(*this); }

  void set_fragment(const fragment_t* fragment) { fragment_ = fragment; }

  void set_compute_context(
      PregelComputeContext<fragment_t, VD_T, MD_T>* compute_comtext) {
    compute_context_ = compute_comtext;
  }

  void set_vertex(vertex_t vertex) { vertex_ = vertex; }

 protected:
  const fragment_t* fragment_;
  PregelComputeContext<fragment_t, VD_T, MD_T>* compute_context_;

  vertex_t vertex_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_VERTEX_H_

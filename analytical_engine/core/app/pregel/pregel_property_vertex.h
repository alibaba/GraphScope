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

#ifndef ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_PROPERTY_VERTEX_H_
#define ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_PROPERTY_VERTEX_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "boost/lexical_cast.hpp"

#include "grape/grape.h"
#include "grape/utils/iterator_pair.h"

#include "core/app/pregel/aggregators/aggregator.h"
#include "core/app/pregel/aggregators/aggregator_factory.h"
#include "core/context/i_context.h"

namespace gs {

template <typename FRAG_T, typename VD_T, typename MD_T>
class PregelPropertyComputeContext;

template <typename FRAG_T, typename VD_T, typename MD_T>
class PregelPropertyNeighbor;

template <typename FRAG_T, typename VD_T, typename MD_T>
class PregelPropertyAdjList;
/**
 * @brief PregelPropertyVertex provides methods to access the edges attached to
 * it. The communication-related methods also are provided to send messages to
 * its neighbors. Compared with PregelVertex, this class is designed for the
 * labeled graph.
 * @tparam FRAG_T
 * @tparam VD_T
 * @tparam MD_T
 */
template <typename FRAG_T, typename VD_T, typename MD_T>
class PregelPropertyVertex {
  using fragment_t = FRAG_T;
  using vertex_t = typename fragment_t::vertex_t;
  using label_id_t = typename fragment_t::label_id_t;
  using prop_id_t = typename fragment_t::prop_id_t;
  using adj_list_t = typename fragment_t::adj_list_t;

 public:
  using vd_t = VD_T;
  using md_t = MD_T;

  PregelPropertyVertex() = default;

  PregelPropertyVertex(const fragment_t* fragment, vertex_t vertex)
      : fragment_(fragment), vertex_(vertex) {}

  std::string id() {
    return boost::lexical_cast<std::string>(fragment_->GetId(vertex_));
  }

  std::string label() const {
    return compute_context_->schema()->GetVertexLabelName(label_id_);
  }

  label_id_t label_id() const { return label_id_; }

  std::vector<std::pair<std::string, std::string>> properties() const {
    return compute_context_->schema()->GetVertexPropertyListByLabel(label_id_);
  }

  std::string get_str(prop_id_t prop_id) const {
    return fragment_->template GetData<std::string>(vertex_, prop_id);
  }

  std::string get_str(const std::string& name) const {
    prop_id_t prop_id =
        compute_context_->schema()->GetVertexPropertyId(label_id_, name);
    return get_str(prop_id);
  }

  double get_double(prop_id_t prop_id) const {
    return fragment_->template GetData<double>(vertex_, prop_id);
  }

  double get_double(const std::string& name) const {
    prop_id_t prop_id =
        compute_context_->schema()->GetVertexPropertyId(label_id_, name);
    return get_double(prop_id);
  }

  int64_t get_int(prop_id_t prop_id) const {
    return fragment_->template GetData<int64_t>(vertex_, prop_id);
  }

  int64_t get_int(const std::string& name) const {
    prop_id_t prop_id =
        compute_context_->schema()->GetVertexPropertyId(label_id_, name);
    return get_int(prop_id);
  }

  void set_value(const VD_T& value) {
    compute_context_->set_vertex_value(*this, value);
  }
  void set_value(const VD_T&& value) {
    compute_context_->set_vertex_value(*this, std::move(value));
  }

  const VD_T& value() { return compute_context_->get_vertex_value(*this); }

  vertex_t vertex() const { return vertex_; }

  PregelPropertyAdjList<FRAG_T, VD_T, MD_T> outgoing_edges(
      label_id_t e_label_id) {
    return PregelPropertyAdjList<FRAG_T, VD_T, MD_T>(
        fragment_, compute_context_,
        fragment_->GetOutgoingAdjList(vertex_, e_label_id));
  }

  PregelPropertyAdjList<FRAG_T, VD_T, MD_T> outgoing_edges(
      const std::string& e_label) {
    label_id_t e_label_id = compute_context_->schema()->GetEdgeLabelId(e_label);
    return PregelPropertyAdjList<FRAG_T, VD_T, MD_T>(
        fragment_, compute_context_,
        fragment_->GetOutgoingAdjList(vertex_, e_label_id));
  }

  PregelPropertyAdjList<FRAG_T, VD_T, MD_T> incoming_edges(
      label_id_t e_label_id) {
    return PregelPropertyAdjList<FRAG_T, VD_T, MD_T>(
        fragment_, compute_context_,
        fragment_->GetIncomingAdjList(vertex_, e_label_id));
  }

  PregelPropertyAdjList<FRAG_T, VD_T, MD_T> incoming_edges(
      const std::string& e_label) {
    label_id_t e_label_id = compute_context_->schema()->GetEdgeLabelId(e_label);
    return PregelPropertyAdjList<FRAG_T, VD_T, MD_T>(
        fragment_, compute_context_,
        fragment_->GetIncomingAdjList(vertex_, e_label_id));
  }

  void send(const PregelPropertyVertex& v, const MD_T& value) {
    compute_context_->send_message(v.vertex(), value);
  }

  void send(const PregelPropertyVertex& v, MD_T&& value) {
    compute_context_->send_message(v.vertex(), std::move(value));
  }

  void vote_to_halt() { compute_context_->vote_to_halt(*this); }

  void set_fragment(const fragment_t* fragment) { fragment_ = fragment; }

  void set_compute_context(
      PregelPropertyComputeContext<FRAG_T, VD_T, MD_T>* compute_comtext) {
    compute_context_ = compute_comtext;
  }

  void set_vertex(vertex_t vertex) { vertex_ = vertex; }

  void set_label_id(label_id_t label_id) { label_id_ = label_id; }

 private:
  const fragment_t* fragment_;
  PregelPropertyComputeContext<fragment_t, VD_T, MD_T>* compute_context_;

  vertex_t vertex_;
  label_id_t label_id_;
};

/**
 * @brief PregelPropertyNeighbor holds the neighbor, context, and fragment
 * instance.
 * @tparam FRAG_T
 * @tparam VD_T
 * @tparam MD_T
 */
template <typename FRAG_T, typename VD_T, typename MD_T>
class PregelPropertyNeighbor {
 public:
  using VertexT = PregelPropertyVertex<FRAG_T, VD_T, MD_T>;
  using fragment_t = FRAG_T;
  using nbr_t = typename fragment_t::nbr_t;
  using label_id_t = typename fragment_t::label_id_t;
  using prop_id_t = typename fragment_t::prop_id_t;

  PregelPropertyNeighbor() {}
  ~PregelPropertyNeighbor() {}

  PregelPropertyNeighbor(
      const fragment_t* fragment,
      PregelPropertyComputeContext<fragment_t, VD_T, MD_T>* compute_context,
      nbr_t&& nbr)
      : fragment_(fragment),
        compute_context_(compute_context),
        nbr_(std::move(nbr)) {}

  PregelPropertyNeighbor(
      const fragment_t* fragment,
      PregelPropertyComputeContext<fragment_t, VD_T, MD_T>* compute_context,
      const nbr_t& nbr)
      : fragment_(fragment), compute_context_(compute_context), nbr_(nbr) {}

  VertexT vertex() {
    VertexT pregel_vertex(fragment_, nbr_.neighbor());
    pregel_vertex.set_compute_context(compute_context_);
    pregel_vertex.set_label_id(fragment_->vertex_label(nbr_.neighbor()));
    return pregel_vertex;
  }

  PregelPropertyNeighbor& operator++() noexcept {
    ++nbr_;
    return *this;
  }

  const PregelPropertyNeighbor operator++(int) noexcept {
    return PregelPropertyNeighbor(fragment_, nbr_++);
  }

  std::string get_str(prop_id_t prop_id) {
    return nbr_.template get_data<std::string>(prop_id);
  }

  double get_double(int prop_id) {
    return nbr_.template get_data<double>(prop_id);
  }

  int64_t get_int(int prop_id) {
    return nbr_.template get_data<int64_t>(prop_id);
  }

  bool operator==(const PregelPropertyNeighbor& rhs) {
    return (fragment_ == rhs.fragment_) && (nbr_ == rhs.nbr_);
  }

  bool operator!=(const PregelPropertyNeighbor& rhs) { return !(*this == rhs); }

 private:
  const fragment_t* fragment_;
  PregelPropertyComputeContext<fragment_t, VD_T, MD_T>* compute_context_;
  nbr_t nbr_;
};

/**
 * @brief PregelPropertyAdjList wraps fragment_t::adj_list_t and provides
 * iterator to traverse neighbors.
 * @tparam FRAG_T
 * @tparam VD_T
 * @tparam MD_T
 */
template <typename FRAG_T, typename VD_T, typename MD_T>
class PregelPropertyAdjList {
  using fragment_t = FRAG_T;
  using nbr_t = typename fragment_t::nbr_t;
  using nbr_iterator_t = const nbr_t*;
  using adj_list_t = typename fragment_t::adj_list_t;

 public:
  PregelPropertyAdjList()
      : fragment_(nullptr), compute_context_(nullptr), adj_list_() {}
  PregelPropertyAdjList(
      const fragment_t* fragment,
      PregelPropertyComputeContext<fragment_t, VD_T, MD_T>* compute_context,
      adj_list_t adj_list)
      : fragment_(fragment),
        compute_context_(compute_context),
        adj_list_(adj_list) {}
  ~PregelPropertyAdjList() = default;

  class iterator {
    using pointer_type = PregelPropertyNeighbor<FRAG_T, VD_T, MD_T>*;
    using reference_type = PregelPropertyNeighbor<FRAG_T, VD_T, MD_T>&;

   public:
    iterator() noexcept : nbr_() {}
    iterator(
        const fragment_t* frag,
        PregelPropertyComputeContext<fragment_t, VD_T, MD_T>* compute_context,
        nbr_t&& nbr)
        : nbr_(frag, compute_context, nbr) {}
    iterator(
        const fragment_t* frag,
        PregelPropertyComputeContext<fragment_t, VD_T, MD_T>* compute_context,
        const nbr_t& nbr)
        : nbr_(frag, compute_context, nbr) {}

    reference_type operator*() noexcept { return nbr_; }
    pointer_type operator->() noexcept { return &nbr_; }

    iterator& operator++() noexcept {
      ++nbr_;
      return *this;
    }

    const iterator operator++(int) noexcept {
      return iterator(nbr_.fragment(), nbr_.compute_context(),
                      std::move(nbr_++));
    }

    bool operator==(const iterator& rhs) noexcept { return nbr_ == rhs.nbr_; }

    bool operator!=(const iterator& rhs) noexcept { return nbr_ != rhs.nbr_; }

   private:
    PregelPropertyNeighbor<FRAG_T, VD_T, MD_T> nbr_;
  };

  iterator begin() {
    return iterator(fragment_, compute_context_, adj_list_.begin());
  }
  iterator end() {
    return iterator(fragment_, compute_context_, adj_list_.end());
  }

  size_t size() { return adj_list_.size(); }

 private:
  const fragment_t* fragment_;
  PregelPropertyComputeContext<fragment_t, VD_T, MD_T>* compute_context_;
  adj_list_t adj_list_;
};

/**
 * @brief PregelPropertyComputeContext holds the properties of the graph and
 * messages during the computation.
 * @tparam FRAG_T
 * @tparam VD_T
 * @tparam MD_T
 */
template <typename FRAG_T, typename VD_T, typename MD_T>
class PregelPropertyComputeContext {
  using fragment_t = FRAG_T;
  using pregel_vertex_t = PregelPropertyVertex<FRAG_T, VD_T, MD_T>;
  using vid_t = typename fragment_t::vid_t;
  using label_id_t = typename fragment_t::label_id_t;
  using prop_id_t = typename fragment_t::prop_id_t;
  using vertex_t = typename fragment_t::vertex_t;

 public:
  // exposes to PregelContext
  using vd_t = VD_T;

  PregelPropertyComputeContext(
      std::vector<typename FRAG_T::template vertex_array_t<VD_T>>& vertex_data,
      const vineyard::PropertyGraphSchema& schema)
      : schema_(&schema), vertex_data_(vertex_data) {}

  void init(const fragment_t& frag) {
    label_id_t v_label_num = frag.vertex_label_num();
    label_id_t e_label_num = frag.edge_label_num();

    halted_.resize(v_label_num);
    messages_in_.resize(v_label_num);
    messages_out_.resize(v_label_num);

    inner_vertex_num_ = 0;
    for (label_id_t v_label = 0; v_label != v_label_num; ++v_label) {
      auto vertices = frag.Vertices(v_label);
      messages_out_[v_label].Init(vertices, {});

      auto inner_vertices = frag.InnerVertices(v_label);
      messages_in_[v_label].Init(inner_vertices, {});
      halted_[v_label].Init(inner_vertices, false);
      inner_vertex_num_ += inner_vertices.size();
    }

    step_ = 0;
    voted_to_halt_num_ = 0;
    enable_combine_ = false;
    vertex_label_num_ = v_label_num;
    edge_label_num_ = e_label_num;
  }

  void inc_step() { step_++; }
  int superstep() const { return step_; }

  label_id_t vertex_label_num() const { return vertex_label_num_; }
  label_id_t edge_label_num() const { return edge_label_num_; }

  prop_id_t vertex_property_num(label_id_t v_label_id) const {
    return fragment_->vertex_property_num(v_label_id);
  }

  prop_id_t vertex_property_num(const std::string& v_label) const {
    label_id_t v_label_id = get_vertex_label_id_by_name(v_label);
    return vertex_property_num(v_label_id);
  }

  prop_id_t edge_property_num(label_id_t e_label_id) const {
    return fragment_->edge_property_num(e_label_id);
  }

  prop_id_t edge_property_num(const std::string& e_label) const {
    label_id_t e_label_id = get_edge_label_id_by_name(e_label);
    return edge_property_num(e_label_id);
  }

  void set_vertex_value(const pregel_vertex_t& vertex, const VD_T& value) {
    vertex_data_[vertex.label_id()][vertex.vertex()] = value;
  }

  void set_vertex_value(const pregel_vertex_t& vertex, const VD_T&& value) {
    vertex_data_[vertex.label_id()][vertex.vertex()] = std::move(value);
  }

  const VD_T& get_vertex_value(const pregel_vertex_t& v) const {
    return vertex_data_[v.label_id()][v.vertex()];
  }

  std::vector<std::string> vertex_labels() const {
    return schema_->GetVertexLabels();
  }

  std::vector<std::string> edge_labels() const {
    return schema_->GetEdgeLabels();
  }

  std::string get_vertex_label_by_id(label_id_t v_label_id) const {
    return schema_->GetVertexLabelName(v_label_id);
  }

  label_id_t get_vertex_label_id_by_name(const std::string& name) const {
    return schema_->GetVertexLabelId(name);
  }

  std::string get_edge_label_by_id(label_id_t e_label_id) const {
    return schema_->GetEdgeLabelName(e_label_id);
  }

  label_id_t get_edge_label_id_by_name(const std::string& name) const {
    return schema_->GetEdgeLabelId(name);
  }

  std::vector<std::pair<std::string, std::string>> vertex_properties(
      const std::string& label) const {
    return schema_->GetVertexPropertyListByLabel(label);
  }

  std::vector<std::pair<std::string, std::string>> vertex_properties(
      label_id_t label_id) const {
    return schema_->GetVertexPropertyListByLabel(label_id);
  }

  std::vector<std::pair<std::string, std::string>> edge_properties(
      const std::string& label) const {
    return schema_->GetEdgePropertyListByLabel(label);
  }

  std::vector<std::pair<std::string, std::string>> edge_properties(
      label_id_t label_id) const {
    return schema_->GetEdgePropertyListByLabel(label_id);
  }

  prop_id_t get_vertex_property_id_by_name(const std::string& v_label,
                                           const std::string& name) const {
    label_id_t v_label_id = schema_->GetVertexLabelId(v_label);
    return get_vertex_property_id_by_name(v_label_id, name);
  }

  prop_id_t get_vertex_property_id_by_name(label_id_t v_label_id,
                                           const std::string& name) const {
    return schema_->GetVertexPropertyId(v_label_id, name);
  }

  std::string get_vertex_property_by_id(const std::string& v_label,
                                        prop_id_t v_prop_id) const {
    label_id_t v_label_id = schema_->GetVertexLabelId(v_label);
    return get_vertex_property_by_id(v_label_id, v_prop_id);
  }

  std::string get_vertex_property_by_id(label_id_t v_label_id,
                                        prop_id_t v_prop_id) const {
    return schema_->GetVertexPropertyName(v_label_id, v_prop_id);
  }

  prop_id_t get_edge_property_id_by_name(const std::string& e_label,
                                         const std::string& name) const {
    label_id_t e_label_id = schema_->GetEdgeLabelId(e_label);
    return get_edge_property_id_by_name(e_label_id, name);
  }

  prop_id_t get_edge_property_id_by_name(label_id_t e_label_id,
                                         const std::string& name) const {
    return schema_->GetEdgePropertyId(e_label_id, name);
  }

  std::string get_edge_property_by_id(const std::string& e_label,
                                      prop_id_t e_prop_id) const {
    label_id_t e_label_id = schema_->GetEdgeLabelId(e_label);
    return get_edge_property_by_id(e_label_id, e_prop_id);
  }

  std::string get_edge_property_by_id(label_id_t e_label_id,
                                      prop_id_t e_prop_id) const {
    return schema_->GetEdgePropertyName(e_label_id, e_prop_id);
  }

  void send_message(const vertex_t& v, const MD_T& value) {
    if (enable_combine_) {
      label_id_t label = fragment_->vertex_label(v);
      messages_out_[label][v].emplace_back(value);
    } else {
      if (fragment_->IsOuterVertex(v)) {
        message_manager_->SyncStateOnOuterVertex<fragment_t, MD_T>(*fragment_,
                                                                   v, value);
      } else {
        label_id_t label = fragment_->vertex_label(v);
        messages_out_[label][v].emplace_back(value);
      }
    }
  }

  void send_message(const vertex_t& v, MD_T&& value) {
    if (enable_combine_) {
      label_id_t label = fragment_->vertex_label(v);
      messages_out_[label][v].emplace_back(std::move(value));
    } else {
      if (fragment_->IsOuterVertex(v)) {
        message_manager_->SyncStateOnOuterVertex<fragment_t, MD_T>(*fragment_,
                                                                   v, value);
      } else {
        label_id_t label = fragment_->vertex_label(v);
        messages_out_[label][v].emplace_back(std::move(value));
      }
    }
  }

  template <typename COMBINATOR_T>
  void apply_combine(COMBINATOR_T& cb) {
    for (int label_id = 0; label_id < vertex_label_num_; ++label_id) {
      auto vertices = fragment_->Vertices(label_id);
      for (auto v : vertices) {
        auto& msgs = messages_out_[label_id][v];
        if (!msgs.empty()) {
          MD_T ret = cb.CombineMessages(grape::IteratorPair<MD_T*>(
              &msgs[0], &msgs[0] + static_cast<ptrdiff_t>(msgs.size())));
          msgs.clear();
          msgs.emplace_back(std::move(ret));
        }
      }
    }
  }

  void before_comm() {
    for (int label_id = 0; label_id < vertex_label_num_; ++label_id) {
      auto inner_vertices = fragment_->InnerVertices(label_id);
      for (auto v : inner_vertices) {
        messages_in_[label_id][v].clear();
        messages_in_[label_id][v].swap(messages_out_[label_id][v]);
        if (!messages_in_[label_id][v].empty()) {
          activate(v);
        }
      }
    }
  }

  bool active(const vertex_t& v) {
    int label = fragment_->vertex_label(v);
    return !halted_[label][v];
  }

  void activate(const vertex_t& v) {
    int label = fragment_->vertex_label(v);
    if (halted_[label][v] == true) {
      halted_[label][v] = false;
      --voted_to_halt_num_;
    }
  }

  void vote_to_halt(const pregel_vertex_t& vertex) {
    if (halted_[vertex.label_id()][vertex.vertex()] == false) {
      halted_[vertex.label_id()][vertex.vertex()] = true;
      ++voted_to_halt_num_;
    }
  }

  bool all_halted() { return voted_to_halt_num_ == inner_vertex_num_; }

  typename FRAG_T::template vertex_array_t<std::vector<MD_T>>& messages_in(
      int label_id) {
    return messages_in_[label_id];
  }

  typename FRAG_T::template vertex_array_t<std::vector<MD_T>>& messages_out(
      int label_id) {
    return messages_out_[label_id];
  }

  typename FRAG_T::template vertex_array_t<VD_T>& vertex_data(int label_id) {
    return vertex_data_[label_id];
  }

  void clear_for_next_round() {
    if (!enable_combine_) {
      for (int label_id = 0; label_id < vertex_label_num_; ++label_id) {
        auto inner_vertices = fragment_->InnerVertices(label_id);
        for (auto v : inner_vertices) {
          messages_in_[label_id][v].clear();
          messages_in_[label_id][v].swap(messages_out_[label_id][v]);
          if (!messages_in_[label_id][v].empty()) {
            activate(v);
          }
        }
      }
    }
  }

  void enable_combine() { enable_combine_ = true; }
  void set_fragment(const fragment_t* fragment) { fragment_ = fragment; }
  void set_message_manager(grape::DefaultMessageManager* message_manager) {
    message_manager_ = message_manager;
  }

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

  std::unordered_map<std::string, std::shared_ptr<IAggregator>>& aggregators() {
    return aggregators_;
  }

  void register_aggregator(const std::string& name, PregelAggregatorType type) {
    if (aggregators_.find(name) == aggregators_.end()) {
      aggregators_.emplace(name, AggregatorFactory::CreateAggregator(type));
      aggregators_.at(name)->Init();
    }
  }

  // `class_name` is used to index the user-defined aggregator
  // this function only can be used in python mode
  void register_aggregator(const std::string& name,
                           const std::string& class_name) {
    if ((aggregators_.find(name) == aggregators_.end()) &&
        (aggregators_.find(class_name) != aggregators_.end())) {
      aggregators_.emplace(name, aggregators_.at(class_name)->clone());
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

  size_t get_total_vertices_num() { return fragment_->GetTotalNodesNum(); }

  vineyard::ObjectID vertex_map_id() { return fragment_->vertex_map_id(); }

  const vineyard::PropertyGraphSchema* schema() const { return schema_; }

 private:
  const fragment_t* fragment_;
  grape::DefaultMessageManager* message_manager_;

  // graph schema
  const vineyard::PropertyGraphSchema* schema_;

  std::vector<typename FRAG_T::template vertex_array_t<VD_T>>& vertex_data_;

  size_t voted_to_halt_num_;
  std::vector<typename FRAG_T::template vertex_array_t<bool>> halted_;

  std::vector<typename FRAG_T::template vertex_array_t<std::vector<MD_T>>>
      messages_out_;
  std::vector<typename FRAG_T::template vertex_array_t<std::vector<MD_T>>>
      messages_in_;

  size_t inner_vertex_num_;
  label_id_t vertex_label_num_;
  label_id_t edge_label_num_;

  bool enable_combine_;

  int step_;
  std::unordered_map<std::string, std::string> config_;
  std::unordered_map<std::string, std::shared_ptr<IAggregator>> aggregators_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_PROPERTY_VERTEX_H_

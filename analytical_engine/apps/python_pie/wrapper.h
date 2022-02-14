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

#ifndef ANALYTICAL_ENGINE_APPS_PYTHON_PIE_WRAPPER_H_
#define ANALYTICAL_ENGINE_APPS_PYTHON_PIE_WRAPPER_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "boost/lexical_cast.hpp"

#include "vineyard/graph/fragment/arrow_fragment.h"

#include "apps/python_pie/aggregate_factory.h"
#include "core/app/property_auto_app_base.h"

namespace gs {

template <typename FRAG_T>
class PIEAdjList;

template <typename FRAG_T>
class PythonPIEFragment {
  using fragment_t = FRAG_T;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using eid_t = typename fragment_t::eid_t;
  using prop_id_t = typename fragment_t::prop_id_t;
  using label_id_t = typename fragment_t::label_id_t;
  using vertex_range_t = typename fragment_t::vertex_range_t;
  using vertex_t = typename fragment_t::vertex_t;
  using nbr_t = typename fragment_t::nbr_t;
  using vertex_map_t = typename fragment_t::vertex_map_t;
  using adj_list_t = PIEAdjList<fragment_t>;

 public:
  PythonPIEFragment() = default;
  ~PythonPIEFragment() {}

  fid_t fid() { return fragment_->fid(); }

  fid_t fnum() { return fragment_->fnum(); }

  label_id_t vertex_label_num() const { return fragment_->vertex_label_num(); }

  label_id_t vertex_label(const vertex_t& v) const {
    return fragment_->vertex_label(v);
  }

  int64_t vertex_offset(const vertex_t& v) const {
    return fragment_->vertex_offset(v);
  }

  label_id_t edge_label_num() const { return fragment_->edge_label_num(); }

  std::shared_ptr<arrow::Table> vertex_data_table(label_id_t i) const {
    return fragment_->vertex_data_table(i);
  }

  std::shared_ptr<arrow::Table> edge_data_table(label_id_t i) const {
    return fragment_->edge_data_table(i);
  }

  size_t get_total_nodes_num() const { return fragment_->GetTotalNodesNum(); }

  size_t get_inner_nodes_num(label_id_t label_id) const {
    return fragment_->GetInnerVerticesNum(label_id);
  }

  size_t get_outer_nodes_num(label_id_t label_id) const {
    return fragment_->GetOuterVerticesNum(label_id);
  }

  size_t get_nodes_num(label_id_t label_id) const {
    return nodes(label_id).size();
  }

  vertex_range_t nodes(label_id_t label_id) const {
    return fragment_->Vertices(label_id);
  }
  vertex_range_t inner_nodes(label_id_t label_id) const {
    return fragment_->InnerVertices(label_id);
  }
  vertex_range_t outer_nodes(label_id_t label_id) const {
    return fragment_->OuterVertices(label_id);
  }
  int get_node_fid(const vertex_t& v) const { return fragment_->GetFragId(v); }

  bool is_inner_node(const vertex_t& v) { return fragment_->IsInnerVertex(v); }
  bool is_outer_node(const vertex_t& v) { return fragment_->IsOuterVertex(v); }
  bool get_node(label_id_t label, const std::string& oid, vertex_t& v) {
    return fragment_->GetVertex(label, boost::lexical_cast<oid_t>(oid), v);
  }
  bool get_inner_node(label_id_t label, const std::string& oid, vertex_t& v) {
    return fragment_->GetInnerVertex(label, boost::lexical_cast<oid_t>(oid), v);
  }
  bool get_outer_node(label_id_t label, const std::string& oid, vertex_t& v) {
    return fragment_->GetOuterVertex(label, boost::lexical_cast<oid_t>(oid), v);
  }
  bool get_node_by_gid(vid_t gid, vertex_t& v) {
    return fragment_->Gid2Vertex(gid, v);
  }
  std::string get_node_id(const vertex_t& v) const {
    return boost::lexical_cast<std::string>(fragment_->GetId(v));
  }

  uint64_t get_inner_node_gid(const vertex_t& v) {
    return fragment_->GetInnerVertexGid(v);
  }
  uint64_t get_outer_node_gid(const vertex_t& v) {
    return fragment_->GetOuterVertexGid(v);
  }
  bool get_gid_by_oid(const std::string& oid, vid_t& gid) {
    return fragment_->Oid2Gid(boost::lexical_cast<oid_t>(oid), gid);
  }
  adj_list_t get_outgoing_edges(const vertex_t& v, label_id_t e_label) {
    return adj_list_t(fragment_->GetOutgoingAdjList(v, e_label));
  }
  adj_list_t get_incoming_edges(const vertex_t& v, label_id_t e_label) {
    return adj_list_t(fragment_->GetIncomingAdjList(v, e_label));
  }
  bool has_child(const vertex_t& v, label_id_t e_label) {
    return fragment_->HasChild(v, e_label);
  }
  bool has_parent(const vertex_t& v, label_id_t e_label) {
    return fragment_->HasParent(v, e_label);
  }
  int get_indegree(const vertex_t& v, label_id_t e_label) {
    return fragment_->GetLocalInDegree(v, e_label);
  }
  int get_outdegree(const vertex_t& v, label_id_t e_label) {
    return fragment_->GetLocalInDegree(v, e_label);
  }
  std::string get_str(const vertex_t& v, prop_id_t prop_id) const {
    return fragment_->template GetData<std::string>(v, prop_id);
  }
  double get_double(const vertex_t& v, prop_id_t prop_id) const {
    return fragment_->template GetData<double>(v, prop_id);
  }
  int64_t get_int(const vertex_t& v, prop_id_t prop_id) const {
    return fragment_->template GetData<int64_t>(v, prop_id);
  }
  bool Gid2Vertex(const vid_t& gid, vertex_t& v) const {
    return fragment_->Gid2Vertex(gid, v);
  }
  // schema
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
  std::vector<std::string> vertex_labels() const {
    return fragment_->schema().GetVertexLabels();
  }
  std::vector<std::string> edge_labels() const {
    return fragment_->schema().GetEdgeLabels();
  }
  std::string get_vertex_label_by_id(label_id_t v_label_id) const {
    return fragment_->schema().GetVertexLabelName(v_label_id);
  }
  label_id_t get_vertex_label_id_by_name(const std::string& name) const {
    return fragment_->schema().GetVertexLabelId(name);
  }
  std::string get_edge_label_by_id(label_id_t e_label_id) const {
    return fragment_->schema().GetEdgeLabelName(e_label_id);
  }
  label_id_t get_edge_label_id_by_name(const std::string& name) const {
    return fragment_->schema().GetEdgeLabelId(name);
  }
  std::vector<std::pair<std::string, std::string>> vertex_properties(
      const std::string& label) const {
    return fragment_->schema().GetVertexPropertyListByLabel(label);
  }
  std::vector<std::pair<std::string, std::string>> vertex_properties(
      label_id_t label_id) const {
    return fragment_->schema().GetVertexPropertyListByLabel(label_id);
  }
  std::vector<std::pair<std::string, std::string>> edge_properties(
      const std::string& label) const {
    return fragment_->schema().GetEdgePropertyListByLabel(label);
  }
  std::vector<std::pair<std::string, std::string>> edge_properties(
      label_id_t label_id) const {
    return fragment_->schema().GetEdgePropertyListByLabel(label_id);
  }
  prop_id_t get_vertex_property_id_by_name(const std::string& v_label,
                                           const std::string& name) const {
    label_id_t v_label_id = fragment_->schema().GetVertexLabelId(v_label);
    return get_vertex_property_id_by_name(v_label_id, name);
  }
  prop_id_t get_vertex_property_id_by_name(label_id_t v_label_id,
                                           const std::string& name) const {
    return fragment_->schema().GetVertexPropertyId(v_label_id, name);
  }
  std::string get_vertex_property_by_id(const std::string& v_label,
                                        prop_id_t v_prop_id) const {
    label_id_t v_label_id = fragment_->schema().GetVertexLabelId(v_label);
    return get_vertex_property_by_id(v_label_id, v_prop_id);
  }
  std::string get_vertex_property_by_id(label_id_t v_label_id,
                                        prop_id_t v_prop_id) const {
    return fragment_->schema().GetVertexPropertyName(v_label_id, v_prop_id);
  }
  prop_id_t get_edge_property_id_by_name(const std::string& e_label,
                                         const std::string& name) const {
    label_id_t e_label_id = fragment_->schema().GetEdgeLabelId(e_label);
    return get_edge_property_id_by_name(e_label_id, name);
  }
  prop_id_t get_edge_property_id_by_name(label_id_t e_label_id,
                                         const std::string& name) const {
    return fragment_->schema().GetEdgePropertyId(e_label_id, name);
  }
  std::string get_edge_property_by_id(const std::string& e_label,
                                      prop_id_t e_prop_id) const {
    label_id_t e_label_id = fragment_->schema().GetEdgeLabelId(e_label);
    return get_edge_property_by_id(e_label_id, e_prop_id);
  }
  std::string get_edge_property_by_id(label_id_t e_label_id,
                                      prop_id_t e_prop_id) const {
    return fragment_->schema().GetEdgePropertyName(e_label_id, e_prop_id);
  }

  vid_t Vertex2Gid(const vertex_t& v) const { return fragment_->Vertex2Gid(v); }

  std::shared_ptr<vertex_map_t> GetVertexMap() {
    return fragment_->GetVertexMap();
  }

  void set_fragment(const fragment_t* fragment) { fragment_ = fragment; }

 private:
  const fragment_t* fragment_;
};

template <typename FRAG_T, typename VD_T, typename MD_T>
class PythonPIEComputeContext {
  using fragment_t = FRAG_T;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using eid_t = typename fragment_t::eid_t;
  using prop_id_t = typename fragment_t::prop_id_t;
  using label_id_t = typename fragment_t::label_id_t;
  using vertex_range_t = typename fragment_t::vertex_range_t;
  using vertex_t = typename fragment_t::vertex_t;
  using nbr_t = typename fragment_t::nbr_t;
  using adj_list_t = typename fragment_t::adj_list_t;
  using vertex_map_t = typename fragment_t::vertex_map_t;

 public:
  // exposes to PregelContext
  using vd_t = VD_T;

  explicit PythonPIEComputeContext(
      std::vector<grape::VertexArray<typename FRAG_T::vertices_t, VD_T>>& data)
      : superstep_(0), data_(data) {}
  ~PythonPIEComputeContext() {}

  void init(const fragment_t& frag) {
    superstep_ = 0;
    auto v_label_num = frag.vertex_label_num();
    for (label_id_t v_label = 0; v_label < v_label_num; v_label++) {
      partial_result_.emplace_back(data_[v_label]);
    }
  }

  void inc_superstep() { superstep_++; }

  int superstep() { return superstep_; }

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

  void set_node_value(vertex_t& v, VD_T value) {
    auto label = fragment_->vertex_label(v);
    partial_result_[label].SetValue(v, value);
  }

  VD_T get_node_value(const vertex_t& v) {
    auto label = fragment_->vertex_label(v);
    return partial_result_[label][v];
  }

  void init_value(vertex_range_t vertices, label_id_t label, VD_T value,
                  const std::function<bool(MD_T*, MD_T&&)>& aggregator) {
    partial_result_[label].Init(vertices, value, aggregator);
  }

  void init_value(vertex_range_t vertices, label_id_t label, VD_T value,
                  PIEAggregateType type) {
    partial_result_[label].Init(vertices, value,
                                AggregateFactory::CreateAggregate<MD_T>(type));
  }

  bool is_updated(const vertex_t& v) {
    auto label = fragment_->vertex_label(v);
    return partial_result_[label].IsUpdated(v);
  }

  grape::SyncBuffer<typename FRAG_T::vertices_t, VD_T>& partial_result(
      label_id_t label) {
    return partial_result_[label];
  }

  void register_sync_buffer(label_id_t label_id,
                            grape::MessageStrategy message_strategy) {
    CHECK(fragment_);
    message_manager_->RegisterSyncBuffer(
        *fragment_, label_id, &partial_result_[label_id], message_strategy);
  }

  void set_fragment(const fragment_t* fragment) { fragment_ = fragment; }

  void set_message_manager(
      PropertyAutoMessageManager<fragment_t>* message_manager) {
    message_manager_ = message_manager;
  }

 private:
  int superstep_;
  std::unordered_map<std::string, std::string> config_;

  const fragment_t* fragment_;
  PropertyAutoMessageManager<fragment_t>* message_manager_;

  // message auto parallel
  std::vector<grape::VertexArray<typename FRAG_T::vertices_t, VD_T>>& data_;
  std::vector<grape::SyncBuffer<typename FRAG_T::vertices_t, VD_T>>
      partial_result_;
};

template <typename FRAG_T>
class PIEAdjList {
  using fragment_t = FRAG_T;
  using nbr_t = typename fragment_t::nbr_t;
  using nbr_iterator_t = const nbr_t*;
  using adj_list_t = typename fragment_t::adj_list_t;

 public:
  PIEAdjList() : adj_list_() {}
  explicit PIEAdjList(adj_list_t adj_list) : adj_list_(adj_list) {}
  ~PIEAdjList() = default;

  class iterator {
    using pointer_type = nbr_t*;
    using reference_type = nbr_t&;

   public:
    iterator() noexcept : nbr_() {}
    explicit iterator(nbr_t&& nbr) : nbr_(std::move(nbr)) {}
    explicit iterator(const nbr_t& nbr) : nbr_(nbr) {}
    iterator& operator=(const nbr_t& nbr) { nbr_ = nbr; }
    iterator& operator=(nbr_t&& nbr) { nbr_ = std::move(nbr); }

    reference_type operator*() noexcept { return nbr_; }
    pointer_type operator->() noexcept { return &nbr_; }

    iterator& operator++() noexcept {
      ++nbr_;
      return *this;
    }

    const iterator operator++(int) noexcept {
      return iterator(std::move(nbr_++));
    }

    bool operator==(const iterator& rhs) noexcept { return nbr_ == rhs.nbr_; }

    bool operator!=(const iterator& rhs) noexcept { return nbr_ != rhs.nbr_; }

   private:
    nbr_t nbr_;
  };

  iterator begin() { return iterator(adj_list_.begin()); }
  iterator end() { return iterator(adj_list_.end()); }

  size_t size() { return adj_list_.size(); }

 private:
  adj_list_t adj_list_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PYTHON_PIE_WRAPPER_H_

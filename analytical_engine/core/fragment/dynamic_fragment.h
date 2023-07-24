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

#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_H_

#ifdef NETWORKX

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <limits>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "grape/fragment/basic_fragment_mutator.h"
#include "grape/fragment/csr_edgecut_fragment_base.h"
#include "grape/graph/de_mutable_csr.h"
#include "grape/utils/bitset.h"
#include "grape/utils/vertex_set.h"
#include "vineyard/graph/fragment/property_graph_types.h"

#include "core/config.h"
#include "core/object/dynamic.h"
#include "core/utils/convert_utils.h"
#include "core/utils/partitioner.h"
#include "proto/types.pb.h"

namespace gs {

struct DynamicFragmentTraits {
  using oid_t = dynamic::Value;
  using vid_t = vineyard::property_graph_types::VID_TYPE;
  using vdata_t = dynamic::Value;
  using edata_t = dynamic::Value;
  using nbr_t = grape::Nbr<vid_t, edata_t>;
  using vertex_map_t = grape::GlobalVertexMap<oid_t, vid_t>;
  using inner_vertices_t = grape::VertexRange<vid_t>;
  using outer_vertices_t = grape::VertexRange<vid_t>;
  using vertices_t = grape::DualVertexRange<vid_t>;
  using sub_vertices_t = grape::VertexVector<vid_t>;

  using fragment_adj_list_t =
      grape::FilterAdjList<vid_t, edata_t, std::function<bool(const nbr_t&)>>;
  using fragment_const_adj_list_t =
      grape::FilterConstAdjList<vid_t, edata_t,
                                std::function<bool(const nbr_t&)>>;

  using csr_t = grape::DeMutableCSR<vid_t, nbr_t>;
  using csr_builder_t = grape::DeMutableCSRBuilder<vid_t, nbr_t>;
  using mirror_vertices_t = std::vector<grape::Vertex<vid_t>>;
};

class DynamicFragment
    : public grape::CSREdgecutFragmentBase<
          dynamic::Value, vineyard::property_graph_types::VID_TYPE,
          dynamic::Value, dynamic::Value, DynamicFragmentTraits> {
 public:
  using oid_t = dynamic::Value;
  using vid_t = vineyard::property_graph_types::VID_TYPE;
  using vdata_t = dynamic::Value;
  using edata_t = dynamic::Value;
  using traits_t = DynamicFragmentTraits;
  using base_t =
      grape::CSREdgecutFragmentBase<oid_t, vid_t, vdata_t, edata_t, traits_t>;
  using internal_vertex_t = grape::internal::Vertex<vid_t, vdata_t>;
  using edge_t = grape::Edge<vid_t, edata_t>;
  using nbr_t = grape::Nbr<vid_t, edata_t>;
  using vertex_t = grape::Vertex<vid_t>;

  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kOnlyOut;

  static constexpr double dense_threshold = 0.003;

  using vertex_map_t = typename traits_t::vertex_map_t;
  using partitioner_t = typename vertex_map_t::partitioner_t;
  using mutation_t = grape::Mutation<vid_t, vdata_t, edata_t>;

  using IsEdgeCut = std::true_type;
  using IsVertexCut = std::false_type;

  using inner_vertices_t = typename traits_t::inner_vertices_t;
  using outer_vertices_t = typename traits_t::outer_vertices_t;
  using vertices_t = typename traits_t::vertices_t;
  using fragment_adj_list_t = typename traits_t::fragment_adj_list_t;
  using fragment_const_adj_list_t =
      typename traits_t::fragment_const_adj_list_t;

  template <typename T>
  using inner_vertex_array_t = grape::VertexArray<inner_vertices_t, T>;

  template <typename T>
  using outer_vertex_array_t = grape::VertexArray<outer_vertices_t, T>;

  template <typename T>
  using vertex_array_t = grape::VertexArray<vertices_t, T>;

  using vertex_range_t = inner_vertices_t;

  explicit DynamicFragment(std::shared_ptr<vertex_map_t> vm_ptr)
      : grape::FragmentBase<oid_t, vid_t, vdata_t, edata_t, traits_t>(vm_ptr) {}
  virtual ~DynamicFragment() = default;

  using base_t::buildCSR;
  using base_t::init;
  using base_t::IsInnerVertexGid;
  void Init(fid_t fid, bool directed, std::vector<internal_vertex_t>& vertices,
            std::vector<edge_t>& edges) override {
    init(fid, directed);

    load_strategy_ = directed ? grape::LoadStrategy::kBothOutIn
                              : grape::LoadStrategy::kOnlyOut;

    ovnum_ = 0;
    static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
    if (load_strategy_ == grape::LoadStrategy::kOnlyIn) {
      for (auto& e : edges) {
        if (IsInnerVertexGid(e.dst)) {
          if (!IsInnerVertexGid(e.src)) {
            parseOrAddOuterVertexGid(e.src);
          }
        } else {
          e.src = invalid_vid;
        }
      }
    } else if (load_strategy_ == grape::LoadStrategy::kOnlyOut) {
      for (auto& e : edges) {
        if (IsInnerVertexGid(e.src)) {
          if (!IsInnerVertexGid(e.dst)) {
            parseOrAddOuterVertexGid(e.dst);
          }
        } else {
          e.src = invalid_vid;
        }
      }
    } else if (load_strategy_ == grape::LoadStrategy::kBothOutIn) {
      for (auto& e : edges) {
        if (IsInnerVertexGid(e.src)) {
          if (!IsInnerVertexGid(e.dst)) {
            parseOrAddOuterVertexGid(e.dst);
          }
        } else {
          if (IsInnerVertexGid(e.dst)) {
            parseOrAddOuterVertexGid(e.src);
          } else {
            e.src = invalid_vid;
          }
        }
      }
    }

    initVertexMembersOfFragment();
    initOuterVerticesOfFragment();

    buildCSR(this->Vertices(), edges, load_strategy_);

    ivdata_.clear();
    ivdata_.resize(ivnum_, dynamic::Value(rapidjson::kObjectType));
    if (sizeof(internal_vertex_t) > sizeof(vid_t)) {
      for (auto& v : vertices) {
        vid_t gid = v.vid;
        if (id_parser_.get_fragment_id(gid) == fid_) {
          ivdata_[id_parser_.get_local_id(gid)] = std::move(v.vdata);
        }
      }
    }

    initSchema();
  }

  // Init an empty fragment.
  void Init(fid_t fid, bool directed) {
    std::vector<internal_vertex_t> empty_vertices;
    std::vector<edge_t> empty_edges;
    Init(fid, directed, empty_vertices, empty_edges);
  }

  // Init fragment from arrow property fragment.
  void Init(fid_t fid, bool directed,
            std::vector<std::vector<internal_vertex_t>>& vertices,
            std::vector<std::vector<edge_t>>& edges,
            std::vector<int>& inner_oe_degree,
            std::vector<int>& outer_oe_degree,
            std::vector<int>& inner_ie_degree,
            std::vector<int>& outer_ie_degree, uint32_t thread_num) {
    init(fid, directed);
    load_strategy_ = directed ? grape::LoadStrategy::kBothOutIn
                              : grape::LoadStrategy::kOnlyOut;

    ovnum_ = 0;
    if (load_strategy_ == grape::LoadStrategy::kOnlyOut) {
      for (auto& vec : edges) {
        for (auto& e : vec) {
          if (!IsInnerVertexGid(e.dst)) {
            parseOrAddOuterVertexGid(e.dst);
          }
        }
      }
    } else if (load_strategy_ == grape::LoadStrategy::kBothOutIn) {
      for (auto& vec : edges) {
        for (auto& e : vec) {
          if (IsInnerVertexGid(e.src)) {
            if (!IsInnerVertexGid(e.dst)) {
              parseOrAddOuterVertexGid(e.dst);
            }
          } else {
            parseOrAddOuterVertexGid(e.src);
          }
        }
      }
    }

    initVertexMembersOfFragment();
    initOuterVerticesOfFragment();

    buildCSRParallel(edges, inner_oe_degree, outer_oe_degree, inner_ie_degree,
                     outer_ie_degree, thread_num);

    ivdata_.clear();
    ivdata_.resize(ivnum_);
    // process vertices data parallel
    if (sizeof(internal_vertex_t) > sizeof(vid_t)) {
      parallel_for(
          vertices.begin(), vertices.end(),
          [&](uint32_t tid, std::vector<internal_vertex_t>& vs) {
            for (auto& v : vs) {
              ivdata_[v.vid] = std::move(v.vdata);
            }
          },
          thread_num, 1);
    }

    initSchema();
  }

  using base_t::Gid2Lid;
  using base_t::ie_;
  using base_t::oe_;
  using base_t::vm_ptr_;
  void Mutate(mutation_t& mutation) {
    vertex_t v;
    if (!mutation.vertices_to_remove.empty() &&
        static_cast<double>(mutation.vertices_to_remove.size()) /
                static_cast<double>(this->GetVerticesNum()) <
            0.1) {
      std::set<vertex_t> sparse_set;
      for (auto gid : mutation.vertices_to_remove) {
        if (Gid2Vertex(gid, v) && IsAliveVertex(v)) {
          if (IsInnerVertex(v)) {
            if (load_strategy_ == grape::LoadStrategy::kBothOutIn) {
              ie_.remove_vertex(v.GetValue());
            }
            oe_.remove_vertex(v.GetValue());
            iv_alive_.reset_bit(v.GetValue());
            --alive_ivnum_;
            is_selfloops_.reset_bit(v.GetValue());
          } else {
            ov_alive_.reset_bit(outerVertexLidToIndex(v.GetValue()));
          }
          sparse_set.insert(v);
        }
      }
      if (!sparse_set.empty()) {
        auto func = [&sparse_set](vid_t i, const nbr_t& e) {
          return sparse_set.find(e.neighbor) != sparse_set.end();
        };
        if (load_strategy_ == grape::LoadStrategy::kBothOutIn) {
          ie_.remove_if(func);
        }
        oe_.remove_if(func);
      }
    } else if (!mutation.vertices_to_remove.empty()) {
      grape::DenseVertexSet<vertices_t> dense_bitset(Vertices());
      for (auto gid : mutation.vertices_to_remove) {
        if (Gid2Vertex(gid, v) && IsAliveVertex(v)) {
          if (IsInnerVertex(v)) {
            if (load_strategy_ == grape::LoadStrategy::kBothOutIn) {
              ie_.remove_vertex(v.GetValue());
            }
            oe_.remove_vertex(v.GetValue());
            iv_alive_.reset_bit(v.GetValue());
            --alive_ivnum_;
            is_selfloops_.reset_bit(v.GetValue());
          } else {
            ov_alive_.reset_bit(outerVertexLidToIndex(v.GetValue()));
          }
          dense_bitset.Insert(v);
        }
      }
      auto func = [&dense_bitset](vid_t i, const nbr_t& e) {
        return dense_bitset.Exist(e.neighbor);
      };
      if (!dense_bitset.Empty()) {
        if (load_strategy_ == grape::LoadStrategy::kBothOutIn) {
          ie_.remove_if(func);
        }
        oe_.remove_if(func);
      }
    }
    if (!mutation.edges_to_remove.empty()) {
      removeEdges(mutation.edges_to_remove);
    }
    if (!mutation.edges_to_update.empty()) {
      for (auto& e : mutation.edges_to_update) {
        if (IsInnerVertexGid(e.src)) {
          e.src = id_parser_.get_local_id(e.src);
        } else {
          e.src = parseOuterVertexGid(e.src);
        }
        if (IsInnerVertexGid(e.dst)) {
          e.dst = id_parser_.get_local_id(e.dst);
        } else {
          e.dst = parseOuterVertexGid(e.dst);
        }
      }
      updateEdges(mutation.edges_to_update);
    }
    {
      auto& edges_to_add = mutation.edges_to_add;
      static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
      vid_t old_ovnum = ovgid_.size();

      // parseOrAddOuterVertexGid will update ovnum_
      for (auto& e : edges_to_add) {
        if (IsInnerVertexGid(e.src)) {
          e.src = id_parser_.get_local_id(e.src);
          if (IsInnerVertexGid(e.dst)) {
            e.dst = id_parser_.get_local_id(e.dst);
          } else {
            mutation.vertices_to_add.emplace_back(e.dst);
            e.dst = parseOrAddOuterVertexGid(e.dst);
          }
        } else {
          if (IsInnerVertexGid(e.dst)) {
            mutation.vertices_to_add.emplace_back(e.src);
            e.src = parseOrAddOuterVertexGid(e.src);
            e.dst = id_parser_.get_local_id(e.dst);
          } else {
            e.src = invalid_vid;
          }
        }
      }
      vid_t new_ivnum = vm_ptr_->GetInnerVertexSize(fid_);
      vid_t new_ovnum = ovgid_.size();
      assert(new_ovnum == ovnum_);
      assert(new_ivnum >= ivnum_ && new_ovnum >= old_ovnum);
      is_selfloops_.resize(new_ivnum);
      oe_.add_vertices(new_ivnum - ivnum_, new_ovnum - old_ovnum);
      ie_.add_vertices(new_ivnum - ivnum_, new_ovnum - old_ovnum);
      this->ivnum_ = new_ivnum;
      if (old_ovnum != new_ovnum) {
        initOuterVerticesOfFragment();
      }
      if (!edges_to_add.empty()) {
        addEdges(edges_to_add);
      }

      this->inner_vertices_.SetRange(0, new_ivnum);
      this->outer_vertices_.SetRange(id_parser_.max_local_id() - new_ovnum,
                                     id_parser_.max_local_id());
      this->vertices_.SetRange(0, new_ivnum,
                               id_parser_.max_local_id() - new_ovnum,
                               id_parser_.max_local_id());
    }
    ivdata_.resize(this->ivnum_, dynamic::Value(rapidjson::kObjectType));
    iv_alive_.resize(this->ivnum_);
    ov_alive_.resize(this->ovnum_);
    alive_ovnum_ = this->ovnum_;
    for (auto& v : mutation.vertices_to_add) {
      vid_t lid;
      if (IsInnerVertexGid(v.vid)) {
        this->InnerVertexGid2Lid(v.vid, lid);
        ivdata_[lid].Update(v.vdata);
        if (iv_alive_.get_bit(lid) == false) {
          iv_alive_.set_bit(lid);
          ++alive_ivnum_;
        }
      } else {
        if (this->OuterVertexGid2Lid(v.vid, lid)) {
          auto index = outerVertexLidToIndex(lid);
          if (ov_alive_.get_bit(index) == false) {
            ov_alive_.set_bit(index);
          }
        }
      }
    }
    for (auto& v : mutation.vertices_to_update) {
      vid_t lid;
      if (IsInnerVertexGid(v.vid)) {
        this->InnerVertexGid2Lid(v.vid, lid);
        ivdata_[lid] = std::move(v.vdata);
      }
    }
  }

  void PrepareToRunApp(const grape::CommSpec& comm_spec,
                       grape::PrepareConf conf) override {
    base_t::PrepareToRunApp(comm_spec, conf);
    if (conf.need_split_edges_by_fragment) {
      LOG(ERROR) << "MutableEdgecutFragment cannot split edges by fragment";
    } else if (conf.need_split_edges) {
      splitEdges(comm_spec);
    }
  }

  inline size_t GetEdgeNum() const override {
    size_t res = this->directed_ ? oe_.head_edge_num() + ie_.head_edge_num()
                                 : oe_.head_edge_num() + is_selfloops_.count();
    return res;
  }

  using base_t::InnerVertices;
  using base_t::IsInnerVertex;
  using base_t::OuterVertices;

  vid_t GetVerticesNum() const { return alive_ivnum_ + alive_ovnum_; }

  vid_t GetInnerVerticesNum() const { return alive_ivnum_; }

  vid_t GetOuterVerticesNum() const { return alive_ovnum_; }

  inline const vdata_t& GetData(const vertex_t& v) const override {
    CHECK(IsInnerVertex(v));
    return ivdata_[v.GetValue()];
  }

  inline void SetData(const vertex_t& v, const vdata_t& val) override {
    CHECK(IsInnerVertex(v));
    ivdata_[v.GetValue()] = val;
  }

  bool OuterVertexGid2Lid(vid_t gid, vid_t& lid) const override {
    auto iter = ovg2i_.find(gid);
    if (iter != ovg2i_.end()) {
      lid = iter->second;
      return true;
    } else {
      return false;
    }
  }

  vid_t GetOuterVertexGid(vertex_t v) const override {
    return ovgid_[outerVertexLidToIndex(v.GetValue())];
  }

  bool IsOuterVertexGid(vid_t gid) const {
    return ovg2i_.find(gid) != ovg2i_.end();
  }

  inline bool Gid2Vertex(const vid_t& gid, vertex_t& v) const override {
    fid_t fid = id_parser_.get_fragment_id(gid);
    if (fid == fid_) {
      v.SetValue(id_parser_.get_local_id(gid));
      return true;
    } else {
      auto iter = ovg2i_.find(gid);
      if (iter != ovg2i_.end()) {
        v.SetValue(iter->second);
        return true;
      } else {
        return false;
      }
    }
  }

  inline vid_t Vertex2Gid(const vertex_t& v) const override {
    if (IsInnerVertex(v)) {
      return id_parser_.generate_global_id(fid_, v.GetValue());
    } else {
      return ovgid_[outerVertexLidToIndex(v.GetValue())];
    }
  }

  void ClearGraph(std::shared_ptr<vertex_map_t> vm_ptr) {
    vm_ptr_.reset();
    vm_ptr_ = vm_ptr;
    Init(fid_, directed_);
  }

  void ClearEdges() {
    if (load_strategy_ == grape::LoadStrategy::kBothOutIn) {
      ie_.clear_edges();
    }
    oe_.clear_edges();

    // clear outer_vertices map
    ovgid_.clear();
    ovg2i_.clear();
    ov_alive_.clear();
    this->ovnum_ = 0;
    this->alive_ovnum_ = 0;
    is_selfloops_.clear();
  }

  void CopyFrom(std::shared_ptr<DynamicFragment> source,
                const std::string& copy_type = "identical") {
    init(source->fid_, source->directed_);
    load_strategy_ = source->load_strategy_;
    copyVertices(source);

    // copy edges
    auto vnum = id_parser_.max_local_id();
    ie_.init_head_and_tail(0, vnum);
    oe_.init_head_and_tail(0, vnum);
    ie_.add_vertices(ivnum_, ovnum_);
    oe_.add_vertices(ivnum_, ovnum_);
    if (copy_type == "identical") {
      std::vector<int> inner_oe_degree_to_add(ivnum_, 0),
          inner_ie_degree_to_add(ivnum_, 0), outer_oe_degree_to_add(ovnum_, 0),
          outer_ie_degree_to_add(ovnum_, 0);
      for (vid_t i = 0; i < ivnum_; ++i) {
        inner_oe_degree_to_add[i] = source->oe_.degree(i);
        inner_ie_degree_to_add[i] = source->ie_.degree(i);
      }

      for (vid_t i = 0; i < ovnum_; ++i) {
        outer_oe_degree_to_add[i] =
            source->oe_.degree(outerVertexIndexToLid(i));
        outer_ie_degree_to_add[i] =
            source->ie_.degree(outerVertexIndexToLid(i));
      }

      oe_.reserve_edges_dense(inner_oe_degree_to_add, outer_oe_degree_to_add);
      ie_.reserve_edges_dense(inner_ie_degree_to_add, outer_ie_degree_to_add);

      for (vid_t i = 0; i < ivnum_; ++i) {
        auto ie_begin = source->ie_.get_begin(i);
        auto ie_end = source->ie_.get_end(i);
        auto oe_begin = source->oe_.get_begin(i);
        auto oe_end = source->oe_.get_end(i);
        for (auto iter = ie_begin; iter != ie_end; ++iter) {
          ie_.put_edge(i, *iter);
        }
        for (auto iter = oe_begin; iter != oe_end; ++iter) {
          oe_.put_edge(i, *iter);
        }
      }

      for (vid_t i = outerVertexIndexToLid(ovnum_ - 1); i < vnum; ++i) {
        auto ie_begin = source->ie_.get_begin(i);
        auto ie_end = source->ie_.get_end(i);
        auto oe_begin = source->oe_.get_begin(i);
        auto oe_end = source->oe_.get_end(i);
        for (auto iter = ie_begin; iter != ie_end; ++iter) {
          ie_.put_edge(i, *iter);
        }
        for (auto iter = oe_begin; iter != oe_end; ++iter) {
          oe_.put_edge(i, *iter);
        }
      }
    } else if (copy_type == "reverse") {
      assert(directed_);
      std::vector<int> inner_oe_degree_to_add(ivnum_, 0),
          inner_ie_degree_to_add(ivnum_, 0), outer_oe_degree_to_add(ovnum_, 0),
          outer_ie_degree_to_add(ovnum_, 0);
      for (vid_t i = 0; i < ivnum_; ++i) {
        inner_oe_degree_to_add[i] = source->ie_.degree(i);
        inner_ie_degree_to_add[i] = source->oe_.degree(i);
      }

      for (vid_t i = 0; i < ovnum_; ++i) {
        outer_oe_degree_to_add[i] =
            source->ie_.degree(outerVertexIndexToLid(i));
        outer_ie_degree_to_add[i] =
            source->oe_.degree(outerVertexIndexToLid(i));
      }

      oe_.reserve_edges_dense(inner_oe_degree_to_add, outer_oe_degree_to_add);
      ie_.reserve_edges_dense(inner_ie_degree_to_add, outer_ie_degree_to_add);

      for (vid_t i = 0; i < ivnum_; ++i) {
        auto ie_begin = source->ie_.get_begin(i);
        auto ie_end = source->ie_.get_end(i);
        auto oe_begin = source->oe_.get_begin(i);
        auto oe_end = source->oe_.get_end(i);
        for (auto iter = oe_begin; iter != oe_end; ++iter) {
          ie_.put_edge(i, *iter);
        }
        for (auto iter = ie_begin; iter != ie_end; ++iter) {
          oe_.put_edge(i, *iter);
        }
      }

      for (vid_t i = outerVertexIndexToLid(ovnum_ - 1); i < vnum; ++i) {
        auto ie_begin = source->ie_.get_begin(i);
        auto ie_end = source->ie_.get_end(i);
        auto oe_begin = source->oe_.get_begin(i);
        auto oe_end = source->oe_.get_end(i);
        for (auto iter = oe_begin; iter != oe_end; ++iter) {
          ie_.put_edge(i, *iter);
        }
        for (auto iter = ie_begin; iter != ie_end; ++iter) {
          oe_.put_edge(i, *iter);
        }
      }
    } else {
      LOG(ERROR) << "Unsupported copy type: " << copy_type;
    }

    this->schema_.CopyFrom(source->schema_);
  }

  // generate directed graph from original undirected graph.
  void ToDirectedFrom(std::shared_ptr<DynamicFragment> source) {
    assert(!source->directed_);
    init(source->fid_, true);
    load_strategy_ = grape::LoadStrategy::kBothOutIn;
    copyVertices(source);

    // both inner and outer vertices with the empty slots
    auto vnum = id_parser_.max_local_id();
    ie_.init_head_and_tail(0, vnum);
    oe_.init_head_and_tail(0, vnum);
    ie_.add_vertices(ivnum_, ovnum_);
    oe_.add_vertices(ivnum_, ovnum_);

    std::vector<int> inner_degree_to_add(ivnum_, 0),
        outer_degree_to_add(ovnum_, 0);
    for (vid_t i = 0; i < ivnum_; ++i) {
      inner_degree_to_add[i] = source->oe_.degree(i);
    }

    for (vid_t i = 0; i < ovnum_; ++i) {
      outer_degree_to_add[i] = source->oe_.degree(outerVertexIndexToLid(i));
    }

    ie_.reserve_edges_dense(inner_degree_to_add, outer_degree_to_add);
    oe_.reserve_edges_dense(inner_degree_to_add, outer_degree_to_add);

    for (vid_t i = 0; i < ivnum_; ++i) {
      auto begin = source->oe_.get_begin(i);
      auto end = source->oe_.get_end(i);
      for (auto iter = begin; iter != end; ++iter) {
        ie_.put_edge(i, *iter);
        oe_.put_edge(i, *iter);
      }
    }

    this->schema_.CopyFrom(source->schema_);
  }

  // generate undirected graph from original directed graph.
  void ToUndirectedFrom(std::shared_ptr<DynamicFragment> source) {
    assert(source->directed_);
    init(source->fid_, false);
    load_strategy_ = grape::LoadStrategy::kOnlyOut;
    copyVertices(source);

    // both inner and outer vertices with the empty slots
    // only use oe_ in the undirected graph
    auto vnum = id_parser_.max_local_id();
    oe_.init_head_and_tail(0, vnum);
    oe_.add_vertices(ivnum_, ovnum_);

    mutation_t mutation;
    vid_t gid;
    for (auto& v : source->InnerVertices()) {
      gid = Vertex2Gid(v);
      for (const auto& e : source->GetOutgoingAdjList(v)) {
        mutation.edges_to_add.emplace_back(gid, Vertex2Gid(e.neighbor), e.data);
      }
      for (const auto& e : source->GetIncomingAdjList(v)) {
        if (IsOuterVertex(e.neighbor)) {
          mutation.edges_to_add.emplace_back(gid, Vertex2Gid(e.neighbor),
                                             e.data);
        }
      }
    }

    Mutate(mutation);
    this->schema_.CopyFrom(source->schema_);
  }

  // induce a subgraph that contains the induced_vertices and the edges between
  // those vertices or a edge subgraph that contains the induced_edges and the
  // nodes incident to induced_edges.
  void InduceSubgraph(
      std::shared_ptr<DynamicFragment> source,
      const std::vector<oid_t>& induced_vertices,
      const std::vector<std::pair<oid_t, oid_t>>& induced_edges) {
    Init(source->fid_, source->directed_);

    mutation_t mutation;
    if (induced_edges.empty()) {
      induceFromVertices(source, induced_vertices, mutation.edges_to_add);
    } else {
      induceFromEdges(source, induced_edges, mutation.edges_to_add);
    }
    Mutate(mutation);
  }

  inline bool Oid2Gid(const oid_t& oid, vid_t& gid) const {
    return vm_ptr_->_GetGid(oid, gid);
  }

  inline size_t selfloops_num() const { return is_selfloops_.count(); }

  inline bool HasNode(const oid_t& node) const {
    vid_t gid;
    return this->vm_ptr_->_GetGid(fid_, node, gid) &&
           iv_alive_.get_bit(id_parser_.get_local_id(gid));
  }

  inline bool HasEdge(const oid_t& u, const oid_t& v) const {
    vid_t uid, vid;
    if (vm_ptr_->_GetGid(u, uid) && vm_ptr_->_GetGid(v, vid)) {
      vid_t ulid, vlid;
      if (IsInnerVertexGid(uid) && InnerVertexGid2Lid(uid, ulid) &&
          Gid2Lid(vid, vlid) && iv_alive_.get_bit(ulid)) {
        auto iter = oe_.binary_find(ulid, vlid);
        if (iter != oe_.get_end(ulid)) {
          return true;
        }
      } else if (IsInnerVertexGid(vid) && InnerVertexGid2Lid(vid, vlid) &&
                 Gid2Lid(uid, ulid) && iv_alive_.get_bit(vlid)) {
        auto iter = directed_ ? ie_.binary_find(vlid, ulid)
                              : oe_.binary_find(vlid, ulid);
        auto end = directed_ ? ie_.get_end(vlid) : oe_.get_end(vlid);
        if (iter != end) {
          return true;
        }
      }
    }
    return false;
  }

  inline bool GetEdgeData(const oid_t& u_oid, const oid_t& v_oid,
                          edata_t& data) const {
    vid_t uid, vid;
    if (vm_ptr_->_GetGid(u_oid, uid) && vm_ptr_->_GetGid(v_oid, vid)) {
      vid_t ulid, vlid;
      if (IsInnerVertexGid(uid) && InnerVertexGid2Lid(uid, ulid) &&
          Gid2Lid(vid, vlid) && iv_alive_.get_bit(ulid)) {
        auto iter = oe_.binary_find(ulid, vlid);
        if (iter != oe_.get_end(ulid)) {
          data = iter->data;
          return true;
        }
      } else if (IsInnerVertexGid(vid) && InnerVertexGid2Lid(vid, vlid) &&
                 Gid2Lid(uid, ulid) && iv_alive_.get_bit(vlid)) {
        auto iter = directed_ ? ie_.binary_find(vlid, ulid)
                              : oe_.binary_find(vlid, ulid);
        auto end = directed_ ? ie_.get_end(vlid) : oe_.get_end(vlid);
        if (iter != end) {
          data = iter->data;
          return true;
        }
      }
    }
    return false;
  }

  inline bool IsAliveInnerVertex(const vertex_t& v) const {
    return iv_alive_.get_bit(v.GetValue());
  }

  inline bool IsAliveVertex(const vertex_t& v) const {
    return IsInnerVertex(v)
               ? iv_alive_.get_bit(v.GetValue())
               : ov_alive_.get_bit(outerVertexLidToIndex(v.GetValue()));
  }

  const dynamic::Value& GetSchema() { return schema_; }

 public:
  using base_t::GetOutgoingAdjList;
  inline adj_list_t GetIncomingAdjList(const vertex_t& v) override {
    if (!this->directed_) {
      return adj_list_t(oe_.get_begin(v.GetValue()), oe_.get_end(v.GetValue()));
    }
    return adj_list_t(ie_.get_begin(v.GetValue()), ie_.get_end(v.GetValue()));
  }

  inline const_adj_list_t GetIncomingAdjList(const vertex_t& v) const override {
    if (!this->directed_) {
      return const_adj_list_t(oe_.get_begin(v.GetValue()),
                              oe_.get_end(v.GetValue()));
    }
    return const_adj_list_t(ie_.get_begin(v.GetValue()),
                            ie_.get_end(v.GetValue()));
  }

  fragment_adj_list_t GetOutgoingAdjList(const vertex_t& v,
                                         fid_t dst_fid) override {
    return fragment_adj_list_t(
        get_oe_begin(v), get_oe_end(v), [this, dst_fid](const nbr_t& nbr) {
          return this->GetFragId(nbr.get_neighbor()) == dst_fid;
        });
  }

  fragment_const_adj_list_t GetOutgoingAdjList(const vertex_t& v,
                                               fid_t dst_fid) const override {
    return fragment_const_adj_list_t(
        get_oe_begin(v), get_oe_end(v), [this, dst_fid](const nbr_t& nbr) {
          return this->GetFragId(nbr.get_neighbor()) == dst_fid;
        });
  }

  fragment_adj_list_t GetIncomingAdjList(const vertex_t& v,
                                         fid_t dst_fid) override {
    if (!this->directed_) {
      return fragment_adj_list_t(
          get_oe_begin(v), get_oe_end(v), [this, dst_fid](const nbr_t& nbr) {
            return this->GetFragId(nbr.get_neighbor()) == dst_fid;
          });
    }
    return fragment_adj_list_t(
        get_ie_begin(v), get_ie_end(v), [this, dst_fid](const nbr_t& nbr) {
          return this->GetFragId(nbr.get_neighbor()) == dst_fid;
        });
  }

  fragment_const_adj_list_t GetIncomingAdjList(const vertex_t& v,
                                               fid_t dst_fid) const override {
    if (!this->directed_) {
      return fragment_const_adj_list_t(
          get_oe_begin(v), get_oe_end(v), [this, dst_fid](const nbr_t& nbr) {
            return this->GetFragId(nbr.get_neighbor()) == dst_fid;
          });
    }
    return fragment_const_adj_list_t(
        get_ie_begin(v), get_ie_end(v), [this, dst_fid](const nbr_t& nbr) {
          return this->GetFragId(nbr.get_neighbor()) == dst_fid;
        });
  }

 public:
  using base_t::get_ie_begin;
  using base_t::get_ie_end;
  using base_t::get_oe_begin;
  using base_t::get_oe_end;

 public:
  using adj_list_t = typename base_t::adj_list_t;
  using const_adj_list_t = typename base_t::const_adj_list_t;
  inline adj_list_t GetIncomingInnerVertexAdjList(const vertex_t& v) override {
    assert(IsInnerVertex(v));
    return adj_list_t(get_ie_begin(v), iespliter_[v]);
  }

  inline const_adj_list_t GetIncomingInnerVertexAdjList(
      const vertex_t& v) const override {
    assert(IsInnerVertex(v));
    return const_adj_list_t(get_ie_begin(v), iespliter_[v]);
  }

  inline adj_list_t GetIncomingOuterVertexAdjList(const vertex_t& v) override {
    assert(IsInnerVertex(v));
    return adj_list_t(iespliter_[v], get_ie_end(v));
  }

  inline const_adj_list_t GetIncomingOuterVertexAdjList(
      const vertex_t& v) const override {
    assert(IsInnerVertex(v));
    return const_adj_list_t(iespliter_[v], get_ie_end(v));
  }

  inline adj_list_t GetOutgoingInnerVertexAdjList(const vertex_t& v) override {
    assert(IsInnerVertex(v));
    return adj_list_t(get_oe_begin(v), oespliter_[v]);
  }

  inline const_adj_list_t GetOutgoingInnerVertexAdjList(
      const vertex_t& v) const override {
    assert(IsInnerVertex(v));
    return const_adj_list_t(get_oe_begin(v), oespliter_[v]);
  }

  inline adj_list_t GetOutgoingOuterVertexAdjList(const vertex_t& v) override {
    assert(IsInnerVertex(v));
    return adj_list_t(oespliter_[v], get_oe_end(v));
  }

  inline const_adj_list_t GetOutgoingOuterVertexAdjList(
      const vertex_t& v) const override {
    assert(IsInnerVertex(v));
    return const_adj_list_t(oespliter_[v], get_oe_end(v));
  }

 private:
  inline vid_t outerVertexLidToIndex(vid_t lid) const {
    return id_parser_.max_local_id() - lid - 1;
  }

  inline vid_t outerVertexIndexToLid(vid_t index) const {
    return id_parser_.max_local_id() - index - 1;
  }

  void splitEdges(const grape::CommSpec& comm_spec) {
    auto& inner_vertices = InnerVertices();
    iespliter_.Init(inner_vertices);
    oespliter_.Init(inner_vertices);

    int concurrency =
        (std::thread::hardware_concurrency() + comm_spec.local_num() - 1) /
        comm_spec.local_num();
    vineyard::parallel_for(
        static_cast<vid_t>(0), static_cast<vid_t>(inner_vertices.size()),
        [this, &inner_vertices](const vid_t& offset) {
          vertex_t v = *(inner_vertices.begin() + offset);
          size_t inner_neighbor_count = 0;
          auto ie = GetIncomingAdjList(v);
          for (auto& e : ie) {
            if (IsInnerVertex(e.neighbor)) {
              ++inner_neighbor_count;
            }
          }
          iespliter_[v] = get_ie_begin(v) + inner_neighbor_count;

          inner_neighbor_count = 0;
          auto oe = GetOutgoingAdjList(v);
          for (auto& e : oe) {
            if (IsInnerVertex(e.neighbor)) {
              ++inner_neighbor_count;
            }
          }
          oespliter_[v] = get_oe_begin(v) + inner_neighbor_count;
        },
        concurrency, 1024);
  }

  vid_t parseOrAddOuterVertexGid(vid_t gid) {
    auto iter = ovg2i_.find(gid);
    if (iter != ovg2i_.end()) {
      return iter->second;
    } else {
      ++ovnum_;
      vid_t lid = id_parser_.max_local_id() - ovnum_;
      ovgid_.push_back(gid);
      ovg2i_.emplace(gid, lid);
      return lid;
    }
  }

  vid_t parseOuterVertexGid(vid_t gid) {
    auto iter = ovg2i_.find(gid);
    if (iter != ovg2i_.end()) {
      return iter->second;
    } else {
      assert(false);
      return (-1);
    }
  }

  void initOuterVerticesOfFragment() {
    outer_vertices_of_frag_.resize(fnum_);
    for (auto& vec : outer_vertices_of_frag_) {
      vec.clear();
    }
    for (vid_t i = 0; i < ovnum_; ++i) {
      fid_t fid = id_parser_.get_fragment_id(ovgid_[i]);
      outer_vertices_of_frag_[fid].push_back(
          vertex_t(outerVertexIndexToLid(i)));
    }
  }

  void addEdges(std::vector<edge_t>& edges) {
    double rate = 0;
    if (directed_) {
      rate = static_cast<double>(edges.size()) /
             static_cast<double>(oe_.edge_num());
    } else {
      rate = 2.0 * static_cast<double>(edges.size()) /
             static_cast<double>(oe_.edge_num());
    }

    if (rate < dense_threshold) {
      addEdgesSparse(edges);
    } else {
      addEdgesDense(edges);
    }
  }

  void addEdgesDense(std::vector<edge_t>& edges) {
    static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
    if (load_strategy_ == grape::LoadStrategy::kBothOutIn) {
      std::vector<int> inner_oe_degree_to_add(ivnum_, 0),
          inner_ie_degree_to_add(ivnum_, 0), outer_oe_degree_to_add(ovnum_, 0),
          outer_ie_degree_to_add(ovnum_, 0);
      // reserve edges
      for (auto& e : edges) {
        if (e.src == invalid_vid) {
          continue;
        }
        if (e.src < ivnum_) {
          ++inner_oe_degree_to_add[e.src];
        } else {
          ++outer_oe_degree_to_add[outerVertexLidToIndex(e.src)];
        }
        if (e.dst < ivnum_) {
          ++inner_ie_degree_to_add[e.dst];
        } else {
          ++outer_ie_degree_to_add[outerVertexLidToIndex(e.dst)];
        }
      }
      oe_.reserve_edges_dense(inner_oe_degree_to_add, outer_oe_degree_to_add);
      ie_.reserve_edges_dense(inner_ie_degree_to_add, outer_ie_degree_to_add);

      // add edges
      std::fill(inner_oe_degree_to_add.begin(), inner_oe_degree_to_add.end(),
                0);
      std::fill(outer_oe_degree_to_add.begin(), outer_oe_degree_to_add.end(),
                0);
      std::fill(inner_ie_degree_to_add.begin(), inner_ie_degree_to_add.end(),
                0);
      std::fill(outer_ie_degree_to_add.begin(), outer_ie_degree_to_add.end(),
                0);
      for (auto& e : edges) {
        if (e.src == invalid_vid) {
          continue;
        }
        if (updateOrAddEdgeOutIn(e)) {
          if (e.src < ivnum_) {
            ++inner_oe_degree_to_add[e.src];
          } else {
            ++outer_oe_degree_to_add[outerVertexLidToIndex(e.src)];
          }
          if (e.dst < ivnum_) {
            ++inner_ie_degree_to_add[e.dst];
          } else {
            ++outer_ie_degree_to_add[outerVertexLidToIndex(e.dst)];
          }
        }
      }
      oe_.sort_neighbors_dense(inner_oe_degree_to_add, outer_oe_degree_to_add);
      ie_.sort_neighbors_dense(inner_ie_degree_to_add, outer_ie_degree_to_add);
    } else {
      std::vector<int> inner_oe_degree_to_add(ivnum_, 0),
          outer_oe_degree_to_add(ovnum_, 0);
      // reserve edges
      for (auto& e : edges) {
        if (e.src == invalid_vid) {
          continue;
        }
        assert(!(e.src >= ivnum_ && e.dst >= ivnum_));
        if (e.src < ivnum_) {
          ++inner_oe_degree_to_add[e.src];
        } else {
          ++outer_oe_degree_to_add[outerVertexLidToIndex(e.src)];
        }
        if (e.dst < ivnum_) {
          ++inner_oe_degree_to_add[e.dst];
        } else {
          ++outer_oe_degree_to_add[outerVertexLidToIndex(e.dst)];
        }
      }
      oe_.reserve_edges_dense(inner_oe_degree_to_add, outer_oe_degree_to_add);

      // add edges
      std::fill(inner_oe_degree_to_add.begin(), inner_oe_degree_to_add.end(),
                0);
      std::fill(outer_oe_degree_to_add.begin(), outer_oe_degree_to_add.end(),
                0);
      for (auto& e : edges) {
        if (e.src == invalid_vid) {
          continue;
        }
        if (updateOrAddEdgeOut(e)) {
          if (e.src < ivnum_) {
            ++inner_oe_degree_to_add[e.src];
          } else {
            ++outer_oe_degree_to_add[outerVertexLidToIndex(e.src)];
          }
          if (e.dst < ivnum_ && e.src != e.dst) {
            ++inner_oe_degree_to_add[e.dst];
          } else if (e.src != e.dst) {
            ++outer_oe_degree_to_add[outerVertexLidToIndex(e.dst)];
          }
        }
      }
      oe_.sort_neighbors_dense(inner_oe_degree_to_add, outer_oe_degree_to_add);
    }
  }

  void addEdgesSparse(std::vector<edge_t>& edges) {
    static constexpr vid_t invalid_vid = std::numeric_limits<vid_t>::max();
    if (load_strategy_ == grape::LoadStrategy::kBothOutIn) {
      std::map<vid_t, int> oe_degree_to_add, ie_degree_to_add;
      // reserve edges
      for (auto& e : edges) {
        if (e.src == invalid_vid) {
          continue;
        }
        ++oe_degree_to_add[e.src];
        ++ie_degree_to_add[e.dst];
      }
      oe_.reserve_edges_sparse(oe_degree_to_add);
      ie_.reserve_edges_sparse(ie_degree_to_add);

      // add edges
      oe_degree_to_add.clear();
      ie_degree_to_add.clear();
      for (auto& e : edges) {
        if (e.src == invalid_vid) {
          continue;
        }
        if (updateOrAddEdgeOutIn(e)) {
          ++oe_degree_to_add[e.src];
          ++ie_degree_to_add[e.dst];
        }
      }
      oe_.sort_neighbors_sparse(oe_degree_to_add);
      ie_.sort_neighbors_sparse(ie_degree_to_add);
    } else {
      std::map<vid_t, int> oe_degree_to_add;
      // reserve edges
      for (auto& e : edges) {
        if (e.src == invalid_vid) {
          continue;
        }
        ++oe_degree_to_add[e.src];
        ++oe_degree_to_add[e.dst];
      }
      oe_.reserve_edges_sparse(oe_degree_to_add);

      // add edges
      oe_degree_to_add.clear();
      for (auto& e : edges) {
        if (e.src == invalid_vid) {
          continue;
        }
        if (updateOrAddEdgeOut(e)) {
          ++oe_degree_to_add[e.src];
          if (e.src != e.dst) {
            ++oe_degree_to_add[e.dst];
          }
        }
      }
      oe_.sort_neighbors_sparse(oe_degree_to_add);
    }
  }

  // Return true if add a new edge, otherwise false.
  bool updateOrAddEdgeOut(const edge_t& e) {
    bool ret = false;  // assume it just update existed edge.

    {
      auto iter = oe_.find(e.src, e.dst);
      if (iter == oe_.get_end(e.src)) {
        oe_.put_edge(e.src, nbr_t(e.dst, e.edata));
        ret = true;
      } else {
        iter->data.Update(e.edata);
      }
      if (ret && e.src == e.dst) {
        is_selfloops_.set_bit(e.src);
        return ret;
      }
    }

    {
      auto iter = oe_.find(e.dst, e.src);
      if (iter == oe_.get_end(e.dst)) {
        oe_.put_edge(e.dst, nbr_t(e.src, e.edata));
        ret = true;
      } else {
        iter->data.Update(e.edata);
      }
    }
    return ret;
  }

  // Return true if add a new edge, otherwise false.
  bool updateOrAddEdgeOutIn(const edge_t& e) {
    bool ret = false;  // assume it just update existed edge.

    {
      auto iter = oe_.find(e.src, e.dst);
      if (iter == oe_.get_end(e.src)) {
        oe_.put_edge(e.src, nbr_t(e.dst, e.edata));
        ret = true;
      } else {
        iter->data.Update(e.edata);
      }
      if (ret && e.src == e.dst) {
        is_selfloops_.set_bit(e.src);
      }
    }

    {
      auto iter = ie_.find(e.dst, e.src);
      if (iter == ie_.get_end(e.dst)) {
        ie_.put_edge(e.dst, nbr_t(e.src, e.edata));
        ret = true;
      } else {
        iter->data.Update(e.edata);
      }
    }
    return ret;
  }

  void removeEdges(std::vector<std::pair<vid_t, vid_t>>& edges) {
    for (auto& e : edges) {
      if (!(Gid2Lid(e.first, e.first) && Gid2Lid(e.second, e.second))) {
        continue;
      }
      if (e.first == e.second) {
        this->is_selfloops_.reset_bit(e.first);
      }
    }
    oe_.remove_edges(edges);
    ie_.remove_reversed_edges(edges);

    if (!directed_) {
      oe_.remove_reversed_edges(edges);
    }
  }

  void updateEdges(std::vector<edge_t>& edges) {
    oe_.update_edges(edges);
    if (directed_) {
      ie_.update_reversed_edges(edges);
    } else {
      oe_.update_reversed_edges(edges);
    }
  }

  void copyVertices(std::shared_ptr<DynamicFragment>& source) {
    this->ivnum_ = source->ivnum_;
    this->ovnum_ = source->ovnum_;
    this->alive_ivnum_ = source->alive_ivnum_;
    this->alive_ovnum_ = source->alive_ovnum_;
    this->fnum_ = source->fnum_;
    this->iv_alive_.copy(source->iv_alive_);
    this->ov_alive_.copy(source->ov_alive_);
    this->is_selfloops_.copy(source->is_selfloops_);

    ovg2i_ = source->ovg2i_;
    ovgid_.resize(ovnum_);
    memcpy(&ovgid_[0], &(source->ovgid_[0]), ovnum_ * sizeof(vid_t));

    ivdata_.clear();
    ivdata_.resize(ivnum_);
    for (size_t i = 0; i < ivnum_; ++i) {
      ivdata_[i] = source->ivdata_[i];
    }

    this->inner_vertices_.SetRange(0, ivnum_);
    this->outer_vertices_.SetRange(id_parser_.max_local_id() - ovnum_,
                                   id_parser_.max_local_id());
    this->vertices_.SetRange(0, ivnum_, id_parser_.max_local_id() - ovnum_,
                             id_parser_.max_local_id());
  }

  // induce subgraph from induced_nodes
  void induceFromVertices(std::shared_ptr<DynamicFragment>& source,
                          const std::vector<oid_t>& induced_vertices,
                          std::vector<edge_t>& edges) {
    vertex_t vertex;
    vid_t gid, dst_gid;
    for (const auto& oid : induced_vertices) {
      if (source->GetVertex(oid, vertex)) {
        if (source->IsInnerVertex(vertex)) {
          // store the vertex data
          CHECK(vm_ptr_->_GetGid(fid_, oid, gid));
          auto lid = id_parser_.get_local_id(gid);
          ivdata_[lid] = source->GetData(vertex);
        } else {
          continue;  // ignore outer vertex.
        }

        for (const auto& e : source->GetOutgoingAdjList(vertex)) {
          auto dst_oid = source->GetId(e.get_neighbor());
          if (std::find(induced_vertices.begin(), induced_vertices.end(),
                        dst_oid) != induced_vertices.end()) {
            CHECK(Oid2Gid(dst_oid, dst_gid));
            edges.emplace_back(gid, dst_gid, e.get_data());
          }
        }
        if (directed_) {
          // filter the cross-fragment incoming edges
          for (const auto& e : source->GetIncomingAdjList(vertex)) {
            if (source->IsOuterVertex(e.get_neighbor())) {
              auto dst_oid = source->GetId(e.get_neighbor());
              if (std::find(induced_vertices.begin(), induced_vertices.end(),
                            dst_oid) != induced_vertices.end()) {
                CHECK(Oid2Gid(dst_oid, dst_gid));
                edges.emplace_back(dst_gid, gid, e.get_data());
              }
            }
          }
        }
      }
    }
  }

  // induce edge_subgraph from induced_edges
  void induceFromEdges(
      std::shared_ptr<DynamicFragment>& source,
      const std::vector<std::pair<oid_t, oid_t>>& induced_edges,
      std::vector<edge_t>& edges) {
    vertex_t vertex;
    vid_t gid, dst_gid;
    edata_t edata;
    for (auto& e : induced_edges) {
      const auto& src_oid = e.first;
      const auto& dst_oid = e.second;
      if (source->HasEdge(src_oid, dst_oid)) {
        if (vm_ptr_->_GetGid(fid_, src_oid, gid)) {
          // src is inner vertex
          auto lid = id_parser_.get_local_id(gid);
          CHECK(source->GetVertex(src_oid, vertex));
          ivdata_[lid] = source->GetData(vertex);
          CHECK(vm_ptr_->_GetGid(dst_oid, dst_gid));
          CHECK(source->GetEdgeData(src_oid, dst_oid, edata));
          edges.emplace_back(gid, dst_gid, edata);
          if (gid != dst_gid && id_parser_.get_fragment_id(dst_gid) == fid_) {
            // dst is inner vertex too
            CHECK(source->GetVertex(dst_oid, vertex));
            ivdata_[id_parser_.get_local_id(dst_gid)] = source->GetData(vertex);
          }
        } else if (vm_ptr_->_GetGid(fid_, dst_oid, dst_gid)) {
          // dst is inner vertex but src is outer vertex
          CHECK(source->GetVertex(dst_oid, vertex));
          ivdata_[id_parser_.get_local_id(dst_gid)] = source->GetData(vertex);
          CHECK(vm_ptr_->_GetGid(src_oid, gid));
          source->GetEdgeData(src_oid, dst_oid, edata);
          if (directed_) {
            edges.emplace_back(gid, dst_gid, edata);
          } else {
            edges.emplace_back(dst_gid, gid, edata);
          }
        }
      }
    }
  }

  void initVertexMembersOfFragment() {
    alive_ivnum_ = ivnum_;
    alive_ovnum_ = ovnum_;
    iv_alive_.init(ivnum_);
    ov_alive_.init(ovnum_);
    for (size_t i = 0; i < ivnum_; i++) {
      iv_alive_.set_bit(i);
    }
    for (size_t i = 0; i < ovnum_; i++) {
      ov_alive_.set_bit(i);
    }
    is_selfloops_.init(ivnum_);

    this->inner_vertices_.SetRange(0, ivnum_);
    this->outer_vertices_.SetRange(id_parser_.max_local_id() - ovnum_,
                                   id_parser_.max_local_id());
    this->vertices_.SetRange(0, ivnum_, id_parser_.max_local_id() - ovnum_,
                             id_parser_.max_local_id());
  }

  void buildCSRParallel(std::vector<std::vector<edge_t>>& edges,
                        const std::vector<int>& inner_oe_degree,
                        const std::vector<int>& outer_oe_degree,
                        const std::vector<int>& inner_ie_degree,
                        const std::vector<int>& outer_ie_degree,
                        uint32_t thread_num) {
    auto vnum = id_parser_.max_local_id();
    ie_.init_head_and_tail(0, vnum);
    oe_.init_head_and_tail(0, vnum);
    oe_.add_vertices(ivnum_, ovnum_);
    ie_.add_vertices(ivnum_, ovnum_);

    // parse edges, global id to local id
    parallel_for(
        edges.begin(), edges.end(),
        [&](uint32_t tid, std::vector<edge_t>& es) {
          if (load_strategy_ == grape::LoadStrategy::kOnlyOut) {
            for (auto& e : es) {
              CHECK(InnerVertexGid2Lid(e.src, e.src));
              CHECK(Gid2Lid(e.dst, e.dst));
            }
          } else {
            for (auto& e : es) {
              CHECK(Gid2Lid(e.src, e.src));
              CHECK(Gid2Lid(e.dst, e.dst));
            }
          }
        },
        thread_num, 1);

    // insert the edges
    insertEdgesParallel(edges, inner_oe_degree, outer_oe_degree,
                        inner_ie_degree, outer_ie_degree, thread_num);
  }

  void insertEdgesParallel(std::vector<std::vector<edge_t>>& edges,
                           const std::vector<int>& inner_oe_degree,
                           const std::vector<int>& outer_oe_degree,
                           const std::vector<int>& inner_ie_degree,
                           const std::vector<int>& outer_ie_degree,
                           uint32_t thread_num) {
    auto insert_edges_out_in = [&](uint32_t tid, std::vector<edge_t>& es) {
      dynamic::Value tmp_data;  // avoid to use default allocator on parallel
      for (auto& e : es) {
        if (e.src < ivnum_) {
          if (e.dst < ivnum_) {
            tmp_data.CopyFrom(e.edata, (*allocators_)[tid]);
            nbr_t nbr(e.dst, std::move(tmp_data));
            oe_.put_edge(e.src, std::move(nbr));
          } else {
            // avoid copy
            nbr_t nbr(e.dst, std::move(e.edata));
            oe_.put_edge(e.src, std::move(nbr));
          }
        } else {
          nbr_t nbr(e.src, std::move(e.edata));
          ie_.put_edge(e.dst, std::move(nbr));
        }
      }
    };
    auto insert_edges_out = [&](uint32_t tid, std::vector<edge_t>& es) {
      for (auto& e : es) {
        nbr_t nbr(e.dst, std::move(e.edata));
        oe_.put_edge(e.src, std::move(nbr));
      }
    };

    oe_.reserve_edges_dense(inner_oe_degree, outer_oe_degree);
    if (load_strategy_ == grape::LoadStrategy::kBothOutIn) {
      ie_.reserve_edges_dense(inner_ie_degree, outer_ie_degree);
      parallel_for(edges.begin(), edges.end(), insert_edges_out_in, thread_num,
                   1);
      // The incoming edges may not store in the same thread vector,
      // can't be parallel process.
      for (auto& vec : edges) {
        for (auto& e : vec) {
          if (e.src < ivnum_ && e.dst < ivnum_) {
            nbr_t nbr(e.src, std::move(e.edata));
            ie_.put_edge(e.dst, std::move(nbr));
          }
        }
      }
      ie_.sort_neighbors_dense(inner_ie_degree, outer_ie_degree);
    } else {
      parallel_for(edges.begin(), edges.end(), insert_edges_out, thread_num, 1);
    }
    oe_.sort_neighbors_dense(inner_oe_degree, outer_oe_degree);
  }

  void initSchema() {
    schema_.SetObject();
    schema_.Insert("vertex", dynamic::Value(rapidjson::kObjectType));
    schema_.Insert("edge", dynamic::Value(rapidjson::kObjectType));
  }

 private:
  using base_t::ivnum_;
  vid_t ovnum_;
  vid_t alive_ivnum_, alive_ovnum_;
  using base_t::directed_;
  using base_t::fid_;
  using base_t::fnum_;
  using base_t::id_parser_;
  grape::LoadStrategy load_strategy_;

  ska::flat_hash_map<vid_t, vid_t> ovg2i_;
  std::vector<vid_t> ovgid_;
  grape::Array<vdata_t, grape::Allocator<vdata_t>> ivdata_;
  grape::Bitset iv_alive_;
  grape::Bitset ov_alive_;
  grape::Bitset is_selfloops_;

  grape::VertexArray<inner_vertices_t, nbr_t*> iespliter_, oespliter_;

  // allocators for parallel convert
  std::shared_ptr<std::vector<dynamic::AllocatorT>> allocators_;

  dynamic::Value schema_;

  using base_t::outer_vertices_of_frag_;

  template <typename _vdata_t, typename _edata_t>
  friend class DynamicProjectedFragment;

  template <typename FRAG_T>
  friend class ArrowToDynamicConverter;

  friend class DynamicFragmentMutator;
};

class DynamicFragmentMutator {
  using fragment_t = DynamicFragment;
  using vertex_map_t = typename fragment_t::vertex_map_t;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using vdata_t = typename fragment_t::vdata_t;
  using edata_t = typename fragment_t::edata_t;
  using mutation_t = typename fragment_t::mutation_t;
  using partitioner_t = typename vertex_map_t::partitioner_t;

 public:
  explicit DynamicFragmentMutator(const grape::CommSpec& comm_spec,
                                  std::shared_ptr<fragment_t> fragment)
      : comm_spec_(comm_spec),
        fragment_(fragment),
        vm_ptr_(fragment->GetVertexMap()) {
    comm_spec_.Dup();
  }

  ~DynamicFragmentMutator() = default;

  void ModifyVertices(dynamic::Value& vertices_to_modify,
                      const dynamic::Value& common_attrs,
                      const rpc::ModifyType& modify_type) {
    mutation_t mutation;
    auto& partitioner = vm_ptr_->GetPartitioner();
    oid_t oid;
    vid_t gid;
    vdata_t v_data;
    fid_t v_fid, fid = fragment_->fid();
    for (auto& v : vertices_to_modify) {
      v_data = common_attrs;
      // v could be [id, attrs] or id
      if (v.IsArray() && v.Size() == 2 && v[1].IsObject()) {
        oid = std::move(v[0]);
        v_data.Update(vdata_t(v[1]));
      } else {
        oid = std::move(v);
      }
      v_fid = partitioner.GetPartitionId(oid);
      if (modify_type == rpc::NX_ADD_NODES) {
        vm_ptr_->AddVertex(std::move(oid), gid);
        if (v_data.IsObject() && !v_data.GetObject().ObjectEmpty()) {
          for (const auto& prop : v_data.GetObject()) {
            if (!fragment_->schema_["vertex"].HasMember(prop.name)) {
              dynamic::Value key(prop.name);
              fragment_->schema_["vertex"].AddMember(
                  key,
                  dynamic::DynamicType2RpcType(dynamic::GetType(prop.value)),
                  dynamic::Value::allocator_);
            }
          }
        }
        if (v_fid == fid) {
          mutation.vertices_to_add.emplace_back(gid, std::move(v_data));
        }
      } else {
        // UPDATE or DELETE, if not exist the node, continue.
        if (!vm_ptr_->_GetGid(v_fid, oid, gid)) {
          continue;
        }
      }
      if (modify_type == rpc::NX_UPDATE_NODES && v_fid == fid) {
        mutation.vertices_to_update.emplace_back(gid, std::move(v_data));
      }
      if (modify_type == rpc::NX_DEL_NODES &&
          (v_fid == fid || fragment_->IsOuterVertexGid(gid))) {
        mutation.vertices_to_remove.emplace_back(gid);
      }
    }
    fragment_->Mutate(mutation);
  }

  void ModifyEdges(dynamic::Value& edges_to_modify,
                   const dynamic::Value& common_attrs,
                   const rpc::ModifyType modify_type,
                   const std::string weight) {
    edata_t e_data;
    oid_t src, dst;
    vid_t src_gid, dst_gid, lid;
    fid_t src_fid, dst_fid, fid = fragment_->fid();
    auto& partitioner = vm_ptr_->GetPartitioner();
    mutation_t mutation;
    mutation.edges_to_add.reserve(edges_to_modify.Size());
    mutation.vertices_to_add.reserve(edges_to_modify.Size() * 2);
    for (auto& e : edges_to_modify) {
      // the edge could be [src, dst] or [srs, dst, value] or [src, dst,
      // {"key": val}]
      e_data = common_attrs;
      if (e.Size() == 3) {
        if (weight.empty()) {
          e_data.Update(edata_t(e[2]));
        } else {
          e_data.Insert(weight, edata_t(e[2]));
        }
      }
      src = std::move(e[0]);
      dst = std::move(e[1]);
      src_fid = partitioner.GetPartitionId(src);
      dst_fid = partitioner.GetPartitionId(dst);
      if (modify_type == rpc::NX_ADD_EDGES) {
        bool src_new_add = vm_ptr_->AddVertex(std::move(src), src_gid);
        bool dst_new_add = vm_ptr_->AddVertex(std::move(dst), dst_gid);
        if (src_fid == fid) {
          fragment_->InnerVertexGid2Lid(src_gid, lid);
          if (src_new_add || (fragment_->iv_alive_.cardinality() > lid &&
                              !fragment_->iv_alive_.get_bit(lid))) {
            vdata_t empty_data(rapidjson::kObjectType);
            mutation.vertices_to_add.emplace_back(src_gid,
                                                  std::move(empty_data));
          }
        }
        if (dst_fid == fid) {
          fragment_->InnerVertexGid2Lid(dst_gid, lid);
          if (dst_new_add || (fragment_->iv_alive_.cardinality() > lid &&
                              !fragment_->iv_alive_.get_bit(lid))) {
            vdata_t empty_data(rapidjson::kObjectType);
            mutation.vertices_to_add.emplace_back(dst_gid,
                                                  std::move(empty_data));
          }
        }
        if (e_data.IsObject() && !e_data.GetObject().ObjectEmpty()) {
          for (const auto& prop : e_data.GetObject()) {
            if (!fragment_->schema_["edge"].HasMember(prop.name)) {
              dynamic::Value key(prop.name);
              fragment_->schema_["edge"].AddMember(
                  key,
                  dynamic::DynamicType2RpcType(dynamic::GetType(prop.value)),
                  dynamic::Value::allocator_);
            }
          }
        }
      } else {
        if (!vm_ptr_->_GetGid(src_fid, src, src_gid) ||
            !vm_ptr_->_GetGid(dst_fid, dst, dst_gid)) {
          continue;
        }
      }
      if (modify_type == rpc::NX_ADD_EDGES) {
        if (src_fid == fid || dst_fid == fid) {
          mutation.edges_to_add.emplace_back(src_gid, dst_gid,
                                             std::move(e_data));
        }
      } else if (modify_type == rpc::NX_DEL_EDGES) {
        if (src_fid == fid || dst_fid == fid) {
          mutation.edges_to_remove.emplace_back(src_gid, dst_gid);
        }
      } else if (modify_type == rpc::NX_UPDATE_EDGES) {
        if (src_fid == fid || dst_fid == fid) {
          mutation.edges_to_update.emplace_back(src_gid, dst_gid,
                                                std::move(e_data));
        }
      }
    }
    fragment_->Mutate(mutation);
  }

 private:
  grape::CommSpec comm_spec_;
  std::shared_ptr<fragment_t> fragment_;
  std::shared_ptr<vertex_map_t> vm_ptr_;
};

}  // namespace gs

#endif  // NETWORKX
#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_H_

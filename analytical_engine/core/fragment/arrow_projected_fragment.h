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

#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_PROJECTED_FRAGMENT_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_PROJECTED_FRAGMENT_H_

#include <limits>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "boost/lexical_cast.hpp"

#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/common/util/version.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/vertex_map/arrow_vertex_map.h"

#include "core/context/context_protocols.h"
#include "core/fragment/arrow_projected_fragment_base.h"
#include "core/vertex_map/arrow_projected_vertex_map.h"

namespace gs {

namespace arrow_projected_fragment_impl {
template <typename T>
class TypedArray {
 public:
  using value_type = T;

  TypedArray() : buffer_(NULL) {}

  explicit TypedArray(std::shared_ptr<arrow::Array> array) {
    if (array == nullptr) {
      buffer_ = NULL;
    } else {
      buffer_ = std::dynamic_pointer_cast<
                    typename vineyard::ConvertToArrowType<T>::ArrayType>(array)
                    ->raw_values();
    }
  }

  void Init(std::shared_ptr<arrow::Array> array) {
    if (array == nullptr) {
      buffer_ = NULL;
    } else {
      buffer_ = std::dynamic_pointer_cast<
                    typename vineyard::ConvertToArrowType<T>::ArrayType>(array)
                    ->raw_values();
    }
  }

  value_type operator[](size_t loc) const { return buffer_[loc]; }

 private:
  const T* buffer_;
};

template <>
class TypedArray<grape::EmptyType> {
 public:
  using value_type = grape::EmptyType;

  TypedArray() {}

  explicit TypedArray(std::shared_ptr<arrow::Array>) {}

  void Init(std::shared_ptr<arrow::Array>) {}

  value_type operator[](size_t) const { return {}; }
};

template <>
struct TypedArray<std::string> {
 public:
  using value_type = arrow::util::string_view;
  TypedArray() : array_(NULL) {}
  explicit TypedArray(std::shared_ptr<arrow::Array> array) {
    if (array == nullptr) {
      array_ = NULL;
    } else {
      array_ = std::dynamic_pointer_cast<arrow::LargeStringArray>(array).get();
    }
  }

  void Init(std::shared_ptr<arrow::Array> array) {
    if (array == nullptr) {
      array_ = NULL;
    } else {
      array_ = std::dynamic_pointer_cast<arrow::LargeStringArray>(array).get();
    }
  }

  value_type operator[](size_t loc) const { return array_->GetView(loc); }

 private:
  arrow::LargeStringArray* array_;
};

/**
 * @brief This is the internal representation of a neighbor vertex
 *
 * @tparam VID_T VID type
 * @tparam EID_T Edge id type
 * @tparam EDATA_T Edge data type
 */
template <typename VID_T, typename EID_T, typename EDATA_T>
class Nbr {
  using vid_t = VID_T;
  using eid_t = EID_T;
  using nbr_unit_t = vineyard::property_graph_utils::NbrUnit<VID_T, EID_T>;

 public:
  Nbr(const nbr_unit_t* nbr, TypedArray<EDATA_T> edata_array)
      : nbr_(nbr), edata_array_(edata_array) {}

  Nbr(const Nbr& rhs) : nbr_(rhs.nbr_), edata_array_(rhs.edata_array_) {}

  grape::Vertex<vid_t> neighbor() const {
    return grape::Vertex<vid_t>(nbr_->vid);
  }

  grape::Vertex<vid_t> get_neighbor() const {
    return grape::Vertex<vid_t>(nbr_->vid);
  }

  eid_t edge_id() const { return nbr_->eid; }

  typename TypedArray<EDATA_T>::value_type data() const {
    return edata_array_[nbr_->eid];
  }

  typename TypedArray<EDATA_T>::value_type get_data() const {
    return edata_array_[nbr_->eid];
  }

  inline const Nbr& operator++() const {
    ++nbr_;
    return *this;
  }

  inline Nbr operator++(int) const {
    Nbr ret(*this);
    ++ret;
    return ret;
  }

  inline const Nbr& operator--() const {
    --nbr_;
    return *this;
  }

  inline Nbr operator--(int) const {
    Nbr ret(*this);
    --ret;
    return ret;
  }

  inline bool operator==(const Nbr& rhs) const { return nbr_ == rhs.nbr_; }

  inline bool operator!=(const Nbr& rhs) const { return nbr_ != rhs.nbr_; }

  inline bool operator<(const Nbr& rhs) const { return nbr_ < rhs.nbr_; }

  inline const Nbr& operator*() const { return *this; }

  inline const Nbr* operator->() const { return this; }

 private:
  const mutable nbr_unit_t* nbr_;
  TypedArray<EDATA_T> edata_array_;
};

/**
 * @brief This is the specialized Nbr for grape::EmptyType data type
 * @tparam VID_T
 * @tparam EID_T
 */
template <typename VID_T, typename EID_T>
class Nbr<VID_T, EID_T, grape::EmptyType> {
  using vid_t = VID_T;
  using eid_t = EID_T;
  using nbr_unit_t = vineyard::property_graph_utils::NbrUnit<VID_T, EID_T>;

 public:
  explicit Nbr(const nbr_unit_t* nbr) : nbr_(nbr) {}

  Nbr(const Nbr& rhs) : nbr_(rhs.nbr_) {}

  grape::Vertex<vid_t> neighbor() const {
    return grape::Vertex<vid_t>(nbr_->vid);
  }

  grape::Vertex<vid_t> get_neighbor() const {
    return grape::Vertex<vid_t>(nbr_->vid);
  }

  eid_t edge_id() const { return nbr_->eid; }

  grape::EmptyType data() const { return grape::EmptyType(); }

  grape::EmptyType get_data() const { return grape::EmptyType(); }

  inline const Nbr& operator++() const {
    ++nbr_;
    return *this;
  }

  inline Nbr operator++(int) const {
    Nbr ret(*this);
    ++ret;
    return ret;
  }

  inline const Nbr& operator--() const {
    --nbr_;
    return *this;
  }

  inline Nbr operator--(int) const {
    Nbr ret(*this);
    --ret;
    return ret;
  }

  inline bool operator==(const Nbr& rhs) const { return nbr_ == rhs.nbr_; }
  inline bool operator!=(const Nbr& rhs) const { return nbr_ != rhs.nbr_; }

  inline bool operator<(const Nbr& rhs) const { return nbr_ < rhs.nbr_; }

  inline const Nbr& operator*() const { return *this; }

  inline const Nbr* operator->() const { return this; }

 private:
  const mutable nbr_unit_t* nbr_;
};

/**
 * @brief This is the internal representation of neighbors for a vertex.
 *
 * @tparam VID_T VID type
 * @tparam EID_T Edge id type
 * @tparam EDATA_T Edge data type
 */
template <typename VID_T, typename EID_T, typename EDATA_T>
class AdjList {
  using vid_t = VID_T;
  using eid_t = EID_T;
  using nbr_unit_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;

 public:
  AdjList() : begin_(NULL), end_(NULL) {}

  AdjList(const nbr_unit_t* begin, const nbr_unit_t* end,
          TypedArray<EDATA_T> edata_array)
      : begin_(begin), end_(end), edata_array_(edata_array) {}

  Nbr<VID_T, EID_T, EDATA_T> begin() const {
    return Nbr<VID_T, EID_T, EDATA_T>(begin_, edata_array_);
  }

  Nbr<VID_T, EID_T, EDATA_T> end() const {
    return Nbr<VID_T, EID_T, EDATA_T>(end_, edata_array_);
  }

  size_t Size() const { return end_ - begin_; }

  inline bool Empty() const { return end_ == begin_; }

  inline bool NotEmpty() const { return !Empty(); }

 private:
  const nbr_unit_t* begin_;
  const nbr_unit_t* end_;
  TypedArray<EDATA_T> edata_array_;
};

template <typename VID_T, typename EID_T>
class AdjList<VID_T, EID_T, grape::EmptyType> {
  using vid_t = VID_T;
  using eid_t = EID_T;
  using nbr_unit_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;

 public:
  AdjList() : begin_(NULL), end_(NULL) {}

  AdjList(const nbr_unit_t* begin, const nbr_unit_t* end,
          TypedArray<grape::EmptyType>)
      : begin_(begin), end_(end) {}

  Nbr<VID_T, EID_T, grape::EmptyType> begin() const {
    return Nbr<VID_T, EID_T, grape::EmptyType>(begin_);
  }

  Nbr<VID_T, EID_T, grape::EmptyType> end() const {
    return Nbr<VID_T, EID_T, grape::EmptyType>(end_);
  }

  size_t Size() const { return end_ - begin_; }

  inline bool Empty() const { return end_ == begin_; }

  inline bool NotEmpty() const { return !Empty(); }

 private:
  const nbr_unit_t* begin_;
  const nbr_unit_t* end_;
};

}  // namespace arrow_projected_fragment_impl

/**
 * @brief This class represents the fragment projected from ArrowFragment which
 * contains only one vertex label and edge label. The fragment has no label and
 * property.
 *
 * @tparam OID_T OID type
 * @tparam VID_T VID type
 * @tparam VDATA_T The type of data attached with the vertex
 * @tparam EDATA_T The type of data attached with the edge
 */
template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class ArrowProjectedFragment
    : public ArrowProjectedFragmentBase,
      public vineyard::BareRegistered<
          ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T>> {
 public:
  using oid_t = OID_T;
  using vid_t = VID_T;
  using internal_oid_t = typename vineyard::InternalType<oid_t>::type;
  using eid_t = vineyard::property_graph_types::EID_TYPE;
  using vertex_range_t = grape::VertexRange<vid_t>;
  using vertex_t = grape::Vertex<vid_t>;
  using nbr_t = arrow_projected_fragment_impl::Nbr<vid_t, eid_t, EDATA_T>;
  using nbr_unit_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;
  using adj_list_t =
      arrow_projected_fragment_impl::AdjList<vid_t, eid_t, EDATA_T>;
  using const_adj_list_t =
      arrow_projected_fragment_impl::AdjList<vid_t, eid_t, EDATA_T>;
  using vertex_map_t = ArrowProjectedVertexMap<internal_oid_t, vid_t>;
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;
  using prop_id_t = vineyard::property_graph_types::PROP_ID_TYPE;
  using vdata_t = VDATA_T;
  using edata_t = EDATA_T;
  using property_graph_t = vineyard::ArrowFragment<oid_t, vid_t>;

  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using eid_array_t = typename vineyard::ConvertToArrowType<eid_t>::ArrayType;

  template <typename DATA_T>
  using vertex_array_t = grape::VertexArray<DATA_T, vid_t>;

  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

#if defined(VINEYARD_VERSION) && defined(VINEYARD_VERSION_MAJOR)
#if VINEYARD_VERSION >= 2007
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<ArrowProjectedFragment<oid_t, vid_t, vdata_t, edata_t>>{
            new ArrowProjectedFragment<oid_t, vid_t, vdata_t, edata_t>()});
  }
#endif
#else
  static std::shared_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::make_shared<
            ArrowProjectedFragment<oid_t, vid_t, vdata_t, edata_t>>());
  }
#endif

  ~ArrowProjectedFragment() {}

  static std::shared_ptr<ArrowProjectedFragment<oid_t, vid_t, vdata_t, edata_t>>
  Project(std::shared_ptr<vineyard::ArrowFragment<oid_t, vid_t>> fragment,
          const std::string& v_label_str, const std::string& v_prop_str,
          const std::string& e_label_str, const std::string& e_prop_str) {
    label_id_t v_label = boost::lexical_cast<label_id_t>(v_label_str);
    label_id_t e_label = boost::lexical_cast<label_id_t>(e_label_str);
    prop_id_t v_prop = boost::lexical_cast<label_id_t>(v_prop_str);
    prop_id_t e_prop = boost::lexical_cast<label_id_t>(e_prop_str);

    vineyard::Client& client =
        *dynamic_cast<vineyard::Client*>(fragment->meta().GetClient());
    std::shared_ptr<vertex_map_t> vm =
        vertex_map_t::Project(fragment->vm_ptr_, v_label);
    vineyard::ObjectMeta meta;
    if (v_prop == -1) {
      if (!std::is_same<vdata_t, grape::EmptyType>::value) {
        LOG(ERROR) << "Vertex data type of projected fragment is not "
                      "consistent with property.";
        return nullptr;
      }
    } else {
      if (!fragment->vertex_tables_[v_label]->field(v_prop)->type()->Equals(
              vineyard::ConvertToArrowType<vdata_t>::TypeValue())) {
        LOG(ERROR) << "Vertex data type of projected fragment is not "
                      "consistent with property.";
        return nullptr;
      }
    }
    if (e_prop == -1) {
      if (!std::is_same<edata_t, grape::EmptyType>::value) {
        LOG(ERROR) << "Edge data type of projected fragment is not "
                      "consistent with property.";
        return nullptr;
      }
    } else {
      if (!fragment->edge_tables_[e_label]->field(e_prop)->type()->Equals(
              vineyard::ConvertToArrowType<edata_t>::TypeValue())) {
        LOG(ERROR) << "Edge data type of projected fragment is not "
                      "consistent with property.";
        return nullptr;
      }
    }

    meta.SetTypeName(
        type_name<ArrowProjectedFragment<oid_t, vid_t, vdata_t, edata_t>>());

    meta.AddKeyValue("projected_v_label", v_label);
    meta.AddKeyValue("projected_v_property", v_prop);
    meta.AddKeyValue("projected_e_label", e_label);
    meta.AddKeyValue("projected_e_property", e_prop);

    meta.AddMember("arrow_fragment", fragment->meta());
    meta.AddMember("arrow_projected_vertex_map", vm->meta());

    std::shared_ptr<vineyard::NumericArray<int64_t>> ie_offsets_begin,
        ie_offsets_end;

    size_t nbytes = 0;
    if (fragment->directed()) {
      std::shared_ptr<arrow::Int64Array> ie_offsets_begin_arrow,
          ie_offsets_end_arrow;
      selectEdgeByNeighborLabel(fragment, v_label,
                                fragment->ie_lists_[v_label][e_label],
                                fragment->ie_offsets_lists_[v_label][e_label],
                                ie_offsets_begin_arrow, ie_offsets_end_arrow);
      vineyard::NumericArrayBuilder<int64_t> ie_offsets_begin_builder(
          client, ie_offsets_begin_arrow);
      ie_offsets_begin =
          std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
              ie_offsets_begin_builder.Seal(client));
      vineyard::NumericArrayBuilder<int64_t> ie_offsets_end_builder(
          client, ie_offsets_end_arrow);
      ie_offsets_end =
          std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
              ie_offsets_end_builder.Seal(client));

      nbytes += ie_offsets_begin->nbytes();
      nbytes += ie_offsets_end->nbytes();
    }

    std::shared_ptr<vineyard::NumericArray<int64_t>> oe_offsets_begin,
        oe_offsets_end;
    {
      std::shared_ptr<arrow::Int64Array> oe_offsets_begin_arrow,
          oe_offsets_end_arrow;
      selectEdgeByNeighborLabel(fragment, v_label,
                                fragment->oe_lists_[v_label][e_label],
                                fragment->oe_offsets_lists_[v_label][e_label],
                                oe_offsets_begin_arrow, oe_offsets_end_arrow);
      vineyard::NumericArrayBuilder<int64_t> oe_offsets_begin_builder(
          client, oe_offsets_begin_arrow);
      oe_offsets_begin =
          std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
              oe_offsets_begin_builder.Seal(client));
      vineyard::NumericArrayBuilder<int64_t> oe_offsets_end_builder(
          client, oe_offsets_end_arrow);
      oe_offsets_end =
          std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
              oe_offsets_end_builder.Seal(client));
      nbytes += oe_offsets_begin->nbytes();
      nbytes += oe_offsets_end->nbytes();
    }

    if (fragment->directed()) {
      meta.AddMember("ie_offsets_begin", ie_offsets_begin->meta());
      meta.AddMember("ie_offsets_end", ie_offsets_end->meta());
    }
    meta.AddMember("oe_offsets_begin", oe_offsets_begin->meta());
    meta.AddMember("oe_offsets_end", oe_offsets_end->meta());

    meta.SetNBytes(nbytes);

    vineyard::ObjectID id;
    VINEYARD_CHECK_OK(client.CreateMetaData(meta, id));

    return std::dynamic_pointer_cast<
        ArrowProjectedFragment<oid_t, vid_t, vdata_t, edata_t>>(
        client.GetObject(id));
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();

    vertex_label_ = meta.GetKeyValue<label_id_t>("projected_v_label");
    edge_label_ = meta.GetKeyValue<label_id_t>("projected_e_label");
    vertex_prop_ = meta.GetKeyValue<prop_id_t>("projected_v_property");
    edge_prop_ = meta.GetKeyValue<prop_id_t>("projected_e_property");

    fragment_ = std::make_shared<vineyard::ArrowFragment<oid_t, vid_t>>();
    fragment_->Construct(meta.GetMemberMeta("arrow_fragment"));

    fid_ = fragment_->fid_;
    fnum_ = fragment_->fnum_;
    directed_ = fragment_->directed_;

    if (directed_) {
      vineyard::NumericArray<int64_t> ie_offsets_begin;
      ie_offsets_begin.Construct(meta.GetMemberMeta("ie_offsets_begin"));
      ie_offsets_begin_ = ie_offsets_begin.GetArray();
    }
    if (directed_) {
      vineyard::NumericArray<int64_t> ie_offsets_end;
      ie_offsets_end.Construct(meta.GetMemberMeta("ie_offsets_end"));
      ie_offsets_end_ = ie_offsets_end.GetArray();
    }
    {
      vineyard::NumericArray<int64_t> oe_offsets_begin;
      oe_offsets_begin.Construct(meta.GetMemberMeta("oe_offsets_begin"));
      oe_offsets_begin_ = oe_offsets_begin.GetArray();
    }
    {
      vineyard::NumericArray<int64_t> oe_offsets_end;
      oe_offsets_end.Construct(meta.GetMemberMeta("oe_offsets_end"));
      oe_offsets_end_ = oe_offsets_end.GetArray();
    }

    inner_vertices_ = fragment_->InnerVertices(vertex_label_);
    outer_vertices_ = fragment_->OuterVertices(vertex_label_);
    vertices_ = fragment_->Vertices(vertex_label_);

    ivnum_ = static_cast<vid_t>(inner_vertices_.size());
    ovnum_ = static_cast<vid_t>(outer_vertices_.size());
    tvnum_ = static_cast<vid_t>(vertices_.size());
    if (ivnum_ > 0) {
      ienum_ = static_cast<size_t>(oe_offsets_end_->Value(ivnum_ - 1) -
                                   oe_offsets_begin_->Value(0));
      if (directed_) {
        ienum_ += static_cast<size_t>(ie_offsets_end_->Value(ivnum_ - 1) -
                                      ie_offsets_begin_->Value(0));
      }
    }
    if (ovnum_ > 0) {
      oenum_ = static_cast<size_t>(oe_offsets_end_->Value(tvnum_ - 1) -
                                   oe_offsets_begin_->Value(ivnum_));
      if (directed_) {
        oenum_ += static_cast<size_t>(ie_offsets_end_->Value(tvnum_ - 1) -
                                      ie_offsets_begin_->Value(ivnum_));
      }
    }

    vertex_label_num_ = fragment_->vertex_label_num_;
    edge_label_num_ = fragment_->edge_label_num_;

    if (fragment_->vertex_tables_[vertex_label_]->num_rows() == 0) {
      vertex_data_array_ = nullptr;
    } else {
      vertex_data_array_ = (vertex_prop_ == -1)
                               ? nullptr
                               : (fragment_->vertex_tables_[vertex_label_]
                                      ->column(vertex_prop_)
                                      ->chunk(0));
    }
    ovgid_list_ = fragment_->ovgid_lists_[vertex_label_];
    ovg2l_map_ = fragment_->ovg2l_maps_[vertex_label_];

    if (fragment_->edge_tables_[edge_label_]->num_rows() == 0) {
      edge_data_array_ = nullptr;
    } else {
      edge_data_array_ = (edge_prop_ == -1)
                             ? nullptr
                             : (fragment_->edge_tables_[edge_label_]
                                    ->column(edge_prop_)
                                    ->chunk(0));
    }

    if (directed_) {
      ie_ = fragment_->ie_lists_[vertex_label_][edge_label_];
    }
    oe_ = fragment_->oe_lists_[vertex_label_][edge_label_];

    vm_ptr_ = std::make_shared<vertex_map_t>();
    vm_ptr_->Construct(meta.GetMemberMeta("arrow_projected_vertex_map"));

    vid_parser_.Init(fnum_, vertex_label_num_);

    initPointers();
  }

  void PrepareToRunApp(grape::MessageStrategy strategy, bool need_split_edges) {
    if (strategy == grape::MessageStrategy::kAlongEdgeToOuterVertex) {
      initDestFidList(true, true, iodst_, iodoffset_);
    } else if (strategy ==
               grape::MessageStrategy::kAlongIncomingEdgeToOuterVertex) {
      initDestFidList(true, false, idst_, idoffset_);
    } else if (strategy ==
               grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex) {
      initDestFidList(false, true, odst_, odoffset_);
    }

    if (need_split_edges) {
      ie_spliters_ptr_.clear();
      oe_spliters_ptr_.clear();
      if (directed_) {
        initEdgeSpliters(ie_, ie_offsets_begin_, ie_offsets_end_, ie_spliters_);
        initEdgeSpliters(oe_, oe_offsets_begin_, oe_offsets_end_, oe_spliters_);
        for (auto& vec : ie_spliters_) {
          ie_spliters_ptr_.push_back(vec.data());
        }
        for (auto& vec : oe_spliters_) {
          oe_spliters_ptr_.push_back(vec.data());
        }
      } else {
        initEdgeSpliters(oe_, oe_offsets_begin_, oe_offsets_end_, oe_spliters_);
        for (auto& vec : oe_spliters_) {
          ie_spliters_ptr_.push_back(vec.data());
          oe_spliters_ptr_.push_back(vec.data());
        }
      }
    }

    initOuterVertexRanges();
    initMirrorInfo();
  }

  inline fid_t fid() const { return fid_; }

  inline fid_t fnum() const { return fnum_; }

  inline vertex_range_t Vertices() const { return vertices_; }

  inline vertex_range_t InnerVertices() const { return inner_vertices_; }

  inline vertex_range_t OuterVertices() const { return outer_vertices_; }

  inline vertex_range_t OuterVertices(fid_t fid) const {
    return vertex_range_t(outer_vertex_offsets_[fid],
                          outer_vertex_offsets_[fid + 1]);
  }

  inline const std::vector<vertex_t>& MirrorVertices(fid_t fid) const {
    return mirrors_of_frag_[fid];
  }

  inline bool GetVertex(const oid_t& oid, vertex_t& v) const {
    vid_t gid;
    if (vm_ptr_->GetGid(internal_oid_t(oid), gid)) {
      return (vid_parser_.GetFid(gid) == fid()) ? InnerVertexGid2Vertex(gid, v)
                                                : OuterVertexGid2Vertex(gid, v);
    } else {
      return false;
    }
  }

  inline oid_t GetId(const vertex_t& v) const {
    return IsInnerVertex(v) ? GetInnerVertexId(v) : GetOuterVertexId(v);
  }

  inline fid_t GetFragId(const vertex_t& v) const {
    return IsInnerVertex(v) ? fid_ : vid_parser_.GetFid(GetOuterVertexGid(v));
  }

  inline typename arrow_projected_fragment_impl::TypedArray<VDATA_T>::value_type
  GetData(const vertex_t& v) const {
    return vertex_data_array_accessor_[vid_parser_.GetOffset(v.GetValue())];
  }

  inline bool Gid2Vertex(const vid_t& gid, vertex_t& v) const {
    return (vid_parser_.GetFid(gid) == fid_) ? InnerVertexGid2Vertex(gid, v)
                                             : OuterVertexGid2Vertex(gid, v);
  }

  inline vid_t Vertex2Gid(const vertex_t& v) const {
    return IsInnerVertex(v) ? GetInnerVertexGid(v) : GetOuterVertexGid(v);
  }

  inline vid_t GetInnerVerticesNum() const { return ivnum_; }

  inline vid_t GetOuterVerticesNum() const { return ovnum_; }

  inline vid_t GetVerticesNum() const { return tvnum_; }

  inline size_t GetEdgeNum() const { return ienum_ + oenum_; }

  inline size_t GetTotalVerticesNum() const {
    return vm_ptr_->GetTotalVerticesNum();
  }

  inline bool IsInnerVertex(const vertex_t& v) const {
    return (vid_parser_.GetOffset(v.GetValue()) < static_cast<int64_t>(ivnum_));
  }

  inline bool IsOuterVertex(const vertex_t& v) const {
    return (
        vid_parser_.GetOffset(v.GetValue()) < static_cast<int64_t>(tvnum_) &&
        vid_parser_.GetOffset(v.GetValue()) >= static_cast<int64_t>(ivnum_));
  }

  inline bool GetInnerVertex(const oid_t& oid, vertex_t& v) const {
    vid_t gid;
    if (vm_ptr_->GetGid(internal_oid_t(oid), gid)) {
      if (vid_parser_.GetFid(gid) == fid_) {
        v.SetValue(vid_parser_.GetLid(gid));
        return true;
      }
    }
    return false;
  }

  inline bool GetOuterVertex(const oid_t& oid, vertex_t& v) const {
    vid_t gid;
    if (vm_ptr_->GetGid(internal_oid_t(oid), gid)) {
      return OuterVertexGid2Vertex(gid, v);
    } else {
      return false;
    }
  }

  inline oid_t GetInnerVertexId(const vertex_t& v) const {
    internal_oid_t internal_oid;
    CHECK(vm_ptr_->GetOid(
        vid_parser_.GenerateId(fid_, vid_parser_.GetLabelId(v.GetValue()),
                               vid_parser_.GetOffset(v.GetValue())),
        internal_oid));
    return oid_t(internal_oid);
  }

  inline oid_t GetOuterVertexId(const vertex_t& v) const {
    vid_t gid = GetOuterVertexGid(v);
    internal_oid_t internal_oid;
    CHECK(vm_ptr_->GetOid(gid, internal_oid));
    return oid_t(internal_oid);
  }

  inline oid_t Gid2Oid(const vid_t& gid) const {
    internal_oid_t internal_oid;
    CHECK(vm_ptr_->GetOid(gid, internal_oid));
    return oid_t(internal_oid);
  }

  inline bool Oid2Gid(const oid_t& oid, vid_t& gid) const {
    return vm_ptr_->GetGid(internal_oid_t(oid), gid);
  }

  // For Java use, can not use Oid2Gid(const oid_t & oid, vid_t & gid) since
  // Java can not pass vid_t by reference.
  inline vid_t Oid2Gid(const oid_t& oid) const {
    vid_t gid;
    if (vm_ptr_->GetGid(internal_oid_t(oid), gid)) {
      return gid;
    }
    return std::numeric_limits<vid_t>::max();
  }

  inline bool InnerVertexGid2Vertex(const vid_t& gid, vertex_t& v) const {
    v.SetValue(vid_parser_.GetLid(gid));
    return true;
  }

  inline bool OuterVertexGid2Vertex(const vid_t& gid, vertex_t& v) const {
    auto iter = ovg2l_map_->find(gid);
    if (iter != ovg2l_map_->end()) {
      v.SetValue(iter->second);
      return true;
    } else {
      return false;
    }
  }

  inline vid_t GetOuterVertexGid(const vertex_t& v) const {
    assert(vid_parser_.GetLabelId(v.GetValue()) == vertex_label_);
    return ovgid_list_ptr_[vid_parser_.GetOffset(v.GetValue()) - ivnum_];
  }
  inline vid_t GetInnerVertexGid(const vertex_t& v) const {
    return vid_parser_.GenerateId(fid_, vid_parser_.GetLabelId(v.GetValue()),
                                  vid_parser_.GetOffset(v.GetValue()));
  }

  inline adj_list_t GetIncomingAdjList(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return adj_list_t(&ie_ptr_[ie_offsets_begin_ptr_[offset]],
                      &ie_ptr_[ie_offsets_end_ptr_[offset]],
                      edge_data_array_accessor_);
  }

  inline adj_list_t GetOutgoingAdjList(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return adj_list_t(&oe_ptr_[oe_offsets_begin_ptr_[offset]],
                      &oe_ptr_[oe_offsets_end_ptr_[offset]],
                      edge_data_array_accessor_);
  }

  inline adj_list_t GetIncomingInnerVertexAdjList(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return adj_list_t(&ie_ptr_[ie_offsets_begin_ptr_[offset]],
                      &ie_ptr_[offset < static_cast<int64_t>(ivnum_)
                                   ? ie_spliters_ptr_[0][offset]
                                   : ie_offsets_end_ptr_[offset]],
                      edge_data_array_accessor_);
  }

  inline adj_list_t GetOutgoingInnerVertexAdjList(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return adj_list_t(&oe_ptr_[oe_offsets_begin_ptr_[offset]],
                      &oe_ptr_[offset < static_cast<int64_t>(ivnum_)
                                   ? oe_spliters_ptr_[0][offset]
                                   : oe_offsets_end_ptr_[offset]],
                      edge_data_array_accessor_);
  }

  inline adj_list_t GetIncomingOuterVertexAdjList(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return offset < static_cast<int64_t>(ivnum_)
               ? adj_list_t(&ie_ptr_[ie_spliters_ptr_[0][offset]],
                            &ie_ptr_[ie_offsets_end_ptr_[offset]],
                            edge_data_array_accessor_)
               : adj_list_t();
  }

  inline adj_list_t GetOutgoingOuterVertexAdjList(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return offset < static_cast<int64_t>(ivnum_)
               ? adj_list_t(&oe_ptr_[oe_spliters_ptr_[0][offset]],
                            &oe_ptr_[oe_offsets_end_ptr_[offset]],
                            edge_data_array_accessor_)
               : adj_list_t();
  }

  inline adj_list_t GetIncomingAdjList(const vertex_t& v, fid_t src_fid) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return offset < static_cast<int64_t>(ivnum_)
               ? adj_list_t(&ie_ptr_[ie_spliters_ptr_[src_fid][offset]],
                            &ie_ptr_[ie_spliters_ptr_[src_fid + 1][offset]],
                            edge_data_array_accessor_)
               : (src_fid == fid_ ? GetIncomingAdjList(v) : adj_list_t());
  }

  inline adj_list_t GetOutgoingAdjList(const vertex_t& v, fid_t dst_fid) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return offset < static_cast<int64_t>(ivnum_)
               ? adj_list_t(&oe_ptr_[oe_spliters_ptr_[dst_fid][offset]],
                            &oe_ptr_[oe_spliters_ptr_[dst_fid + 1][offset]],
                            edge_data_array_accessor_)
               : (dst_fid == fid_ ? GetOutgoingAdjList(v) : adj_list_t());
  }

  inline int GetLocalOutDegree(const vertex_t& v) const {
    return GetOutgoingAdjList(v).Size();
  }

  inline int GetLocalInDegree(const vertex_t& v) const {
    return GetIncomingAdjList(v).Size();
  }

  inline grape::DestList IEDests(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    assert(offset < static_cast<int64_t>(ivnum_));
    return grape::DestList(idoffset_[offset], idoffset_[offset + 1]);
  }

  inline grape::DestList OEDests(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    assert(offset < static_cast<int64_t>(ivnum_));
    return grape::DestList(odoffset_[offset], odoffset_[offset + 1]);
  }

  inline grape::DestList IOEDests(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    assert(offset < static_cast<int64_t>(ivnum_));
    return grape::DestList(iodoffset_[offset], iodoffset_[offset + 1]);
  }

  inline bool directed() const { return directed_; }

 private:
  inline static std::pair<int64_t, int64_t> getRangeOfLabel(
      std::shared_ptr<property_graph_t> fragment, label_id_t v_label,
      std::shared_ptr<arrow::FixedSizeBinaryArray> nbr_list, int64_t begin,
      int64_t end) {
    int64_t i, j;
    for (i = begin; i != end; ++i) {
      const nbr_unit_t* ptr =
          reinterpret_cast<const nbr_unit_t*>(nbr_list->GetValue(i));
      if (fragment->vid_parser_.GetLabelId(ptr->vid) == v_label) {
        break;
      }
    }
    for (j = i; j != end; ++j) {
      const nbr_unit_t* ptr =
          reinterpret_cast<const nbr_unit_t*>(nbr_list->GetValue(j));
      if (fragment->vid_parser_.GetLabelId(ptr->vid) != v_label) {
        break;
      }
    }
    return std::make_pair(i, j);
  }

  static bl::result<void> selectEdgeByNeighborLabel(
      std::shared_ptr<property_graph_t> fragment, label_id_t v_label,
      std::shared_ptr<arrow::FixedSizeBinaryArray> nbr_list,
      std::shared_ptr<arrow::Int64Array> offsets,
      std::shared_ptr<arrow::Int64Array>& begins,
      std::shared_ptr<arrow::Int64Array>& ends) {
    arrow::Int64Builder begins_builder, ends_builder;
    vid_t tvnum = fragment->tvnums_[v_label];

    for (vid_t i = 0; i != tvnum; ++i) {
      auto ret = getRangeOfLabel(fragment, v_label, nbr_list, offsets->Value(i),
                                 offsets->Value(i + 1));
      ARROW_OK_OR_RAISE(begins_builder.Append(ret.first));
      ARROW_OK_OR_RAISE(ends_builder.Append(ret.second));
    }
    ARROW_OK_OR_RAISE(begins_builder.Finish(&begins));
    ARROW_OK_OR_RAISE(ends_builder.Finish(&ends));
    return {};
  }

  void initDestFidList(bool in_edge, bool out_edge,
                       std::vector<fid_t>& fid_list,
                       std::vector<fid_t*>& fid_list_offset) {
    if (!fid_list_offset.empty()) {
      return;
    }

    fid_list_offset.resize(ivnum_ + 1, NULL);

    std::set<fid_t> dstset;
    std::vector<int> id_num(ivnum_, 0);

    vertex_t v = inner_vertices_.begin();
    for (vid_t i = 0; i < ivnum_; ++i) {
      dstset.clear();
      if (in_edge) {
        auto es = GetIncomingAdjList(v);
        for (auto& e : es) {
          fid_t f = GetFragId(e.neighbor());
          if (f != fid_) {
            dstset.insert(f);
          }
        }
      }
      if (out_edge) {
        auto es = GetOutgoingAdjList(v);
        for (auto& e : es) {
          fid_t f = GetFragId(e.neighbor());
          if (f != fid_) {
            dstset.insert(f);
          }
        }
      }
      id_num[i] = dstset.size();
      for (auto fid : dstset) {
        fid_list.push_back(fid);
      }
      ++v;
    }

    fid_list.shrink_to_fit();
    fid_list_offset[0] = fid_list.data();
    for (vid_t i = 0; i < ivnum_; ++i) {
      fid_list_offset[i + 1] = fid_list_offset[i] + id_num[i];
    }
  }

  void initEdgeSpliters(std::shared_ptr<arrow::FixedSizeBinaryArray> edge_list,
                        std::shared_ptr<arrow::Int64Array> offsets_begin,
                        std::shared_ptr<arrow::Int64Array> offsets_end,
                        std::vector<std::vector<int64_t>>& spliters) {
    if (!spliters.empty()) {
      return;
    }
    spliters.resize(fnum_ + 1);
    for (auto& vec : spliters) {
      vec.resize(ivnum_);
    }
    std::vector<int> frag_count;
    for (vid_t i = 0; i < ivnum_; ++i) {
      frag_count.clear();
      frag_count.resize(fnum_, 0);
      int64_t begin = offsets_begin->Value(i);
      int64_t end = offsets_end->Value(i);
      for (int64_t j = begin; j != end; ++j) {
        const nbr_unit_t* nbr_ptr =
            reinterpret_cast<const nbr_unit_t*>(edge_list->GetValue(j));
        vertex_t u(nbr_ptr->vid);
        fid_t u_fid = GetFragId(u);
        ++frag_count[u_fid];
      }
      begin += frag_count[fid_];
      frag_count[fid_] = 0;
      spliters[0][i] = begin;
      for (fid_t j = 0; j < fnum_; ++j) {
        begin += frag_count[j];
        spliters[j + 1][i] = begin;
      }
      CHECK_EQ(begin, end);
    }
  }

  void initOuterVertexRanges() {
    if (!outer_vertex_offsets_.empty()) {
      return;
    }
    std::vector<vid_t> outer_vnum(fnum_, 0);
    for (auto v : outer_vertices_) {
      ++outer_vnum[GetFragId(v)];
    }
    CHECK_EQ(outer_vnum[fid_], 0);
    outer_vertex_offsets_.resize(fnum_ + 1);
    outer_vertex_offsets_[0] = outer_vertices_.begin().GetValue();
    for (fid_t i = 0; i < fnum_; ++i) {
      outer_vertex_offsets_[i + 1] = outer_vertex_offsets_[i] + outer_vnum[i];
    }
    CHECK_EQ(outer_vertex_offsets_[fnum_], outer_vertices_.end().GetValue());
  }

  void initMirrorInfo() {
    if (!mirrors_of_frag_.empty()) {
      return;
    }

    mirrors_of_frag_.resize(fnum_);

    std::vector<bool> bm(fnum_, false);
    for (auto v : inner_vertices_) {
      auto es = GetOutgoingAdjList(v);
      for (auto& e : es) {
        fid_t fid = GetFragId(e.get_neighbor());
        bm[fid] = true;
      }
      es = GetIncomingAdjList(v);
      for (auto& e : es) {
        fid_t fid = GetFragId(e.get_neighbor());
        bm[fid] = true;
      }

      for (fid_t i = 0; i != fnum_; ++i) {
        if ((i != fid_) && bm[i]) {
          mirrors_of_frag_[i].push_back(v);
          bm[i] = false;
        }
      }
    }
  }

  void initPointers() {
    if (directed_) {
      ie_offsets_begin_ptr_ = ie_offsets_begin_->raw_values();
      ie_offsets_end_ptr_ = ie_offsets_end_->raw_values();
    } else {
      ie_offsets_begin_ptr_ = oe_offsets_begin_->raw_values();
      ie_offsets_end_ptr_ = oe_offsets_end_->raw_values();
    }
    oe_offsets_begin_ptr_ = oe_offsets_begin_->raw_values();
    oe_offsets_end_ptr_ = oe_offsets_end_->raw_values();

    vertex_data_array_accessor_.Init(vertex_data_array_);
    ovgid_list_ptr_ = ovgid_list_->raw_values();
    edge_data_array_accessor_.Init(edge_data_array_);

    if (directed_) {
      ie_ptr_ = reinterpret_cast<const nbr_unit_t*>(ie_->GetValue(0));
    } else {
      ie_ptr_ = reinterpret_cast<const nbr_unit_t*>(oe_->GetValue(0));
    }
    oe_ptr_ = reinterpret_cast<const nbr_unit_t*>(oe_->GetValue(0));
  }

  vertex_range_t inner_vertices_;
  vertex_range_t outer_vertices_;
  vertex_range_t vertices_;

  fid_t fid_, fnum_;
  bool directed_;

  vid_t ivnum_, ovnum_, tvnum_;
  size_t ienum_{}, oenum_{};

  label_id_t vertex_label_num_, edge_label_num_;
  label_id_t vertex_label_, edge_label_;

  prop_id_t vertex_prop_, edge_prop_;

  std::shared_ptr<arrow::Int64Array> ie_offsets_begin_, ie_offsets_end_;
  const int64_t* ie_offsets_begin_ptr_;
  const int64_t* ie_offsets_end_ptr_;
  std::shared_ptr<arrow::Int64Array> oe_offsets_begin_, oe_offsets_end_;
  const int64_t* oe_offsets_begin_ptr_;
  const int64_t* oe_offsets_end_ptr_;

  std::shared_ptr<arrow::Array> vertex_data_array_;
  arrow_projected_fragment_impl::TypedArray<VDATA_T>
      vertex_data_array_accessor_;

  std::shared_ptr<vid_array_t> ovgid_list_;
  const vid_t* ovgid_list_ptr_;
  std::shared_ptr<vineyard::Hashmap<vid_t, vid_t>> ovg2l_map_;

  std::shared_ptr<arrow::Array> edge_data_array_;
  arrow_projected_fragment_impl::TypedArray<EDATA_T> edge_data_array_accessor_;

  std::shared_ptr<arrow::FixedSizeBinaryArray> ie_, oe_;
  const nbr_unit_t* ie_ptr_;
  const nbr_unit_t* oe_ptr_;

  std::shared_ptr<vertex_map_t> vm_ptr_;

  vineyard::IdParser<vid_t> vid_parser_;

  std::shared_ptr<vineyard::ArrowFragment<oid_t, vid_t>> fragment_;

  std::vector<fid_t> idst_, odst_, iodst_;
  std::vector<fid_t*> idoffset_, odoffset_, iodoffset_;

  std::vector<std::vector<int64_t>> ie_spliters_, oe_spliters_;
  std::vector<const int64_t*> ie_spliters_ptr_, oe_spliters_ptr_;

  std::vector<vid_t> outer_vertex_offsets_;
  std::vector<std::vector<vertex_t>> mirrors_of_frag_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_PROJECTED_FRAGMENT_H_

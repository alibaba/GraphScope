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

#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_APPEND_ONLY_ARROW_FRAGMENT_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_APPEND_ONLY_ARROW_FRAGMENT_H_

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "arrow/util/config.h"

#include "grape/grape.h"
#include "vineyard/common/util/version.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/vertex_map/arrow_vertex_map.h"

#include "core/fragment/append_only_arrow_table.h"
#include "core/vertex_map/extra_vertex_map.h"

namespace gs {
template <typename OID_T, typename VID_T>
class AppendOnlyArrowFragmentBuilder;

namespace append_only_fragment_impl {
/**
 * @brief ExtraNbr is an internal representation for a later appended neighbor.
 * @see gs::AppendOnlyArrowFragment
 */
template <typename VID_T, typename EID_T>
struct ExtraNbr {
 private:
  using prop_id_t = vineyard::property_graph_types::PROP_ID_TYPE;

 public:
  ExtraNbr() : nbr_(NULL), edata_table_(nullptr) {}

  ExtraNbr(typename std::map<VID_T, vineyard::property_graph_utils::NbrUnit<
                                        VID_T, EID_T>>::const_iterator nbr,
           std::shared_ptr<AppendOnlyArrowTable> edata_table,
           const vineyard::IdParser<VID_T>* vid_parser, const VID_T* ivnums)
      : nbr_(nbr),
        edata_table_(std::move(edata_table)),
        vid_parser_(vid_parser),
        ivnums_(ivnums) {}

  ExtraNbr(const ExtraNbr& rhs)
      : nbr_(rhs.nbr_),
        edata_table_(rhs.edata_table_),
        vid_parser_(rhs.vid_parser_),
        ivnums_(rhs.ivnums_) {}

  ExtraNbr(ExtraNbr&& rhs)
      : nbr_(std::move(rhs.nbr_)),
        edata_table_(std::move(rhs.edata_table_)),
        vid_parser_(rhs.vid_parser_),
        ivnums_(rhs.ivnums_) {}

  ExtraNbr& operator=(const ExtraNbr& rhs) {
    nbr_ = rhs.nbr_;
    edata_table_ = rhs.edata_table_;
    vid_parser_ = rhs.vid_parser_;
    ivnums_ = rhs.ivnums_;
  }

  ExtraNbr& operator=(ExtraNbr&& rhs) {
    nbr_ = std::move(rhs.nbr_);
    edata_table_ = std::move(rhs.edata_table_);
    vid_parser_ = rhs.vid_parser_;
    ivnums_ = rhs.ivnums_;
  }

  grape::Vertex<VID_T> neighbor() const {
    auto lid = nbr_->first;
    auto offset_mask = vid_parser_->offset_mask();
    auto offset = vid_parser_->GetOffset(lid);
    auto v_label = vid_parser_->GetLabelId(lid);
    auto ivnum = ivnums_[v_label];
    auto vid = offset < (int64_t) ivnum
                   ? lid
                   : ((lid & ~offset_mask) | (ivnum + offset_mask - offset));

    return grape::Vertex<VID_T>(vid);
  }

  EID_T edge_id() const { return nbr_->second.eid; }

  template <typename T>
  T get_data(prop_id_t prop_id) const {
    return edata_table_->GetValue<T>(prop_id, nbr_->second.eid);
  }

  inline const ExtraNbr& operator++() const {
    ++nbr_;
    return *this;
  }

  inline ExtraNbr operator++(int) const {
    ExtraNbr ret(*this);
    ++ret;
    return ret;
  }

  inline const ExtraNbr& operator--() const {
    --nbr_;
    return *this;
  }

  inline ExtraNbr operator--(int) const {
    ExtraNbr ret(*this);
    --ret;
    return ret;
  }

  inline bool operator==(const ExtraNbr& rhs) const { return nbr_ == rhs.nbr_; }
  inline bool operator!=(const ExtraNbr& rhs) const { return nbr_ != rhs.nbr_; }

  inline bool operator<(const ExtraNbr& rhs) const { return nbr_ < rhs.nbr_; }

  inline const ExtraNbr& operator*() const { return *this; }

 private:
  mutable typename std::map<VID_T, vineyard::property_graph_utils::NbrUnit<
                                       VID_T, EID_T>>::const_iterator nbr_;
  std::shared_ptr<AppendOnlyArrowTable> edata_table_;
  const vineyard::IdParser<VID_T>* vid_parser_;
  const VID_T* ivnums_;
};

/**
 * @brief ExtraAdjList is an internal representation for the later appended
 * neighbors.
 * @see gs::AppendOnlyArrowFragment
 */
template <typename VID_T, typename EID_T>
class ExtraAdjList {
 public:
  ExtraAdjList() = default;

  ExtraAdjList(
      typename std::map<VID_T, vineyard::property_graph_utils::NbrUnit<
                                   VID_T, EID_T>>::const_iterator begin,
      typename std::map<VID_T, vineyard::property_graph_utils::NbrUnit<
                                   VID_T, EID_T>>::const_iterator end,
      std::shared_ptr<AppendOnlyArrowTable> edata_table,
      const vineyard::IdParser<VID_T>* vid_parser, const VID_T* ivnums)
      : begin_(begin),
        end_(end),
        edata_table_(std::move(edata_table)),
        vid_parser_(vid_parser),
        ivnums_(ivnums) {}

  inline ExtraNbr<VID_T, EID_T> begin() const {
    return ExtraNbr<VID_T, EID_T>(begin_, edata_table_, vid_parser_, ivnums_);
  }

  inline ExtraNbr<VID_T, EID_T> end() const {
    return ExtraNbr<VID_T, EID_T>(end_, edata_table_, vid_parser_, ivnums_);
  }

  inline size_t Size() const { return end_ - begin_; }

  inline bool Empty() const { return end_ == begin_; }

  inline bool NotEmpty() const { return end_ != begin_; }

  size_t size() const { return end_ - begin_; }

 private:
  typename std::map<VID_T, vineyard::property_graph_utils::NbrUnit<
                               VID_T, EID_T>>::const_iterator begin_;
  typename std::map<VID_T, vineyard::property_graph_utils::NbrUnit<
                               VID_T, EID_T>>::const_iterator end_;
  std::shared_ptr<AppendOnlyArrowTable> edata_table_;
  const vineyard::IdParser<VID_T>* vid_parser_;
  const VID_T* ivnums_;
};

template <typename T>
typename std::enable_if<!std::is_same<T, std::string>::value, T>::type
get_from_arrow_array(const std::shared_ptr<arrow::Array>& arr, int64_t i) {
  return std::dynamic_pointer_cast<
             typename vineyard::ConvertToArrowType<T>::ArrayType>(arr)
      ->Value(i);
}

template <typename T>
typename std::enable_if<std::is_same<T, std::string>::value, T>::type
get_from_arrow_array(const std::shared_ptr<arrow::Array>& arr, int64_t i) {
  return std::dynamic_pointer_cast<
             typename vineyard::ConvertToArrowType<T>::ArrayType>(arr)
      ->GetString(i);
}
}  // namespace append_only_fragment_impl

/**
 * @brief A labeled fragment can be modified vertices and edges in append
 * fashion. The data in the fragment are stored in two parts. The first part is
 * initial data which will be stored in the vineyard. The second part is later
 * coming data (append data) which will be located in the engine's memory space.
 * @tparam OID_T
 * @tparam VID_T
 */
template <typename OID_T, typename VID_T>
class AppendOnlyArrowFragment
    : public vineyard::Registered<AppendOnlyArrowFragment<OID_T, VID_T>> {
  template <typename EID_T>
  class NbrMapSpace {
    using nbr_unit_t = vineyard::property_graph_utils::NbrUnit<VID_T, EID_T>;

   public:
    NbrMapSpace() : index_(0) {}

    // Create a new linked list
    inline size_t emplace(VID_T vid, EID_T eid) {
      buffer_.resize(index_ + 1);
      buffer_[index_] = new std::map<VID_T, nbr_unit_t>();
      buffer_[index_]->operator[](vid) = nbr_unit_t(vid, eid);
      return index_++;
    }

    // Insert the value to an existing linked list, or update the existing value
    // N.B.: Append only frag does not support update existed value!!
    inline size_t emplace(size_t loc, VID_T vid, EID_T eid, bool& created) {
      if (buffer_[loc]->find(vid) != buffer_[loc]->end()) {
        created = false;
      } else {
        buffer_[loc]->operator[](vid) = nbr_unit_t(vid, eid);
        created = true;
      }
      return loc;
    }

    inline std::map<VID_T, nbr_unit_t>& operator[](size_t loc) {
      return *buffer_[loc];
    }

    inline const std::map<VID_T, nbr_unit_t>& operator[](size_t loc) const {
      return *buffer_[loc];
    }

    void Clear() {
      for (size_t i = 0; i < buffer_.size(); ++i) {
        delete buffer_[i];
        buffer_[i] = nullptr;
      }
      buffer_.clear();
      index_ = 0;
    }

   private:
    std::vector<std::map<VID_T, nbr_unit_t>*> buffer_;
    size_t index_;
  };

 public:
  using oid_t = OID_T;
  using vid_t = VID_T;
  using internal_oid_t = typename vineyard::InternalType<oid_t>::type;
  using eid_t = vineyard::property_graph_types::EID_TYPE;
  using prop_id_t = vineyard::property_graph_types::PROP_ID_TYPE;
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;
  using vertex_range_t = grape::VertexRange<vid_t>;
  using inner_vertices_t = vertex_range_t;
  using outer_vertices_t = vertex_range_t;
  using vertices_t = vertex_range_t;
  using nbr_t = vineyard::property_graph_utils::OffsetNbr<vid_t, eid_t>;
  using nbr_unit_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;
  using adj_list_t =
      vineyard::property_graph_utils::OffsetAdjList<vid_t, eid_t>;
  using extra_nbr_t = append_only_fragment_impl::ExtraNbr<vid_t, eid_t>;
  using extra_adj_list_t =
      append_only_fragment_impl::ExtraAdjList<vid_t, eid_t>;
  using vertex_map_t = vineyard::ArrowVertexMap<internal_oid_t, vid_t>;
  using extra_vertex_map_t = ExtraVertexMap<oid_t, vid_t>;
  using vertex_t = grape::Vertex<vid_t>;

  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using eid_array_t = typename vineyard::ConvertToArrowType<eid_t>::ArrayType;

  template <typename DATA_T>
  using vertex_array_t = grape::VertexArray<vertices_t, DATA_T>;

  template <typename DATA_T>
  using inner_vertex_array_t = grape::VertexArray<inner_vertices_t, DATA_T>;

  template <typename DATA_T>
  using outer_vertex_array_t = grape::VertexArray<outer_vertices_t, DATA_T>;

#if defined(VINEYARD_VERSION) && defined(VINEYARD_VERSION_MAJOR)
#if VINEYARD_VERSION >= 2007
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<AppendOnlyArrowFragment<oid_t, vid_t>>{
            new AppendOnlyArrowFragment<oid_t, vid_t>()});
  }
#endif
#else
  static std::shared_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::make_shared<AppendOnlyArrowFragment<oid_t, vid_t>>());
  }
#endif

 public:
  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();

    this->fid_ = meta.GetKeyValue<fid_t>("fid");
    this->fnum_ = meta.GetKeyValue<fid_t>("fnum");
    this->directed_ = (meta.GetKeyValue<int>("directed") != 0);
    this->vertex_label_num_ = meta.GetKeyValue<label_id_t>("vertex_label_num");
    this->edge_label_num_ = meta.GetKeyValue<label_id_t>("edge_label_num");

    vid_parser_.Init(fnum_, vertex_label_num_);

    this->ivnums_.Construct(meta.GetMemberMeta("ivnums"));
    this->ovnums_.Construct(meta.GetMemberMeta("ovnums"));
    this->tvnums_.Construct(meta.GetMemberMeta("tvnums"));

    CONSTRUCT_TABLE_VECTOR(vertex_tables_, vertex_label_num_, "vertex_tables");
    CONSTRUCT_ARRAY_VECTOR(vid_t, ovgid_lists_, vertex_label_num_,
                           "ovgid_lists");
    ovg2l_maps_.resize(vertex_label_num_);
    for (label_id_t i = 0; i < vertex_label_num_; ++i) {
      ovg2l_maps_[i] = std::make_shared<vineyard::Hashmap<vid_t, vid_t>>();
      ovg2l_maps_[i]->Construct(meta.GetMemberMeta(
          vineyard::generate_name_with_suffix("ovg2l_maps", i)));
    }

#ifdef ENDPOINT_LISTS
    CONSTRUCT_ARRAY_VECTOR(vid_t, edge_src_, edge_label_num_, "edge_src");
    CONSTRUCT_ARRAY_VECTOR(vid_t, edge_dst_, edge_label_num_, "edge_dst");
#endif

    CONSTRUCT_TABLE_VECTOR(edge_tables_, edge_label_num_, "edge_tables");

    if (directed_) {
      CONSTRUCT_BINARY_ARRAY_VECTOR_VECTOR(ie_lists_, vertex_label_num_,
                                           edge_label_num_, "ie_lists");
    }
    CONSTRUCT_BINARY_ARRAY_VECTOR_VECTOR(oe_lists_, vertex_label_num_,
                                         edge_label_num_, "oe_lists");

    if (directed_) {
      CONSTRUCT_ARRAY_VECTOR_VECTOR(int64_t, ie_offsets_lists_,
                                    vertex_label_num_, edge_label_num_,
                                    "ie_offsets_lists");
    }
    CONSTRUCT_ARRAY_VECTOR_VECTOR(int64_t, oe_offsets_lists_, vertex_label_num_,
                                  edge_label_num_, "oe_offsets_lists");
    vm_ptr_ = std::make_shared<vertex_map_t>();
    vm_ptr_->Construct(meta.GetMemberMeta("vertex_map"));

    initPointers();
    initExtra();
  }

  fid_t fid() const { return fid_; }

  fid_t fnum() const { return fnum_; }

  label_id_t vertex_label_num() const { return vertex_label_num_; }

  label_id_t vertex_label(const vertex_t& v) const {
    return vid_parser_.GetLabelId(v.GetValue());
  }

  int64_t vertex_offset(const vertex_t& v) const {
    return vid_parser_.GetOffset(v.GetValue());
  }

  label_id_t edge_label_num() const { return edge_label_num_; }

  std::shared_ptr<arrow::Table> vertex_data_table(label_id_t i) const {
    return vertex_tables_[i];
  }

  std::shared_ptr<arrow::Table> edge_data_table(label_id_t i) const {
    return edge_tables_[i];
  }

  vertex_range_t Vertices(label_id_t label_id) const {
    return vertex_range_t(
        vid_parser_.GenerateId(0, label_id, 0),
        vid_parser_.GenerateId(0, label_id, curr_tvnums_[label_id]));
  }

  vertex_range_t InnerVertices(label_id_t label_id) const {
    return vertex_range_t(
        vid_parser_.GenerateId(0, label_id, 0),
        vid_parser_.GenerateId(0, label_id, curr_ivnums_[label_id]));
  }

  vertex_range_t OuterVertices(label_id_t label_id) const {
    return vertex_range_t(
        vid_parser_.GenerateId(0, label_id, curr_ivnums_[label_id]),
        vid_parser_.GenerateId(0, label_id, curr_tvnums_[label_id]));
  }

  bool GetVertex(label_id_t label, const oid_t& oid, vertex_t& v) const {
    vid_t gid;
    if (getGid(label, internal_oid_t(oid), gid)) {
      return (vid_parser_.GetFid(gid) == fid_) ? InnerVertexGid2Vertex(gid, v)
                                               : OuterVertexGid2Vertex(gid, v);
    }
    return false;
  }

  oid_t GetId(const vertex_t& v) const {
    return IsInnerVertex(v) ? GetInnerVertexId(v) : GetOuterVertexId(v);
  }

  fid_t GetFragId(const vertex_t& u) const {
    return IsInnerVertex(u) ? fid_ : vid_parser_.GetFid(GetOuterVertexGid(u));
  }

  size_t GetTotalNodesNum() const {
    return vm_ptr_->GetTotalNodesNum() + extra_vm_ptr_->GetTotalNodesNum();
  }

  template <typename T>
  T GetData(const vertex_t& v, prop_id_t prop_id) const {
    auto v_label = vid_parser_.GetLabelId(v.GetValue());
    auto offset = vid_parser_.GetOffset(v.GetValue());

    if (offset < ivnums_[v_label]) {
      return append_only_fragment_impl::get_from_arrow_array<T>(
          vertex_tables_[v_label]->column(prop_id)->chunk(0), offset);
    } else {
      auto idx = offset - ivnums_[v_label];
      return extra_vertex_tables_[v_label]->template GetValue<T>(prop_id, idx);
    }
  }

  bool HasChild(const vertex_t& v, label_id_t e_label) const {
    return GetLocalOutDegree(v, e_label) != 0;
  }

  bool HasParent(const vertex_t& v, label_id_t e_label) const {
    return GetLocalInDegree(v, e_label) != 0;
  }

  int GetLocalOutDegree(const vertex_t& v, label_id_t e_label) const {
    return GetOutgoingAdjList(v, e_label).Size();
  }

  int GetLocalInDegree(const vertex_t& v, label_id_t e_label) const {
    return GetIncomingAdjList(v, e_label).Size();
  }

  bool Gid2Vertex(const vid_t& gid, vertex_t& v) const {
    return (vid_parser_.GetFid(gid) == fid_) ? InnerVertexGid2Vertex(gid, v)
                                             : OuterVertexGid2Vertex(gid, v);
  }

  vid_t Vertex2Gid(const vertex_t& v) const {
    return IsInnerVertex(v) ? GetInnerVertexGid(v) : GetOuterVertexGid(v);
  }

  inline vid_t GetInnerVerticesNum(label_id_t label_id) const {
    return curr_ivnums_[label_id];
  }

  inline vid_t GetOuterVerticesNum(label_id_t label_id) const {
    return curr_ivnums_[label_id];
  }

  inline bool IsInnerVertex(const vertex_t& v) const {
    return vid_parser_.GetOffset(v.GetValue()) <
           (int64_t) curr_ivnums_[vid_parser_.GetLabelId(v.GetValue())];
  }

  inline bool IsOuterVertex(const vertex_t& v) const {
    vid_t offset = vid_parser_.GetOffset(v.GetValue());
    label_id_t label = vid_parser_.GetLabelId(v.GetValue());
    return offset < curr_tvnums_[label] && offset >= curr_ivnums_[label];
  }

  bool GetInnerVertex(label_id_t label, const oid_t& oid, vertex_t& v) const {
    vid_t gid;
    if (getGid(label, internal_oid_t(oid), gid)) {
      if (vid_parser_.GetFid(gid) == fid_) {
        v.SetValue(vid_parser_.GetLid(gid));
        return true;
      }
    }
    return false;
  }

  bool GetOuterVertex(label_id_t label, const oid_t& oid, vertex_t& v) const {
    vid_t gid;
    if (getGid(label, internal_oid_t(oid), gid)) {
      return OuterVertexGid2Vertex(gid, v);
    }
    return false;
  }

  inline oid_t GetInnerVertexId(const vertex_t& v) const {
    vid_t gid =
        vid_parser_.GenerateId(fid_, vid_parser_.GetLabelId(v.GetValue()),
                               vid_parser_.GetOffset(v.GetValue()));
    oid_t oid;
    CHECK(getOid(gid, oid));

    return oid;
  }

  inline oid_t GetOuterVertexId(const vertex_t& v) const {
    vid_t gid = GetOuterVertexGid(v);
    oid_t oid;

    CHECK(getOid(gid, oid));
    return oid;
  }

  inline oid_t Gid2Oid(const vid_t& gid) const {
    oid_t oid;
    CHECK(getOid(gid, oid));

    return oid;
  }

  inline bool Oid2Gid(label_id_t label, const oid_t& oid, vid_t& gid) const {
    return getGid(label, internal_oid_t(oid), gid);
  }

  inline bool InnerVertexGid2Vertex(const vid_t& gid, vertex_t& v) const {
    v.SetValue(vid_parser_.GetLid(gid));
    return true;
  }

  inline bool OuterVertexGid2Vertex(const vid_t& gid, vertex_t& v) const {
    auto v_label = vid_parser_.GetLabelId(gid);
    auto map = ovg2l_maps_[v_label];
    auto iter = map->find(gid);
    auto ivnum = curr_ivnums_[v_label];

    if (iter != map->end()) {
      v.SetValue(ivnum + (vid_parser_.offset_mask() - iter->second));
      return true;
    } else {
      auto extra_map = extra_ovg2l_maps_[v_label];
      auto extra_iter = extra_map.find(gid);

      if (extra_iter != extra_map.end()) {
        v.SetValue(ivnum + (vid_parser_.offset_mask() - extra_iter->second));
        return true;
      }
    }
    return false;
  }

  inline vid_t GetOuterVertexGid(const vertex_t& v) const {
    auto offset = vid_parser_.GetOffset(v.GetValue());
    label_id_t v_label = vid_parser_.GetLabelId(v.GetValue());
    auto idx = offset - curr_ivnums_[v_label];

    if (idx < ovnums_[v_label]) {
      return ovgid_lists_[v_label]->Value(idx);
    } else {
      idx -= ovnums_[v_label];
      CHECK_LT(idx, extra_ovgid_lists_[v_label].size());
      return extra_ovgid_lists_[v_label][idx];
    }
  }

  inline vid_t GetInnerVertexGid(const vertex_t& v) const {
    return vid_parser_.GenerateId(fid_, vid_parser_.GetLabelId(v.GetValue()),
                                  vid_parser_.GetOffset(v.GetValue()));
  }

  inline adj_list_t GetIncomingAdjList(const vertex_t& v,
                                       label_id_t e_label) const {
    vid_t vid = v.GetValue();
    label_id_t v_label = vid_parser_.GetLabelId(vid);

    if (vid < ivnums_[v_label]) {
      int64_t v_offset = vid_parser_.GetOffset(vid);
      const int64_t* offset_array = ie_offsets_ptr_lists_[v_label][e_label];
      const nbr_unit_t* ie = ie_ptr_lists_[v_label][e_label];

      return adj_list_t(&ie[offset_array[v_offset]],
                        &ie[offset_array[v_offset + 1]], edge_tables_[e_label],
                        &vid_parser_, curr_ivnums_.data());
    }
    return adj_list_t();
  }

  inline adj_list_t GetOutgoingAdjList(const vertex_t& v,
                                       label_id_t e_label) const {
    vid_t vid = v.GetValue();
    label_id_t v_label = vid_parser_.GetLabelId(vid);

    if (vid < ivnums_[v_label]) {
      int64_t v_offset = vid_parser_.GetOffset(vid);
      const int64_t* offset_array = oe_offsets_ptr_lists_[v_label][e_label];
      const nbr_unit_t* oe = oe_ptr_lists_[v_label][e_label];

      return adj_list_t(&oe[offset_array[v_offset]],
                        &oe[offset_array[v_offset + 1]], edge_tables_[e_label],
                        &vid_parser_, curr_ivnums_.data());
    }
    return adj_list_t();
  }

  inline extra_adj_list_t GetExtraOutgoingAdjList(const vertex_t& v,
                                                  label_id_t e_label) const {
    vid_t vid = v.GetValue();
    label_id_t v_label = vid_parser_.GetLabelId(vid);
    int64_t v_offset = vid_parser_.GetOffset(vid);
    auto& oe_index = extra_oe_indices_[v_label][e_label];
    int64_t loc = -1;

    if (v_offset < (int64_t) oe_index.size() &&
        (loc = oe_index[v_offset]) >= 0) {
      auto& edge_space = extra_edge_space_array_[e_label];
      auto& edge_table = extra_edge_tables_[e_label];

      return extra_adj_list_t(edge_space[loc].begin(), edge_space[loc].end(),
                              edge_table, &vid_parser_, curr_ivnums_.data());
    }
    return extra_adj_list_t();
  }

  inline extra_adj_list_t GetExtraIncomingAdjList(const vertex_t& v,
                                                  label_id_t e_label) const {
    LOG(FATAL) << "Not implemented.";

    return extra_adj_list_t();
  }

  inline grape::DestList IEDests(const vertex_t& v, label_id_t e_label) const {
    LOG(FATAL) << "Not implemented.";

    return {nullptr, nullptr};
  }

  inline grape::DestList OEDests(const vertex_t& v, label_id_t e_label) const {
    LOG(FATAL) << "Not implemented.";

    return {nullptr, nullptr};
  }

  inline grape::DestList IOEDests(const vertex_t& v, label_id_t e_label) const {
    LOG(FATAL) << "Not implemented.";

    return {nullptr, nullptr};
  }

  std::shared_ptr<vertex_map_t> GetVertexMap() { return vm_ptr_; }

  void PrepareToRunApp(const grape::CommSpec& comm_spec, grape::PrepareConf conf) {
  }

  std::shared_ptr<extra_vertex_map_t> GetExtraVertexMap() {
    return extra_vm_ptr_;
  }

  std::shared_ptr<AppendOnlyArrowTable> extra_edge_data_table(
      label_id_t i) const {
    return extra_edge_tables_[i];
  }

 private:
  void initPointers() {
    oe_ptr_lists_.resize(vertex_label_num_);
    oe_offsets_ptr_lists_.resize(vertex_label_num_);

    idst_.resize(vertex_label_num_);
    odst_.resize(vertex_label_num_);
    iodst_.resize(vertex_label_num_);

    idoffset_.resize(vertex_label_num_);
    odoffset_.resize(vertex_label_num_);
    iodoffset_.resize(vertex_label_num_);

    for (label_id_t i = 0; i < vertex_label_num_; ++i) {
      oe_ptr_lists_[i].resize(edge_label_num_);
      oe_offsets_ptr_lists_[i].resize(edge_label_num_);

      idst_[i].resize(edge_label_num_);
      odst_[i].resize(edge_label_num_);
      iodst_[i].resize(edge_label_num_);

      idoffset_[i].resize(edge_label_num_);
      odoffset_[i].resize(edge_label_num_);
      iodoffset_[i].resize(edge_label_num_);

      for (label_id_t j = 0; j < edge_label_num_; ++j) {
        oe_ptr_lists_[i][j] =
            reinterpret_cast<const nbr_unit_t*>(oe_lists_[i][j]->GetValue(0));
        oe_offsets_ptr_lists_[i][j] = oe_offsets_lists_[i][j]->raw_values();
      }
    }

    if (directed_) {
      ie_ptr_lists_.resize(vertex_label_num_);
      ie_offsets_ptr_lists_.resize(vertex_label_num_);
      for (label_id_t i = 0; i < vertex_label_num_; ++i) {
        ie_ptr_lists_[i].resize(edge_label_num_);
        ie_offsets_ptr_lists_[i].resize(edge_label_num_);
        for (label_id_t j = 0; j < edge_label_num_; ++j) {
          ie_ptr_lists_[i][j] =
              reinterpret_cast<const nbr_unit_t*>(ie_lists_[i][j]->GetValue(0));
          ie_offsets_ptr_lists_[i][j] = ie_offsets_lists_[i][j]->raw_values();
        }
      }
    } else {
      ie_ptr_lists_ = oe_ptr_lists_;
      ie_offsets_ptr_lists_ = oe_offsets_ptr_lists_;
    }
  }

  void initExtra() {
    extra_vm_ptr_ = std::make_shared<extra_vertex_map_t>();
    extra_vm_ptr_->Init(vm_ptr_);
    curr_ivnums_.resize(vertex_label_num_);
    curr_ovnums_.resize(vertex_label_num_);
    curr_tvnums_.resize(vertex_label_num_);
    extra_vertex_tables_.resize(vertex_label_num_);
    extra_edge_tables_.resize(edge_label_num_);
    extra_ovgid_lists_.resize(vertex_label_num_);
    extra_ovg2l_maps_.resize(vertex_label_num_);
    extra_oe_indices_.resize(vertex_label_num_);
    extra_edge_space_array_.resize(edge_label_num_);
    extra_oe_nums_.resize(edge_label_num_, 0);

    for (label_id_t v_label = 0; v_label < vertex_label_num_; v_label++) {
      extra_vertex_tables_[v_label] = std::make_shared<AppendOnlyArrowTable>();
      curr_ivnums_[v_label] = ivnums_[v_label];
      curr_ovnums_[v_label] = ovnums_[v_label];
      curr_tvnums_[v_label] = tvnums_[v_label];
      extra_oe_indices_[v_label].resize(edge_label_num_);
    }

    for (label_id_t e_label = 0; e_label < edge_label_num_; e_label++) {
      extra_edge_tables_[e_label] = std::make_shared<AppendOnlyArrowTable>();
    }
  }

  bool addOutgoingEdge(vid_t src_lid, vid_t dst_lid, label_id_t e_label,
                       eid_t eid) {
    auto src_label = vid_parser_.GetLabelId(src_lid);
    auto src_offset = vid_parser_.GetOffset(src_lid);

    CHECK_LT(src_offset, curr_ivnums_[src_label]);

    // first, check dst exists in the csr or not
    if (src_offset < (int64_t) ivnums_[src_label]) {
      auto offset_array = oe_offsets_ptr_lists_[src_label][e_label];
      auto oe = oe_ptr_lists_[src_label][e_label];
      auto target = nbr_unit_t(dst_lid, 0);

      if (std::binary_search(&oe[offset_array[src_offset]],
                             &oe[offset_array[src_offset + 1]], target,
                             [](const nbr_unit_t& l, const nbr_unit_t& r) {
                               return l.vid < r.vid;
                             })) {
        return false;
      }
    }

    auto& extra_oe_index = extra_oe_indices_[src_label][e_label];
    auto& extra_edge_space = extra_edge_space_array_[e_label];
    CHECK_LE(src_offset, extra_oe_index.size());
    auto pos = extra_oe_index[src_offset];
    bool created = false;

    // then, check or insert the mutable part: extra_edge_space
    if (pos == -1) {
      extra_oe_index[src_offset] = extra_edge_space.emplace(dst_lid, eid);
      created = true;
    } else {
      extra_oe_index[src_offset] =
          extra_edge_space.emplace(pos, dst_lid, eid, created);
    }
    return created;
  }

  bool ovg2l(const vid_t gid, vid_t& lid) {
    auto v_label = vid_parser_.GetLabelId(gid);
    auto& ovg2l_map = ovg2l_maps_[v_label];
    auto& extra_ovg2l_map = extra_ovg2l_maps_[v_label];
    auto iter_base = ovg2l_map->find(gid);
    bool found = false;

    if (iter_base != ovg2l_map->end()) {
      lid = iter_base->second;
      found = true;
    } else {
      auto iter_extra = extra_ovg2l_map.find(gid);
      if (iter_extra != extra_ovg2l_map.end()) {
        lid = iter_extra->second;
        found = true;
      }
    }
    return found;
  }

  bool getGid(label_id_t label, const oid_t& oid, vid_t& gid) const {
    return vm_ptr_->GetGid(label, oid, gid) || extra_vm_ptr_->GetGid(oid, gid);
  }

  bool getOid(const vid_t gid, oid_t& oid) const {
    internal_oid_t internal_oid;
    if (vm_ptr_->GetOid(gid, internal_oid)) {
      oid = oid_t(internal_oid);
      return true;
    }
    return extra_vm_ptr_->GetOid(gid, oid);
  }

  fid_t fid_, fnum_;
  bool directed_;
  label_id_t vertex_label_num_;
  label_id_t edge_label_num_;

  vineyard::Array<vid_t> ivnums_, ovnums_, tvnums_;

  std::vector<std::shared_ptr<arrow::Table>> vertex_tables_;
  std::vector<std::shared_ptr<vid_array_t>> ovgid_lists_;
  std::vector<std::shared_ptr<vineyard::Hashmap<vid_t, vid_t>>> ovg2l_maps_;

#ifdef ENDPOINT_LISTS
  std::vector<std::shared_ptr<vid_array_t>> edge_src_, edge_dst_;
#endif
  std::vector<std::shared_ptr<arrow::Table>> edge_tables_;

  std::vector<std::vector<std::shared_ptr<arrow::FixedSizeBinaryArray>>>
      ie_lists_, oe_lists_;
  std::vector<std::vector<const nbr_unit_t*>> ie_ptr_lists_, oe_ptr_lists_;
  std::vector<std::vector<std::shared_ptr<arrow::Int64Array>>>
      ie_offsets_lists_, oe_offsets_lists_;
  std::vector<std::vector<const int64_t*>> ie_offsets_ptr_lists_,
      oe_offsets_ptr_lists_;

  std::vector<std::vector<std::vector<fid_t>>> idst_, odst_, iodst_;
  std::vector<std::vector<std::vector<fid_t*>>> idoffset_, odoffset_,
      iodoffset_;

  std::shared_ptr<vertex_map_t> vm_ptr_;

  vineyard::IdParser<vid_t> vid_parser_;

  // Append related members
  std::shared_ptr<extra_vertex_map_t> extra_vm_ptr_;
  // base vertices are included
  std::vector<vid_t> curr_ivnums_, curr_ovnums_, curr_tvnums_;

  std::vector<std::shared_ptr<AppendOnlyArrowTable>> extra_vertex_tables_;
  std::vector<std::shared_ptr<AppendOnlyArrowTable>> extra_edge_tables_;

  std::vector<std::vector<vid_t>> extra_ovgid_lists_;
  std::vector<ska::flat_hash_map<vid_t, vid_t>> extra_ovg2l_maps_;

  // v_label->e_label->index
  std::vector<std::vector<std::vector<int64_t>>> extra_oe_indices_;
  std::vector<NbrMapSpace<eid_t>> extra_edge_space_array_;

  std::vector<eid_t> extra_oe_nums_;

  template <typename _OID_T, typename _VID_T>
  friend class AppendOnlyArrowFragmentBuilder;

  template <typename _OID_T, typename _VID_T>
  friend class ArrowFragmentAppender;

  template <typename _OID_T, typename _VID_T>
  friend class ExtraVertexMap;
};

template <typename OID_T, typename VID_T>
class AppendOnlyArrowFragmentBuilder : public vineyard::ObjectBuilder {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using internal_oid_t = typename vineyard::InternalType<oid_t>::type;
  using eid_t = vineyard::property_graph_types::EID_TYPE;
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;
  using prop_id_t = vineyard::property_graph_types::PROP_ID_TYPE;
  using vertex_map_t = vineyard::ArrowVertexMap<internal_oid_t, vid_t>;
  using fragment_t = AppendOnlyArrowFragment<OID_T, VID_T>;

 public:
  explicit AppendOnlyArrowFragmentBuilder(vineyard::Client& client) {}

  void set_fid(fid_t fid) { fid_ = fid; }
  void set_fnum(fid_t fnum) { fnum_ = fnum; }
  void set_directed(bool directed) { directed_ = directed; }

  void set_label_num(label_id_t vertex_label_num, label_id_t edge_label_num) {
    vertex_label_num_ = vertex_label_num;
    edge_label_num_ = edge_label_num;

    vertex_tables_.resize(vertex_label_num_);
    ovgid_lists_.resize(vertex_label_num_);
    ovg2l_maps_.resize(vertex_label_num_);

#ifdef ENDPOINT_LISTS
    edge_src_.resize(edge_label_num_);
    edge_dst_.resize(edge_label_num_);
#endif
    edge_tables_.resize(edge_label_num_);

    if (directed_) {
      ie_lists_.resize(vertex_label_num_);
      ie_offsets_lists_.resize(vertex_label_num_);
    }
    oe_lists_.resize(vertex_label_num_);
    oe_offsets_lists_.resize(vertex_label_num_);

    for (label_id_t i = 0; i < vertex_label_num_; ++i) {
      if (directed_) {
        ie_lists_[i].resize(edge_label_num_);
        ie_offsets_lists_[i].resize(edge_label_num_);
      }
      oe_lists_[i].resize(edge_label_num_);
      oe_offsets_lists_[i].resize(edge_label_num_);
    }
  }

  void set_ivnums(const vineyard::Array<vid_t>& ivnums) { ivnums_ = ivnums; }

  void set_ovnums(const vineyard::Array<vid_t>& ovnums) { ovnums_ = ovnums; }

  void set_tvnums(const vineyard::Array<vid_t>& tvnums) { tvnums_ = tvnums; }

  void set_vertex_table(label_id_t label,
                        std::shared_ptr<vineyard::Table> table) {
    assert(vertex_tables_.size() > label);
    vertex_tables_[label] = std::move(table);
  }

  void set_ovgid_list(
      label_id_t label,
      std::shared_ptr<vineyard::NumericArray<vid_t>> ovgid_list) {
    assert(ovgid_lists_.size() > label);
    ovgid_lists_[label] = ovgid_list;
  }

  void set_ovg2l_map(
      label_id_t label,
      std::shared_ptr<vineyard::Hashmap<vid_t, vid_t>> ovg2l_map) {
    assert(ovg2l_maps_.size() > label);
    ovg2l_maps_[label] = ovg2l_map;
  }

#ifdef ENDPOINT_LISTS
  void set_edge_src(label_id_t label,
                    std::shared_ptr<vineyard::NumericArray<vid_t>> edge_src) {
    assert(edge_src_.size() > label);
    edge_src_[label] = edge_src;
  }

  void set_edge_dst(label_id_t label,
                    std::shared_ptr<vineyard::NumericArray<vid_t>> edge_dst) {
    assert(edge_dst_.size() > label);
    edge_dst_[label] = edge_dst;
  }
#endif

  void set_edge_table(label_id_t label,
                      std::shared_ptr<vineyard::Table> table) {
    assert(edge_tables_.size() > label);
    edge_tables_[label] = std::move(table);
  }

  void set_in_edge_list(
      label_id_t v_label, label_id_t e_label,
      std::shared_ptr<vineyard::FixedSizeBinaryArray> in_edge_list) {
    assert(ie_lists_.size() > v_label);
    assert(ie_lists_[v_label].size() > e_label);
    ie_lists_[v_label][e_label] = std::move(in_edge_list);
  }

  void set_out_edge_list(
      label_id_t v_label, label_id_t e_label,
      std::shared_ptr<vineyard::FixedSizeBinaryArray> out_edge_list) {
    assert(oe_lists_.size() > v_label);
    assert(oe_lists_[v_label].size() > e_label);
    oe_lists_[v_label][e_label] = std::move(out_edge_list);
  }

  void set_in_edge_offsets(
      label_id_t v_label, label_id_t e_label,
      std::shared_ptr<vineyard::NumericArray<int64_t>> in_edge_offsets) {
    assert(ie_offsets_lists_.size() > v_label);
    assert(ie_offsets_lists_[v_label].size() > e_label);
    ie_offsets_lists_[v_label][e_label] = std::move(in_edge_offsets);
  }

  void set_out_edge_offsets(
      label_id_t v_label, label_id_t e_label,
      std::shared_ptr<vineyard::NumericArray<int64_t>> out_edge_offsets) {
    assert(oe_offsets_lists_.size() > v_label);
    assert(oe_offsets_lists_[v_label].size() > e_label);
    oe_offsets_lists_[v_label][e_label] = std::move(out_edge_offsets);
  }

  void set_vertex_map(std::shared_ptr<vertex_map_t> vm_ptr) {
    vm_ptr_ = vm_ptr;
  }

#define ASSIGN_ARRAY_VECTOR(src_array_vec, dst_array_vec) \
  do {                                                    \
    dst_array_vec.resize(src_array_vec.size());           \
    for (size_t i = 0; i < src_array_vec.size(); ++i) {   \
      dst_array_vec[i] = src_array_vec[i]->GetArray();    \
    }                                                     \
  } while (0)

#define ASSIGN_ARRAY_VECTOR_VECTOR(src_array_vec, dst_array_vec) \
  do {                                                           \
    dst_array_vec.resize(src_array_vec.size());                  \
    for (size_t i = 0; i < src_array_vec.size(); ++i) {          \
      dst_array_vec[i].resize(src_array_vec[i].size());          \
      for (size_t j = 0; j < src_array_vec[i].size(); ++j) {     \
        dst_array_vec[i][j] = src_array_vec[i][j]->GetArray();   \
      }                                                          \
    }                                                            \
  } while (0)

#define ASSIGN_TABLE_VECTOR(src_table_vec, dst_table_vec) \
  do {                                                    \
    dst_table_vec.resize(src_table_vec.size());           \
    for (size_t i = 0; i < src_table_vec.size(); ++i) {   \
      dst_table_vec[i] = src_table_vec[i]->GetTable();    \
    }                                                     \
  } while (0)

#define GENERATE_VEC_META(prefix, vec, label_num)                           \
  do {                                                                      \
    for (label_id_t i = 0; i < label_num; ++i) {                            \
      frag->meta_.AddMember(vineyard::generate_name_with_suffix(prefix, i), \
                            vec[i]->meta());                                \
      nbytes += vec[i]->nbytes();                                           \
    }                                                                       \
  } while (0)

#define GENERATE_VEC_VEC_META(prefix, vec, v_label_num, e_label_num) \
  do {                                                               \
    for (label_id_t i = 0; i < v_label_num; ++i) {                   \
      for (label_id_t j = 0; j < e_label_num; ++j) {                 \
        frag->meta_.AddMember(                                       \
            vineyard::generate_name_with_suffix(prefix, i, j),       \
            vec[i][j]->meta());                                      \
        nbytes += vec[i][j]->nbytes();                               \
      }                                                              \
    }                                                                \
  } while (0)

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) override {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);

    VINEYARD_CHECK_OK(this->Build(client));

    auto frag = std::make_shared<fragment_t>();

    frag->fid_ = fid_;
    frag->fnum_ = fnum_;
    frag->directed_ = directed_;
    frag->vertex_label_num_ = vertex_label_num_;
    frag->edge_label_num_ = edge_label_num_;

    frag->ivnums_ = ivnums_;
    frag->ovnums_ = ovnums_;
    frag->tvnums_ = tvnums_;

    ASSIGN_TABLE_VECTOR(vertex_tables_, frag->vertex_tables_);
    ASSIGN_ARRAY_VECTOR(ovgid_lists_, frag->ovgid_lists_);

#ifdef ENDPOINT_LISTS
    ASSIGN_ARRAY_VECTOR(edge_src_, frag->edge_src_);
    ASSIGN_ARRAY_VECTOR(edge_dst_, frag->edge_dst_);
#endif
    ASSIGN_TABLE_VECTOR(edge_tables_, frag->edge_tables_);

    if (directed_) {
      ASSIGN_ARRAY_VECTOR_VECTOR(ie_lists_, frag->ie_lists_);
      ASSIGN_ARRAY_VECTOR_VECTOR(ie_offsets_lists_, frag->ie_offsets_lists_);
    }

    ASSIGN_ARRAY_VECTOR_VECTOR(oe_lists_, frag->oe_lists_);
    ASSIGN_ARRAY_VECTOR_VECTOR(oe_offsets_lists_, frag->oe_offsets_lists_);

    frag->meta_.SetTypeName(type_name<fragment_t>());

    frag->meta_.AddKeyValue("fid", fid_);
    frag->meta_.AddKeyValue("fnum", fnum_);
    frag->meta_.AddKeyValue("directed", static_cast<int>(directed_));
    frag->meta_.AddKeyValue("vertex_label_num", vertex_label_num_);
    frag->meta_.AddKeyValue("oid_type", vineyard::TypeName<oid_t>::Get());
    frag->meta_.AddKeyValue("vid_type", vineyard::TypeName<vid_t>::Get());

    for (label_id_t i = 0; i < vertex_label_num_; ++i) {
      std::shared_ptr<arrow::Table> table = frag->vertex_tables_[i];
      int prop_num = table->num_columns();
      frag->meta_.AddKeyValue("vertex_property_num_" + std::to_string(i),
                              std::to_string(prop_num));
      std::string prefix = "vertex_property_type_" + std::to_string(i) + "_";
      for (prop_id_t j = 0; j < prop_num; ++j) {
        frag->meta_.AddKeyValue(
            prefix + std::to_string(j),
            vineyard::arrow_type_to_string(table->field(j)->type()));
      }
    }

    frag->meta_.AddKeyValue("edge_label_num", edge_label_num_);
    for (label_id_t i = 0; i < edge_label_num_; ++i) {
      std::shared_ptr<arrow::Table> table = frag->edge_tables_[i];
      int prop_num = table->num_columns();
      frag->meta_.AddKeyValue("edge_property_num_" + std::to_string(i),
                              std::to_string(prop_num));
      std::string prefix = "edge_property_type_" + std::to_string(i) + "_";
      for (prop_id_t j = 0; j < prop_num; ++j) {
        frag->meta_.AddKeyValue(
            prefix + std::to_string(j),
            vineyard::arrow_type_to_string(table->field(j)->type()));
      }
    }

    size_t nbytes = 0;
    frag->meta_.AddMember("ivnums", ivnums_.meta());
    nbytes += ivnums_.nbytes();
    frag->meta_.AddMember("ovnums", ovnums_.meta());
    nbytes += ovnums_.nbytes();
    frag->meta_.AddMember("tvnums", tvnums_.meta());
    nbytes += tvnums_.nbytes();

    GENERATE_VEC_META("vertex_tables", vertex_tables_, vertex_label_num_);
    GENERATE_VEC_META("ovgid_lists", ovgid_lists_, vertex_label_num_);
    GENERATE_VEC_META("ovg2l_maps", ovg2l_maps_, vertex_label_num_);
#ifdef ENDPOINT_LISTS
    GENERATE_VEC_META("edge_src", edge_src_, edge_label_num_);
    GENERATE_VEC_META("edge_dst", edge_dst_, edge_label_num_);
#endif
    GENERATE_VEC_META("edge_tables", edge_tables_, edge_label_num_);
    if (directed_) {
      GENERATE_VEC_VEC_META("ie_lists", ie_lists_, vertex_label_num_,
                            edge_label_num_);
      GENERATE_VEC_VEC_META("ie_offsets_lists", ie_offsets_lists_,
                            vertex_label_num_, edge_label_num_);
    }
    GENERATE_VEC_VEC_META("oe_lists", oe_lists_, vertex_label_num_,
                          edge_label_num_);
    GENERATE_VEC_VEC_META("oe_offsets_lists", oe_offsets_lists_,
                          vertex_label_num_, edge_label_num_);

    frag->meta_.AddMember("vertex_map", vm_ptr_->meta());

    frag->meta_.SetNBytes(nbytes);

    VINEYARD_CHECK_OK(client.CreateMetaData(frag->meta_, frag->id_));
    this->set_sealed(true);

    VINEYARD_CHECK_OK(client.GetMetaData(frag->id_, frag->meta_));
    frag->Construct(frag->meta_);
    return std::static_pointer_cast<vineyard::Object>(frag);
  }

#undef ASSIGN_ARRAY_VECTOR
#undef ASSIGN_ARRAY_VECTOR_VECTOR
#undef ASSIGN_TABLE_VECTOR
#undef GENERATE_VEC_META
#undef GENERATE_VEC_VEC_META

 private:
  fid_t fid_, fnum_;
  bool directed_;
  label_id_t vertex_label_num_;
  label_id_t edge_label_num_;

  vineyard::Array<vid_t> ivnums_, ovnums_, tvnums_;

  std::vector<std::shared_ptr<vineyard::Table>> vertex_tables_;
  std::vector<std::shared_ptr<vineyard::NumericArray<vid_t>>> ovgid_lists_;
  std::vector<std::shared_ptr<vineyard::Hashmap<vid_t, vid_t>>> ovg2l_maps_;

#ifdef ENDPOINT_LISTS
  std::vector<std::shared_ptr<vineyard::NumericArray<vid_t>>> edge_src_,
      edge_dst_;
#endif
  std::vector<std::shared_ptr<vineyard::Table>> edge_tables_;

  std::vector<std::vector<std::shared_ptr<vineyard::FixedSizeBinaryArray>>>
      ie_lists_, oe_lists_;
  std::vector<std::vector<std::shared_ptr<vineyard::NumericArray<int64_t>>>>
      ie_offsets_lists_, oe_offsets_lists_;

  std::shared_ptr<vertex_map_t> vm_ptr_;
};

template <typename OID_T, typename VID_T>
class BasicAppendOnlyArrowFragmentBuilder
    : public AppendOnlyArrowFragmentBuilder<OID_T, VID_T> {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using internal_oid_t = typename vineyard::InternalType<oid_t>::type;
  using eid_t = vineyard::property_graph_types::EID_TYPE;
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;
  using vertex_map_t = vineyard::ArrowVertexMap<internal_oid_t, vid_t>;
  using fragment_t = AppendOnlyArrowFragment<OID_T, VID_T>;
  using nbr_unit_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;

 public:
  explicit BasicAppendOnlyArrowFragmentBuilder(
      vineyard::Client& client, std::shared_ptr<vertex_map_t> vm_ptr)
      : AppendOnlyArrowFragmentBuilder<oid_t, vid_t>(client), vm_ptr_(vm_ptr) {}

  vineyard::Status Build(vineyard::Client& client) override {
    this->set_fid(fid_);
    this->set_fnum(fnum_);
    this->set_directed(directed_);
    this->set_label_num(vertex_label_num_, edge_label_num_);

    {
      vineyard::ArrayBuilder<vid_t> ivnums_builder(client, ivnums_);
      vineyard::ArrayBuilder<vid_t> ovnums_builder(client, ovnums_);
      vineyard::ArrayBuilder<vid_t> tvnums_builder(client, tvnums_);
      this->set_ivnums(*std::dynamic_pointer_cast<vineyard::Array<vid_t>>(
          ivnums_builder.Seal(client)));
      this->set_ovnums(*std::dynamic_pointer_cast<vineyard::Array<vid_t>>(
          ovnums_builder.Seal(client)));
      this->set_tvnums(*std::dynamic_pointer_cast<vineyard::Array<vid_t>>(
          tvnums_builder.Seal(client)));
    }

    for (label_id_t i = 0; i < vertex_label_num_; ++i) {
      vineyard::TableBuilder vt(client, vertex_tables_[i]);
      this->set_vertex_table(
          i, std::dynamic_pointer_cast<vineyard::Table>(vt.Seal(client)));

      vineyard::NumericArrayBuilder<vid_t> ovgid_list_builder(client,
                                                              ovgid_lists_[i]);
      this->set_ovgid_list(
          i, std::dynamic_pointer_cast<vineyard::NumericArray<vid_t>>(
                 ovgid_list_builder.Seal(client)));

      vineyard::HashmapBuilder<vid_t, vid_t> ovg2l_builder(
          client, std::move(ovg2l_maps_[i]));
      this->set_ovg2l_map(
          i, std::dynamic_pointer_cast<vineyard::Hashmap<vid_t, vid_t>>(
                 ovg2l_builder.Seal(client)));
    }

    for (label_id_t i = 0; i < edge_label_num_; ++i) {
#ifdef ENDPOINT_LISTS
      {
        vineyard::NumericArrayBuilder<vid_t> esa(client, edge_src_[i]);
        this->set_edge_src(
            i, std::dynamic_pointer_cast<vineyard::NumericArray<vid_t>>(
                   esa.Seal(client)));
      }
      {
        vineyard::NumericArrayBuilder<vid_t> eda(client, edge_dst_[i]);
        this->set_edge_dst(
            i, std::dynamic_pointer_cast<vineyard::NumericArray<vid_t>>(
                   eda.Seal(client)));
      }
#endif
      {
        vineyard::TableBuilder et(client, edge_tables_[i]);
        this->set_edge_table(
            i, std::dynamic_pointer_cast<vineyard::Table>(et.Seal(client)));
      }
    }

    for (label_id_t i = 0; i < vertex_label_num_; ++i) {
      for (label_id_t j = 0; j < edge_label_num_; ++j) {
        if (directed_) {
          vineyard::FixedSizeBinaryArrayBuilder ie_builder(client,
                                                           ie_lists_[i][j]);
          this->set_in_edge_list(
              i, j,
              std::dynamic_pointer_cast<vineyard::FixedSizeBinaryArray>(
                  ie_builder.Seal(client)));
        }
        {
          vineyard::FixedSizeBinaryArrayBuilder oe_builder(client,
                                                           oe_lists_[i][j]);
          this->set_out_edge_list(
              i, j,
              std::dynamic_pointer_cast<vineyard::FixedSizeBinaryArray>(
                  oe_builder.Seal(client)));
        }
        if (directed_) {
          vineyard::NumericArrayBuilder<int64_t> ieo(client,
                                                     ie_offsets_lists_[i][j]);
          this->set_in_edge_offsets(
              i, j,
              std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
                  ieo.Seal(client)));
        }
        {
          vineyard::NumericArrayBuilder<int64_t> oeo(client,
                                                     oe_offsets_lists_[i][j]);
          this->set_out_edge_offsets(
              i, j,
              std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
                  oeo.Seal(client)));
        }
      }
    }

    this->set_vertex_map(vm_ptr_);
    return vineyard::Status::OK();
  }

  bl::result<void> Init(
      fid_t fid, fid_t fnum,
      std::vector<std::shared_ptr<arrow::Table>>&& vertex_tables,
      std::vector<std::shared_ptr<arrow::Table>>&& edge_tables,
      bool directed = true) {
    fid_ = fid;
    fnum_ = fnum;
    directed_ = directed;
    vertex_label_num_ = vertex_tables.size();
    edge_label_num_ = edge_tables.size();

    vid_parser_.Init(fnum_, vertex_label_num_);

    BOOST_LEAF_CHECK(initVertices(std::move(vertex_tables)));
    BOOST_LEAF_CHECK(initEdges(std::move(edge_tables)));
    return {};
  }

 private:
  vid_t map_ov_lid(vid_t ov_lid, vid_t ivnum,
                   const vineyard::IdParser<vid_t>& id_parser) {
    auto fid = id_parser.GetFid(ov_lid);
    auto idx = id_parser.GetOffset(ov_lid) - ivnum;
    auto label = id_parser.GetLabelId(ov_lid);
    CHECK_EQ(fid, 0);
    CHECK_GE(idx, 0);
    return id_parser.GenerateId(0, label, id_parser.offset_mask() - idx);
  }

  int64_t offset2idx(int64_t offset, vid_t ivnum,
                     const vineyard::IdParser<vid_t>& id_parser) {
    return offset < (int64_t) ivnum
               ? offset
               : (ivnum + (id_parser.offset_mask() - offset));
  }

  // | prop_0 | prop_1 | ... |
  bl::result<void> initVertices(
      std::vector<std::shared_ptr<arrow::Table>>&& vertex_tables) {
    assert(vertex_tables.size() == vertex_label_num_);
    vertex_tables_.resize(vertex_label_num_);
    ivnums_.resize(vertex_label_num_);
    ovnums_.resize(vertex_label_num_);
    tvnums_.resize(vertex_label_num_);
    for (size_t i = 0; i < vertex_tables.size(); ++i) {
#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
      ARROW_OK_OR_RAISE(vertex_tables[i]->CombineChunks(
          arrow::default_memory_pool(), &vertex_tables_[i]));
#else
      ARROW_OK_ASSIGN_OR_RAISE(
          vertex_tables_[i],
          vertex_tables[i]->CombineChunks(arrow::default_memory_pool()));
#endif
      ivnums_[i] = vertex_tables_[i]->num_rows();
    }
    return {};
  }

  void collect_outer_vertices(
      std::shared_ptr<typename vineyard::ConvertToArrowType<vid_t>::ArrayType>
          gid_array) {
    const vid_t* arr = gid_array->raw_values();
    int64_t length = gid_array->length();
    for (int64_t i = 0; i < length; ++i) {
      if (vid_parser_.GetFid(arr[i]) != fid_) {
        collected_ovgids_[vid_parser_.GetLabelId(arr[i])].push_back(arr[i]);
      }
    }
  }

  bl::result<void> generate_outer_vertices_map() {
    ovg2l_maps_.resize(vertex_label_num_);
    ovgid_lists_.resize(vertex_label_num_);
    for (label_id_t i = 0; i < vertex_label_num_; ++i) {
      auto& cur_list = collected_ovgids_[i];
      std::sort(cur_list.begin(), cur_list.end());

      auto& cur_map = ovg2l_maps_[i];
      typename vineyard::ConvertToArrowType<vid_t>::BuilderType vec_builder;

      vid_t cur_id = vid_parser_.GenerateId(0, i, ivnums_[i]);
      if (!cur_list.empty()) {
        cur_map.emplace(cur_list[0],
                        map_ov_lid(cur_id, ivnums_[i], vid_parser_));
        ARROW_OK_OR_RAISE(vec_builder.Append(cur_list[0]));
        ++cur_id;
      }

      size_t cur_list_length = cur_list.size();
      for (size_t k = 1; k < cur_list_length; ++k) {
        if (cur_list[k] != cur_list[k - 1]) {
          cur_map.emplace(cur_list[k],
                          map_ov_lid(cur_id, ivnums_[i], vid_parser_));
          ARROW_OK_OR_RAISE(vec_builder.Append(cur_list[k]));
          ++cur_id;
        }
      }

      ARROW_OK_OR_RAISE(vec_builder.Finish(&ovgid_lists_[i]));

      ovnums_[i] = ovgid_lists_[i]->length();
      tvnums_[i] = ivnums_[i] + ovnums_[i];
    }
    collected_ovgids_.clear();
    return {};
  }

  bl::result<void> generate_local_id_list(
      std::shared_ptr<typename vineyard::ConvertToArrowType<vid_t>::ArrayType>
          gid_list,
      std::shared_ptr<typename vineyard::ConvertToArrowType<vid_t>::ArrayType>&
          lid_list) {
    typename vineyard::ConvertToArrowType<vid_t>::BuilderType builder;
    const vid_t* vec = gid_list->raw_values();
    int64_t length = gid_list->length();
    for (int64_t i = 0; i < length; ++i) {
      vid_t gid = vec[i];
      if (vid_parser_.GetFid(gid) == fid_) {
        ARROW_OK_OR_RAISE(builder.Append(vid_parser_.GenerateId(
            0, vid_parser_.GetLabelId(gid), vid_parser_.GetOffset(gid))));
      } else {
        ARROW_OK_OR_RAISE(
            builder.Append(ovg2l_maps_[vid_parser_.GetLabelId(gid)].at(gid)));
      }
    }
    ARROW_OK_OR_RAISE(builder.Finish(&lid_list));
    return {};
  }

  // | src_id(generated) | dst_id(generated) | prop_0 | prop_1
  // | ... |
  bl::result<void> initEdges(
      std::vector<std::shared_ptr<arrow::Table>>&& edge_tables) {
    assert(edge_tables.size() == edge_label_num_);
#ifndef ENDPOINT_LISTS
    std::vector<std::shared_ptr<
        typename vineyard::ConvertToArrowType<vid_t>::ArrayType>>
        edge_src_, edge_dst_;
#endif
    edge_src_.resize(edge_label_num_);
    edge_dst_.resize(edge_label_num_);

    edge_tables_.resize(edge_label_num_);

    collected_ovgids_.resize(vertex_label_num_);

    for (auto& edge_table : edge_tables) {
      std::shared_ptr<arrow::Table> combined_table;
#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
      ARROW_OK_OR_RAISE(edge_table->CombineChunks(arrow::default_memory_pool(),
                                                  &combined_table));
#else
      ARROW_OK_ASSIGN_OR_RAISE(
          combined_table,
          edge_table->CombineChunks(arrow::default_memory_pool()));
#endif
      edge_table.swap(combined_table);

      collect_outer_vertices(
          std::dynamic_pointer_cast<
              typename vineyard::ConvertToArrowType<vid_t>::ArrayType>(
              edge_table->column(0)->chunk(0)));
      collect_outer_vertices(
          std::dynamic_pointer_cast<
              typename vineyard::ConvertToArrowType<vid_t>::ArrayType>(
              edge_table->column(1)->chunk(0)));
    }

    generate_outer_vertices_map();

    for (size_t i = 0; i < edge_tables.size(); ++i) {
      generate_local_id_list(
          std::dynamic_pointer_cast<
              typename vineyard::ConvertToArrowType<vid_t>::ArrayType>(
              edge_tables[i]->column(0)->chunk(0)),
          edge_src_[i]);
      generate_local_id_list(
          std::dynamic_pointer_cast<
              typename vineyard::ConvertToArrowType<vid_t>::ArrayType>(
              edge_tables[i]->column(1)->chunk(0)),
          edge_dst_[i]);

      std::shared_ptr<arrow::Table> tmp_table0;
#if defined(ARROW_VERSION) && ARROW_VERSION < 17000
      ARROW_OK_OR_RAISE(edge_tables[i]->RemoveColumn(0, &tmp_table0));
      ARROW_OK_OR_RAISE(tmp_table0->RemoveColumn(0, &edge_tables_[i]));
#else
      ARROW_OK_ASSIGN_OR_RAISE(tmp_table0, edge_tables[i]->RemoveColumn(0));
      ARROW_OK_ASSIGN_OR_RAISE(edge_tables_[i], tmp_table0->RemoveColumn(0));
#endif

      edge_tables[i].reset();
    }

    oe_lists_.resize(vertex_label_num_);
    oe_offsets_lists_.resize(vertex_label_num_);
    if (directed_) {
      ie_lists_.resize(vertex_label_num_);
      ie_offsets_lists_.resize(vertex_label_num_);
    }

    for (label_id_t v_label = 0; v_label < vertex_label_num_; ++v_label) {
      oe_lists_[v_label].resize(edge_label_num_);
      oe_offsets_lists_[v_label].resize(edge_label_num_);
      if (directed_) {
        ie_lists_[v_label].resize(edge_label_num_);
        ie_offsets_lists_[v_label].resize(edge_label_num_);
      }
    }
    for (label_id_t e_label = 0; e_label < edge_label_num_; ++e_label) {
      std::vector<std::shared_ptr<arrow::FixedSizeBinaryArray>> sub_ie_lists(
          vertex_label_num_);
      std::vector<std::shared_ptr<arrow::FixedSizeBinaryArray>> sub_oe_lists(
          vertex_label_num_);
      std::vector<std::shared_ptr<arrow::Int64Array>> sub_ie_offset_lists(
          vertex_label_num_);
      std::vector<std::shared_ptr<arrow::Int64Array>> sub_oe_offset_lists(
          vertex_label_num_);
      if (directed_) {
        generate_directed_csr(edge_src_[e_label], edge_dst_[e_label],
                              sub_oe_lists, sub_oe_offset_lists);
        generate_directed_csr(edge_dst_[e_label], edge_src_[e_label],
                              sub_ie_lists, sub_ie_offset_lists);
      } else {
        generate_undirected_csr(edge_src_[e_label], edge_dst_[e_label],
                                sub_oe_lists, sub_oe_offset_lists);
      }

      for (label_id_t v_label = 0; v_label < vertex_label_num_; ++v_label) {
        if (directed_) {
          ie_lists_[v_label][e_label] = sub_ie_lists[v_label];
          ie_offsets_lists_[v_label][e_label] = sub_ie_offset_lists[v_label];
        }
        oe_lists_[v_label][e_label] = sub_oe_lists[v_label];
        oe_offsets_lists_[v_label][e_label] = sub_oe_offset_lists[v_label];
      }
    }
    return {};
  }

  bl::result<void> generate_directed_csr(
      std::shared_ptr<vid_array_t> src_list,
      std::shared_ptr<vid_array_t> dst_list,
      std::vector<std::shared_ptr<arrow::FixedSizeBinaryArray>>& edges,
      std::vector<std::shared_ptr<arrow::Int64Array>>& edge_offsets) {
    std::vector<std::vector<int>> degree(vertex_label_num_);
    std::vector<int64_t> actual_edge_num(vertex_label_num_, 0);
    for (label_id_t v_label = 0; v_label != vertex_label_num_; ++v_label) {
      degree[v_label].resize(tvnums_[v_label], 0);
    }
    int64_t edge_num = src_list->length();
    for (int64_t i = 0; i < edge_num; ++i) {
      vid_t src_id = src_list->Value(i);
      label_id_t v_label = vid_parser_.GetLabelId(src_id);
      auto ivnum = ivnums_[v_label];
      auto v_offset = vid_parser_.GetOffset(src_id);
      auto idx = offset2idx(v_offset, ivnum, vid_parser_);

      ++degree[v_label][idx];
    }
    std::vector<std::vector<int64_t>> offsets(vertex_label_num_);
    for (label_id_t v_label = 0; v_label != vertex_label_num_; ++v_label) {
      auto& offset_vec = offsets[v_label];
      offset_vec.resize(tvnums_[v_label] + 1);
      auto& degree_vec = degree[v_label];
      offset_vec[0] = 0;
      for (vid_t i = 0; i < tvnums_[v_label]; ++i) {
        offset_vec[i + 1] = offset_vec[i] + degree_vec[i];
      }
      actual_edge_num[v_label] = offset_vec[tvnums_[v_label]];
      arrow::Int64Builder builder;
      ARROW_OK_OR_RAISE(builder.AppendValues(offset_vec));
      ARROW_OK_OR_RAISE(builder.Finish(&edge_offsets[v_label]));
    }

    std::vector<vineyard::PodArrayBuilder<nbr_unit_t>> edge_builders(
        vertex_label_num_);
    for (label_id_t v_label = 0; v_label != vertex_label_num_; ++v_label) {
      ARROW_OK_OR_RAISE(
          edge_builders[v_label].Resize(actual_edge_num[v_label]));
    }

    eid_t cur_eid = 0;

    for (int64_t i = 0; i < edge_num; ++i) {
      vid_t src_id = src_list->Value(i);
      label_id_t v_label = vid_parser_.GetLabelId(src_id);
      int64_t v_offset = vid_parser_.GetOffset(src_id);
      auto idx = offset2idx(v_offset, ivnums_[v_label], vid_parser_);
      nbr_unit_t* ptr =
          edge_builders[v_label].MutablePointer(offsets[v_label][idx]);
      ptr->vid = dst_list->Value(i);
      ptr->eid = cur_eid;
      ++cur_eid;
      ++offsets[v_label][idx];
    }

    for (label_id_t v_label = 0; v_label != vertex_label_num_; ++v_label) {
      auto& builder = edge_builders[v_label];
      auto tvnum = tvnums_[v_label];
      auto offsets = edge_offsets[v_label];
      for (vid_t i = 0; i < tvnum; ++i) {
        nbr_unit_t* begin = builder.MutablePointer(offsets->Value(i));
        nbr_unit_t* end = builder.MutablePointer(offsets->Value(i + 1));
        std::sort(begin, end, [](const nbr_unit_t& lhs, const nbr_unit_t& rhs) {
          return lhs.vid < rhs.vid;
        });
      }
      ARROW_OK_OR_RAISE(
          edge_builders[v_label].Advance(actual_edge_num[v_label]));
      ARROW_OK_OR_RAISE(edge_builders[v_label].Finish(&edges[v_label]));
    }
    return {};
  }

  bl::result<void> generate_undirected_csr(
      std::shared_ptr<vid_array_t> src_list,
      std::shared_ptr<vid_array_t> dst_list,
      std::vector<std::shared_ptr<arrow::FixedSizeBinaryArray>>& edges,
      std::vector<std::shared_ptr<arrow::Int64Array>>& edge_offsets) {
    std::vector<std::vector<int>> degree(vertex_label_num_);
    std::vector<int64_t> actual_edge_num(vertex_label_num_, 0);
    for (label_id_t v_label = 0; v_label != vertex_label_num_; ++v_label) {
      degree[v_label].resize(tvnums_[v_label], 0);
    }
    int64_t edge_num = src_list->length();
    for (int64_t i = 0; i < edge_num; ++i) {
      vid_t src_id = src_list->Value(i);
      vid_t dst_id = dst_list->Value(i);
      auto src_label = vid_parser_.GetLabelId(src_id);
      auto dst_label = vid_parser_.GetLabelId(dst_id);
      auto src_idx = offset2idx(vid_parser_.GetOffset(src_id),
                                ivnums_[src_label], vid_parser_);
      auto dst_idx = offset2idx(vid_parser_.GetOffset(dst_id),
                                ivnums_[dst_label], vid_parser_);

      ++degree[src_label][src_idx];
      ++degree[dst_label][dst_idx];
    }
    std::vector<std::vector<int64_t>> offsets(vertex_label_num_);
    for (label_id_t v_label = 0; v_label != vertex_label_num_; ++v_label) {
      auto& offset_vec = offsets[v_label];
      offset_vec.resize(tvnums_[v_label] + 1);
      auto& degree_vec = degree[v_label];
      offset_vec[0] = 0;
      for (vid_t i = 0; i < tvnums_[v_label]; ++i) {
        offset_vec[i + 1] = offset_vec[i] + degree_vec[i];
      }
      actual_edge_num[v_label] = offset_vec[tvnums_[v_label]];
      arrow::Int64Builder builder;
      ARROW_OK_OR_RAISE(builder.AppendValues(offset_vec));
      ARROW_OK_OR_RAISE(builder.Finish(&edge_offsets[v_label]));
    }

    std::vector<vineyard::PodArrayBuilder<nbr_unit_t>> edge_builders(
        vertex_label_num_);
    for (label_id_t v_label = 0; v_label != vertex_label_num_; ++v_label) {
      ARROW_OK_OR_RAISE(
          edge_builders[v_label].Resize(actual_edge_num[v_label]));
    }

    eid_t cur_eid = 0;

    for (int64_t i = 0; i < edge_num; ++i) {
      vid_t src_id = src_list->Value(i);
      vid_t dst_id = dst_list->Value(i);
      label_id_t src_label = vid_parser_.GetLabelId(src_id);
      label_id_t dst_label = vid_parser_.GetLabelId(dst_id);
      auto src_idx = offset2idx(vid_parser_.GetOffset(src_id),
                                ivnums_[src_label], vid_parser_);
      auto dst_idx = offset2idx(vid_parser_.GetOffset(dst_id),
                                ivnums_[dst_label], vid_parser_);

      nbr_unit_t* src_ptr =
          edge_builders[src_label].MutablePointer(offsets[src_label][src_idx]);
      src_ptr->vid = dst_id;
      src_ptr->eid = cur_eid;
      ++offsets[src_label][src_idx];

      nbr_unit_t* dst_ptr =
          edge_builders[dst_label].MutablePointer(offsets[dst_label][dst_idx]);
      dst_ptr->vid = src_id;
      dst_ptr->eid = cur_eid;
      ++offsets[dst_label][dst_idx];

      ++cur_eid;
    }

    for (label_id_t v_label = 0; v_label != vertex_label_num_; ++v_label) {
      auto& builder = edge_builders[v_label];
      auto tvnum = tvnums_[v_label];
      auto offsets = edge_offsets[v_label];
      for (vid_t i = 0; i < tvnum; ++i) {
        nbr_unit_t* begin = builder.MutablePointer(offsets->Value(i));
        nbr_unit_t* end = builder.MutablePointer(offsets->Value(i + 1));
        std::sort(begin, end, [](const nbr_unit_t& lhs, const nbr_unit_t& rhs) {
          return lhs.vid < rhs.vid;
        });
      }
      ARROW_OK_OR_RAISE(
          edge_builders[v_label].Advance(actual_edge_num[v_label]));
      ARROW_OK_OR_RAISE(edge_builders[v_label].Finish(&edges[v_label]));
    }
    return {};
  }

  fid_t fid_, fnum_;
  bool directed_;
  label_id_t vertex_label_num_;
  label_id_t edge_label_num_;

  std::vector<vid_t> ivnums_, ovnums_, tvnums_;

  std::vector<std::shared_ptr<arrow::Table>> vertex_tables_;
  std::vector<
      std::shared_ptr<typename vineyard::ConvertToArrowType<vid_t>::ArrayType>>
      ovgid_lists_;
  std::vector<std::vector<vid_t>> collected_ovgids_;
  std::vector<ska::flat_hash_map<vid_t, vid_t>> ovg2l_maps_;

#ifdef ENDPOINT_LISTS
  std::vector<
      std::shared_ptr<typename vineyard::ConvertToArrowType<vid_t>::ArrayType>>
      edge_src_, edge_dst_;
#endif
  std::vector<std::shared_ptr<arrow::Table>> edge_tables_;

  std::vector<std::vector<std::shared_ptr<arrow::FixedSizeBinaryArray>>>
      ie_lists_, oe_lists_;
  std::vector<std::vector<std::shared_ptr<arrow::Int64Array>>>
      ie_offsets_lists_, oe_offsets_lists_;

  std::shared_ptr<vertex_map_t> vm_ptr_;

  vineyard::IdParser<vid_t> vid_parser_;
};

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_APPEND_ONLY_ARROW_FRAGMENT_H_

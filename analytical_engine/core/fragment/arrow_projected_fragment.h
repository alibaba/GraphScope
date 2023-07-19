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

#include <cassert>
#include <cstddef>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_primitive.h"

#include "grape/fragment/fragment_base.h"
#include "grape/graph/adj_list.h"
#include "grape/types.h"
#include "grape/utils/vertex_array.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/common/util/config.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/fragment/property_graph_types.h"

#include "core/config.h"
#include "core/fragment/arrow_projected_fragment_base.h"  // IWYU pragma: export
#include "core/vertex_map/arrow_projected_vertex_map.h"
#include "proto/types.pb.h"

namespace arrow {
class Array;
}

namespace gs {

namespace arrow_projected_fragment_impl {
template <typename T>
class TypedArray {
 public:
  using value_type = T;

  TypedArray() : buffer_(NULL), length_(0) {}

  TypedArray(const T* _buffer, size_t length)
      : buffer_(_buffer), length_(length) {}

  explicit TypedArray(std::shared_ptr<arrow::Array> array) {
    if (array == nullptr) {
      buffer_ = NULL;
      length_ = 0;
    } else {
      buffer_ = std::dynamic_pointer_cast<
                    typename vineyard::ConvertToArrowType<T>::ArrayType>(array)
                    ->raw_values();
      length_ = array->length();
    }
  }

  void Init(std::shared_ptr<arrow::Array> array) {
    if (array == nullptr) {
      buffer_ = NULL;
      length_ = 0;
    } else {
      buffer_ = std::dynamic_pointer_cast<
                    typename vineyard::ConvertToArrowType<T>::ArrayType>(array)
                    ->raw_values();
      length_ = array->length();
    }
  }

  void Init(vineyard::Array<T>& array) {
    buffer_ = array.data();
    length_ = array.size();
  }

  value_type operator[](size_t loc) const { return buffer_[loc]; }

  size_t GetLength() const { return length_; }

 private:
  const T* buffer_;
  size_t length_;
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
  using value_type = vineyard::arrow_string_view;
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

  size_t GetLength() const { return array_ == NULL ? 0 : array_->length(); }

  char* GetRawData() {
    return reinterpret_cast<char*>(const_cast<uint8_t*>(array_->raw_data()));
  }

  size_t GetRawDataLength() {
    int64_t arr_length = array_->length();
    return array_->value_offset(arr_length);
  }

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
  mutable const nbr_unit_t* nbr_;
  TypedArray<EDATA_T> edata_array_;
};

template <typename VID_T, typename EID_T, typename EDATA_T>
class CompactNbr {
  using vid_t = VID_T;
  using eid_t = EID_T;
  using nbr_unit_t = vineyard::property_graph_utils::NbrUnit<VID_T, EID_T>;

 public:
  CompactNbr(const uint8_t* nbr, const size_t offset, const size_t size,
             TypedArray<EDATA_T> edata_array)
      : nbr_(nbr),
        next_(nbr),
        size_(size),
        edata_array_(edata_array),
        current_(0) {
    decode();

    // move the pointer to the correct offset after first decode
    for (size_t i = 0; i < offset % batch_size; ++i) {
      ++(*this);
    }
  }

  CompactNbr(const CompactNbr& rhs)
      : nbr_(rhs.nbr_),
        next_(rhs.next),
        size_(rhs.size_),
        edata_array_(rhs.edata_array_),
        data_(rhs.data_),
        current_(rhs.current_) {}

  grape::Vertex<vid_t> neighbor() const {
    return grape::Vertex<vid_t>(data_[current_ % batch_size].vid);
  }

  grape::Vertex<vid_t> get_neighbor() const {
    return grape::Vertex<vid_t>(data_[current_ % batch_size].vid);
  }

  eid_t edge_id() const { return data_[current_ % batch_size].eid; }

  typename TypedArray<EDATA_T>::value_type data() const {
    return edata_array_[data_[current_ % batch_size].eid];
  }

  typename TypedArray<EDATA_T>::value_type get_data() const {
    return edata_array_[data_[current_ % batch_size].eid];
  }

  inline const CompactNbr& operator++() const {
    VID_T prev_vid = data_[current_ % batch_size].vid;
    current_ += 1;
    decode();
    data_[current_ % batch_size].vid += prev_vid;
    return *this;
  }

  inline CompactNbr operator++(int) const {
    CompactNbr ret(*this);
    ++(*this);
    return ret;
  }

  inline bool operator==(const CompactNbr& rhs) const {
    return nbr_ == rhs.nbr_;
  }

  inline bool operator!=(const CompactNbr& rhs) const {
    return nbr_ != rhs.nbr_;
  }

  inline bool operator<(const CompactNbr& rhs) const { return nbr_ < rhs.nbr_; }

  inline const CompactNbr& operator*() const { return *this; }

  inline const CompactNbr* operator->() const { return this; }

 private:
  inline void decode() const {
    if (likely((current_ % batch_size != 0) || current_ >= size_)) {
      if (unlikely(current_ == size_)) {
        nbr_ = next_;
      }
      return;
    }
    nbr_ = next_;
    size_t n =
        (current_ + batch_size) < size_ ? batch_size : (size_ - current_);
    next_ = v8dec32(const_cast<unsigned char*>(
                        reinterpret_cast<const unsigned char*>(next_)),
                    n * element_size, reinterpret_cast<uint32_t*>(data_));
  }

  static constexpr size_t element_size = sizeof(nbr_unit_t) / sizeof(uint32_t);
  static constexpr size_t batch_size = VARINT_ENCODING_BATCH_SIZE;
  mutable const uint8_t *nbr_, *next_ = nullptr;
  mutable size_t size_;
  TypedArray<EDATA_T> edata_array_;

  mutable nbr_unit_t data_[batch_size];
  mutable size_t current_ = 0;
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

template <typename VID_T, typename EID_T>
class CompactNbr<VID_T, EID_T, grape::EmptyType> {
  using vid_t = VID_T;
  using eid_t = EID_T;
  using nbr_unit_t = vineyard::property_graph_utils::NbrUnit<VID_T, EID_T>;

 public:
  explicit CompactNbr(const nbr_unit_t* nbr, const size_t offset,
                      const size_t size)
      : nbr_(nbr), next_(nbr), size_(size), current_(0) {
    decode();

    // move the pointer to the correct offset after first decode
    for (size_t i = 0; i < offset % batch_size; ++i) {
      ++(*this);
    }
  }

  CompactNbr(const CompactNbr& rhs)
      : nbr_(rhs.nbr_),
        next_(rhs.next),
        size_(rhs.size_),
        data_(rhs.data_),
        current_(rhs.current_) {}

  grape::Vertex<vid_t> neighbor() const {
    return grape::Vertex<vid_t>(data_[current_ % batch_size].vid);
  }

  grape::Vertex<vid_t> get_neighbor() const {
    return grape::Vertex<vid_t>(data_[current_ % batch_size].vid);
  }

  eid_t edge_id() const { return data_[current_ % batch_size].eid; }

  grape::EmptyType data() const { return grape::EmptyType(); }

  grape::EmptyType get_data() const { return grape::EmptyType(); }

  inline const CompactNbr& operator++() const {
    VID_T prev_vid = data_[current_ % batch_size].vid;
    current_ += 1;
    decode();
    data_[current_ % batch_size].vid += prev_vid;
    return *this;
  }

  inline CompactNbr operator++(int) const {
    CompactNbr ret(*this);
    ++ret;
    return ret;
  }

  inline bool operator==(const CompactNbr& rhs) const {
    return nbr_ == rhs.nbr_;
  }
  inline bool operator!=(const CompactNbr& rhs) const {
    return nbr_ != rhs.nbr_;
  }

  inline bool operator<(const CompactNbr& rhs) const { return nbr_ < rhs.nbr_; }

  inline const CompactNbr& operator*() const { return *this; }

  inline const CompactNbr* operator->() const { return this; }

 private:
  inline void decode() const {
    if (likely((current_ % batch_size != 0) || current_ >= size_)) {
      if (unlikely(current_ == size_)) {
        nbr_ = next_;
      }
      return;
    }
    nbr_ = next_;
    size_t n =
        (current_ + batch_size) < size_ ? batch_size : (size_ - current_);
    next_ = v8dec32(const_cast<unsigned char*>(
                        reinterpret_cast<const unsigned char*>(next_)),
                    n * element_size, reinterpret_cast<uint32_t*>(data_));
  }

  static constexpr size_t element_size = sizeof(nbr_unit_t) / sizeof(uint32_t);
  static constexpr size_t batch_size = VARINT_ENCODING_BATCH_SIZE;
  mutable const uint8_t *nbr_, *next_ = nullptr;
  mutable size_t size_;

  mutable nbr_unit_t data_[batch_size];
  mutable size_t current_ = 0;
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

template <typename VID_T, typename EID_T, typename EDATA_T>
class CompactAdjList {
  using vid_t = VID_T;
  using eid_t = EID_T;
  using nbr_unit_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;

 public:
  CompactAdjList() : begin_(NULL), end_(NULL), offset_(0), size_(0) {}

  CompactAdjList(const CompactAdjList& nbrs)
      : begin_(nbrs.begin_),
        end_(nbrs.end_),
        offset_(nbrs.offset_),
        size_(nbrs.size_),
        edata_array_(nbrs.edata_array_) {}

  CompactAdjList(CompactAdjList&& nbrs)
      : begin_(nbrs.begin_),
        end_(nbrs.end_),
        offset_(nbrs.offset_),
        size_(nbrs.size_),
        edata_array_(nbrs.edata_array_) {}

  CompactAdjList(const uint8_t* begin, const uint8_t* end, const size_t offset,
                 const size_t size, TypedArray<EDATA_T> edata_array)
      : begin_(begin),
        end_(end),
        offset_(offset),
        size_(size),
        edata_array_(edata_array) {}

  CompactAdjList& operator=(const CompactAdjList& rhs) {
    begin_ = rhs.begin_;
    end_ = rhs.end_;
    offset_ = rhs.offset_;
    size_ = rhs.size_;
    edata_array_ = rhs.edata_array_;
    return *this;
  }

  CompactAdjList& operator=(CompactAdjList&& rhs) {
    begin_ = rhs.begin_;
    end_ = rhs.end_;
    offset_ = rhs.offset_;
    size_ = rhs.size_;
    edata_array_ = rhs.edata_array_;
    return *this;
  }

  CompactNbr<VID_T, EID_T, EDATA_T> begin() const {
    return CompactNbr<VID_T, EID_T, EDATA_T>(begin_, offset_, size_,
                                             edata_array_);
  }

  CompactNbr<VID_T, EID_T, EDATA_T> end() const {
    return CompactNbr<VID_T, EID_T, EDATA_T>(end_, offset_, 0, edata_array_);
  }

  size_t Size() const { return size_; }

  size_t Offset() const { return offset_; }

  inline bool Empty() const { return end_ == begin_; }

  inline bool NotEmpty() const { return !Empty(); }

 private:
  const uint8_t* begin_;
  const uint8_t* end_;
  size_t offset_ = 0, size_ = 0;

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

template <typename VID_T, typename EID_T>
class CompactAdjList<VID_T, EID_T, grape::EmptyType> {
  using vid_t = VID_T;
  using eid_t = EID_T;
  using nbr_unit_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;

 public:
  CompactAdjList() : begin_(NULL), end_(NULL), offset_(0), size_(0) {}

  CompactAdjList(const CompactAdjList& nbrs)
      : begin_(nbrs.begin_),
        end_(nbrs.end_),
        offset_(nbrs.offset_),
        size_(nbrs.size_) {}

  CompactAdjList(CompactAdjList&& nbrs)
      : begin_(nbrs.begin_),
        end_(nbrs.end_),
        offset_(nbrs.offset_),
        size_(nbrs.size_) {}

  CompactAdjList(const uint8_t* begin, const uint8_t* end, const size_t offset,
                 const size_t size, TypedArray<grape::EmptyType>)
      : begin_(begin), end_(end), offset_(offset), size_(size) {}

  CompactAdjList& operator=(const CompactAdjList& rhs) {
    begin_ = rhs.begin_;
    end_ = rhs.end_;
    offset_ = rhs.offset_;
    size_ = rhs.size_;
    return *this;
  }

  CompactAdjList& operator=(CompactAdjList&& rhs) {
    begin_ = rhs.begin_;
    end_ = rhs.end_;
    offset_ = rhs.offset_;
    size_ = rhs.size_;
    return *this;
  }

  CompactNbr<VID_T, EID_T, grape::EmptyType> begin() const {
    return CompactNbr<VID_T, EID_T, grape::EmptyType>(begin_, offset_, size_);
  }

  CompactNbr<VID_T, EID_T, grape::EmptyType> end() const {
    return CompactNbr<VID_T, EID_T, grape::EmptyType>(end_, offset_, 0);
  }

  size_t Size() const { return size_; }

  size_t Offset() const { return offset_; }

  inline bool Empty() const { return end_ == begin_; }

  inline bool NotEmpty() const { return !Empty(); }

 private:
  const uint8_t* begin_;
  const uint8_t* end_;
  size_t offset_ = 0, size_ = 0;
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
template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T,
          typename VERTEX_MAP_T = vineyard::ArrowVertexMap<
              typename vineyard::InternalType<OID_T>::type, VID_T>,
          bool COMPACT = false>
class ArrowProjectedFragment
    : public ArrowProjectedFragmentBase,
      public vineyard::BareRegistered<ArrowProjectedFragment<
          OID_T, VID_T, VDATA_T, EDATA_T, VERTEX_MAP_T, COMPACT>> {
 public:
  using oid_t = OID_T;
  using vid_t = VID_T;
  using internal_oid_t = typename vineyard::InternalType<oid_t>::type;
  using eid_t = vineyard::property_graph_types::EID_TYPE;
  using vertex_range_t = grape::VertexRange<vid_t>;
  using inner_vertices_t = vertex_range_t;
  using outer_vertices_t = vertex_range_t;
  using vertices_t = vertex_range_t;
  using sub_vertices_t = vertex_range_t;

  using vertex_t = grape::Vertex<vid_t>;
  using nbr_t = arrow_projected_fragment_impl::Nbr<vid_t, eid_t, EDATA_T>;
  using compact_nbr_t =
      arrow_projected_fragment_impl::CompactNbr<vid_t, eid_t, EDATA_T>;
  using nbr_unit_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;
  using adj_list_t =
      arrow_projected_fragment_impl::AdjList<vid_t, eid_t, EDATA_T>;
  using compact_adj_list_t =
      arrow_projected_fragment_impl::CompactAdjList<vid_t, eid_t, EDATA_T>;
  using const_adj_list_t = adj_list_t;
  using const_compact_adj_list_t = compact_adj_list_t;
  using property_vertex_map_t = VERTEX_MAP_T;
  using vertex_map_t =
      ArrowProjectedVertexMap<internal_oid_t, vid_t, property_vertex_map_t>;
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;
  using prop_id_t = vineyard::property_graph_types::PROP_ID_TYPE;
  using vdata_t = VDATA_T;
  using edata_t = EDATA_T;
  using property_graph_t =
      vineyard::ArrowFragment<oid_t, vid_t, property_vertex_map_t, COMPACT>;

  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using eid_array_t = typename vineyard::ConvertToArrowType<eid_t>::ArrayType;

  template <typename DATA_T>
  using vertex_array_t = grape::VertexArray<vertices_t, DATA_T>;

  template <typename DATA_T>
  using inner_vertex_array_t = grape::VertexArray<inner_vertices_t, DATA_T>;

  template <typename DATA_T>
  using outer_vertex_array_t = grape::VertexArray<outer_vertices_t, DATA_T>;

  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

#if defined(VINEYARD_VERSION) && defined(VINEYARD_VERSION_MAJOR)
#if VINEYARD_VERSION >= 2007
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<ArrowProjectedFragment<oid_t, vid_t, vdata_t, edata_t,
                                               property_vertex_map_t, COMPACT>>{
            new ArrowProjectedFragment<oid_t, vid_t, vdata_t, edata_t,
                                       property_vertex_map_t, COMPACT>()});
  }
#endif
#else
  static std::shared_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::make_shared<ArrowProjectedFragment<oid_t, vid_t, vdata_t, edata_t,
                                                property_vertex_map_t>>());
  }
#endif

  ~ArrowProjectedFragment() {}

  static std::shared_ptr<ArrowProjectedFragment<oid_t, vid_t, vdata_t, edata_t,
                                                property_vertex_map_t, COMPACT>>
  Project(
      std::shared_ptr<
          vineyard::ArrowFragment<oid_t, vid_t, property_vertex_map_t, COMPACT>>
          fragment,
      const label_id_t& v_label, const prop_id_t& v_prop,
      const label_id_t& e_label, const prop_id_t& e_prop) {
    vineyard::Client& client =
        *dynamic_cast<vineyard::Client*>(fragment->meta().GetClient());
    std::shared_ptr<vertex_map_t> vm =
        vertex_map_t::Project(fragment->vm_ptr_, v_label);
    vineyard::ObjectMeta meta;
    if (v_prop == -1) {
      if (!std::is_same<vdata_t, grape::EmptyType>::value) {
        LOG(ERROR) << "Vertex data type of projected fragment is not "
                      "consistent with property, expect "
                   << type_name<grape::EmptyType>() << ", got "
                   << type_name<vdata_t>();
        return nullptr;
      }
    } else if (v_prop < 0 ||
               static_cast<size_t>(v_prop) >=
                   fragment->vertex_tables_[v_label]->num_columns()) {
      LOG(ERROR) << "v_prop " << v_prop << " is out of range";
      return nullptr;
    } else {
      auto prop_type = fragment->vertex_tables_[v_label]->field(v_prop)->type();
      auto vdata_type = vineyard::ConvertToArrowType<vdata_t>::TypeValue();
      if (!prop_type->Equals(vdata_type)) {
        LOG(ERROR) << "Vertex data type of projected fragment is not "
                      "consistent with property, expect "
                   << prop_type->ToString() << ", got "
                   << vdata_type->ToString();
        return nullptr;
      }
    }
    if (e_prop == -1) {
      if (!std::is_same<edata_t, grape::EmptyType>::value) {
        LOG(ERROR) << "Edge data type of projected fragment is not "
                      "consistent with property, expect "
                   << type_name<grape::EmptyType>() << ", got "
                   << type_name<edata_t>();
        return nullptr;
      }
    } else if (e_prop < 0 ||
               static_cast<size_t>(e_prop) >=
                   fragment->edge_tables_[e_label]->num_columns()) {
      LOG(ERROR) << "e_prop " << e_prop << " is out of range";
      return nullptr;
    } else {
      auto prop_type = fragment->edge_tables_[e_label]->field(e_prop)->type();
      auto edata_type = vineyard::ConvertToArrowType<edata_t>::TypeValue();
      if (!prop_type->Equals(edata_type)) {
        LOG(ERROR) << "Edge data type of projected fragment is not "
                      "consistent with property, expect "
                   << prop_type->ToString() << ", got "
                   << edata_type->ToString();
        return nullptr;
      }
    }

    meta.SetTypeName(
        type_name<ArrowProjectedFragment<oid_t, vid_t, vdata_t, edata_t,
                                         property_vertex_map_t, COMPACT>>());

    meta.AddKeyValue("projected_v_label", v_label);
    meta.AddKeyValue("projected_v_property", v_prop);
    meta.AddKeyValue("projected_e_label", e_label);
    meta.AddKeyValue("projected_e_property", e_prop);

    meta.AddMember("arrow_fragment", fragment->meta());
    meta.AddMember("arrow_projected_vertex_map", vm->meta());
    meta.AddKeyValue("vertex_label_num_", 1);
    meta.AddKeyValue("edge_label_num_", 1);

    std::shared_ptr<vineyard::NumericArray<int64_t>> ie_offsets_begin,
        ie_offsets_end;
    std::shared_ptr<vineyard::NumericArray<int64_t>> ie_boffsets_begin,
        ie_boffsets_end;

    size_t nbytes = 0;
    if (fragment->directed()) {
      vineyard::FixedInt64Builder ie_offsets_begin_builder(
          client, fragment->tvnums_[v_label]);
      vineyard::FixedInt64Builder ie_offsets_end_builder(
          client, fragment->tvnums_[v_label]);
      std::shared_ptr<vineyard::FixedInt64Builder> ie_boffsets_begin_builder;
      std::shared_ptr<vineyard::FixedInt64Builder> ie_boffsets_end_builder;
      if (COMPACT) {
        ie_boffsets_begin_builder =
            std::make_shared<vineyard::FixedInt64Builder>(
                client, fragment->tvnums_[v_label]);
        ie_boffsets_end_builder = std::make_shared<vineyard::FixedInt64Builder>(
            client, fragment->tvnums_[v_label]);
        selectEdgeByNeighborLabel(
            fragment, v_label,
            fragment->compact_ie_lists_[v_label][e_label]->GetArray(),
            fragment->ie_offsets_lists_[v_label][e_label]->GetArray(),
            fragment->ie_boffsets_lists_[v_label][e_label]->GetArray(),
            ie_offsets_begin_builder.data(), ie_offsets_end_builder.data(),
            ie_boffsets_begin_builder->data(), ie_boffsets_end_builder->data());
      } else {
        selectEdgeByNeighborLabel(
            fragment, v_label,
            fragment->ie_lists_[v_label][e_label]->GetArray(),
            fragment->ie_offsets_lists_[v_label][e_label]->GetArray(),
            ie_offsets_begin_builder.data(), ie_offsets_end_builder.data());
      }

      ie_offsets_begin =
          std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
              ie_offsets_begin_builder.Seal(client));
      ie_offsets_end =
          std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
              ie_offsets_end_builder.Seal(client));
      nbytes += ie_offsets_begin->nbytes();
      nbytes += ie_offsets_end->nbytes();
      if (COMPACT) {
        ie_boffsets_begin =
            std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
                ie_boffsets_begin_builder->Seal(client));
        ie_boffsets_end =
            std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
                ie_boffsets_end_builder->Seal(client));
        nbytes += ie_boffsets_begin->nbytes();
        nbytes += ie_boffsets_end->nbytes();
      }
    }

    std::shared_ptr<vineyard::NumericArray<int64_t>> oe_offsets_begin,
        oe_offsets_end;
    std::shared_ptr<vineyard::NumericArray<int64_t>> oe_boffsets_begin,
        oe_boffsets_end;
    {
      vineyard::FixedInt64Builder oe_offsets_begin_builder(
          client, fragment->tvnums_[v_label]);
      vineyard::FixedInt64Builder oe_offsets_end_builder(
          client, fragment->tvnums_[v_label]);
      std::shared_ptr<vineyard::FixedInt64Builder> oe_boffsets_begin_builder;
      std::shared_ptr<vineyard::FixedInt64Builder> oe_boffsets_end_builder;
      if (COMPACT) {
        oe_boffsets_begin_builder =
            std::make_shared<vineyard::FixedInt64Builder>(
                client, fragment->tvnums_[v_label]);
        oe_boffsets_end_builder = std::make_shared<vineyard::FixedInt64Builder>(
            client, fragment->tvnums_[v_label]);
        selectEdgeByNeighborLabel(
            fragment, v_label,
            fragment->compact_oe_lists_[v_label][e_label]->GetArray(),
            fragment->oe_offsets_lists_[v_label][e_label]->GetArray(),
            fragment->oe_boffsets_lists_[v_label][e_label]->GetArray(),
            oe_offsets_begin_builder.data(), oe_offsets_end_builder.data(),
            oe_boffsets_begin_builder->data(), oe_boffsets_end_builder->data());
      } else {
        selectEdgeByNeighborLabel(
            fragment, v_label,
            fragment->oe_lists_[v_label][e_label]->GetArray(),
            fragment->oe_offsets_lists_[v_label][e_label]->GetArray(),
            oe_offsets_begin_builder.data(), oe_offsets_end_builder.data());
      }

      oe_offsets_begin =
          std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
              oe_offsets_begin_builder.Seal(client));
      oe_offsets_end =
          std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
              oe_offsets_end_builder.Seal(client));
      nbytes += oe_offsets_begin->nbytes();
      nbytes += oe_offsets_end->nbytes();
      if (COMPACT) {
        oe_boffsets_begin =
            std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
                oe_boffsets_begin_builder->Seal(client));
        oe_boffsets_end =
            std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
                oe_boffsets_end_builder->Seal(client));
        nbytes += oe_boffsets_begin->nbytes();
        nbytes += oe_boffsets_end->nbytes();
      }
    }

    if (fragment->directed()) {
      meta.AddMember("ie_offsets_begin", ie_offsets_begin->meta());
      meta.AddMember("ie_offsets_end", ie_offsets_end->meta());
      meta.AddMember("ie_offsets_base",
                     fragment->ie_offsets_lists_[v_label][e_label]->meta());
      if (COMPACT) {
        meta.AddMember("ie_boffsets_begin", ie_boffsets_begin->meta());
        meta.AddMember("ie_boffsets_end", ie_boffsets_end->meta());
      }
    }
    meta.AddMember("oe_offsets_begin", oe_offsets_begin->meta());
    meta.AddMember("oe_offsets_end", oe_offsets_end->meta());
    meta.AddMember("oe_offsets_base",
                   fragment->oe_offsets_lists_[v_label][e_label]->meta());
    if (COMPACT) {
      meta.AddMember("oe_boffsets_begin", oe_boffsets_begin->meta());
      meta.AddMember("oe_boffsets_end", oe_boffsets_end->meta());
    }

    meta.SetNBytes(nbytes);

    vineyard::ObjectID id;
    VINEYARD_CHECK_OK(client.CreateMetaData(meta, id));

    return std::dynamic_pointer_cast<ArrowProjectedFragment<
        oid_t, vid_t, vdata_t, edata_t, property_vertex_map_t, COMPACT>>(
        client.GetObject(id));
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();

    vertex_label_ = meta.GetKeyValue<label_id_t>("projected_v_label");
    edge_label_ = meta.GetKeyValue<label_id_t>("projected_e_label");
    vertex_prop_ = meta.GetKeyValue<prop_id_t>("projected_v_property");
    edge_prop_ = meta.GetKeyValue<prop_id_t>("projected_e_property");

    fragment_ = std::make_shared<vineyard::ArrowFragment<
        oid_t, vid_t, property_vertex_map_t, COMPACT>>();
    fragment_->Construct(meta.GetMemberMeta("arrow_fragment"));

    fid_ = fragment_->fid_;
    fnum_ = fragment_->fnum_;
    directed_ = fragment_->directed_;

    if (directed_) {
      vineyard::NumericArray<int64_t> ie_offsets_begin, ie_offsets_end,
          ie_offsets_base;
      ie_offsets_begin.Construct(meta.GetMemberMeta("ie_offsets_begin"));
      ie_offsets_begin_ = ie_offsets_begin.GetArray();
      ie_offsets_end.Construct(meta.GetMemberMeta("ie_offsets_end"));
      ie_offsets_end_ = ie_offsets_end.GetArray();
      ie_offsets_base.Construct(meta.GetMemberMeta("ie_offsets_base"));
      ie_offsets_base_ = ie_offsets_base.GetArray();
      if (COMPACT) {
        vineyard::NumericArray<int64_t> ie_boffsets_begin, ie_boffsets_end;
        ie_boffsets_begin.Construct(meta.GetMemberMeta("ie_boffsets_begin"));
        ie_boffsets_begin_ = ie_boffsets_begin.GetArray();
        ie_boffsets_end.Construct(meta.GetMemberMeta("ie_boffsets_end"));
        ie_boffsets_end_ = ie_boffsets_end.GetArray();
      }
    }

    vineyard::NumericArray<int64_t> oe_offsets_begin, oe_offsets_end,
        oe_offsets_base;
    oe_offsets_begin.Construct(meta.GetMemberMeta("oe_offsets_begin"));
    oe_offsets_begin_ = oe_offsets_begin.GetArray();
    oe_offsets_end.Construct(meta.GetMemberMeta("oe_offsets_end"));
    oe_offsets_end_ = oe_offsets_end.GetArray();
    oe_offsets_base.Construct(meta.GetMemberMeta("oe_offsets_base"));
    oe_offsets_base_ = oe_offsets_base.GetArray();
    if (COMPACT) {
      vineyard::NumericArray<int64_t> oe_boffsets_begin, oe_boffsets_end;
      oe_boffsets_begin.Construct(meta.GetMemberMeta("oe_boffsets_begin"));
      oe_boffsets_begin_ = oe_boffsets_begin.GetArray();
      oe_boffsets_end.Construct(meta.GetMemberMeta("oe_boffsets_end"));
      oe_boffsets_end_ = oe_boffsets_end.GetArray();
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

    ovgid_list_ = fragment_->ovgid_lists_[vertex_label_]->GetArray();
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

    if (COMPACT) {
      if (directed_) {
        compact_ie_ = fragment_->compact_ie_lists_[vertex_label_][edge_label_]
                          ->GetArray();
      }
      compact_oe_ =
          fragment_->compact_oe_lists_[vertex_label_][edge_label_]->GetArray();

    } else {
      if (directed_) {
        ie_ = fragment_->ie_lists_[vertex_label_][edge_label_]->GetArray();
      }
      oe_ = fragment_->oe_lists_[vertex_label_][edge_label_]->GetArray();
    }

    vm_ptr_ = std::make_shared<vertex_map_t>();
    vm_ptr_->Construct(meta.GetMemberMeta("arrow_projected_vertex_map"));

    vid_parser_.Init(fnum_, vertex_label_num_);

    initPointers();
  }

  void PrepareToRunApp(const grape::CommSpec& comm_spec,
                       grape::PrepareConf conf) {
    if (conf.message_strategy ==
        grape::MessageStrategy::kAlongEdgeToOuterVertex) {
      initDestFidList(comm_spec, true, true, iodst_, iodoffset_);
    } else if (conf.message_strategy ==
               grape::MessageStrategy::kAlongIncomingEdgeToOuterVertex) {
      initDestFidList(comm_spec, true, false, idst_, idoffset_);
    } else if (conf.message_strategy ==
               grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex) {
      initDestFidList(comm_spec, false, true, odst_, odoffset_);
    }

    initOuterVertexRanges();
    if (conf.need_mirror_info) {
      initMirrorInfo();
    }

    if (conf.need_split_edges || conf.need_split_edges_by_fragment) {
      if (COMPACT) {
        LOG(ERROR)
            << "The edge splitter cannot be built on compacted fragment.";
        return;
      }
      ie_spliters_ptr_.clear();
      oe_spliters_ptr_.clear();
      if (directed_) {
        initEdgeSpliters(comm_spec, ie_, ie_offsets_begin_, ie_offsets_end_,
                         ie_spliters_);
        initEdgeSpliters(comm_spec, oe_, oe_offsets_begin_, oe_offsets_end_,
                         oe_spliters_);
        for (auto& vec : ie_spliters_) {
          ie_spliters_ptr_.push_back(vec.data());
        }
        for (auto& vec : oe_spliters_) {
          oe_spliters_ptr_.push_back(vec.data());
        }
      } else {
        initEdgeSpliters(comm_spec, oe_, oe_offsets_begin_, oe_offsets_end_,
                         oe_spliters_);
        for (auto& vec : oe_spliters_) {
          ie_spliters_ptr_.push_back(vec.data());
          oe_spliters_ptr_.push_back(vec.data());
        }
      }
    }
  }

  inline fid_t fid() const { return fid_; }

  inline fid_t fnum() const { return fnum_; }

  inline label_id_t vertex_label() const { return vertex_label_; }

  inline label_id_t edge_label() const { return edge_label_; }

  inline prop_id_t vertex_prop_id() const { return vertex_prop_; }

  inline prop_id_t edge_prop_id() const { return edge_prop_; }

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

  inline internal_oid_t GetInternalId(const vertex_t& v) const {
    return IsInnerVertex(v) ? GetInnerVertexInternalId(v)
                            : GetOuterVertexInternalId(v);
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

  inline size_t GetInEdgeNum() const { return ienum_; }

  inline size_t GetOutEdgeNum() const { return oenum_; }

  /* Get outging edges num from this frag*/
  inline size_t GetOutgoingEdgeNum() const {
    return static_cast<size_t>(oe_offsets_end_->Value(ivnum_ - 1) -
                               oe_offsets_begin_->Value(0));
  }

  /* Get incoming edges num to this frag*/
  inline size_t GetIncomingEdgeNum() const {
    return static_cast<size_t>(ie_offsets_end_->Value(ivnum_ - 1) -
                               ie_offsets_begin_->Value(0));
  }

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
    if (vm_ptr_->GetGid(fid_, internal_oid_t(oid), gid)) {
      v.SetValue(vid_parser_.GetLid(gid));
      return true;
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
    return oid_t(GetInnerVertexInternalId(v));
  }

  inline internal_oid_t GetInnerVertexInternalId(const vertex_t& v) const {
    internal_oid_t internal_oid;
    CHECK(vm_ptr_->GetOid(
        vid_parser_.GenerateId(fid_, vid_parser_.GetLabelId(v.GetValue()),
                               vid_parser_.GetOffset(v.GetValue())),
        internal_oid));
    return internal_oid;
  }

  inline oid_t GetOuterVertexId(const vertex_t& v) const {
    return oid_t(GetOuterVertexInternalId(v));
  }

  inline internal_oid_t GetOuterVertexInternalId(const vertex_t& v) const {
    vid_t gid = GetOuterVertexGid(v);
    internal_oid_t internal_oid;
    CHECK(vm_ptr_->GetOid(gid, internal_oid));
    return internal_oid;
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

  template <bool COMPACT_ = COMPACT>
  inline typename std::enable_if<!COMPACT_, adj_list_t>::type
  GetIncomingAdjList(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return adj_list_t(&ie_ptr_[ie_offsets_begin_ptr_[offset]],
                      &ie_ptr_[ie_offsets_end_ptr_[offset]],
                      edge_data_array_accessor_);
  }

  template <bool COMPACT_ = COMPACT>
  inline typename std::enable_if<COMPACT_, compact_adj_list_t>::type
  GetIncomingAdjList(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return compact_adj_list_t(
        &compact_ie_ptr_[ie_boffsets_begin_ptr_[offset]],
        &compact_ie_ptr_[ie_boffsets_end_ptr_[offset]],
        ie_offsets_begin_ptr_[offset] - ie_offsets_base_ptr_[offset],
        ie_offsets_end_ptr_[offset] - ie_offsets_begin_ptr_[offset],
        edge_data_array_accessor_);
  }

  template <bool COMPACT_ = COMPACT>
  inline typename std::enable_if<!COMPACT_, adj_list_t>::type
  GetOutgoingAdjList(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return adj_list_t(&oe_ptr_[oe_offsets_begin_ptr_[offset]],
                      &oe_ptr_[oe_offsets_end_ptr_[offset]],
                      edge_data_array_accessor_);
  }

  template <bool COMPACT_ = COMPACT>
  inline typename std::enable_if<COMPACT_, compact_adj_list_t>::type
  GetOutgoingAdjList(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return compact_adj_list_t(
        &compact_oe_ptr_[oe_boffsets_begin_ptr_[offset]],
        &compact_oe_ptr_[oe_boffsets_end_ptr_[offset]],
        oe_offsets_begin_ptr_[offset] - oe_offsets_base_ptr_[offset],
        oe_offsets_end_ptr_[offset] - oe_offsets_begin_ptr_[offset],
        edge_data_array_accessor_);
  }

  template <bool COMPACT_ = COMPACT>
  inline typename std::enable_if<!COMPACT_, adj_list_t>::type
  GetIncomingInnerVertexAdjList(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return adj_list_t(&ie_ptr_[ie_offsets_begin_ptr_[offset]],
                      &ie_ptr_[offset < static_cast<int64_t>(ivnum_)
                                   ? ie_spliters_ptr_[0][offset]
                                   : ie_offsets_end_ptr_[offset]],
                      edge_data_array_accessor_);
  }

  template <bool COMPACT_ = COMPACT>
  inline typename std::enable_if<!COMPACT_, adj_list_t>::type
  GetOutgoingInnerVertexAdjList(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return adj_list_t(&oe_ptr_[oe_offsets_begin_ptr_[offset]],
                      &oe_ptr_[offset < static_cast<int64_t>(ivnum_)
                                   ? oe_spliters_ptr_[0][offset]
                                   : oe_offsets_end_ptr_[offset]],
                      edge_data_array_accessor_);
  }

  template <bool COMPACT_ = COMPACT>
  inline typename std::enable_if<!COMPACT_, adj_list_t>::type
  GetIncomingOuterVertexAdjList(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return offset < static_cast<int64_t>(ivnum_)
               ? adj_list_t(&ie_ptr_[ie_spliters_ptr_[0][offset]],
                            &ie_ptr_[ie_offsets_end_ptr_[offset]],
                            edge_data_array_accessor_)
               : adj_list_t();
  }

  template <bool COMPACT_ = COMPACT>
  inline typename std::enable_if<!COMPACT_, adj_list_t>::type
  GetOutgoingOuterVertexAdjList(const vertex_t& v) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return offset < static_cast<int64_t>(ivnum_)
               ? adj_list_t(&oe_ptr_[oe_spliters_ptr_[0][offset]],
                            &oe_ptr_[oe_offsets_end_ptr_[offset]],
                            edge_data_array_accessor_)
               : adj_list_t();
  }

  template <bool COMPACT_ = COMPACT>
  inline typename std::enable_if<!COMPACT_, adj_list_t>::type
  GetIncomingAdjList(const vertex_t& v, fid_t src_fid) const {
    int64_t offset = vid_parser_.GetOffset(v.GetValue());
    return offset < static_cast<int64_t>(ivnum_)
               ? adj_list_t(&ie_ptr_[ie_spliters_ptr_[src_fid][offset]],
                            &ie_ptr_[ie_spliters_ptr_[src_fid + 1][offset]],
                            edge_data_array_accessor_)
               : (src_fid == fid_ ? GetIncomingAdjList(v) : adj_list_t());
  }

  template <bool COMPACT_ = COMPACT>
  inline typename std::enable_if<!COMPACT_, adj_list_t>::type
  GetOutgoingAdjList(const vertex_t& v, fid_t dst_fid) const {
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

  inline std::shared_ptr<vertex_map_t> GetVertexMap() { return vm_ptr_; }

  inline const std::shared_ptr<vertex_map_t> GetVertexMap() const {
    return vm_ptr_;
  }

  inline bool directed() const { return directed_; }

  template <bool COMPACT_ = COMPACT>
  inline typename std::enable_if<!COMPACT_, const nbr_unit_t*>::type
  get_out_edges_ptr() const {
    return oe_ptr_;
  }

  template <bool COMPACT_ = COMPACT>
  inline typename std::enable_if<!COMPACT_, const nbr_unit_t*>::type
  get_in_edges_ptr() const {
    return ie_ptr_;
  }

  inline const int64_t* get_oe_offsets_begin_ptr() const {
    return oe_offsets_begin_ptr_;
  }

  inline const int64_t* get_oe_offsets_end_ptr() const {
    return oe_offsets_end_ptr_;
  }

  inline const int64_t* get_ie_offsets_begin_ptr() const {
    return ie_offsets_begin_ptr_;
  }
  inline const int64_t* get_ie_offsets_end_ptr() const {
    return ie_offsets_end_ptr_;
  }

  inline arrow_projected_fragment_impl::TypedArray<EDATA_T>&
  get_edata_array_accessor() {
    return edge_data_array_accessor_;
  }

  inline arrow_projected_fragment_impl::TypedArray<VDATA_T>&
  get_vdata_array_accessor() {
    return vertex_data_array_accessor_;
  }

  std::shared_ptr<
      vineyard::ArrowFragment<oid_t, vid_t, property_vertex_map_t, COMPACT>>
  get_arrow_fragment() {
    return fragment_;
  }

  vineyard::ObjectID vertex_map_id() const {
    return fragment_->vertex_map_id();
  }

  bool local_vertex_map() const { return fragment_->local_vertex_map(); }

  bool compact_edges() const { return fragment_->compact_edges(); }

  bool use_perfect_hash() const { return vm_ptr_->use_perfect_hash(); }

 private:
  /**
   * @brief For edges (indicated by nbr_list[begin:end)) of a given vertex,
   *        range of destinations with vertex label `v_label`.
   *
   * For CSRs that compacted with varint and delta encoding, bisect is not
   * applicable and a sequential scan is required.
   *
   * The bisect version implements the same logic with the sequential one, but
   * leverage the information that the AdjList is sorted by local id for each
   * vertices.
   *
   * N.B.: use ref to avoid the shared_ptr copy
   */
  inline static std::pair<int64_t, int64_t> getRangeOfLabel(
      const std::shared_ptr<property_graph_t>& fragment, label_id_t v_label,
      const std::shared_ptr<arrow::FixedSizeBinaryArray>& nbr_list,
      int64_t begin, int64_t end) {
    auto& id_parser = fragment->vid_parser_;

    int64_t i = 0, j = 0;
    const nbr_unit_t* nbrs =
        reinterpret_cast<const nbr_unit_t*>(nbr_list->raw_values());
    const nbr_unit_t* left = nbrs + begin;
    const nbr_unit_t* right = nbrs + end;

    // find the beginning
    //
    // https://en.cppreference.com/w/cpp/algorithm/lower_bound
    {
      const nbr_unit_t *first = left, *last = right;
      size_t count = last - first, step = 0;
      const nbr_unit_t* iter = nullptr;
      while (count > 0) {
        step = count / 2;
        iter = first + step;
        if (id_parser.GetLabelId(iter->vid) < v_label) {
          first = ++iter;
          count -= step + 1;
        } else {
          count = step;
        }
      }
      i = first - left + begin;
    }
    {
      // find the end
      // https://en.cppreference.com/w/cpp/algorithm/upper_bound
      const nbr_unit_t *first = left, *last = right;
      size_t count = last - first, step = 0;
      const nbr_unit_t* iter = nullptr;
      while (count > 0) {
        step = count / 2;
        iter = first + step;
        if (id_parser.GetLabelId(iter->vid) <= v_label) {
          first = ++iter;
          count -= step + 1;
        } else {
          count = step;
        }
      }
      j = first - left + begin;
    }
    return std::make_pair(i, j);
  }

  inline static std::pair<std::pair<int64_t, int64_t>,
                          std::pair<int64_t, int64_t>>
  getRangeOfLabel(const std::shared_ptr<property_graph_t>& fragment,
                  label_id_t v_label,
                  const std::shared_ptr<arrow::UInt8Array>& nbr_list,
                  int64_t begin, int64_t end, int64_t bbegin, int64_t bend) {
    // pre condition: the nbr list is not empty
    assert(begin < end && bbegin < bend);

    auto& id_parser = fragment->vid_parser_;
    static constexpr size_t element_size =
        sizeof(nbr_unit_t) / sizeof(uint32_t);
    static constexpr int64_t batch_size = VARINT_ENCODING_BATCH_SIZE;
    nbr_unit_t data_[batch_size];  // NOLINT(runtime/arrays)
    VID_T prev_vid_ = 0;

    int64_t i = end, j = begin, bi = bbegin, bj = bend;
    const uint8_t* nbrs = nbr_list->raw_values();
    const uint8_t* prev_nbrs = nbrs + bbegin;

    for (int64_t k = begin; k < end; k += batch_size) {
      size_t n = (k + batch_size) < end ? batch_size : (end - k);
      nbrs = v8dec32(const_cast<unsigned char*>(
                         reinterpret_cast<const unsigned char*>(prev_nbrs)),
                     n * element_size, reinterpret_cast<uint32_t*>(data_));
      // we cannot skip as we need delta decoding to understand the value of vid
      for (size_t m = 0; m <= n; ++m) {
        VID_T vid = data_[m].vid + prev_vid_;
        prev_vid_ = vid;
        if (i == end) {
          label_id_t label = id_parser.GetLabelId(vid);
          if (label == v_label) {
            i = k + m;
            // points to the begin of this batch
            bi = prev_nbrs - nbr_list->raw_values();
          }
        }
        if (i != end && j == begin) {
          label_id_t label = id_parser.GetLabelId(data_[m].vid);
          if (label != v_label) {
            j = k + m;
            // points to the end of this batch
            bj = nbrs - nbr_list->raw_values();
            break;
          }
        }
        if (j != begin) {
          break;
        }
      }
      if (j != begin) {
        break;
      }
      prev_nbrs = nbrs;  // move to th next batch
    }
    if (j == begin) {  // reach the end of this nbr list.
      j = end;
    }
    return std::make_pair(std::make_pair(i, j), std::make_pair(bi, bj));
  }

  /**
   * @brief For each vertex `v` in fragment, select the range of edges
   *        with destination that has label `v_label` in CSR.
   */
  static boost::leaf::result<void> selectEdgeByNeighborLabel(
      const std::shared_ptr<property_graph_t>& fragment, label_id_t v_label,
      const std::shared_ptr<arrow::FixedSizeBinaryArray>& nbr_list,
      const std::shared_ptr<arrow::Int64Array>& offsets, int64_t* begins,
      int64_t* ends) {
    const int64_t* offset_values = offsets->raw_values();
    vineyard::parallel_for(
        static_cast<vid_t>(0), fragment->tvnums_[v_label],
        [&](vid_t i) {
          int64_t begin = offset_values[i], end = offset_values[i + 1];
          if (begin == end) {  // optimize for vertices that has no edge.
            begins[i] = begin;
            ends[i] = end;
          } else {
            auto range =
                getRangeOfLabel(fragment, v_label, nbr_list, begin, end);
            begins[i] = range.first;
            ends[i] = range.second;
          }
        },
        std::thread::hardware_concurrency(), 1024);
    return {};
  }

  static boost::leaf::result<void> selectEdgeByNeighborLabel(
      const std::shared_ptr<property_graph_t>& fragment, label_id_t v_label,
      const std::shared_ptr<arrow::UInt8Array>& nbr_list,
      const std::shared_ptr<arrow::Int64Array>& offsets,
      const std::shared_ptr<arrow::Int64Array>& boffsets, int64_t* begins,
      int64_t* ends, int64_t* bbegins, int64_t* bends) {
    const int64_t* offset_values = offsets->raw_values();
    const int64_t* boffset_values = boffsets->raw_values();
    vineyard::parallel_for(
        static_cast<vid_t>(0), fragment->tvnums_[v_label],
        [&](vid_t i) {
          int64_t begin = offset_values[i], end = offset_values[i + 1];
          int64_t bbegin = boffset_values[i], bend = boffset_values[i + 1];
          if (begin == end) {  // optimize for vertices that has no edge.
            begins[i] = begin;
            ends[i] = end;
            bbegins[i] = bbegin;
            bends[i] = bend;
          } else {
            auto range = getRangeOfLabel(fragment, v_label, nbr_list, begin,
                                         end, bbegin, bend);
            begins[i] = range.first.first;
            ends[i] = range.first.second;
            bbegins[i] = range.second.first;
            bends[i] = range.second.second;
          }
        },
        std::thread::hardware_concurrency(), 1024);
    return {};
  }

  void initDestFidList(const grape::CommSpec& comm_spec, const bool in_edge,
                       const bool out_edge, std::vector<fid_t>& fid_list,
                       std::vector<fid_t*>& fid_list_offset) {
    if (!fid_list_offset.empty()) {
      return;
    }
    fid_list_offset.resize(ivnum_ + 1, NULL);

    int concurrency =
        (std::thread::hardware_concurrency() + comm_spec.local_num() - 1) /
        comm_spec.local_num();

    // don't use std::vector<bool> due to its specialization
    std::vector<uint8_t> fid_list_bitmap(ivnum_ * fnum_, 0);
    std::atomic_size_t fid_list_size(0);

    vineyard::parallel_for(
        static_cast<vid_t>(0), static_cast<vid_t>(ivnum_),
        [this, in_edge, out_edge, &fid_list_bitmap,
         &fid_list_size](const vid_t& offset) {
          vertex_t v = *(inner_vertices_.begin() + offset);
          if (in_edge) {
            auto es = GetIncomingAdjList(v);
            fid_t last_fid = -1;
            for (auto& e : es) {
              fid_t f = GetFragId(e.neighbor());
              if (f != last_fid && f != fid_ &&
                  !fid_list_bitmap[offset * fnum_ + f]) {
                last_fid = f;
                fid_list_bitmap[offset * fnum_ + f] = 1;
                fid_list_size.fetch_add(1);
              }
            }
          }
          if (out_edge) {
            auto es = GetOutgoingAdjList(v);
            fid_t last_fid = -1;
            for (auto& e : es) {
              fid_t f = GetFragId(e.neighbor());
              if (f != last_fid && f != fid_ &&
                  !fid_list_bitmap[offset * fnum_ + f]) {
                last_fid = f;
                fid_list_bitmap[offset * fnum_ + f] = 1;
                fid_list_size.fetch_add(1);
              }
            }
          }
        },
        concurrency, 1024);

    fid_list.reserve(fid_list_size.load());
    fid_list_offset[0] = fid_list.data();

    for (vid_t i = 0; i < ivnum_; ++i) {
      size_t nonzero = 0;
      for (fid_t fid = 0; fid < fnum_; ++fid) {
        if (fid_list_bitmap[i * fnum_ + fid]) {
          nonzero += 1;
          fid_list.push_back(fid);
        }
      }
      fid_list_offset[i + 1] = fid_list_offset[i] + nonzero;
    }
  }

  void initDestFidListSeq(const bool in_edge, const bool out_edge,
                          std::vector<fid_t>& fid_list,
                          std::vector<fid_t*>& fid_list_offset) {
    if (!fid_list_offset.empty()) {
      return;
    }

    fid_list_offset.resize(ivnum_ + 1, NULL);

    std::set<fid_t> dstset;
    std::vector<int> id_num(ivnum_, 0);

    vertex_t v = *inner_vertices_.begin();
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

  void initEdgeSpliters(
      const grape::CommSpec& comm_spec,
      const std::shared_ptr<arrow::FixedSizeBinaryArray>& edge_list,
      const std::shared_ptr<arrow::Int64Array>& offsets_begin,
      const std::shared_ptr<arrow::Int64Array>& offsets_end,
      std::vector<std::vector<int64_t>>& spliters) {
    if (!spliters.empty()) {
      return;
    }
    spliters.resize(fnum_ + 1);
    for (auto& vec : spliters) {
      vec.resize(ivnum_);
    }

    int concurrency =
        (std::thread::hardware_concurrency() + comm_spec.local_num() - 1) /
        comm_spec.local_num();

    vineyard::parallel_for(
        static_cast<vid_t>(0), ivnum_,
        [this, &offsets_begin, &offsets_end, &edge_list,
         &spliters](const vid_t i) {
          std::vector<int> frag_count(fnum_, 0);
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
          if (begin != end) {
            LOG(ERROR) << "Unexpected edge spliters for ith vertex " << i
                       << ", begin: " << begin << " vs. end: " << end;
          }
        },
        concurrency, 1024);
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
    outer_vertex_offsets_[0] = outer_vertices_.begin_value();
    for (fid_t i = 0; i < fnum_; ++i) {
      outer_vertex_offsets_[i + 1] = outer_vertex_offsets_[i] + outer_vnum[i];
    }
    CHECK_EQ(outer_vertex_offsets_[fnum_], outer_vertices_.end_value());
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
      ie_offsets_base_ptr_ = ie_offsets_base_->raw_values();
    } else {
      ie_offsets_begin_ptr_ = oe_offsets_begin_->raw_values();
      ie_offsets_end_ptr_ = oe_offsets_end_->raw_values();
      ie_offsets_base_ptr_ = oe_offsets_base_->raw_values();
    }
    oe_offsets_begin_ptr_ = oe_offsets_begin_->raw_values();
    oe_offsets_end_ptr_ = oe_offsets_end_->raw_values();
    oe_offsets_base_ptr_ = oe_offsets_base_->raw_values();
    if (COMPACT) {
      if (directed_) {
        ie_boffsets_begin_ptr_ = ie_boffsets_begin_->raw_values();
        ie_boffsets_end_ptr_ = ie_boffsets_end_->raw_values();
      } else {
        ie_boffsets_begin_ptr_ = oe_boffsets_begin_->raw_values();
        ie_boffsets_end_ptr_ = oe_boffsets_end_->raw_values();
      }
      oe_boffsets_begin_ptr_ = oe_boffsets_begin_->raw_values();
      oe_boffsets_end_ptr_ = oe_boffsets_end_->raw_values();
    }

    vertex_data_array_accessor_.Init(vertex_data_array_);
    ovgid_list_ptr_ = ovgid_list_->raw_values();
    edge_data_array_accessor_.Init(edge_data_array_);

    if (COMPACT) {
      if (directed_) {
        compact_ie_ptr_ = compact_ie_->raw_values();
      } else {
        compact_ie_ptr_ = compact_oe_->raw_values();
      }
      compact_oe_ptr_ = compact_oe_->raw_values();
    } else {
      if (directed_) {
        ie_ptr_ = reinterpret_cast<const nbr_unit_t*>(ie_->GetValue(0));
      } else {
        ie_ptr_ = reinterpret_cast<const nbr_unit_t*>(oe_->GetValue(0));
      }
      oe_ptr_ = reinterpret_cast<const nbr_unit_t*>(oe_->GetValue(0));
    }
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

  std::shared_ptr<arrow::Int64Array> ie_offsets_begin_, ie_offsets_end_,
      ie_offsets_base_;
  const int64_t *ie_offsets_begin_ptr_, *ie_offsets_end_ptr_,
      *ie_offsets_base_ptr_;
  std::shared_ptr<arrow::Int64Array> oe_offsets_begin_, oe_offsets_end_,
      oe_offsets_base_;
  const int64_t *oe_offsets_begin_ptr_, *oe_offsets_end_ptr_,
      *oe_offsets_base_ptr_;

  std::shared_ptr<arrow::Int64Array> ie_boffsets_begin_, ie_boffsets_end_;
  const int64_t *ie_boffsets_begin_ptr_, *ie_boffsets_end_ptr_;
  std::shared_ptr<arrow::Int64Array> oe_boffsets_begin_, oe_boffsets_end_;
  const int64_t *oe_boffsets_begin_ptr_, *oe_boffsets_end_ptr_;

  std::shared_ptr<arrow::Array> vertex_data_array_;
  arrow_projected_fragment_impl::TypedArray<VDATA_T>
      vertex_data_array_accessor_;

  std::shared_ptr<vid_array_t> ovgid_list_;
  const vid_t* ovgid_list_ptr_;
  std::shared_ptr<vineyard::Hashmap<vid_t, vid_t>> ovg2l_map_;

  std::shared_ptr<arrow::Array> edge_data_array_;
  arrow_projected_fragment_impl::TypedArray<EDATA_T> edge_data_array_accessor_;

  std::shared_ptr<arrow::FixedSizeBinaryArray> ie_, oe_;
  const nbr_unit_t *ie_ptr_, *oe_ptr_;

  std::shared_ptr<arrow::UInt8Array> compact_ie_, compact_oe_;
  const uint8_t *compact_ie_ptr_, *compact_oe_ptr_;

  std::shared_ptr<vertex_map_t> vm_ptr_;

  vineyard::IdParser<vid_t> vid_parser_;

  std::shared_ptr<
      vineyard::ArrowFragment<oid_t, vid_t, property_vertex_map_t, COMPACT>>
      fragment_;

  std::vector<fid_t> idst_, odst_, iodst_;
  std::vector<fid_t*> idoffset_, odoffset_, iodoffset_;

  std::vector<std::vector<int64_t>> ie_spliters_, oe_spliters_;
  std::vector<const int64_t*> ie_spliters_ptr_, oe_spliters_ptr_;

  std::vector<vid_t> outer_vertex_offsets_;
  std::vector<std::vector<vertex_t>> mirrors_of_frag_;

  template <typename _OID_T, typename _VID_T, typename _NEW_VDATA_T,
            typename _NEW_EDATA_T>
  friend class ArrowProjectedFragmentMapper;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_PROJECTED_FRAGMENT_H_

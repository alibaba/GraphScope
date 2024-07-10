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
#ifndef ENGINES_HQPS_DATABASE_ADJ_LIST_H_
#define ENGINES_HQPS_DATABASE_ADJ_LIST_H_

#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "flex/engines/hqps_db/core/null_record.h"
#include "flex/storages/rt_mutable_graph/csr/mutable_csr.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/utils/property/types.h"

namespace gs {

namespace mutable_csr_graph_impl {

/**
 * @brief Adj is a simple struct to store a edge from a view of csr. Here we use
 * a variadic template to store multiple properties of the edge.
 * @tparam T The type of the property.
 */
template <typename T>
class Adj {
 public:
  Adj() = default;
  ~Adj() = default;

  Adj(const Adj<T>& other) : neighbor_(other.neighbor_), prop_(other.prop_) {}

  Adj(Adj<T>&& other)
      : neighbor_(other.neighbor_), prop_(std::move(other.prop_)) {}

  inline Adj<T>& operator=(const Adj<T>& from) {
    this->neighbor_ = from.neighbor_;
    this->prop_ = from.prop_;
    return *this;
  }

  vid_t neighbor() const { return neighbor_; }
  const std::tuple<T>& properties() const { return prop_; }

  vid_t neighbor_;
  std::tuple<T> prop_;
};

///////////////////////////// AdjList //////////////////////////////

/**
 * @brief AdjList is a simple class to store a list of edges from a view of csr.
 * Here we use a variadic template to store multiple properties of the edge.
 * @tparam T The type of the property.
 */
template <typename T>
class AdjList {
  using nbr_t = MutableNbr<T>;
  class Iterator {
   public:
    using edge_property_t = std::tuple<T>;
    Iterator()
        : cur_(),
          begin0_(nullptr),
          begin1_(nullptr),
          end0_(nullptr),
          end1_(nullptr) {}
    Iterator(const nbr_t* begin0, const nbr_t* end0, const nbr_t* begin1,
             const nbr_t* end1)
        : cur_(), begin0_(begin0), begin1_(begin1), end0_(end0), end1_(end1) {
      probe_for_next();
    }

    void probe_for_next() {
      if (begin0_ != NULL && begin0_ != end0_) {
        cur_.neighbor_ = begin0_->neighbor;
        std::get<0>(cur_.prop_) = begin0_->data;
        return;
      }
      if (begin1_ != end1_ && begin1_ != NULL) {
        cur_.neighbor_ = begin1_->neighbor;
        std::get<0>(cur_.prop_) = begin1_->data;
        return;
      }
    }

    bool valid() const { return begin0_ != end0_ || begin1_ != end1_; }
    const Adj<T>& operator*() const { return cur_; }
    const Adj<T>* operator->() const { return &cur_; }

    vid_t neighbor() const { return cur_.neighbor(); }
    const std::tuple<T>& properties() const { return cur_.properties(); }

    std::string to_string() const {
      std::stringstream ss;
      ss << "(neighbor: " << cur_.neighbor_
         << ", prop: " << std::get<0>(cur_.prop_) << ")";
      return ss.str();
    }

    Iterator& operator++() {
      if (begin0_ < end0_) {
        ++begin0_;
      } else if (begin1_ < end1_) {
        ++begin1_;
      } else {
        return *this;
      }
      probe_for_next();
      return *this;
    }
    Iterator operator++(int) {
      Iterator tmp = *this;
      ++(*this);
      return tmp;
    }

    bool operator==(const Iterator& rhs) const {
      return begin0_ == rhs.begin0_ && begin1_ == rhs.begin1_;
    }

    bool operator!=(const Iterator& rhs) const {
      return begin1_ != rhs.begin1_ || begin0_ != rhs.begin0_;
    }
    inline Iterator& operator=(const Iterator& from) {
      this->cur_ = from.cur_;
      this->begin0_ = from.begin0_;
      this->end0_ = from.end0_;
      this->begin1_ = from.begin1_;
      this->end1_ = from.end1_;
      return *this;
    }

   private:
    Adj<T> cur_;
    const nbr_t *begin0_, *begin1_;
    const nbr_t *end0_, *end1_;
  };

 public:
  using slice_t = MutableNbrSlice<T>;
  using iterator = Iterator;
  AdjList() = default;
  // copy constructor
  AdjList(const AdjList<T>& adj_list)
      : slice0_(adj_list.slice0_), slice1_(adj_list.slice1_) {}
  // with single slice provided.
  AdjList(const slice_t& slice0) : slice0_(slice0), slice1_() {}
  AdjList(const slice_t& slice0, const slice_t& slice1)
      : slice0_(slice0), slice1_(slice1) {}

  AdjList(AdjList<T>&& adj_list)
      : slice0_(std::move(adj_list.slice0_)),
        slice1_(std::move(adj_list.slice1_)) {}

  AdjList(AdjList<T>& adj_list)
      : slice0_(adj_list.slice0_), slice1_(adj_list.slice1_) {}

  Iterator begin() const {
    return Iterator(slice0_.begin(), slice0_.end(), slice1_.begin(),
                    slice1_.end());
  }

  Iterator end() const {
    return Iterator(slice0_.end(), slice0_.end(), slice1_.end(), slice1_.end());
  }
  size_t size() const { return slice0_.size() + slice1_.size(); }

  AdjList<T>& operator=(const AdjList<T>& other) {
    slice0_ = other.slice0_;
    slice1_ = other.slice1_;
    return *this;
  }

  const slice_t& slice0() const { return slice0_; }
  const slice_t& slice1() const { return slice1_; }

 private:
  slice_t slice0_, slice1_;
};

/**
 * @brief Stores a list of AdjLists, each of which represents the edges of a
 * vertex.
 * @tparam T The type of the property.
 */
template <typename T>
class AdjListArray {
 public:
  using slice_t = MutableNbrSlice<T>;
  AdjListArray(std::vector<std::pair<slice_t, slice_t>>&& slices)
      : slices_(std::move(slices)) {}

  void resize(size_t new_size) { slices_.resize(new_size); }

  void set(size_t i, const AdjList<T>& slice) {
    slices_[i] = std::make_pair(slice.slice0(), slice.slice1());
  }

  AdjListArray(AdjListArray<T>&& adj_list)
      : slices_(std::move(adj_list.slices_)) {}

  size_t size() const { return slices_.size(); }

  AdjList<T> get(size_t i) const {
    return AdjList<T>(slices_[i].first, slices_[i].second);
  }

  void swap(AdjListArray<T>& adj_list) { this->slices_.swap(adj_list.slices_); }

 private:
  std::vector<std::pair<slice_t, slice_t>> slices_;
};

////////////////////////Util functions///////////////////////////

template <size_t I, typename EDATA_T>
void iterate_and_set_slices(
    std::vector<std::pair<MutableNbrSlice<EDATA_T>, MutableNbrSlice<EDATA_T>>>&
        slices,
    const CsrBase* csr, const std::vector<vid_t>& vids) {
  static_assert(I < 2, "I should be 0 or 1");
  // The CsrBase has multiple derived classes, and we need to cast it to the
  // correct one.
  if (csr) {
    using typed_csr_base_t = MutableCsr<EDATA_T>;
    using single_typed_csr_base_t = SingleMutableCsr<EDATA_T>;
    const typed_csr_base_t* casted_csr =
        dynamic_cast<const typed_csr_base_t*>(csr);
    if (casted_csr) {
      for (size_t i = 0; i < vids.size(); ++i) {
        std::get<I>(slices[i]) = casted_csr->get_edges(vids[i]);
      }
    } else {
      LOG(WARNING) << "cast to MutableCSR failed, try single csr";
      const single_typed_csr_base_t* casted_single_csr =
          dynamic_cast<const single_typed_csr_base_t*>(csr);
      if (casted_single_csr) {
        for (size_t i = 0; i < vids.size(); ++i) {
          std::get<I>(slices[i]) = casted_single_csr->get_edges(vids[i]);
        }
      } else {
        LOG(WARNING) << "Cannot cast to MutableCSR or SingleMutableCSR";
      }
    }
  } else {
    LOG(WARNING) << "No such edge, since csr is null";
  }
}

// A helper function to create an AdjListArray<> from two MutableCsr.
//
template <size_t I>
void iterate_and_set_iterators(
    std::vector<std::pair<std::shared_ptr<CsrConstEdgeIterBase>,
                          std::shared_ptr<CsrConstEdgeIterBase>>>& slices,
    const CsrBase* csr, const std::vector<vid_t>& vids) {
  static_assert(I < 2, "I should be 0 or 1");
  if (csr) {
    for (size_t i = 0; i < vids.size(); ++i) {
      std::get<0>(slices[i]) = csr->edge_iter(vids[i]);
    }
  } else {
    LOG(WARNING) << "No such edge, since csr is null";
  }
}

template <typename EDATA_T>
AdjListArray<EDATA_T> create_adj_list_array(const CsrBase* csr0,
                                            const CsrBase* csr1,
                                            const std::vector<vid_t>& vids) {
  using slice_t = MutableNbrSlice<EDATA_T>;
  std::vector<std::pair<slice_t, slice_t>> slices;
  slices.resize(vids.size());
  iterate_and_set_slices<0, EDATA_T>(slices, csr0, vids);
  iterate_and_set_slices<1, EDATA_T>(slices, csr1, vids);
  return AdjListArray<EDATA_T>(std::move(slices));
}

template <typename EDATA_T>
AdjListArray<EDATA_T> create_adj_list_array(const CsrBase* csr,
                                            const std::vector<vid_t>& vids) {
  return create_adj_list_array<EDATA_T>(csr, nullptr, vids);
}

}  // namespace mutable_csr_graph_impl
}  // namespace gs

#endif  // ENGINES_HQPS_DATABASE_ADJ_LIST_H_
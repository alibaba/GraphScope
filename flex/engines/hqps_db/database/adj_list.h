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
#include "flex/utils/property/types.h"

namespace gs {

namespace mutable_csr_graph_impl {

template <typename LabelT>
// Base interface for edge iterator
class EdgeIter {
 public:
  using label_id_t = LabelT;
  EdgeIter() : label_triplet_(), ptr1_(nullptr), prop_names_(nullptr) {}

  EdgeIter(const EdgeIter& other)
      : label_triplet_(other.label_triplet_),
        ptr1_(other.ptr1_),
        prop_names_(other.prop_names_) {}
  EdgeIter(const std::array<LabelT, 3>& label_triplet,
           std::shared_ptr<CsrConstEdgeIterBase> ptr,
           const std::vector<std::string>* prop_names)
      : label_triplet_(label_triplet), ptr1_(ptr), prop_names_(prop_names) {}

  inline void Next() const { ptr1_->next(); }
  inline vid_t GetDstId() const { return ptr1_->get_neighbor(); }

  inline label_id_t GetDstLabel() const { return label_triplet_[1]; }

  inline label_id_t GetSrcLabel() const { return label_triplet_[0]; }

  inline Any GetData() const { return ptr1_->get_data(); }
  inline bool IsValid() const { return ptr1_ && ptr1_->is_valid(); }

  const std::vector<std::string>& GetPropNames() const { return *prop_names_; }

  EdgeIter<LabelT>& operator=(const EdgeIter<LabelT>& rhs) {
    this->ptr1_ = rhs.ptr1_;
    this->label_triplet_ = rhs.label_triplet_;
    this->prop_names_ = rhs.prop_names_;
    return *this;
  }

  size_t Size() const {
    if (ptr1_) {
      return ptr1_->size();
    }
    return 0;
  }

 private:
  std::array<LabelT, 3> label_triplet_;
  std::shared_ptr<CsrConstEdgeIterBase> ptr1_;
  const std::vector<std::string>* prop_names_;
};

// A subGraph is a view of a simple graph, with one src label and one dst label.
// Could be empty.
template <typename LabelT, typename VID_T>
class SubGraph {
 public:
  using iterator = EdgeIter<LabelT>;
  using label_id_t = LabelT;
  SubGraph(const CsrBase* first, const std::array<label_id_t, 3>& label_triplet,
           const std::vector<std::string>& prop_names)
      : first_(first), label_triplet_(label_triplet), prop_names_(prop_names) {}

  inline iterator get_edges(VID_T vid) const {
    if (first_) {
      return iterator(label_triplet_, first_->edge_iter(vid), &prop_names_);
    }
    return iterator(label_triplet_, nullptr, &prop_names_);
  }

  // here the src, dst, refer the src, dst of the csr, not the direction.
  label_id_t GetSrcLabel() const { return label_triplet_[0]; }
  label_id_t GetEdgeLabel() const { return label_triplet_[2]; }
  label_id_t GetDstLabel() const { return label_triplet_[1]; }

  const std::vector<std::string>& GetPropNames() const { return prop_names_; }

 private:
  const CsrBase* first_;
  // We assume first is out edge, second is in edge.
  std::array<label_id_t, 3> label_triplet_;
  std::vector<std::string> prop_names_;
};

template <typename T>
class SinglePropGetter {
 public:
  using value_type = T;
  static constexpr size_t prop_num = 1;
  SinglePropGetter() {}
  SinglePropGetter(std::shared_ptr<TypedRefColumn<T>> c) : column(c) {}

  inline value_type get_view(vid_t vid) const {
    if (vid == NONE || column == nullptr) {
      return NullRecordCreator<value_type>::GetNull();
    }
    return column->get_view(vid);
  }

  inline SinglePropGetter<T>& operator=(const SinglePropGetter<T>& d) {
    column = d.column;
    return *this;
  }

 private:
  std::shared_ptr<TypedRefColumn<T>> column;
};

// Property Getter hold the handle of the property column.

template <typename... T>
class MultiPropGetter {
 public:
  using column_tuple_t = std::tuple<std::shared_ptr<TypedRefColumn<T>>...>;
  using result_tuple_t = std::tuple<T...>;
  static constexpr size_t prop_num = sizeof...(T);
  MultiPropGetter() {}
  MultiPropGetter(column_tuple_t c) : column(c) {}

  inline result_tuple_t get_view(vid_t vid) const {
    if (vid == NONE) {
      return NullRecordCreator<result_tuple_t>::GetNull();
    }
    result_tuple_t ret;
    fill_result_tuple(ret, vid);
    return ret;
  }

  template <size_t I = 0>
  inline typename std::enable_if<I == sizeof...(T), void>::type
  fill_result_tuple(result_tuple_t& ret, vid_t vid) const {}

  template <size_t I = 0>
  inline typename std::enable_if<(I < sizeof...(T)), void>::type
  fill_result_tuple(result_tuple_t& ret, vid_t vid) const {
    using cur_ele_t = typename std::tuple_element<I, result_tuple_t>::type;
    if (std::get<I>(column) == nullptr) {
      std::get<I>(ret) = NullRecordCreator<cur_ele_t>::GetNull();
    } else {
      std::get<I>(ret) = std::get<I>(column)->get_view(vid);
    }
    fill_result_tuple<I + 1>(ret, vid);
  }

  inline MultiPropGetter<T...>& operator=(const MultiPropGetter<T...>& d) {
    column = d.column;
    return *this;
  }

 private:
  column_tuple_t column;
};

template <typename... T>
class Adj {};

template <typename T>
class Adj<T> {
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

template <>
class Adj<grape::EmptyType> {
 public:
  Adj() = default;
  ~Adj() = default;

  Adj(const Adj<grape::EmptyType>& other)
      : neighbor_(other.neighbor_), prop_(other.prop_) {}
  Adj(Adj<grape::EmptyType>&& other)
      : neighbor_(other.neighbor_), prop_(std::move(other.prop_)) {}

  inline Adj<grape::EmptyType>& operator=(const Adj<grape::EmptyType>& from) {
    this->neighbor_ = from.neighbor_;
    this->prop_ = from.prop_;
    return *this;
  }

  vid_t neighbor() const { return neighbor_; }
  const std::tuple<grape::EmptyType>& properties() const { return prop_; }

  vid_t neighbor_;
  std::tuple<grape::EmptyType> prop_;
};

template <typename... T>
class AdjList {};

template <typename T>
class AdjList<T> {
  using nbr_t = MutableNbr<T>;
  class Iterator {
   public:
    Iterator()
        : cur_(),
          begin0_(nullptr),
          begin1_(nullptr),
          end0_(nullptr),
          end1_(nullptr) {}
    Iterator(const nbr_t* begin0, const nbr_t* end0, const nbr_t* begin1,
             const nbr_t* end1)
        : cur_(), begin0_(begin0), begin1_(begin1), end0_(end0), end1_(end1) {
      // probe for next;
      probe_for_next();
    }

    void probe_for_next() {
      if (begin0_ != end0_ && begin0_ != NULL) {
        cur_.neighbor_ = begin0_->neighbor;
        std::get<0>(cur_.prop_) = begin0_->data;
        return;
      }
      // ptr= null is ok, since fast fail on neq

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

template <>
class AdjList<grape::EmptyType> {
  using nbr_t = MutableNbr<grape::EmptyType>;
  class Iterator {
   public:
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
      if (begin0_ != end0_ && begin0_ != NULL) {
        cur_.neighbor_ = begin0_->neighbor;
        return;
      }
      // ptr= null is ok, since fast fail on neq

      if (begin1_ != end1_ && begin1_ != NULL) {
        cur_.neighbor_ = begin1_->neighbor;
        return;
      }
    }

    vid_t neighbor() const { return cur_.neighbor(); }

    const Adj<grape::EmptyType>& operator*() const { return cur_; }
    const Adj<grape::EmptyType>* operator->() const { return &cur_; }

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
    inline Iterator& operator=(const Iterator& from) {
      this->cur_ = from.cur_;
      this->begin0_ = from.begin0_;
      this->end0_ = from.end0_;
      this->begin1_ = from.begin1_;
      this->end1_ = from.end1_;
      return *this;
    }
    bool operator==(const Iterator& rhs) const {
      return begin0_ == rhs.begin0_ && begin1_ == rhs.begin1_;
    }
    bool operator!=(const Iterator& rhs) const {
      return begin1_ != rhs.begin1_ || begin0_ != rhs.begin0_;
    }

   private:
    Adj<grape::EmptyType> cur_;
    const nbr_t *begin0_, *begin1_;
    const nbr_t *end0_, *end1_;
  };

 public:
  using iterator = Iterator;
  using slice_t = MutableNbrSlice<grape::EmptyType>;
  AdjList() = default;
  AdjList(const slice_t& slice) : slice0_(slice), slice1_() {}

  AdjList(const slice_t& slice0, const slice_t& slice1)
      : slice0_(slice0), slice1_(slice1) {}

  AdjList(AdjList<grape::EmptyType>&& adj_list)
      : slice0_(std::move(adj_list.slice0_)),
        slice1_(std::move(adj_list.slice1_)) {}

  AdjList(const AdjList<grape::EmptyType>& adj_list)
      : slice0_(adj_list.slice0_), slice1_(adj_list.slice1_) {}

  Iterator begin() const {
    return Iterator(slice0_.begin(), slice0_.end(), slice1_.begin(),
                    slice1_.end());
  }
  Iterator end() const {
    return Iterator(slice0_.end(), slice0_.end(), slice1_.end(), slice1_.end());
  }
  size_t size() const { return slice0_.size() + slice1_.size(); }

  AdjList<grape::EmptyType>& operator=(const AdjList<grape::EmptyType>& other) {
    slice0_ = other.slice0_;
    slice1_ = other.slice1_;
    return *this;
  }

  // slice0_ getter
  const slice_t& slice0() const { return slice0_; }
  // slice1_ getter
  const slice_t& slice1() const { return slice1_; }

 private:
  slice_t slice0_, slice1_;
};

template <typename... T>
class AdjListArray {};

template <typename T>
class AdjListArray<T> {
 public:
  using csr_base_t = CsrBase;
  using typed_csr_base_t = MutableCsr<T>;
  using single_typed_csr_base_t = SingleMutableCsr<T>;
  using slice_t = MutableNbrSlice<T>;
  AdjListArray() = default;
  AdjListArray(const csr_base_t* csr, const std::vector<vid_t>& vids)
      : flag_(false) {
    slices_.reserve(vids.size());
    if (csr) {
      const typed_csr_base_t* casted_csr =
          dynamic_cast<const typed_csr_base_t*>(csr);
      for (auto v : vids) {
        slices_.emplace_back(
            std::make_pair(casted_csr->get_edges(v), slice_t()));
      }
    } else {
      LOG(WARNING) << "cast to MutableCSR failed, try single csr";
      const single_typed_csr_base_t* casted_single_csr =
          dynamic_cast<const single_typed_csr_base_t*>(csr);
      if (casted_single_csr) {
        for (auto v : vids) {
          slices_.emplace_back(
              std::make_pair(casted_single_csr->get_edges(v), slice_t()));
        }
      } else {
        LOG(WARNING) << "No such edge, since csr is null";
        for (size_t i = 0; i < vids.size(); ++i) {
          slices_.emplace_back(std::make_pair(slice_t(), slice_t()));
        }
      }
    }
  }
  AdjListArray(const csr_base_t* csr0, const csr_base_t* csr1,
               const std::vector<vid_t>& vids)
      : flag_(true) {
    slices_.reserve(vids.size());

    const typed_csr_base_t* casted_csr0 =
        dynamic_cast<const typed_csr_base_t*>(csr0);
    const typed_csr_base_t* casted_csr1 =
        dynamic_cast<const typed_csr_base_t*>(csr1);
    for (auto v : vids) {
      if (casted_csr0 && casted_csr1) {
        slices_.emplace_back(std::make_pair(casted_csr0->get_edges(v),
                                            casted_csr1->get_edges(v)));
      } else if (casted_csr0 && !casted_csr1) {
        slices_.emplace_back(
            std::make_pair(casted_csr0->get_edges(v), slice_t()));
      } else if (!casted_csr0 && casted_csr1) {
        slices_.emplace_back(
            std::make_pair(slice_t(), casted_csr1->get_edges(v)));
      } else {
        slices_.emplace_back(std::make_pair(slice_t(), slice_t()));
      }
    }
  }

  void resize(size_t new_size) { slices_.resize(new_size); }

  void set(size_t i, const AdjList<T>& slice) {
    slices_[i] = std::make_pair(slice.slice0(), slice.slice1());
  }

  AdjListArray(AdjListArray<T>&& adj_list)
      : slices_(std::move(adj_list.slices_)), flag_(adj_list.flag_) {}

  size_t size() const { return slices_.size(); }

  bool get_flag() const { return flag_; }
  void set_flag(bool flag) { flag_ = flag; }

  AdjList<T> get(size_t i) const {
    if (flag_) {
      return AdjList<T>(slices_[i].first, slices_[i].second);
    } else {
      return AdjList<T>(slices_[i].first);
    }
  }

  void swap(AdjListArray<T>& adj_list) {
    this->slices_.swap(adj_list.slices_);
    bool tmp_flag = flag_;
    flag_ = adj_list.flag_;
    adj_list.flag_ = tmp_flag;
  }

 private:
  std::vector<std::pair<slice_t, slice_t>> slices_;
  bool flag_;
};

template <>
class AdjListArray<grape::EmptyType> {
 public:
  // MutableCSR.
  using csr_base_t = CsrBase;
  using typed_csr_base_t = MutableCsr<grape::EmptyType>;
  using single_typed_csr_base_t = SingleMutableCsr<grape::EmptyType>;
  using slice_t = MutableNbrSlice<grape::EmptyType>;
  AdjListArray() = default;
  AdjListArray(const csr_base_t* csr, const std::vector<vid_t>& vids)
      : flag_(false) {
    if (!csr) {
      LOG(ERROR) << "csr is null before cast ";
    }
    slices_.reserve(vids.size());
    const typed_csr_base_t* casted_csr =
        dynamic_cast<const typed_csr_base_t*>(csr);
    if (casted_csr) {
      for (auto v : vids) {
        slices_.emplace_back(
            std::make_pair(casted_csr->get_edges(v), slice_t()));
      }
    } else {
      VLOG(10) << "casted to MutableCSR Failed, try single csr";
      const single_typed_csr_base_t* casted_single_csr =
          dynamic_cast<const single_typed_csr_base_t*>(csr);
      if (casted_single_csr) {
        for (auto v : vids) {
          slices_.emplace_back(
              std::make_pair(casted_single_csr->get_edges(v), slice_t()));
        }
      } else {
        LOG(WARNING) << "No such edge, since csr is null";
        for (size_t i = 0; i < vids.size(); ++i) {
          slices_.emplace_back(std::make_pair(slice_t(), slice_t()));
        }
      }
    }
  }

  AdjListArray(const csr_base_t* csr0, const csr_base_t* csr1,
               const std::vector<vid_t>& vids)
      : flag_(true) {
    slices_.reserve(vids.size());
    const typed_csr_base_t* casted_csr0 =
        dynamic_cast<const typed_csr_base_t*>(csr0);
    const typed_csr_base_t* casted_csr1 =
        dynamic_cast<const typed_csr_base_t*>(csr1);

    for (auto v : vids) {
      if (casted_csr0 && casted_csr1) {
        slices_.emplace_back(std::make_pair(casted_csr0->get_edges(v),
                                            casted_csr1->get_edges(v)));
      } else if (casted_csr0 && !casted_csr1) {
        slices_.emplace_back(
            std::make_pair(casted_csr0->get_edges(v), slice_t()));
      } else if (!casted_csr0 && casted_csr1) {
        slices_.emplace_back(
            std::make_pair(slice_t(), casted_csr1->get_edges(v)));
      } else {
        slices_.emplace_back(std::make_pair(slice_t(), slice_t()));
      }
    }
  }
  // move constructor
  AdjListArray(AdjListArray<grape::EmptyType>&& adj_list)
      : slices_(std::move(adj_list.slices_)), flag_(adj_list.flag_) {}

  size_t size() const { return slices_.size(); }

  void resize(size_t new_size) { slices_.resize(new_size); }

  bool get_flag() const { return flag_; }
  void set_flag(bool flag) { flag_ = flag; }

  void set(size_t i, const AdjList<grape::EmptyType>& slice) {
    slices_[i] = std::make_pair(slice.slice0(), slice.slice1());
  }

  AdjList<grape::EmptyType> get(size_t i) const {
    if (flag_) {
      return AdjList<grape::EmptyType>(slices_[i].first, slices_[i].second);
    } else {
      return AdjList<grape::EmptyType>(slices_[i].first);
    }
  }

  void swap(AdjListArray<grape::EmptyType>& adj_list) {
    this->slices_.swap(adj_list.slices_);
    bool tmp_flag = flag_;
    flag_ = adj_list.flag_;
    adj_list.flag_ = tmp_flag;
  }

 private:
  std::vector<std::pair<slice_t, slice_t>> slices_;
  bool flag_;
};

class Nbr {
 public:
  Nbr() = default;
  explicit Nbr(vid_t neighbor) : neighbor_(neighbor) {}
  ~Nbr() = default;

  inline vid_t neighbor() const { return neighbor_; }

 private:
  vid_t neighbor_;
};

class NbrList {
 public:
  NbrList(const Nbr* b, const Nbr* e) : begin_(b), end_(e) {}
  NbrList() : begin_(nullptr), end_(nullptr) {}
  ~NbrList() = default;

  const Nbr* begin() const { return begin_; }
  const Nbr* end() const { return end_; }
  inline size_t size() const { return end_ - begin_; }

 private:
  const Nbr* begin_;
  const Nbr* end_;
};

class NbrListArray {
 public:
  NbrListArray() {}
  ~NbrListArray() = default;

  NbrList get(size_t index) const {
    auto& list = nbr_lists_[index];
    return NbrList(list.data(), list.data() + list.size());
  }

  void put(std::vector<Nbr>&& list) { nbr_lists_.push_back(std::move(list)); }

  size_t size() const { return nbr_lists_.size(); }

  void resize(size_t size) { nbr_lists_.resize(size); }

  std::vector<Nbr>& get_vector(size_t index) { return nbr_lists_[index]; }

 private:
  std::vector<std::vector<Nbr>> nbr_lists_;
};

}  // namespace mutable_csr_graph_impl
}  // namespace gs

#endif  // ENGINES_HQPS_DATABASE_ADJ_LIST_H_
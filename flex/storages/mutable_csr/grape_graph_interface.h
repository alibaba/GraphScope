#ifndef GRAPHSCOPE_MUTABLE_CSR_GRAPE_GRAPH_INTERFACE_H_
#define GRAPHSCOPE_MUTABLE_CSR_GRAPE_GRAPH_INTERFACE_H_

#include <tuple>

#include "flex/engines/hqps/engine/params.h"
#include "flex/storages/mutable_csr/graph_db.h"
#include "flex/storages/mutable_csr/types.h"

#include "flex/engines/hqps/engine/null_record.h"
namespace gs {

namespace grape_graph_impl {

// for null vid, return null.
template <typename T>
class SinglePropGetter {
 public:
  SinglePropGetter() {}
  SinglePropGetter(std::shared_ptr<TypedRefColumn<T>> c) : column(c) {
    CHECK(column.get() != nullptr);
  }

  inline T get_view(vid_t vid) const {
    if (vid == NONE) {
      return NullRecordCreator<T>::GetNull();
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

template <typename... T>
class MultiPropGetter {
 public:
  using column_tuple_t = std::tuple<std::shared_ptr<TypedRefColumn<T>>...>;
  MultiPropGetter() {}
  MultiPropGetter(column_tuple_t c) : column(c) {}

  inline std::tuple<T...> get_view(vid_t vid) const {
    if (vid == NONE) {
      return NullRecordCreator<std::tuple<T...>>::GetNull();
    }
    return get_view(vid, std::make_index_sequence<sizeof...(T)>());
  }

  template <size_t... Is>
  inline std::tuple<T...> get_view(vid_t vid,
                                   std::index_sequence<Is...>) const {
    if (vid == NONE) {
      return NullRecordCreator<std::tuple<T...>>::GetNull();
    }
    return std::make_tuple(std::get<Is>(column)->get_view(vid)...);
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
class Adj<> {
 public:
  Adj() = default;
  ~Adj() = default;

  Adj(const Adj<>& other) : neighbor_(other.neighbor_), prop_(other.prop_) {}
  Adj(Adj<>&& other)
      : neighbor_(other.neighbor_), prop_(std::move(other.prop_)) {}

  inline Adj<>& operator=(const Adj<>& from) {
    this->neighbor_ = from.neighbor_;
    this->prop_ = from.prop_;
    return *this;
  }

  vid_t neighbor() const { return neighbor_; }
  const std::tuple<>& properties() const { return prop_; }

  vid_t neighbor_;
  std::tuple<> prop_;
};

template <typename... T>
class AdjList {};

template <typename T>
class AdjList<T> {
  class Iterator {
   public:
    Iterator()
        : cur_(),
          begin0_(nullptr),
          end0_(nullptr),
          begin1_(nullptr),
          end1_(nullptr),
          ts_(0) {}
    Iterator(const TSNbr<T>* begin0, const TSNbr<T>* end0,
             const TSNbr<T>* begin1, const TSNbr<T>* end1, timestamp_t ts)
        : cur_(),
          begin0_(begin0),
          end0_(end0),
          begin1_(begin1),
          end1_(end1),
          ts_(ts) {
      // probe for next;
      probe_for_next();
    }

    void probe_for_next() {
      while (begin0_ != end0_ && begin0_->timestamp > ts_) {
        ++begin0_;
      }
      if (begin0_ != end0_ && begin0_ != NULL) {
        cur_.neighbor_ = begin0_->neighbor;
        std::get<0>(cur_.prop_) = begin0_->data;
        return;
      }
      // ptr= null is ok, since fast fail on neq
      while (begin1_ != end1_ && begin1_->timestamp > ts_) {
        ++begin1_;
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
      this->ts_ = from.ts_;
      return *this;
    }

   private:
    Adj<T> cur_;
    const TSNbr<T>*begin0_, *begin1_;
    const TSNbr<T>*end0_, *end1_;
    timestamp_t ts_;
  };

 public:
  using slice_t = TSNbrSlice<T>;
  using iterator = Iterator;
  AdjList() = default;
  // copy constructor
  AdjList(const AdjList<T>& adj_list)
      : slice0_(adj_list.slice0_),
        slice1_(adj_list.slice1_),
        ts_(adj_list.ts_) {}
  // with sinle slice provided.
  AdjList(const TSNbrSlice<T>& slice0, timestamp_t ts)
      : slice0_(slice0), slice1_(), ts_(ts) {}
  AdjList(const TSNbrSlice<T>& slice0, const TSNbrSlice<T>& slice1,
          timestamp_t ts)
      : slice0_(slice0), slice1_(slice1), ts_(ts) {}

  AdjList(AdjList<T>&& adj_list)
      : slice0_(std::move(adj_list.slice0_)),
        slice1_(std::move(adj_list.slice1_)),
        ts_(adj_list.ts_) {}

  AdjList(AdjList<T>& adj_list)
      : slice0_(adj_list.slice0_),
        slice1_(adj_list.slice1_),
        ts_(adj_list.ts_) {}

  Iterator begin() const {
    return Iterator(slice0_.begin(), slice0_.end(), slice1_.begin(),
                    slice1_.end(), ts_);
  }
  Iterator end() const {
    return Iterator(slice0_.end(), slice0_.end(), slice1_.end(), slice1_.end(),
                    ts_);
  }
  size_t size() const { return slice0_.size() + slice1_.size(); }

  AdjList<T>& operator=(const AdjList<T>& other) {
    slice0_ = other.slice0_;
    slice1_ = other.slice1_;
    ts_ = other.ts_;
    return *this;
  }

  const TSNbrSlice<T>& slice0() const { return slice0_; }
  const TSNbrSlice<T>& slice1() const { return slice1_; }

 private:
  TSNbrSlice<T> slice0_, slice1_;
  timestamp_t ts_;
};

template <>
class AdjList<> {
  using nbr_t = TSNbr<grape::EmptyType>;
  class Iterator {
   public:
    Iterator()
        : cur_(),
          begin0_(nullptr),
          end0_(nullptr),
          begin1_(nullptr),
          end1_(nullptr),
          ts_(0) {}
    Iterator(const nbr_t* begin0, const nbr_t* end0, const nbr_t* begin1,
             const nbr_t* end1, timestamp_t ts)
        : cur_(), begin0_(begin0), end0_(end0), begin1_(begin1), end1_(end1) {
      probe_for_next();
    }

    void probe_for_next() {
      while (begin0_ != end0_ && begin0_->timestamp > ts_) {
        ++begin0_;
      }
      if (begin0_ != end0_ && begin0_ != NULL) {
        cur_.neighbor_ = begin0_->neighbor;
        return;
      }
      // ptr= null is ok, since fast fail on neq
      while (begin1_ != end1_ && begin1_->timestamp > ts_) {
        ++begin1_;
      }

      if (begin1_ != end1_ && begin1_ != NULL) {
        cur_.neighbor_ = begin1_->neighbor;
        return;
      }
    }

    vid_t neighbor() const { return cur_.neighbor(); }

    const Adj<>& operator*() const { return cur_; }
    const Adj<>* operator->() const { return &cur_; }

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
      this->ts_ = from.ts_;
      return *this;
    }
    bool operator==(const Iterator& rhs) const {
      return begin0_ == rhs.begin0_ && begin1_ == rhs.begin1_;
    }
    bool operator!=(const Iterator& rhs) const {
      return begin1_ != rhs.begin1_ || begin0_ != rhs.begin0_;
    }

   private:
    Adj<> cur_;
    const nbr_t *begin0_, *begin1_;
    const nbr_t *end0_, *end1_;
    timestamp_t ts_;
  };

 public:
  using iterator = Iterator;
  using slice_t = TSNbrSlice<grape::EmptyType>;
  AdjList() = default;
  AdjList(const slice_t& slice, timestamp_t ts)
      : slice0_(slice), slice1_(), ts_(ts) {}

  AdjList(const slice_t& slice0, const slice_t& slice1, timestamp_t ts)
      : slice0_(slice0), slice1_(slice1), ts_(ts) {}

  AdjList(AdjList<>&& adj_list)
      : slice0_(std::move(adj_list.slice0_)),
        slice1_(std::move(adj_list.slice1_)),
        ts_(adj_list.ts_) {}

  AdjList(const AdjList<>& adj_list)
      : slice0_(adj_list.slice0_),
        slice1_(adj_list.slice1_),
        ts_(adj_list.ts_) {}

  Iterator begin() const {
    return Iterator(slice0_.begin(), slice0_.end(), slice1_.begin(),
                    slice1_.end(), ts_);
  }
  Iterator end() const {
    return Iterator(slice0_.end(), slice0_.end(), slice1_.end(), slice1_.end(),
                    ts_);
  }
  size_t size() const { return slice0_.size() + slice1_.size(); }

  AdjList<>& operator=(const AdjList<>& other) {
    slice0_ = other.slice0_;
    slice1_ = other.slice1_;
    ts_ = other.ts_;
    return *this;
  }

  // slice0_ getter
  const TSNbrSlice<grape::EmptyType>& slice0() const { return slice0_; }
  // slice1_ getter
  const TSNbrSlice<grape::EmptyType>& slice1() const { return slice1_; }

 private:
  TSNbrSlice<grape::EmptyType> slice0_, slice1_;
  timestamp_t ts_;
};

template <typename... T>
class AdjListArray {};

template <typename T>
class AdjListArray<T> {
 public:
  using slice_t = TSNbrSlice<T>;
  AdjListArray() = default;
  AdjListArray(const TSCsrBase* csr, const std::vector<vid_t>& vids,
               timestamp_t ts)
      : ts_(ts), flag_(false) {
    LOG(INFO) << "before cast";
    slices_.reserve(vids.size());
    const TypedTSCsrBase<T>* casted_csr =
        dynamic_cast<const TypedTSCsrBase<T>*>(csr);
    for (auto v : vids) {
      slices_.emplace_back(
          std::make_pair(casted_csr->get_edges(v), TSNbrSlice<T>()));
    }
  }
  AdjListArray(const TSCsrBase* csr0, const TSCsrBase* csr1,
               const std::vector<vid_t>& vids, timestamp_t ts)
      : ts_(ts), flag_(true) {
    LOG(INFO) << "before cast";
    slices_.reserve(vids.size());

    const TypedTSCsrBase<T>* casted_csr0 =
        dynamic_cast<const TypedTSCsrBase<T>*>(csr0);
    const TypedTSCsrBase<T>* casted_csr1 =
        dynamic_cast<const TypedTSCsrBase<T>*>(csr1);
    for (auto v : vids) {
      slices_.emplace_back(
          std::make_pair(casted_csr0->get_edges(v), casted_csr1->get_edges(v)));
    }
  }

  void resize(size_t new_size) { slices_.resize(new_size); }

  void set(size_t i, const AdjList<T>& slice) {
    slices_[i] = std::make_pair(slice.slice0(), slice.slice1());
  }

  AdjListArray(AdjListArray<T>&& adj_list)
      : slices_(std::move(adj_list.slices_)),
        ts_(adj_list.ts_),
        flag_(adj_list.flag_) {}

  size_t size() const { return slices_.size(); }

  AdjList<T> get(size_t i) const {
    if (flag_) {
      return AdjList<T>(slices_[i].first, slices_[i].second, ts_);
    } else {
      return AdjList<T>(slices_[i].first, ts_);
    }
  }

  void swap(AdjListArray<T>& adj_list) {
    this->slices_.swap(adj_list.slices_);
    timestamp_t tmp = ts_;
    ts_ = adj_list.ts_;
    adj_list.ts_ = tmp;
    bool tmp_flag = flag_;
    flag_ = adj_list.flag_;
    adj_list.flag_ = tmp_flag;
  }

 private:
  std::vector<std::pair<slice_t, slice_t>> slices_;
  timestamp_t ts_;
  bool flag_;
};

template <>
class AdjListArray<> {
 public:
  using slice_t = TSNbrSlice<grape::EmptyType>;
  AdjListArray() = default;
  AdjListArray(const TSCsrBase* csr, const std::vector<vid_t>& vids,
               timestamp_t ts)
      : ts_(ts), flag_(false) {
    slices_.reserve(vids.size());
    const TypedTSCsrBase<grape::EmptyType>* casted_csr =
        dynamic_cast<const TypedTSCsrBase<grape::EmptyType>*>(csr);
    for (auto v : vids) {
      auto edges = casted_csr->get_edges(v);
      slices_.emplace_back(std::make_pair(casted_csr->get_edges(v), slice_t()));
    }
  }

  AdjListArray(const TSCsrBase* csr0, const TSCsrBase* csr1,
               const std::vector<vid_t>& vids, timestamp_t ts)
      : ts_(ts), flag_(true) {
    slices_.reserve(vids.size());
    const TypedTSCsrBase<grape::EmptyType>* casted_csr0 =
        dynamic_cast<const TypedTSCsrBase<grape::EmptyType>*>(csr0);
    const TypedTSCsrBase<grape::EmptyType>* casted_csr1 =
        dynamic_cast<const TypedTSCsrBase<grape::EmptyType>*>(csr1);
    LOG(INFO) << "after cast";

    for (auto v : vids) {
      slices_.emplace_back(
          std::make_pair(casted_csr0->get_edges(v), casted_csr1->get_edges(v)));
    }
  }
  // move constructor
  AdjListArray(AdjListArray<>&& adj_list)
      : slices_(std::move(adj_list.slices_)),
        ts_(adj_list.ts_),
        flag_(adj_list.flag_) {}

  size_t size() const { return slices_.size(); }

  void resize(size_t new_size) { slices_.resize(new_size); }

  void set(size_t i, const AdjList<>& slice) {
    slices_[i] = std::make_pair(slice.slice0(), slice.slice1());
  }

  AdjList<> get(size_t i) const {
    if (flag_) {
      return AdjList<>(slices_[i].first, slices_[i].second, ts_);
    } else {
      return AdjList<>(slices_[i].first, ts_);
    }
  }

  void swap(AdjListArray<>& adj_list) {
    this->slices_.swap(adj_list.slices_);
    timestamp_t tmp = ts_;
    ts_ = adj_list.ts_;
    adj_list.ts_ = tmp;
    bool tmp_flag = flag_;
    flag_ = adj_list.flag_;
    adj_list.flag_ = tmp_flag;
  }

 private:
  std::vector<std::pair<slice_t, slice_t>> slices_;
  timestamp_t ts_;
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

}  // namespace grape_graph_impl

template <size_t I = 0, typename... T>
void get_tuple_from_column_tuple(
    size_t index, std::tuple<T...>& t,
    const std::tuple<std::shared_ptr<TypedRefColumn<T>>...>& columns) {
  auto ptr = std::get<I>(columns);
  if (ptr) {
    std::get<I>(t) = ptr->get_view(index);
  }

  if constexpr (I + 1 < sizeof...(T)) {
    get_tuple_from_column_tuple<I + 1>(index, t, columns);
  }
}

template <size_t I = 0, typename... T, typename... COL_T>
void get_tuple_from_hete_column_tuple(size_t index, std::tuple<T...>& t,
                                      const std::tuple<COL_T...>& columns) {
  auto ptr = std::get<I>(columns);
  if (ptr) {
    std::get<I>(t) = ptr->get_view(index);
  }

  if constexpr (I + 1 < sizeof...(T)) {
    get_tuple_from_hete_column_tuple<I + 1>(index, t, columns);
  }
}

template <size_t I = 0, typename... T>
void get_tuple_column_from_graph(
    const GraphDB& graph, label_t label,
    const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
        prop_names,
    std::tuple<std::shared_ptr<TypedRefColumn<T>>...>& columns) {
  // TODO: support label_property
  using PT = std::tuple_element_t<I, std::tuple<T...>>;
  std::get<I>(columns) = std::dynamic_pointer_cast<TypedRefColumn<PT>>(
      graph.get_vertex_property_column_x(label, prop_names[I]));
  if (std::get<I>(columns) == nullptr) {}
  if constexpr (I + 1 < sizeof...(T)) {
    get_tuple_column_from_graph<I + 1>(graph, label, prop_names, columns);
  }
}

template <typename PropT,
          typename std::enable_if<gs::is_label_key_prop<PropT>::value>::type* =
              nullptr>
auto get_single_column_from_graph_with_property(const GraphDB& graph,
                                                label_t label,
                                                const PropT& prop) {
  auto prop_name = prop.name;
  CHECK(prop_name == "label" || prop_name == "Label" || prop_name == "LABEL");
  return std::make_shared<LabelRefColumn>(label);
}

template <typename PropT,
          typename std::enable_if<!gs::is_label_key_prop<PropT>::value>::type* =
              nullptr>
auto get_single_column_from_graph_with_property(const GraphDB& graph,
                                                label_t label,
                                                const PropT& prop) {
  return std::dynamic_pointer_cast<TypedRefColumn<typename PropT::prop_t>>(
      graph.get_vertex_property_column_x(label, prop.name));
}
template <typename... PropT, size_t... Is>
auto get_tuple_column_from_graph_with_property_impl(
    const GraphDB& graph, label_t label, const std::tuple<PropT...>& props,
    std::index_sequence<Is...>) {
  return std::make_tuple(get_single_column_from_graph_with_property(
      graph, label, std::get<Is>(props))...);
}

template <typename... PropT>
auto get_tuple_column_from_graph_with_property(
    const GraphDB& graph, label_t label, const std::tuple<PropT...>& props) {
  return get_tuple_column_from_graph_with_property_impl(
      graph, label, props, std::make_index_sequence<sizeof...(PropT)>());
}

class GrapeGraphInterface {
  GraphDB graph_;
  bool initialized_ = false;

 public:
  GraphDB& GetGraphDB() { return graph_; }

  using vertex_id_t = vid_t;
  using outer_vertex_id_t = oid_t;
  using label_id_t = uint8_t;

  using nbr_list_array_t = grape_graph_impl::NbrListArray;

  template <typename... T>
  using adj_list_array_t = grape_graph_impl::AdjListArray<T...>;

  template <typename... T>
  using adj_list_t = grape_graph_impl::AdjList<T...>;

  template <typename... T>
  using adj_t = grape_graph_impl::Adj<T...>;

  using nbr_t = grape_graph_impl::Nbr;

  using nbr_list_t = grape_graph_impl::NbrList;

  template <typename T>
  using single_prop_getter_t = grape_graph_impl::SinglePropGetter<T>;

  template <typename... T>
  using multi_prop_getter_t = grape_graph_impl::MultiPropGetter<T...>;

  static constexpr bool is_grape = true;

  static GrapeGraphInterface& get();

  void Open(const std::string& yaml_path, const std::string& data_path) {
    graph_.Init(yaml_path, data_path, 1);
    initialized_ = true;
  }

  void Open(const std::string& data_path){
    graph_.Init("", data_path, 1);
    initialized_ = true;
  }

  bool Initialized() const { return initialized_; }

  label_id_t GetVertexLabelId(const std::string& label) const {
    return graph_.schema().get_vertex_label_id(label);
  }

  label_id_t GetEdgeLabelId(const std::string& label) const {
    return graph_.schema().get_edge_label_id(label);
  }

  template <typename FUNC_T, typename... PropT>
  void ScanVertices(int64_t time_stamp, const std::string& label,
                    const std::tuple<PropT...>& props,
                    const FUNC_T& func) const {
    auto label_id = graph_.schema().get_vertex_label_id(label);
    return ScanVertices(time_stamp, label_id, props, func);
  }

  template <typename FUNC_T, typename... PropT>
  void ScanVertices(int64_t time_stamp, const label_id_t& label_id,
                    const std::tuple<PropT...>& props,
                    const FUNC_T& func) const {
    // std::tuple<std::shared_ptr<TypedRefColumn<T>>...> columns;
    // get_tuple_column_from_graph(graph_, label_id, prop_names, columns);
    auto columns =
        get_tuple_column_from_graph_with_property(graph_, label_id, props);
    auto vnum = graph_.graph().vertex_num(label_id);
    std::tuple<typename PropT::prop_t...> t;
    for (auto v = 0; v != vnum; ++v) {
      get_tuple_from_hete_column_tuple(v, t, columns);
      func(v, t);
    }
  }

  vertex_id_t ScanVerticesWithOid(int64_t time_stamp, const std::string& label,
                                  outer_vertex_id_t oid) const {
    auto label_id = graph_.schema().get_vertex_label_id(label);
    vertex_id_t vid;
    CHECK(graph_.graph().get_lid(label_id, oid, vid));
    return vid;
  }

  vertex_id_t ScanVerticesWithOid(int64_t time_stamp,
                                  const label_id_t& label_id,
                                  outer_vertex_id_t oid) const {
    vertex_id_t vid;
    CHECK(graph_.graph().get_lid(label_id, oid, vid));
    return vid;
  }

  template <typename FUNC_T>
  void ScanVerticesWithoutProperty(int64_t ts, const std::string& label,
                                   const FUNC_T& func) const {
    auto label_id = graph_.schema().get_vertex_label_id(label);
    auto vnum = graph_.graph().vertex_num(label_id);
    for (auto v = 0; v != vnum; ++v) {
      func(v);
    }
  }

  template <typename... T>
  std::pair<std::vector<vertex_id_t>, std::vector<std::tuple<T...>>>
  GetVertexPropsFromOid(
      int64_t ts, const std::string& label, const std::vector<int64_t> oids,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    auto label_id = graph_.schema().get_vertex_label_id(label);
    std::tuple<const TypedColumn<T>*...> columns;
    get_tuple_column_from_graph(graph_, label_id, prop_names, columns);
    std::vector<vertex_id_t> vids(oids.size());
    std::vector<std::tuple<T...>> props(oids.size());

    for (size_t i = 0; i < oids.size(); ++i) {
      graph_.graph().get_lid(label_id, oids[i], vids[i]);
      get_tuple_from_column_tuple(vids[i], props[i], columns);
    }

    return std::make_pair(std::move(vids), std::move(props));
  }

  template <typename... T>
  std::vector<std::tuple<T...>> GetVertexPropsFromVid(
      int64_t ts, const std::string& label,
      const std::vector<vertex_id_t>& vids,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    auto label_id = graph_.schema().get_vertex_label_id(label);
    std::tuple<std::shared_ptr<TypedRefColumn<T>>...> columns;
    get_tuple_column_from_graph(graph_, label_id, prop_names, columns);
    std::vector<std::tuple<T...>> props(vids.size());
    fetch_properties_in_column(vids, props, columns);
    return std::move(props);
  }

  template <typename... T>
  std::vector<std::tuple<T...>> GetVertexPropsFromVid(
      int64_t ts, const label_id_t& label_id,
      const std::vector<vertex_id_t>& vids,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    // auto label_id = graph_.schema().get_vertex_label_id(label);
    CHECK(label_id < graph_.schema().vertex_label_num());
    std::tuple<std::shared_ptr<TypedRefColumn<T>>...> columns;
    get_tuple_column_from_graph(graph_, label_id, prop_names, columns);
    std::vector<std::tuple<T...>> props(vids.size());
    fetch_properties_in_column(vids, props, columns);
    return std::move(props);
  }

  // Get props from multiple label of vertices.
  // NOTE: performance not good, use v2.
  template <typename... T, size_t num_labels>
  std::vector<std::tuple<T...>> GetVertexPropsFromVid(
      int64_t ts, const std::vector<vertex_id_t>& vids,
      const std::array<std::string, num_labels>& labels,
      const std::array<std::vector<int32_t>, num_labels>& vid_inds,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    std::vector<std::tuple<T...>> props(vids.size());
    std::vector<label_t> label_ids;
    for (auto label : labels) {
      label_ids.emplace_back(graph_.schema().get_vertex_label_id(label));
    }
    using column_tuple_t = std::tuple<std::shared_ptr<TypedRefColumn<T>>...>;
    std::vector<column_tuple_t> columns;
    columns.resize(label_ids.size());
    for (auto i = 0; i < label_ids.size(); ++i) {
      get_tuple_column_from_graph(graph_, label_ids[i], prop_names, columns[i]);
    }

    VLOG(10) << "start getting vertices's property";
    double t0 = -grape::GetCurrentTime();
    fetch_properties<0>(props, columns, vids, vid_inds);
    t0 += grape::GetCurrentTime();
    VLOG(10) << "Finish getting vertices's property, cost: " << t0;

    return std::move(props);
  }

  // Get props from multiple label of vertices.
  template <typename... T, size_t num_labels,
            typename std::enable_if<(num_labels == 2)>::type* = nullptr>
  std::vector<std::tuple<T...>> GetVertexPropsFromVidV2(
      int64_t ts, const std::vector<vertex_id_t>& vids,
      const std::array<std::string, num_labels>& labels, const Bitset& bitset,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    size_t total_size = vids.size();
    std::vector<std::tuple<T...>> props(total_size);
    std::vector<label_t> label_ids;
    for (auto label : labels) {
      label_ids.emplace_back(graph_.schema().get_vertex_label_id(label));
    }
    using column_tuple_t = std::tuple<std::shared_ptr<TypedRefColumn<T>>...>;
    std::vector<column_tuple_t> columns;
    columns.resize(label_ids.size());
    for (auto i = 0; i < label_ids.size(); ++i) {
      get_tuple_column_from_graph(graph_, label_ids[i], prop_names, columns[i]);
    }

    fetch_propertiesV2<0>(props, columns, vids, bitset);

    return std::move(props);
  }

  template <typename... T, size_t num_labels,
            typename std::enable_if<(num_labels == 2)>::type* = nullptr>
  std::vector<std::tuple<T...>> GetVertexPropsFromVidV2(
      int64_t ts, const std::vector<vertex_id_t>& vids,
      const std::array<label_id_t, num_labels>& labels, const Bitset& bitset,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    size_t total_size = vids.size();
    std::vector<std::tuple<T...>> props(total_size);
    std::vector<label_t> label_ids;
    for (auto label : labels) {
      CHECK(label < graph_.schema().vertex_label_num());
      label_ids.emplace_back(label);
      // label_ids.emplace_back(graph_.schema().get_vertex_label_id(label));
    }
    using column_tuple_t = std::tuple<std::shared_ptr<TypedRefColumn<T>>...>;
    std::vector<column_tuple_t> columns;
    columns.resize(label_ids.size());
    for (auto i = 0; i < label_ids.size(); ++i) {
      get_tuple_column_from_graph(graph_, label_ids[i], prop_names, columns[i]);
    }

    fetch_propertiesV2<0>(props, columns, vids, bitset);

    return std::move(props);
  }

  template <size_t Is, typename... T, typename column_tuple_t,
            typename std::enable_if<(Is < sizeof...(T))>::type* = nullptr>
  void fetch_propertiesV2(std::vector<std::tuple<T...>>& props,
                          std::vector<column_tuple_t>& columns,
                          const std::vector<vertex_id_t>& vids,
                          const Bitset& bitset) const {
    // auto index_seq = std::make_index_sequence<sizeof...(T)>{};

    {
      auto& column_tuple0 = columns[0];
      auto& column_tuple1 = columns[1];
      auto ptr0 = std::get<Is>(column_tuple0);
      auto ptr1 = std::get<Is>(column_tuple1);
      if (ptr0 && ptr1) {
        for (auto i = 0; i < vids.size(); ++i) {
          if (bitset.get_bit(i)) {
            std::get<Is>(props[i]) = ptr0->get_view(vids[i]);
          } else {
            std::get<Is>(props[i]) = ptr1->get_view(vids[i]);
          }
        }
      } else if (ptr0) {
        for (auto i = 0; i < vids.size(); ++i) {
          if (bitset.get_bit(i)) {
            std::get<Is>(props[i]) = ptr0->get_view(vids[i]);
          }
        }
      } else if (ptr1) {
        for (auto i = 0; i < vids.size(); ++i) {
          if (!bitset.get_bit(i)) {
            std::get<Is>(props[i]) = ptr1->get_view(vids[i]);
          }
        }
      } else {
        LOG(INFO) << "skip for column " << Is;
      }
    }
    fetch_propertiesV2<Is + 1>(props, columns, vids, bitset);
  }

  template <size_t Is = 0, typename... T, typename column_tuple_t>
  void fetch_properties_in_column(const std::vector<vertex_id_t>& vids,
                                  std::vector<std::tuple<T...>>& props,
                                  column_tuple_t& column) const {
    // auto index_seq = std::make_index_sequence<sizeof...(T)>{};

    auto& cur_column = std::get<Is>(column);
    if (cur_column) {
      for (auto i = 0; i < vids.size(); ++i) {
        std::get<Is>(props[i]) = cur_column->get_view(vids[i]);
      }
    }

    if constexpr (Is + 1 < sizeof...(T)) {
      fetch_properties_in_column<Is + 1>(vids, props, column);
    }
  }

  template <size_t Is, typename... T, typename column_tuple_t,
            typename std::enable_if<(Is >= sizeof...(T))>::type* = nullptr>
  void fetch_propertiesV2(std::vector<std::tuple<T...>>& props,
                          std::vector<column_tuple_t>& columns,
                          const std::vector<vertex_id_t>& vids,
                          const Bitset& bitset) const {}

  template <size_t Is, typename... T, typename column_tuple_t,
            size_t num_labels,
            typename std::enable_if<(Is < sizeof...(T))>::type* = nullptr>
  void fetch_properties(
      std::vector<std::tuple<T...>>& props,
      std::vector<column_tuple_t>& columns,
      const std::vector<vertex_id_t>& vids,
      const std::array<std::vector<int32_t>, num_labels>& vid_inds) const {
    // auto index_seq = std::make_index_sequence<sizeof...(T)>{};

    for (size_t i = 0; i < num_labels; ++i) {
      auto column_tuple = columns[i];
      auto ptr = std::get<Is>(column_tuple);
      if (ptr) {
        for (auto j = 0; j < vid_inds[i].size(); ++j) {
          auto vid_ind = vid_inds[i][j];
          auto vid = vids[vid_ind];
          std::get<Is>(props[vid_ind]) = ptr->get_view(vid);
        }
      } else {
        LOG(INFO) << "skip for column " << Is;
      }
    }

    fetch_properties<Is + 1>(props, columns, vids, vid_inds);
  }

  template <size_t Is, typename... T, typename column_tuple_t,
            size_t num_labels,
            typename std::enable_if<(Is >= sizeof...(T))>::type* = nullptr>
  void fetch_properties(
      std::vector<std::tuple<T...>>& props,
      std::vector<column_tuple_t>& columns,
      const std::vector<vertex_id_t>& vids,
      const std::array<std::vector<int32_t>, num_labels>& vid_inds) const {}

  template <size_t Is, typename... T, typename column_tuple_t,
            size_t num_labels,
            typename std::enable_if<(Is < sizeof...(T))>::type* = nullptr>
  void visit_properties(
      std::vector<std::tuple<T...>>& props,
      std::vector<column_tuple_t>& columns,
      const std::vector<vertex_id_t>& vids,
      const std::array<std::vector<int32_t>, num_labels>& vid_inds) const {
    // auto index_seq = std::make_index_sequence<sizeof...(T)>{};

    for (size_t i = 0; i < num_labels; ++i) {
      auto column_tuple = columns[i];
      auto ptr = std::get<Is>(column_tuple);
      if (ptr) {
        std::tuple_element_t<Is, std::tuple<T...>> tmp;
        for (auto j = 0; j < vid_inds[i].size(); ++j) {
          auto vid_ind = vid_inds[i][j];
          auto vid = vids[vid_ind];
          tmp = ptr->get_view(vid);
        }
        VLOG(10) << tmp;
      } else {
        LOG(INFO) << "skip for column " << Is;
      }
    }

    visit_properties<Is + 1>(props, columns, vids, vid_inds);
  }

  template <size_t Is, typename... T, typename column_tuple_t,
            size_t num_labels,
            typename std::enable_if<(Is >= sizeof...(T))>::type* = nullptr>
  void visit_properties(
      std::vector<std::tuple<T...>>& props,
      std::vector<column_tuple_t>& columns,
      const std::vector<vertex_id_t>& vids,
      const std::array<std::vector<int32_t>, num_labels>& vid_inds) const {}

  template <typename... T>
  grape_graph_impl::AdjListArray<T...> GetEdges(
      timestamp_t ts, const label_id_t& src_label_id,
      const label_id_t& dst_label_id, const label_id_t& edge_label_id,
      const std::vector<vertex_id_t>& vids, const std::string& direction_str,
      size_t limit,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    if (direction_str == "out" || direction_str == "Out" ||
        direction_str == "OUT") {
      auto csr =
          graph_.graph().get_oe_csr(src_label_id, dst_label_id, edge_label_id);
      return grape_graph_impl::AdjListArray<T...>(csr, vids, ts);
    } else if (direction_str == "in" || direction_str == "In" ||
               direction_str == "IN") {
      auto csr =
          graph_.graph().get_ie_csr(dst_label_id, src_label_id, edge_label_id);
      return grape_graph_impl::AdjListArray<T...>(csr, vids, ts);
    } else if (direction_str == "both" || direction_str == "Both" ||
               direction_str == "BOTH") {
      auto csr0 =
          graph_.graph().get_oe_csr(src_label_id, dst_label_id, edge_label_id);
      auto csr1 =
          graph_.graph().get_ie_csr(dst_label_id, src_label_id, edge_label_id);
      CHECK(csr0);
      CHECK(csr1);
      return grape_graph_impl::AdjListArray<T...>(csr0, csr1, vids, ts);
    } else {
      // LOG(FATAL) << "Not implemented - " << direction_str;
      throw std::runtime_error("Not implemented - " + direction_str);
    }
  }

  template <typename... T>
  grape_graph_impl::AdjListArray<T...> GetEdges(
      timestamp_t ts, const std::string& src_label,
      const std::string& dst_label, const std::string& edge_label,
      const std::vector<vertex_id_t>& vids, const std::string& direction_str,
      size_t limit,
      const std::array<std::string, std::tuple_size_v<std::tuple<T...>>>&
          prop_names) const {
    auto src_label_id = graph_.schema().get_vertex_label_id(src_label);
    auto dst_label_id = graph_.schema().get_vertex_label_id(dst_label);
    auto edge_label_id = graph_.schema().get_edge_label_id(edge_label);

    return GetEdges<T...>(ts, src_label_id, dst_label_id, edge_label_id, vids,
                          direction_str, limit, prop_names);
  }

  std::pair<std::vector<vertex_id_t>, std::vector<size_t>> GetOtherVerticesV2(
      timestamp_t ts, const std::string& src_label,
      const std::string& dst_label, const std::string& edge_label,
      const std::vector<vertex_id_t>& vids, const std::string& direction_str,
      size_t limit) const {
    auto src_label_id = graph_.schema().get_vertex_label_id(src_label);
    auto dst_label_id = graph_.schema().get_vertex_label_id(dst_label);
    auto edge_label_id = graph_.schema().get_edge_label_id(edge_label);

    return GetOtherVerticesV2(ts, src_label_id, dst_label_id, edge_label_id,
                              vids, direction_str, limit);
  }

  // return the vids, and offset array.
  std::pair<std::vector<vertex_id_t>, std::vector<size_t>> GetOtherVerticesV2(
      timestamp_t ts, const label_id_t& src_label_id,
      const label_id_t& dst_label_id, const label_id_t& edge_label_id,
      const std::vector<vertex_id_t>& vids, const std::string& direction_str,
      size_t limit) const {
    std::vector<vertex_id_t> ret_v;
    std::vector<size_t> ret_offset;

    if (direction_str == "out" || direction_str == "Out" ||
        direction_str == "OUT") {
      auto csr =
          graph_.graph().get_oe_csr(src_label_id, dst_label_id, edge_label_id);
      auto size = 0;
      for (auto i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        size += csr->edge_iter(v)->size();
      }
      ret_v.reserve(size);
      ret_offset.reserve(vids.size());
      ret_offset.emplace_back(0);

      for (auto i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        auto iter = csr->edge_iter(v);
        while (iter->is_valid()) {
          if (iter->get_timestamp() <= ts) {
            ret_v.emplace_back(iter->get_neighbor());
          }
          iter->next();
        }
        ret_offset.emplace_back(ret_v.size());
      }
    } else if (direction_str == "in" || direction_str == "In" ||
               direction_str == "IN") {
      auto csr =
          graph_.graph().get_ie_csr(dst_label_id, src_label_id, edge_label_id);
      auto size = 0;
      for (auto i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        size += csr->edge_iter(v)->size();
      }
      ret_v.reserve(size);
      ret_offset.reserve(vids.size());
      ret_offset.emplace_back(0);

      for (auto i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        auto iter = csr->edge_iter(v);
        while (iter->is_valid()) {
          if (iter->get_timestamp() <= ts) {
            ret_v.emplace_back(iter->get_neighbor());
          }
          iter->next();
        }
        ret_offset.emplace_back(ret_v.size());
      }
    } else if (direction_str == "both" || direction_str == "Both" ||
               direction_str == "BOTH") {
      auto ie_csr =
          graph_.graph().get_ie_csr(dst_label_id, src_label_id, edge_label_id);
      auto oe_csr =
          graph_.graph().get_oe_csr(src_label_id, dst_label_id, edge_label_id);
      auto size = 0;
      for (auto i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        size += ie_csr->edge_iter(v)->size();
        size += oe_csr->edge_iter(v)->size();
      }
      ret_v.reserve(size);
      ret_offset.reserve(vids.size() + 1);
      ret_offset.emplace_back(0);
      for (auto i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        {
          auto iter = ie_csr->edge_iter(v);
          while (iter->is_valid()) {
            if (iter->get_timestamp() <= ts) {
              ret_v.emplace_back(iter->get_neighbor());
            }
            iter->next();
          }
        }
        {
          auto iter = oe_csr->edge_iter(v);
          while (iter->is_valid()) {
            if (iter->get_timestamp() <= ts) {
              ret_v.emplace_back(iter->get_neighbor());
            }
            iter->next();
          }
        }
        ret_offset.emplace_back(ret_v.size());
      }
    } else {
      LOG(FATAL) << "Not implemented - " << direction_str;
    }
    return std::make_pair(std::move(ret_v), std::move(ret_offset));
  }

  grape_graph_impl::NbrListArray GetOtherVertices(
      timestamp_t ts, const std::string& src_label,
      const std::string& dst_label, const std::string& edge_label,
      const std::vector<vertex_id_t>& vids, const std::string& direction_str,
      size_t limit) const {
    auto src_label_id = graph_.schema().get_vertex_label_id(src_label);
    auto dst_label_id = graph_.schema().get_vertex_label_id(dst_label);
    auto edge_label_id = graph_.schema().get_edge_label_id(edge_label);
    return GetOtherVertices(ts, src_label_id, dst_label_id, edge_label_id, vids,
                            direction_str, limit);
  }

  grape_graph_impl::NbrListArray GetOtherVertices(
      timestamp_t ts, const label_id_t& src_label_id,
      const label_id_t& dst_label_id, const label_id_t& edge_label_id,
      const std::vector<vertex_id_t>& vids, const std::string& direction_str,
      size_t limit) const {
    grape_graph_impl::NbrListArray ret;

    if (direction_str == "out" || direction_str == "Out" ||
        direction_str == "OUT") {
      auto csr =
          graph_.graph().get_oe_csr(src_label_id, dst_label_id, edge_label_id);
      ret.resize(vids.size());
      for (size_t i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        auto iter = csr->edge_iter(v);
        auto& vec = ret.get_vector(i);
        while (iter->is_valid()) {
          if (iter->get_timestamp() <= ts) {
            vec.push_back(grape_graph_impl::Nbr(iter->get_neighbor()));
          }
          iter->next();
        }
      }
    } else if (direction_str == "in" || direction_str == "In" ||
               direction_str == "IN") {
      auto csr =
          graph_.graph().get_ie_csr(dst_label_id, src_label_id, edge_label_id);
      ret.resize(vids.size());
      for (size_t i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        auto iter = csr->edge_iter(v);
        auto& vec = ret.get_vector(i);
        while (iter->is_valid()) {
          if (iter->get_timestamp() <= ts) {
            vec.push_back(grape_graph_impl::Nbr(iter->get_neighbor()));
          }
          iter->next();
        }
      }
    } else if (direction_str == "both" || direction_str == "Both" ||
               direction_str == "BOTH") {
      ret.resize(vids.size());
      auto ocsr =
          graph_.graph().get_oe_csr(src_label_id, dst_label_id, edge_label_id);
      auto icsr =
          graph_.graph().get_ie_csr(dst_label_id, src_label_id, edge_label_id);
      for (size_t i = 0; i < vids.size(); ++i) {
        auto v = vids[i];
        auto& vec = ret.get_vector(i);
        auto iter = ocsr->edge_iter(v);
        while (iter->is_valid()) {
          if (iter->get_timestamp() <= ts) {
            vec.push_back(grape_graph_impl::Nbr(iter->get_neighbor()));
          }
          iter->next();
        }
        iter = icsr->edge_iter(v);
        while (iter->is_valid()) {
          if (iter->get_timestamp() <= ts) {
            vec.push_back(grape_graph_impl::Nbr(iter->get_neighbor()));
          }
          iter->next();
        }
      }
    } else {
      LOG(FATAL) << "Not implemented - " << direction_str;
    }
    return ret;
  }

  template <typename... T>
  grape_graph_impl::MultiPropGetter<T...> GetMultiPropGetter(
      timestamp_t ts, const std::string& label,
      const std::array<std::string, sizeof...(T)>& prop_names) const {
    auto label_id = graph_.schema().get_vertex_label_id(label);
    static constexpr auto ind_seq = std::make_index_sequence<sizeof...(T)>();
    using column_tuple_t = std::tuple<std::shared_ptr<TypedRefColumn<T>>...>;
    column_tuple_t columns;
    get_tuple_column_from_graph(graph_, label_id, prop_names, columns);
    return grape_graph_impl::MultiPropGetter<T...>(columns);
  }

  template <typename... T>
  grape_graph_impl::MultiPropGetter<T...> GetMultiPropGetter(
      timestamp_t ts, const label_id_t& label_id,
      const std::array<std::string, sizeof...(T)>& prop_names) const {
    static constexpr auto ind_seq = std::make_index_sequence<sizeof...(T)>();
    using column_tuple_t = std::tuple<std::shared_ptr<TypedRefColumn<T>>...>;
    column_tuple_t columns;
    get_tuple_column_from_graph(graph_, label_id, prop_names, columns);
    return grape_graph_impl::MultiPropGetter<T...>(columns);
  }

  template <typename T>
  grape_graph_impl::SinglePropGetter<T> GetSinglePropGetter(
      timestamp_t ts, const std::string& label,
      const std::string& prop_name) const {
    auto label_id = graph_.schema().get_vertex_label_id(label);
    using column_t = std::shared_ptr<TypedRefColumn<T>>;
    column_t column;
    column = std::dynamic_pointer_cast<TypedRefColumn<T>>(
        graph_.get_vertex_property_column_x(label_id, prop_name));
    return grape_graph_impl::SinglePropGetter<T>(std::move(column));
  }

  template <typename T>
  grape_graph_impl::SinglePropGetter<T> GetSinglePropGetter(
      timestamp_t ts, const label_id_t& label_id,
      const std::string& prop_name) const {
    using column_t = std::shared_ptr<TypedRefColumn<T>>;
    column_t column;
    column = std::dynamic_pointer_cast<TypedRefColumn<T>>(
        graph_.get_vertex_property_column_x(label_id, prop_name));
    return grape_graph_impl::SinglePropGetter<T>(std::move(column));
  }

  template <typename T>
  std::shared_ptr<TypedRefColumn<T>> GetTypedRefColumn(
      label_t& label_id, const NamedProperty<T>& named_prop) const {
    using column_t = std::shared_ptr<TypedRefColumn<T>>;
    column_t column;
    return std::dynamic_pointer_cast<TypedRefColumn<T>>(
        graph_.get_vertex_property_column_x(label_id, named_prop.names[0]));
  }
};

}  // namespace gs

#endif  // GRAPHSCOPE_MUTABLE_CSR_GRAPE_GRAPH_INTERFACE_H_

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

#ifndef RUNTIME_COMMON_RT_ANY_H_
#define RUNTIME_COMMON_RT_ANY_H_

#include "flex/proto_generated_gie/results.pb.h"
#include "flex/proto_generated_gie/type.pb.h"

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/common/types.h"
#include "flex/utils/app_utils.h"

namespace gs {

namespace runtime {

class PathImpl {
 public:
  static std::shared_ptr<PathImpl> make_path_impl(label_t label, vid_t v) {
    auto new_path = std::make_shared<PathImpl>();
    new_path->path_.push_back(std::make_pair(label, v));
    return new_path;
  }
  static std::shared_ptr<PathImpl> make_path_impl(
      label_t label, std::vector<vid_t>& path_ids) {
    auto new_path = std::make_shared<PathImpl>();
    for (auto id : path_ids) {
      new_path->path_.push_back({label, id});
    }
    return new_path;
  }
  std::shared_ptr<PathImpl> expand(label_t label, vid_t v) const {
    auto new_path = std::make_shared<PathImpl>();
    new_path->path_ = path_;
    new_path->path_.push_back(std::make_pair(label, v));
    return new_path;
  }

  std::string to_string() const {
    std::string str;
    for (size_t i = 0; i < path_.size(); ++i) {
      str += "(" + std::to_string(static_cast<int>(path_[i].first)) + ", " +
             std::to_string(path_[i].second) + ")";
      if (i != path_.size() - 1) {
        str += "->";
      }
    }
    return str;
  }

  std::pair<label_t, vid_t> get_end() const { return path_.back(); }
  std::pair<label_t, vid_t> get_start() const { return path_.front(); }
  bool operator<(const PathImpl& p) const { return path_ < p.path_; }
  bool operator==(const PathImpl& p) const { return path_ == p.path_; }
  std::vector<std::pair<label_t, vid_t>> path_;
};
class Path {
 public:
  Path() : impl_(nullptr) {}
  static Path make_path(const std::shared_ptr<PathImpl>& impl) {
    Path new_path;
    new_path.impl_ = impl.get();
    return new_path;
  }

  std::string to_string() const { return impl_->to_string(); }

  int32_t len() const { return impl_->path_.size(); }
  std::pair<label_t, vid_t> get_end() const { return impl_->get_end(); }

  std::pair<label_t, vid_t> get_start() const { return impl_->get_start(); }
  bool operator<(const Path& p) const { return *impl_ < *(p.impl_); }
  bool operator==(const Path& p) const { return *(impl_) == *(p.impl_); }

  PathImpl* impl_;
};
class RTAny;

class ListImplBase {
 public:
  virtual ~ListImplBase() = default;
  virtual bool operator<(const ListImplBase& p) const = 0;
  virtual bool operator==(const ListImplBase& p) const = 0;
  virtual size_t size() const = 0;
  virtual RTAny get(size_t idx) const = 0;
};

class List {
 public:
  static List make_list(const std::shared_ptr<ListImplBase>& impl) {
    List new_list;
    new_list.impl_ = impl.get();
    return new_list;
  }

  bool operator<(const List& p) const { return *impl_ < *(p.impl_); }
  bool operator==(const List& p) const { return *(impl_) == *(p.impl_); }
  size_t size() const { return impl_->size(); }
  RTAny get(size_t idx) const;

  ListImplBase* impl_;
};

class Tuple {
 public:
  ~Tuple() {
    if (props_ != nullptr) {
      // delete props_;
    }
  }
  void init(std::vector<RTAny>&& vals) {
    props_ = new std::vector<RTAny>(vals);
  }
  // not support for path
  Tuple dup() const {
    Tuple new_tuple;
    new_tuple.props_ = new std::vector<RTAny>(*props_);
    return new_tuple;
  }

  bool operator<(const Tuple& p) const { return *props_ < *(p.props_); }
  bool operator==(const Tuple& p) const { return *props_ == *(p.props_); }
  size_t size() const { return props_->size(); }
  const RTAny& get(size_t idx) const { return (*props_)[idx]; }

 private:
  const std::vector<RTAny>* props_;
};

class MapImpl {
 public:
  static MapImpl make_map_impl(const std::vector<std::string>* keys,
                               const std::vector<RTAny>* values) {
    MapImpl map_impl;
    map_impl.keys = keys;
    map_impl.values = values;
    return map_impl;
  }
  size_t size() const { return keys->size(); }

  const std::vector<std::string>* keys;
  const std::vector<RTAny>* values;
};

class RTAnyType {
 public:
  enum class RTAnyTypeImpl {
    kVertex,
    kEdge,
    kI64Value,
    kU64Value,
    kI32Value,
    kF64Value,
    kBoolValue,
    kStringValue,
    kVertexSetValue,
    kStringSetValue,
    kUnknown,
    kDate32,
    kPath,
    kNull,
    kTuple,
    kList,
    kMap,
    kEmpty,
    kRecordView,
  };
  static const RTAnyType kVertex;
  static const RTAnyType kEdge;
  static const RTAnyType kI64Value;
  static const RTAnyType kU64Value;
  static const RTAnyType kI32Value;
  static const RTAnyType kF64Value;
  static const RTAnyType kBoolValue;
  static const RTAnyType kStringValue;
  static const RTAnyType kVertexSetValue;
  static const RTAnyType kStringSetValue;
  static const RTAnyType kUnknown;
  static const RTAnyType kDate32;
  static const RTAnyType kPath;
  static const RTAnyType kNull;
  static const RTAnyType kTuple;
  static const RTAnyType kList;
  static const RTAnyType kMap;
  static const RTAnyType kEmpty;
  static const RTAnyType kRecordView;

  RTAnyType() : type_enum_(RTAnyTypeImpl::kUnknown) {}
  RTAnyType(const RTAnyType& other)
      : type_enum_(other.type_enum_), null_able_(other.null_able_) {}
  RTAnyType(RTAnyTypeImpl type) : type_enum_(type), null_able_(false) {}
  RTAnyType(RTAnyTypeImpl type, bool null_able)
      : type_enum_(type), null_able_(null_able) {}
  bool operator==(const RTAnyType& other) const {
    return type_enum_ == other.type_enum_;
  }
  RTAnyTypeImpl type_enum_;
  bool null_able_;
};

PropertyType rt_type_to_property_type(RTAnyType type);

class Map {
 public:
  static Map make_map(MapImpl impl) {
    Map m;
    m.map_ = impl;
    return m;
  }
  std::pair<const std::vector<std::string>*, const std::vector<RTAny>*>
  key_vals() const {
    return std::make_pair(map_.keys, map_.values);
  }

  MapImpl map_;
};

struct pod_string_view {
  const char* data_;
  size_t size_;
  pod_string_view() = default;
  pod_string_view(const pod_string_view& other) = default;
  pod_string_view(const char* data) : data_(data), size_(strlen(data_)) {}
  pod_string_view(const char* data, size_t size) : data_(data), size_(size) {}
  pod_string_view(const std::string& str)
      : data_(str.data()), size_(str.size()) {}
  pod_string_view(const std::string_view& str)
      : data_(str.data()), size_(str.size()) {}
  const char* data() const { return data_; }
  size_t size() const { return size_; }

  std::string to_string() const { return std::string(data_, size_); }
};
struct EdgeData {
  // PropertyType type;

  template <typename T>
  T as() const {
    if constexpr (std::is_same_v<T, int32_t>) {
      return value.i32_val;
    } else if constexpr (std::is_same_v<T, int64_t>) {
      return value.i64_val;
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      return value.u64_val;
    } else if constexpr (std::is_same_v<T, double>) {
      return value.f64_val;
    } else if constexpr (std::is_same_v<T, bool>) {
      return value.b_val;
    } else if constexpr (std::is_same_v<T, std::string_view>) {
      return value.str_val;
    } else if constexpr (std::is_same_v<T, grape::EmptyType>) {
      return grape::EmptyType();
    } else if constexpr (std::is_same_v<T, Date>) {
      return Date(value.i64_val);
    } else {
      LOG(FATAL) << "not support for " << typeid(T).name();
    }
  }

  template <typename T>
  explicit EdgeData(T val) {
    if constexpr (std::is_same_v<T, int32_t>) {
      type = RTAnyType::kI32Value;
      value.i32_val = val;
    } else if constexpr (std::is_same_v<T, int64_t>) {
      type = RTAnyType::kI64Value;
      value.i64_val = val;
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      type = RTAnyType::kU64Value;
      value.u64_val = val;
    } else if constexpr (std::is_same_v<T, double>) {
      type = RTAnyType::kF64Value;
      value.f64_val = val;
    } else if constexpr (std::is_same_v<T, bool>) {
      type = RTAnyType::kBoolValue;
      value.b_val = val;
    } else if constexpr (std::is_same_v<T, std::string_view>) {
      type = RTAnyType::kStringValue;
      value.str_val = val;
    } else if constexpr (std::is_same_v<T, grape::EmptyType>) {
      type = RTAnyType::kEmpty;
    } else if constexpr (std::is_same_v<T, Date>) {
      type = RTAnyType::kDate32;
      value.i64_val = val.milli_second;
    } else {
      LOG(FATAL) << "not support for " << typeid(T).name();
    }
  }

  std::string to_string() const {
    if (type == RTAnyType::kI32Value) {
      return std::to_string(value.i32_val);
    } else if (type == RTAnyType::kI64Value) {
      return std::to_string(value.i64_val);
    } else if (type == RTAnyType::kStringValue) {
      return std::string(value.str_val.data(), value.str_val.size());
      return value.str_val.to_string();
    } else if (type == RTAnyType::kNull) {
      return "NULL";
    } else if (type == RTAnyType::kF64Value) {
      return std::to_string(value.f64_val);
    } else if (type == RTAnyType::kBoolValue) {
      return value.b_val ? "true" : "false";
    } else if (type == RTAnyType::kEmpty) {
      return "";
    } else if (type == RTAnyType::kDate32) {
      return std::to_string(value.i64_val);
    } else if (type == RTAnyType::kEmpty) {
      return "";
    } else {
      LOG(FATAL) << "Unexpected property type: "
                 << static_cast<int>(type.type_enum_);
      return "";
    }
  }

  EdgeData() = default;

  EdgeData(const Any& any) {
    switch (any.type.type_enum) {
    case impl::PropertyTypeImpl::kInt64:
      type = RTAnyType::kI64Value;
      value.i64_val = any.value.l;
      break;
    case impl::PropertyTypeImpl::kInt32:
      type = RTAnyType::kI32Value;
      value.i32_val = any.value.i;
      break;
    case impl::PropertyTypeImpl::kStringView:
      type = RTAnyType::kStringValue;
      value.str_val = any.value.s;
      break;
    case impl::PropertyTypeImpl::kDouble:
      type = RTAnyType::kF64Value;
      value.f64_val = any.value.db;
      break;
    case impl::PropertyTypeImpl::kBool:
      type = RTAnyType::kBoolValue;
      value.b_val = any.value.b;
      break;
    case impl::PropertyTypeImpl::kEmpty:
      type = RTAnyType::kEmpty;
      break;
    case impl::PropertyTypeImpl::kDate:
      type = RTAnyType::kDate32;
      value.i64_val = any.value.d.milli_second;
      break;
    default:
      LOG(FATAL) << "Unexpected property type: "
                 << static_cast<int>(any.type.type_enum);
    }
  }

  bool operator<(const EdgeData& e) const {
    if (type == RTAnyType::kI64Value) {
      return value.i64_val < e.value.i64_val;
    } else if (type == RTAnyType::kI32Value) {
      return value.i32_val < e.value.i32_val;
    } else if (type == RTAnyType::kF64Value) {
      return value.f64_val < e.value.f64_val;
    } else if (type == RTAnyType::kBoolValue) {
      return value.b_val < e.value.b_val;
    } else if (type == RTAnyType::kStringValue) {
      return std::string_view(value.str_val.data(), value.str_val.size()) <
             std::string_view(e.value.str_val.data(), e.value.str_val.size());
    } else if (type == RTAnyType::kDate32) {
      return value.i64_val < e.value.i64_val;
    } else {
      return false;
    }
  }

  bool operator==(const EdgeData& e) const {
    if (type == RTAnyType::kI64Value) {
      return value.i64_val == e.value.i64_val;
    } else if (type == RTAnyType::kI32Value) {
      return value.i32_val == e.value.i32_val;
    } else if (type == RTAnyType::kF64Value) {
      return value.f64_val == e.value.f64_val;
    } else if (type == RTAnyType::kBoolValue) {
      return value.b_val == e.value.b_val;
    } else if (type == RTAnyType::kStringValue) {
      return std::string_view(value.str_val.data(), value.str_val.size()) ==
             std::string_view(e.value.str_val.data(), e.value.str_val.size());
    } else if (type == RTAnyType::kDate32) {
      return value.i64_val == e.value.i64_val;
    } else {
      return false;
    }
  }
  RTAnyType type;

  union {
    int32_t i32_val;
    int64_t i64_val;
    uint64_t u64_val;
    double f64_val;
    bool b_val;
    pod_string_view str_val;
    Date date_val;

    // todo: make recordview as a pod type
    // RecordView record;
  } value;
};
class EdgeRecord {
 public:
  EdgeRecord() = default;
  EdgeRecord(LabelTriplet label_triplet, vid_t src, vid_t dst, EdgeData prop,
             Direction dir)
      : label_triplet_(label_triplet),
        src_(src),
        dst_(dst),
        prop_(prop),
        dir_(dir) {}
  bool operator<(const EdgeRecord& e) const {
    return std::tie(src_, dst_, label_triplet_, prop_, dir_) <
           std::tie(e.src_, e.dst_, e.label_triplet_, prop_, dir_);
  }
  bool operator==(const EdgeRecord& e) const {
    return std::tie(src_, dst_, label_triplet_, prop_, dir_) ==
           std::tie(e.src_, e.dst_, e.label_triplet_, prop_, dir_);
  }
  vid_t src() const { return src_; }
  vid_t dst() const { return dst_; }
  LabelTriplet label_triplet() const { return label_triplet_; }
  EdgeData prop() const { return prop_; }
  Direction dir() const { return dir_; }

  LabelTriplet label_triplet_;
  vid_t src_, dst_;
  EdgeData prop_;
  Direction dir_;
};

RTAnyType parse_from_ir_data_type(const ::common::IrDataType& dt);

union RTAnyValue {
  RTAnyValue() : vset(NULL) {}
  ~RTAnyValue() {}

  std::pair<label_t, vid_t> vertex;
  EdgeRecord edge;
  int64_t i64_val;
  uint64_t u64_val;
  int i32_val;
  double f64_val;
  const std::vector<vid_t>* vset;
  const std::set<std::string>* str_set;
  std::string_view str_val;
  Path p;
  Tuple t;
  List list;
  Map map;
  bool b_val;
};

class RTAny {
 public:
  RTAny();
  RTAny(RTAnyType type);
  RTAny(const Any& val);
  RTAny(const EdgeData& val);
  RTAny(const RTAny& rhs);
  RTAny(const Path& p);
  ~RTAny() = default;
  bool is_null() const { return type_ == RTAnyType::kNull; }

  int numerical_cmp(const RTAny& other) const;

  RTAny& operator=(const RTAny& rhs);

  static RTAny from_vertex(label_t l, vid_t v);
  static RTAny from_vertex(const std::pair<label_t, vid_t>& v);
  static RTAny from_edge(const EdgeRecord& v);
  static RTAny from_bool(bool v);
  static RTAny from_int64(int64_t v);
  static RTAny from_uint64(uint64_t v);
  static RTAny from_int32(int v);
  static RTAny from_string(const std::string& str);
  static RTAny from_string(const std::string_view& str);
  static RTAny from_string_set(const std::set<std::string>& str_set);
  static RTAny from_vertex_list(const std::vector<vid_t>& v_set);
  static RTAny from_date32(Date v);
  static RTAny from_tuple(std::vector<RTAny>&& tuple);
  static RTAny from_tuple(const Tuple& tuple);
  static RTAny from_list(const List& list);
  static RTAny from_double(double v);
  static RTAny from_map(const Map& m);

  bool as_bool() const;
  int as_int32() const;
  int64_t as_int64() const;
  uint64_t as_uint64() const;
  int64_t as_date32() const;
  double as_double() const;
  const std::pair<label_t, vid_t>& as_vertex() const;
  const EdgeRecord& as_edge() const;
  const std::set<std::string>& as_string_set() const;
  std::string_view as_string() const;
  const std::vector<vid_t>& as_vertex_list() const;
  Path as_path() const;
  Tuple as_tuple() const;
  List as_list() const;
  Map as_map() const;

  bool operator<(const RTAny& other) const;
  bool operator==(const RTAny& other) const;

  RTAny operator+(const RTAny& other) const;

  RTAny operator-(const RTAny& other) const;
  RTAny operator/(const RTAny& other) const;

  void sink(const gs::ReadTransaction& txn, int id,
            results::Column* column) const;
  void encode_sig(RTAnyType type, Encoder& encoder) const;

  std::string to_string() const;

  RTAnyType type() const;

 private:
  void sink_impl(common::Value* collection) const;
  RTAnyType type_;
  RTAnyValue value_;
};

template <typename T>
struct TypedConverter {};

template <>
struct TypedConverter<bool> {
  static RTAnyType type() { return RTAnyType::kBoolValue; }
  static bool to_typed(const RTAny& val) { return val.as_bool(); }
  static RTAny from_typed(bool val) { return RTAny::from_bool(val); }
  static const std::string name() { return "bool"; }
};
template <>
struct TypedConverter<int> {
  static RTAnyType type() { return RTAnyType::kI32Value; }
  static int to_typed(const RTAny& val) { return val.as_int32(); }
  static RTAny from_typed(int val) { return RTAny::from_int32(val); }
  static const std::string name() { return "int"; }
};

template <>
struct TypedConverter<std::set<std::string>> {
  static RTAnyType type() { return RTAnyType::kStringSetValue; }
  static const std::set<std::string> to_typed(const RTAny& val) {
    return val.as_string_set();
  }
  static RTAny from_typed(const std::set<std::string>& val) {
    return RTAny::from_string_set(val);
  }
  static const std::string name() { return "set<string>"; }
};

template <>
struct TypedConverter<std::vector<vid_t>> {
  static RTAnyType type() { return RTAnyType::kVertexSetValue; }
  static const std::vector<vid_t>& to_typed(const RTAny& val) {
    return val.as_vertex_list();
  }
  static RTAny from_typed(const std::vector<vid_t>& val) {
    return RTAny::from_vertex_list(val);
  }
  static const std::string name() { return "vector<vid_t>"; }
};

template <>
struct TypedConverter<std::string_view> {
  static RTAnyType type() { return RTAnyType::kStringValue; }
  static std::string_view to_typed(const RTAny& val) { return val.as_string(); }
  static RTAny from_typed(const std::string_view& val) {
    return RTAny::from_string(val);
  }
  static const std::string name() { return "string_view"; }
  static std::string_view typed_from_string(const std::string& str) {
    return std::string_view(str.data(), str.size());
  }
};

template <>
struct TypedConverter<uint64_t> {
  static RTAnyType type() { return RTAnyType::kU64Value; }
  static uint64_t to_typed(const RTAny& val) { return val.as_uint64(); }
  static RTAny from_typed(uint64_t val) { return RTAny::from_uint64(val); }
  static const std::string name() { return "uint64"; }
};

template <>
struct TypedConverter<int64_t> {
  static RTAnyType type() { return RTAnyType::kI64Value; }
  static int64_t to_typed(const RTAny& val) { return val.as_int64(); }
  static RTAny from_typed(int64_t val) { return RTAny::from_int64(val); }
  static const std::string name() { return "int64"; }
};

template <>
struct TypedConverter<double> {
  static RTAnyType type() { return RTAnyType::kF64Value; }
  static double to_typed(const RTAny& val) { return val.as_double(); }
  static RTAny from_typed(double val) { return RTAny::from_double(val); }
  static const std::string name() { return "double"; }
};
template <>
struct TypedConverter<Date> {
  static RTAnyType type() { return RTAnyType::kDate32; }
  static Date to_typed(const RTAny& val) { return val.as_date32(); }
  static RTAny from_typed(Date val) { return RTAny::from_date32(val); }
  static const std::string name() { return "date"; }
  static Date typed_from_string(const std::string& str) {
    int64_t val = std::stoll(str);
    return Date(val);
  }
};

template <>
struct TypedConverter<Tuple> {
  static RTAnyType type() { return RTAnyType::kTuple; }
  static Tuple to_typed(const RTAny& val) { return val.as_tuple(); }
  static RTAny from_typed(Tuple val) {
    return RTAny::from_tuple(std::move(val));
  }
  static const std::string name() { return "tuple"; }
};

template <>
struct TypedConverter<Map> {
  static RTAnyType type() { return RTAnyType::kMap; }
  static Map to_typed(const RTAny& val) { return val.as_map(); }
  static RTAny from_typed(Map val) { return RTAny::from_map(val); }
  static const std::string name() { return "map"; }
};
template <typename T>
class ListImpl : ListImplBase {
 public:
  ListImpl() = default;
  static std::shared_ptr<ListImplBase> make_list_impl(std::vector<T>&& vals) {
    auto new_list = new ListImpl<T>();
    new_list->list_ = std::move(vals);
    new_list->is_valid_.resize(new_list->list_.size(), true);
    return std::shared_ptr<ListImplBase>(static_cast<ListImplBase*>(new_list));
  }

  static std::shared_ptr<ListImplBase> make_list_impl(
      const std::vector<RTAny>& vals) {
    auto new_list = new ListImpl<T>();
    for (auto& val : vals) {
      if (val.is_null()) {
        new_list->is_valid_.push_back(false);
        new_list->list_.push_back(T());
      } else {
        new_list->list_.push_back(TypedConverter<T>::to_typed(val));
        new_list->is_valid_.push_back(true);
      }
    }
    return std::shared_ptr<ListImplBase>(static_cast<ListImplBase*>(new_list));
  }

  bool operator<(const ListImplBase& p) const {
    return list_ < (dynamic_cast<const ListImpl<T>&>(p)).list_;
  }
  bool operator==(const ListImplBase& p) const {
    return list_ == (dynamic_cast<const ListImpl<T>&>(p)).list_;
  }
  size_t size() const { return list_.size(); }
  RTAny get(size_t idx) const {
    if (is_valid_[idx]) {
      return TypedConverter<T>::from_typed(list_[idx]);
    } else {
      return RTAny(RTAnyType::kNull);
    }
  }

  std::vector<T> list_;
  std::vector<bool> is_valid_;
};
template <>
class ListImpl<std::string_view> : public ListImplBase {
 public:
  ListImpl() = default;
  static std::shared_ptr<ListImplBase> make_list_impl(
      std::vector<std::string>&& vals) {
    auto new_list = new ListImpl<std::string_view>();
    new_list->list_ = std::move(vals);
    new_list->is_valid_.resize(new_list->list_.size(), true);
    return std::shared_ptr<ListImplBase>(static_cast<ListImplBase*>(new_list));
  }

  static std::shared_ptr<ListImplBase> make_list_impl(
      const std::vector<RTAny>& vals) {
    auto new_list = new ListImpl<std::string_view>();
    for (auto& val : vals) {
      if (val.is_null()) {
        new_list->is_valid_.push_back(false);
        new_list->list_.push_back("");
      } else {
        new_list->list_.push_back(
            std::string(TypedConverter<std::string_view>::to_typed(val)));
        new_list->is_valid_.push_back(true);
      }
    }
    return std::shared_ptr<ListImplBase>(static_cast<ListImplBase*>(new_list));
  }

  bool operator<(const ListImplBase& p) const {
    return list_ < (dynamic_cast<const ListImpl<std::string_view>&>(p)).list_;
  }
  bool operator==(const ListImplBase& p) const {
    return list_ == (dynamic_cast<const ListImpl<std::string_view>&>(p)).list_;
  }
  size_t size() const { return list_.size(); }
  RTAny get(size_t idx) const {
    if (is_valid_[idx]) {
      return TypedConverter<std::string_view>::from_typed(
          std::string_view(list_[idx].data(), list_[idx].size()));
    } else {
      return RTAny(RTAnyType::kNull);
    }
  }

  std::vector<std::string> list_;
  std::vector<bool> is_valid_;
};
class EdgePropVecBase {
 public:
  static std::shared_ptr<EdgePropVecBase> make_edge_prop_vec(PropertyType type);
  virtual ~EdgePropVecBase() = default;
  virtual size_t size() const = 0;
  virtual void resize(size_t size) = 0;
  virtual void reserve(size_t size) = 0;
  virtual void clear() = 0;
  virtual EdgeData get(size_t idx) const = 0;

  virtual PropertyType type() const = 0;
  virtual void set_any(size_t idx, EdgePropVecBase* other,
                       size_t other_idx) = 0;
};
template <typename T>
class EdgePropVec : public EdgePropVecBase {
 public:
  ~EdgePropVec() {}

  void push_back(const T& val) { prop_data_.push_back(val); }
  void emplace_back(T&& val) { prop_data_.emplace_back(std::move(val)); }
  size_t size() const override { return prop_data_.size(); }

  EdgeData get(size_t idx) const override { return EdgeData(prop_data_[idx]); }

  T get_view(size_t idx) const { return prop_data_[idx]; }
  void resize(size_t size) override { prop_data_.resize(size); }
  void clear() override { prop_data_.clear(); }
  void reserve(size_t size) override { prop_data_.reserve(size); }
  T operator[](size_t idx) const { return prop_data_[idx]; }
  void set(size_t idx, const T& val) {
    if (prop_data_.size() <= idx) {
      prop_data_.resize(idx + 1);
    }
    prop_data_[idx] = val;
  }

  PropertyType type() const override { return AnyConverter<T>::type(); }

  void set_any(size_t idx, EdgePropVecBase* other, size_t other_idx) override {
    CHECK(dynamic_cast<EdgePropVec<T>*>(other) != nullptr);
    set(idx, dynamic_cast<EdgePropVec<T>*>(other)->get_view(other_idx));
  }

 private:
  std::vector<T> prop_data_;
};

template <>
class EdgePropVec<grape::EmptyType> : public EdgePropVecBase {
 public:
  ~EdgePropVec() {}
  void push_back(const grape::EmptyType& val) { size_++; }
  void emplace_back(grape::EmptyType&& val) { size_++; }
  size_t size() const override { return size_; }

  EdgeData get(size_t idx) const override {
    return EdgeData(grape::EmptyType());
  }

  grape::EmptyType get_view(size_t idx) const { return grape::EmptyType(); }
  void resize(size_t size) override { size_ = size; }
  void clear() override {}
  void reserve(size_t size) override {}
  grape::EmptyType operator[](size_t idx) const { return grape::EmptyType(); }
  void set(size_t idx, const grape::EmptyType& val) {}

  PropertyType type() const override { return PropertyType::kEmpty; }

  void set_any(size_t idx, EdgePropVecBase* other, size_t other_idx) override {}
  size_t size_;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_RT_ANY_H_
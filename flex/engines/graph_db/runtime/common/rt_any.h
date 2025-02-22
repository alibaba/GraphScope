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

#include "flex/engines/graph_db/runtime/common/graph_interface.h"
#include "flex/engines/graph_db/runtime/common/types.h"
#include "flex/utils/app_utils.h"

namespace gs {

namespace runtime {

class CObject {
 public:
  virtual ~CObject() = default;
};

using Arena = std::vector<std::unique_ptr<CObject>>;

struct ArenaRef : public CObject {
  ArenaRef(const std::shared_ptr<Arena>& arena) : arena_(arena) {}
  std::shared_ptr<Arena> arena_;
};

class VertexRecord {
 public:
  bool operator<(const VertexRecord& v) const {
    if (label_ == v.label_) {
      return vid_ < v.vid_;
    } else {
      return label_ < v.label_;
    }
  }
  bool operator==(const VertexRecord& v) const {
    return label_ == v.label_ && vid_ == v.vid_;
  }

  label_t label() const { return label_; }
  vid_t vid() const { return vid_; }
  label_t label_;
  vid_t vid_;
};

struct VertexRecordHash {
  std::size_t operator()(const VertexRecord& v) const {
    return std::hash<vid_t>()(v.vid_) ^ std::hash<label_t>()(v.label_);
  }
  std::size_t operator()(const std::pair<VertexRecord, VertexRecord>& v) const {
    return std::hash<vid_t>()(v.first.vid_) ^
           std::hash<label_t>()(v.first.label_) ^
           std::hash<vid_t>()(v.second.vid_) ^
           std::hash<label_t>()(v.second.label_);
  }
};
struct Relation {
  label_t label;
  vid_t src;
  vid_t dst;
  bool operator<(const Relation& r) const {
    return std::tie(label, src, dst) < std::tie(r.label, r.src, r.dst);
  }
  bool operator==(const Relation& r) const {
    return std::tie(label, src, dst) == std::tie(r.label, r.src, r.dst);
  }
  VertexRecord start_node() const { return {label, src}; }
  VertexRecord end_node() const { return {label, dst}; }
};

class PathImpl : public CObject {
 public:
  static std::unique_ptr<PathImpl> make_path_impl(label_t label, vid_t v) {
    auto new_path = std::make_unique<PathImpl>();
    new_path->path_.push_back({label, v});
    return new_path;
  }
  static std::unique_ptr<PathImpl> make_path_impl(
      label_t label, std::vector<vid_t>& path_ids) {
    auto new_path = std::make_unique<PathImpl>();
    for (auto id : path_ids) {
      new_path->path_.push_back({label, id});
    }
    return new_path;
  }

  static std::unique_ptr<PathImpl> make_path_impl(
      const std::vector<VertexRecord>& path) {
    auto new_path = std::make_unique<PathImpl>();
    new_path->path_.insert(new_path->path_.end(), path.begin(), path.end());
    return new_path;
  }

  std::unique_ptr<PathImpl> expand(label_t label, vid_t v) const {
    auto new_path = std::make_unique<PathImpl>();
    new_path->path_ = path_;
    new_path->path_.push_back({label, v});
    return new_path;
  }

  std::string to_string() const {
    std::string str;
    for (size_t i = 0; i < path_.size(); ++i) {
      str += "(" + std::to_string(static_cast<int>(path_[i].label_)) + ", " +
             std::to_string(path_[i].vid_) + ")";
      if (i != path_.size() - 1) {
        str += "->";
      }
    }
    return str;
  }

  VertexRecord get_end() const { return path_.back(); }
  VertexRecord get_start() const { return path_.front(); }
  bool operator<(const PathImpl& p) const { return path_ < p.path_; }
  bool operator==(const PathImpl& p) const { return path_ == p.path_; }
  std::vector<VertexRecord> path_;
};
class Path {
 public:
  Path() = default;
  Path(PathImpl* impl) : impl_(impl) {}

  std::string to_string() const { return impl_->to_string(); }

  int32_t len() const { return impl_->path_.size(); }
  VertexRecord get_end() const { return impl_->get_end(); }

  std::vector<Relation> relationships() const {
    std::vector<Relation> relations;
    for (size_t i = 0; i < impl_->path_.size() - 1; ++i) {
      Relation r;
      r.label = impl_->path_[i].label_;
      r.src = impl_->path_[i].vid_;
      r.dst = impl_->path_[i + 1].vid_;
      relations.push_back(r);
    }
    return relations;
  }

  std::vector<VertexRecord> nodes() { return impl_->path_; }

  VertexRecord get_start() const { return impl_->get_start(); }
  bool operator<(const Path& p) const { return *impl_ < *(p.impl_); }
  bool operator==(const Path& p) const { return *(impl_) == *(p.impl_); }

  PathImpl* impl_;
};

class RTAny;
enum class RTAnyType;

class ListImplBase : public CObject {
 public:
  virtual ~ListImplBase() = default;
  virtual bool operator<(const ListImplBase& p) const = 0;
  virtual bool operator==(const ListImplBase& p) const = 0;
  virtual size_t size() const = 0;
  virtual RTAnyType type() const = 0;
  virtual RTAny get(size_t idx) const = 0;
};

template <typename T>
class ListImpl;

class List {
 public:
  List() = default;
  List(ListImplBase* impl) : impl_(impl) {}

  bool operator<(const List& p) const { return *impl_ < *(p.impl_); }
  bool operator==(const List& p) const { return *(impl_) == *(p.impl_); }
  size_t size() const { return impl_->size(); }
  RTAny get(size_t idx) const;
  RTAnyType elem_type() const;
  ListImplBase* impl_;
};

class SetImplBase : public CObject {
 public:
  virtual ~SetImplBase() = default;
  virtual bool operator<(const SetImplBase& p) const = 0;
  virtual bool operator==(const SetImplBase& p) const = 0;
  virtual size_t size() const = 0;
  virtual std::vector<RTAny> values() const = 0;
  virtual void insert(const RTAny& val) = 0;
  virtual bool exists(const RTAny& val) const = 0;
  virtual RTAnyType type() const = 0;
};

template <typename T>
class SetImpl;

class Set {
 public:
  Set() = default;
  Set(SetImplBase* impl) : impl_(impl) {}
  void insert(const RTAny& val) { impl_->insert(val); }
  bool operator<(const Set& p) const { return *impl_ < *(p.impl_); }
  bool operator==(const Set& p) const { return *(impl_) == *(p.impl_); }
  bool exists(const RTAny& val) const { return impl_->exists(val); }
  std::vector<RTAny> values() const { return impl_->values(); }

  RTAnyType elem_type() const;
  size_t size() const { return impl_->size(); }
  SetImplBase* impl_;
};

class TupleImplBase : public CObject {
 public:
  virtual ~TupleImplBase() = default;
  virtual bool operator<(const TupleImplBase& p) const = 0;
  virtual bool operator==(const TupleImplBase& p) const = 0;
  virtual size_t size() const = 0;
  virtual RTAny get(size_t idx) const = 0;
};

template <typename... Args>
class TupleImpl : public TupleImplBase {
 public:
  TupleImpl() = default;
  ~TupleImpl() = default;
  TupleImpl(Args&&... args) : values(std::forward<Args>(args)...) {}
  TupleImpl(std::tuple<Args...>&& args) : values(std::move(args)) {}
  bool operator<(const TupleImplBase& p) const override {
    return values < dynamic_cast<const TupleImpl<Args...>&>(p).values;
  }
  bool operator==(const TupleImplBase& p) const override {
    return values == dynamic_cast<const TupleImpl<Args...>&>(p).values;
  }

  RTAny get(size_t idx) const override;

  size_t size() const override {
    return std::tuple_size_v<std::tuple<Args...>>;
  }
  std::tuple<Args...> values;
};

template <>
class TupleImpl<RTAny> : public TupleImplBase {
 public:
  TupleImpl() = default;
  ~TupleImpl() = default;
  TupleImpl(std::vector<RTAny>&& val) : values(std::move(val)) {}
  bool operator<(const TupleImplBase& p) const override {
    return values < dynamic_cast<const TupleImpl<RTAny>&>(p).values;
  }
  bool operator==(const TupleImplBase& p) const override {
    return values == dynamic_cast<const TupleImpl<RTAny>&>(p).values;
  }
  size_t size() const override { return values.size(); }
  RTAny get(size_t idx) const override;

  std::vector<RTAny> values;
};

class Tuple {
 public:
  template <typename... Args>
  static std::unique_ptr<TupleImplBase> make_tuple_impl(
      std::tuple<Args...>&& args) {
    return std::make_unique<TupleImpl<Args...>>(std::move(args));
  }
  static std::unique_ptr<TupleImplBase> make_generic_tuple_impl(
      std::vector<RTAny>&& args) {
    return std::make_unique<TupleImpl<RTAny>>(std::move(args));
  }

  Tuple() = default;
  Tuple(TupleImplBase* impl) : impl_(impl) {}
  bool operator<(const Tuple& p) const { return *impl_ < *(p.impl_); }
  bool operator==(const Tuple& p) const { return *impl_ == *(p.impl_); }
  size_t size() const { return impl_->size(); }
  RTAny get(size_t idx) const;
  TupleImplBase* impl_;
};

class MapImpl : public CObject {
 public:
  static std::unique_ptr<MapImpl> make_map_impl(
      const std::vector<RTAny>& keys, const std::vector<RTAny>& values) {
    auto new_map = std::make_unique<MapImpl>();
    new_map->keys = keys;
    new_map->values = values;
    return new_map;
  }
  size_t size() const { return keys.size(); }
  bool operator<(const MapImpl& p) const {
    return std::tie(keys, values) < std::tie(p.keys, p.values);
  }
  bool operator==(const MapImpl& p) const {
    return std::tie(keys, values) == std::tie(p.keys, p.values);
  }

  std::vector<RTAny> keys;
  std::vector<RTAny> values;
};

class StringImpl : public CObject {
 public:
  std::string_view str_view() const {
    return std::string_view(str.data(), str.size());
  }
  static std::unique_ptr<StringImpl> make_string_impl(const std::string& str) {
    auto new_str = std::make_unique<StringImpl>();
    new_str->str = str;
    return new_str;
  }
  std::string str;
};

enum class RTAnyType {
  kVertex,
  kEdge,
  kI64Value,
  kU64Value,
  kI32Value,
  kF64Value,
  kBoolValue,
  kStringValue,
  kUnknown,
  kDate32,
  kTimestamp,
  kPath,
  kNull,
  kTuple,
  kList,
  kMap,
  kRelation,
  kSet,
  kEmpty,
  kRecordView,
};

PropertyType rt_type_to_property_type(RTAnyType type);

class Map {
 public:
  static Map make_map(MapImpl* impl) {
    Map m;
    m.map_ = impl;
    return m;
  }
  std::pair<const std::vector<RTAny>, const std::vector<RTAny>> key_vals()
      const {
    return std::make_pair(map_->keys, map_->values);
  }
  bool operator<(const Map& p) const { return *map_ < *(p.map_); }
  bool operator==(const Map& p) const { return *map_ == *(p.map_); }

  MapImpl* map_;
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
// only for pod type
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
      return std::string_view(value.str_val.data(), value.str_val.size());
    } else if constexpr (std::is_same_v<T, grape::EmptyType>) {
      return grape::EmptyType();
    } else if constexpr (std::is_same_v<T, Date>) {
      return Date(value.i64_val);
    } else if constexpr (std::is_same_v<T, RecordView>) {
      return value.record_view;
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
      type = RTAnyType::kTimestamp;
      value.date_val = val;
    } else if constexpr (std::is_same_v<T, RecordView>) {
      type = RTAnyType::kRecordView;
      value.record_view = val;
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
    } else if (type == RTAnyType::kDate32) {
      return value.day_val.to_string();
    } else if (type == RTAnyType::kTimestamp) {
      return std::to_string(value.date_val.milli_second);
    } else if (type == RTAnyType::kEmpty) {
      return "";
    } else if (type == RTAnyType::kRecordView) {
      return value.record_view.to_string();
    } else {
      LOG(FATAL) << "Unexpected property type: " << static_cast<int>(type);
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
      type = RTAnyType::kTimestamp;
      value.date_val = any.value.d;
      break;
    case impl::PropertyTypeImpl::kRecordView:
      type = RTAnyType::kRecordView;
      value.record_view = any.value.record_view;
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
    } else if (type == RTAnyType::kTimestamp) {
      return value.date_val < e.value.date_val;
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
      return value.day_val == e.value.day_val;
    } else if (type == RTAnyType::kTimestamp) {
      return value.date_val == e.value.date_val;
    } else if (type == RTAnyType::kRecordView) {
      return value.record_view == e.value.record_view;
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
    Day day_val;
    RecordView record_view;
    // todo: make recordview as a pod type
    // RecordView record;
  } value;
};
class EdgeRecord {
 public:
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
inline RTAnyType parse_from_ir_data_type(const ::common::IrDataType& dt) {
  switch (dt.type_case()) {
  case ::common::IrDataType::TypeCase::kDataType: {
    const ::common::DataType ddt = dt.data_type();
    switch (ddt.item_case()) {
    case ::common::DataType::kPrimitiveType: {
      const ::common::PrimitiveType pt = ddt.primitive_type();
      switch (pt) {
      case ::common::PrimitiveType::DT_SIGNED_INT32:
        return RTAnyType::kI32Value;
      case ::common::PrimitiveType::DT_SIGNED_INT64:
        return RTAnyType::kI64Value;
      case ::common::PrimitiveType::DT_DOUBLE:
        return RTAnyType::kF64Value;
      case ::common::PrimitiveType::DT_BOOL:
        return RTAnyType::kBoolValue;
      case ::common::PrimitiveType::DT_ANY:
        return RTAnyType::kUnknown;
      default:
        LOG(FATAL) << "unrecognized primitive type - " << pt;
        break;
      }
    }
    case ::common::DataType::kString:
      return RTAnyType::kStringValue;
    case ::common::DataType::kTemporal: {
      if (ddt.temporal().item_case() == ::common::Temporal::kDate32) {
        return RTAnyType::kDate32;
      } else if (ddt.temporal().item_case() == ::common::Temporal::kTimestamp) {
        return RTAnyType::kTimestamp;
      } else {
        LOG(FATAL) << "unrecognized temporal type - "
                   << ddt.temporal().DebugString();
      }
    }
    case ::common::DataType::kArray:
      return RTAnyType::kList;
    default:
      LOG(FATAL) << "unrecognized data type - " << ddt.DebugString();
      break;
    }
  } break;
  case ::common::IrDataType::TypeCase::kGraphType: {
    const ::common::GraphDataType gdt = dt.graph_type();
    switch (gdt.element_opt()) {
    case ::common::GraphDataType_GraphElementOpt::
        GraphDataType_GraphElementOpt_VERTEX:
      return RTAnyType::kVertex;
    case ::common::GraphDataType_GraphElementOpt::
        GraphDataType_GraphElementOpt_EDGE:
      return RTAnyType::kEdge;
    default:
      LOG(FATAL) << "unrecognized graph data type";
      break;
    }
  } break;
  default:
    break;
  }
  return RTAnyType::kUnknown;
}

union RTAnyValue {
  // TODO delete it later
  RTAnyValue() {}
  ~RTAnyValue() {}
  VertexRecord vertex;
  EdgeRecord edge;
  Relation relation;
  int64_t i64_val;
  uint64_t u64_val;
  int i32_val;
  double f64_val;
  Day day;
  Date date;
  std::string_view str_val;
  Path p;
  Tuple t;
  List list;
  Map map;
  Set set;
  bool b_val;
};

class RTAny {
 public:
  RTAny();
  RTAny(RTAnyType type);
  RTAny(const Any& val);
  Any to_any() const;
  RTAny(const EdgeData& val);
  RTAny(const RTAny& rhs);
  RTAny(const Path& p);
  ~RTAny() = default;
  bool is_null() const { return type_ == RTAnyType::kNull; }

  int numerical_cmp(const RTAny& other) const;

  RTAny& operator=(const RTAny& rhs);

  static RTAny from_vertex(label_t l, vid_t v);
  static RTAny from_vertex(VertexRecord v);
  static RTAny from_edge(const EdgeRecord& v);

  static RTAny from_relation(const Relation& r);
  static RTAny from_bool(bool v);
  static RTAny from_int64(int64_t v);
  static RTAny from_uint64(uint64_t v);
  static RTAny from_int32(int v);
  static RTAny from_string(const std::string& str);
  static RTAny from_string(const std::string_view& str);
  static RTAny from_date32(Day v);
  static RTAny from_timestamp(Date v);

  static RTAny from_tuple(const Tuple& tuple);
  static RTAny from_list(const List& list);
  static RTAny from_double(double v);
  static RTAny from_map(const Map& m);
  static RTAny from_set(const Set& s);

  bool as_bool() const;
  int as_int32() const;
  int64_t as_int64() const;
  uint64_t as_uint64() const;
  Day as_date32() const;
  Date as_timestamp() const;
  double as_double() const;
  VertexRecord as_vertex() const;
  const EdgeRecord& as_edge() const;
  std::string_view as_string() const;
  Path as_path() const;
  Tuple as_tuple() const;
  List as_list() const;
  Map as_map() const;
  Set as_set() const;
  Relation as_relation() const;

  bool operator<(const RTAny& other) const;
  bool operator==(const RTAny& other) const;

  RTAny operator+(const RTAny& other) const;

  RTAny operator-(const RTAny& other) const;
  RTAny operator/(const RTAny& other) const;
  RTAny operator%(const RTAny& other) const;

  void sink(const GraphReadInterface& graph, int id,
            results::Column* column) const;

  template <typename GraphInterface>
  void sink(const GraphInterface& graph, Encoder& encoder) const {
    if (type_ == RTAnyType::kList) {
      encoder.put_int(value_.list.size());
      for (size_t i = 0; i < value_.list.size(); ++i) {
        value_.list.get(i).sink(graph, encoder);
      }
    } else if (type_ == RTAnyType::kTuple) {
      for (size_t i = 0; i < value_.t.size(); ++i) {
        value_.t.get(i).sink(graph, encoder);
      }
    } else if (type_ == RTAnyType::kStringValue) {
      encoder.put_string_view(value_.str_val);
    } else if (type_ == RTAnyType::kI64Value) {
      encoder.put_long(value_.i64_val);
    } else if (type_ == RTAnyType::kDate32) {
      encoder.put_long(value_.day.to_timestamp());
    } else if (type_ == RTAnyType::kTimestamp) {
      encoder.put_long(value_.date.milli_second);
    } else if (type_ == RTAnyType::kI32Value) {
      encoder.put_int(value_.i32_val);
    } else if (type_ == RTAnyType::kF64Value) {
      int64_t long_value;
      std::memcpy(&long_value, &value_.f64_val, sizeof(long_value));
      encoder.put_long(long_value);
    } else if (type_ == RTAnyType::kBoolValue) {
      encoder.put_byte(value_.b_val ? static_cast<uint8_t>(1)
                                    : static_cast<uint8_t>(0));
    } else if (type_ == RTAnyType::kSet) {
      encoder.put_int(value_.set.size());
      auto value = value_.set.values();
      for (const auto& val : value) {
        val.sink(graph, encoder);
      }
    } else if (type_ == RTAnyType::kVertex) {
      encoder.put_byte(value_.vertex.label_);
      encoder.put_int(value_.vertex.vid_);
    } else {
      LOG(FATAL) << "not support for " << static_cast<int>(type_);
    }
  }

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
struct TypedConverter<int32_t> {
  static RTAnyType type() { return RTAnyType::kI32Value; }
  static int32_t to_typed(const RTAny& val) { return val.as_int32(); }
  static RTAny from_typed(int val) { return RTAny::from_int32(val); }
  static const std::string name() { return "int"; }
  static int32_t typed_from_string(const std::string& str) {
    return std::stoi(str);
  }
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
  static int64_t typed_from_string(const std::string& str) {
    return std::stoll(str);
  }
};

template <>
struct TypedConverter<double> {
  static RTAnyType type() { return RTAnyType::kF64Value; }
  static double to_typed(const RTAny& val) { return val.as_double(); }
  static RTAny from_typed(double val) { return RTAny::from_double(val); }
  static double typed_from_string(const std::string& str) {
    return std::stod(str);
  }
  static const std::string name() { return "double"; }
};
template <>
struct TypedConverter<Day> {
  static RTAnyType type() { return RTAnyType::kDate32; }
  static Day to_typed(const RTAny& val) { return val.as_date32(); }
  static RTAny from_typed(Day val) { return RTAny::from_date32(val); }
  static const std::string name() { return "day"; }
  static Day typed_from_string(const std::string& str) {
    int64_t val = std::stoll(str);
    return Day(val);
  }
};

template <>
struct TypedConverter<Date> {
  static RTAnyType type() { return RTAnyType::kTimestamp; }
  static Date to_typed(const RTAny& val) { return val.as_timestamp(); }
  static RTAny from_typed(Date val) { return RTAny::from_timestamp(val); }
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

template <>
struct TypedConverter<Set> {
  static RTAnyType type() { return RTAnyType::kSet; }
  static Set to_typed(const RTAny& val) { return val.as_set(); }
  static RTAny from_typed(Set val) { return RTAny::from_set(val); }
  static const std::string name() { return "set"; }
};
template <>
struct TypedConverter<Relation> {
  static RTAnyType type() { return RTAnyType::kRelation; }
  static Relation to_typed(const RTAny& val) { return val.as_relation(); }
  static RTAny from_typed(const Relation& val) {
    return RTAny::from_relation(val);
  }
  static const std::string name() { return "relation"; }
};

template <>
struct TypedConverter<VertexRecord> {
  static RTAnyType type() { return RTAnyType::kVertex; }
  static VertexRecord to_typed(const RTAny& val) { return val.as_vertex(); }
  static RTAny from_typed(VertexRecord val) { return RTAny::from_vertex(val); }
  static const std::string name() { return "vertex"; }
};

template <typename... Args>
RTAny TupleImpl<Args...>::get(size_t idx) const {
  if constexpr (sizeof...(Args) == 2) {
    if (idx == 0) {
      return TypedConverter<std::tuple_element_t<0, std::tuple<Args...>>>::
          from_typed(std::get<0>(values));
    } else if (idx == 1) {
      return TypedConverter<std::tuple_element_t<1, std::tuple<Args...>>>::
          from_typed(std::get<1>(values));
    } else {
      return RTAny(RTAnyType::kNull);
    }
  } else if constexpr (sizeof...(Args) == 3) {
    if (idx == 0) {
      return TypedConverter<std::tuple_element_t<0, std::tuple<Args...>>>::
          from_typed(std::get<0>(values));
    } else if (idx == 1) {
      return TypedConverter<std::tuple_element_t<1, std::tuple<Args...>>>::
          from_typed(std::get<1>(values));
    } else if (idx == 2) {
      return TypedConverter<std::tuple_element_t<2, std::tuple<Args...>>>::
          from_typed(std::get<2>(values));
    } else {
      return RTAny(RTAnyType::kNull);
    }
  } else {
    return RTAny(RTAnyType::kNull);
  }
}

template <typename T>
class ListImpl : ListImplBase {
 public:
  ListImpl() = default;
  static std::unique_ptr<ListImplBase> make_list_impl(std::vector<T>&& vals) {
    auto new_list = new ListImpl<T>();
    new_list->list_ = std::move(vals);
    new_list->is_valid_.resize(new_list->list_.size(), true);
    return std::unique_ptr<ListImplBase>(static_cast<ListImplBase*>(new_list));
  }

  bool operator<(const ListImplBase& p) const {
    return list_ < (dynamic_cast<const ListImpl<T>&>(p)).list_;
  }
  bool operator==(const ListImplBase& p) const {
    return list_ == (dynamic_cast<const ListImpl<T>&>(p)).list_;
  }
  size_t size() const { return list_.size(); }
  RTAnyType type() const override { return TypedConverter<T>::type(); }
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

template <typename T>
class SetImpl : public SetImplBase {
 public:
  SetImpl() = default;
  ~SetImpl() {}
  static std::unique_ptr<SetImplBase> make_set_impl(std::set<T>&& vals) {
    auto new_set = new SetImpl<T>();
    new_set->set_ = std::move(vals);
    return std::unique_ptr<SetImplBase>(static_cast<SetImplBase*>(new_set));
  }

  RTAnyType type() const override { return TypedConverter<T>::type(); }

  bool exists(const RTAny& val) const override {
    return set_.find(TypedConverter<T>::to_typed(val)) != set_.end();
  }
  bool exists(const T& val) const { return set_.find(val) != set_.end(); }

  bool operator<(const SetImplBase& p) const override {
    return set_ < (dynamic_cast<const SetImpl<T>&>(p)).set_;
  }

  bool operator==(const SetImplBase& p) const override {
    return set_ == (dynamic_cast<const SetImpl<T>&>(p)).set_;
  }

  void insert(const RTAny& val) override {
    set_.insert(TypedConverter<T>::to_typed(val));
  }
  void insert(const T& val) { set_.insert(val); }
  size_t size() const override { return set_.size(); }

  std::vector<RTAny> values() const override {
    std::vector<RTAny> res;
    for (const auto& v : set_) {
      res.push_back(TypedConverter<T>::from_typed(v));
    }
    return res;
  }
  std::set<T> set_;
};

template <>
class SetImpl<VertexRecord> : public SetImplBase {
 public:
  SetImpl() = default;
  ~SetImpl() {}

  static std::unique_ptr<SetImplBase> make_set_impl(
      std::set<VertexRecord>&& vals) {
    auto new_set = new SetImpl<VertexRecord>();
    for (auto& v : vals) {
      new_set->set_.insert((1ll * v.vid_) << 8 | v.label_);
    }
    return std::unique_ptr<SetImplBase>(static_cast<SetImplBase*>(new_set));
  }

  std::vector<RTAny> values() const override {
    std::vector<RTAny> res;
    for (auto& v : set_) {
      res.push_back(RTAny::from_vertex(VertexRecord{
          static_cast<label_t>(v & 0xff), static_cast<vid_t>(v >> 8)}));
    }
    return res;
  }

  RTAnyType type() const override { return RTAnyType::kVertex; }

  bool exists(const RTAny& val) const override {
    auto v = TypedConverter<VertexRecord>::to_typed(val);
    return set_.find((1ll * v.vid_) << 8 | v.label_) != set_.end();
  }
  bool exists(VertexRecord val) const {
    return set_.find((1ll * val.vid_) << 8 | val.label_) != set_.end();
  }

  bool operator<(const SetImplBase& p) const override {
    LOG(ERROR) << "not support for set of pair";
    return set_.size() <
           (dynamic_cast<const SetImpl<VertexRecord>&>(p)).set_.size();
  }

  bool operator==(const SetImplBase& p) const override {
    return set_ == (dynamic_cast<const SetImpl<VertexRecord>&>(p)).set_;
  }

  void insert(const RTAny& val) override {
    insert(TypedConverter<VertexRecord>::to_typed(val));
  }
  void insert(VertexRecord val) {
    set_.insert((1ll * val.vid_) << 8 | val.label_);
  }
  size_t size() const override { return set_.size(); }
  std::unordered_set<int64_t> set_;
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
    assert(dynamic_cast<EdgePropVec<T>*>(other) != nullptr);
    set(idx, dynamic_cast<EdgePropVec<T>*>(other)->get_view(other_idx));
  }

 private:
  std::vector<T> prop_data_;
};

template <>
class EdgePropVec<grape::EmptyType> : public EdgePropVecBase {
 public:
  EdgePropVec() : size_(0) {}
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
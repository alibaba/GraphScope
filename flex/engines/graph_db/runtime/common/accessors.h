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

#ifndef RUNTIME_COMMON_ACCESSORS_H_
#define RUNTIME_COMMON_ACCESSORS_H_

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/common/columns/edge_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/path_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/rt_any.h"

namespace gs {

namespace runtime {

class IAccessor {
 public:
  virtual ~IAccessor() = default;
  virtual RTAny eval_path(size_t idx) const = 0;
  virtual RTAny eval_vertex(label_t label, vid_t v, size_t idx) const {
    return this->eval_path(idx);
  }
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const Any& data, size_t idx) const {
    return this->eval_path(idx);
  }

  virtual RTAny eval_path(size_t idx, int) const {
    return this->eval_path(idx);
  }
  virtual RTAny eval_vertex(label_t label, vid_t v, size_t idx, int) const {
    return this->eval_vertex(label, v, idx);
  }
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const Any& data, size_t idx, int) const {
    return this->eval_edge(label, src, dst, data, idx);
  }

  virtual bool is_optional() const { return false; }

  virtual std::string name() const { return "unknown"; }

  virtual std::shared_ptr<IContextColumnBuilder> builder() const {
    // LOG(FATAL) << "not implemented for " << this->name();
    return nullptr;
  }
};

class VertexPathAccessor : public IAccessor {
 public:
  using elem_t = std::pair<label_t, vid_t>;

  VertexPathAccessor(const Context& ctx, int tag)
      : vertex_col_(*std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag))) {}

  bool is_optional() const override { return vertex_col_.is_optional(); }

  elem_t typed_eval_path(size_t idx) const {
    return vertex_col_.get_vertex(idx);
  }

  RTAny eval_path(size_t idx) const override {
    return RTAny::from_vertex(typed_eval_path(idx));
  }

  RTAny eval_path(size_t idx, int) const override {
    if (!vertex_col_.has_value(idx)) {
      return RTAny(RTAnyType::kNull);
    }
    return RTAny::from_vertex(typed_eval_path(idx));
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return vertex_col_.builder();
  }

 private:
  const IVertexColumn& vertex_col_;
};

template <typename KEY_T>
class VertexIdPathAccessor : public IAccessor {
 public:
  using elem_t = KEY_T;
  VertexIdPathAccessor(const ReadTransaction& txn, const Context& ctx, int tag)
      : txn_(txn),
        vertex_col_(*std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag))) {}

  bool is_optional() const override { return vertex_col_.is_optional(); }

  elem_t typed_eval_path(size_t idx) const {
    const auto& v = vertex_col_.get_vertex(idx);
    return AnyConverter<KEY_T>::from_any(txn_.GetVertexId(v.first, v.second));
  }

  RTAny eval_path(size_t idx) const override {
    return RTAny(typed_eval_path(idx));
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return vertex_col_.builder();
  }

 private:
  const ReadTransaction& txn_;
  const IVertexColumn& vertex_col_;
};

class VertexGIdPathAccessor : public IAccessor {
 public:
  using elem_t = int64_t;
  VertexGIdPathAccessor(const Context& ctx, int tag)
      : vertex_col_(*std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag))) {}

  bool is_optional() const override { return vertex_col_.is_optional(); }

  elem_t typed_eval_path(size_t idx) const {
    const auto& v = vertex_col_.get_vertex(idx);
    return encode_unique_vertex_id(v.first, v.second);
  }

  RTAny eval_path(size_t idx) const override {
    return RTAny::from_int64(typed_eval_path(idx));
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return vertex_col_.builder();
  }

 private:
  const IVertexColumn& vertex_col_;
};

template <typename T>
class VertexPropertyPathAccessor : public IAccessor {
 public:
  using elem_t = T;
  VertexPropertyPathAccessor(const ReadTransaction& txn, const Context& ctx,
                             int tag, const std::string& prop_name)
      : vertex_col_(*std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag))) {
    int label_num = txn.schema().vertex_label_num();
    property_columns_.resize(label_num, nullptr);
    for (int i = 0; i < label_num; ++i) {
      property_columns_[i] = dynamic_cast<const TypedColumn<elem_t>*>(
          txn.get_vertex_property_column(static_cast<label_t>(i), prop_name)
              .get());
    }
  }

  bool is_optional() const override {
    if (vertex_col_.is_optional()) {
      return true;
    }
    auto label_set = vertex_col_.get_labels_set();
    for (auto label : label_set) {
      if (property_columns_[label] == nullptr) {
        return true;
      }
    }
    return false;
  }

  elem_t typed_eval_path(size_t idx) const {
    const auto& v = vertex_col_.get_vertex(idx);
    auto col_ptr = property_columns_[v.first];
    if (col_ptr != nullptr) {
      return property_columns_[v.first]->get_view(v.second);
    } else {
      return elem_t();
    }
  }

  RTAny eval_path(size_t idx) const override {
    auto val = TypedConverter<T>::from_typed(typed_eval_path(idx));
    return val;
  }

  RTAny eval_path(size_t idx, int) const override {
    if (!vertex_col_.has_value(idx)) {
      return RTAny(RTAnyType::kNull);
    }
    const auto& v = vertex_col_.get_vertex(idx);
    auto col_ptr = property_columns_[v.first];
    if (col_ptr != nullptr) {
      return TypedConverter<T>::from_typed(col_ptr->get_view(v.second));
    } else {
      return RTAny(RTAnyType::kNull);
    }
  }

 private:
  const IVertexColumn& vertex_col_;
  std::vector<const TypedColumn<elem_t>*> property_columns_;
};

class VertexLabelPathAccessor : public IAccessor {
 public:
  using elem_t = int32_t;
  VertexLabelPathAccessor(const Context& ctx, int tag)
      : vertex_col_(*std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag))) {}

  elem_t typed_eval_path(size_t idx) const {
    return static_cast<int32_t>(vertex_col_.get_vertex(idx).first);
  }

  RTAny eval_path(size_t idx) const override {
    return RTAny(static_cast<int32_t>(typed_eval_path(idx)));
  }

 private:
  const IVertexColumn& vertex_col_;
};

class VertexLabelVertexAccessor : public IAccessor {
 public:
  using elem_t = int64_t;
  VertexLabelVertexAccessor() {}

  elem_t typed_eval_vertex(label_t label, vid_t v, size_t idx) const {
    return static_cast<int64_t>(label);
  }

  RTAny eval_path(size_t idx) const override {
    LOG(FATAL) << "not supposed to reach here...";
    return RTAny();
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    return RTAny::from_int64(label);
  }
};
template <typename T>
class ContextValueAccessor : public IAccessor {
 public:
  using elem_t = T;
  ContextValueAccessor(const Context& ctx, int tag)
      : col_(*std::dynamic_pointer_cast<IValueColumn<elem_t>>(ctx.get(tag))) {}

  elem_t typed_eval_path(size_t idx) const { return col_.get_value(idx); }

  RTAny eval_path(size_t idx) const override { return col_.get_elem(idx); }

  bool is_optional() const override { return col_.is_optional(); }

  RTAny eval_path(size_t idx, int) const override {
    if (!col_.has_value(idx)) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx);
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return col_.builder();
  }

 private:
  const IValueColumn<elem_t>& col_;
};

template <typename KEY_T>
class VertexIdVertexAccessor : public IAccessor {
 public:
  using elem_t = KEY_T;
  VertexIdVertexAccessor(const ReadTransaction& txn) : txn_(txn) {}

  elem_t typed_eval_vertex(label_t label, vid_t v, size_t idx) const {
    return AnyConverter<KEY_T>::from_any(txn_.GetVertexId(label, v));
  }

  RTAny eval_path(size_t idx) const override {
    LOG(FATAL) << "not supposed to reach here...";
    return RTAny();
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    return RTAny(Any(typed_eval_vertex(label, v, idx)));
  }

 private:
  const ReadTransaction& txn_;
};

class VertexGIdVertexAccessor : public IAccessor {
 public:
  using elem_t = int64_t;
  VertexGIdVertexAccessor() {}

  elem_t typed_eval_vertex(label_t label, vid_t v, size_t idx) const {
    return encode_unique_vertex_id(label, v);
  }

  RTAny eval_path(size_t idx) const override {
    LOG(FATAL) << "not supposed to reach here...";
    return RTAny();
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    return RTAny::from_int64(typed_eval_vertex(label, v, idx));
  }
};

template <typename T>
class VertexPropertyVertexAccessor : public IAccessor {
 public:
  using elem_t = T;
  VertexPropertyVertexAccessor(const ReadTransaction& txn,
                               const std::string& prop_name) {
    int label_num = txn.schema().vertex_label_num();
    property_columns_.resize(label_num, nullptr);
    for (int i = 0; i < label_num; ++i) {
      property_columns_[i] = dynamic_cast<const TypedColumn<elem_t>*>(
          txn.get_vertex_property_column(static_cast<label_t>(i), prop_name)
              .get());
    }
  }

  elem_t typed_eval_vertex(label_t label, vid_t v, size_t idx) const {
    if (property_columns_[label] == nullptr) {
      return elem_t();
    }
    return property_columns_[label]->get_view(v);
  }

  RTAny eval_path(size_t idx) const override {
    LOG(FATAL) << "not supposed to reach here...";
    return RTAny();
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx) const override {
    if (property_columns_[label] == nullptr) {
      return RTAny();
    }
    return TypedConverter<T>::from_typed(property_columns_[label]->get_view(v));
  }

  RTAny eval_vertex(label_t label, vid_t v, size_t idx, int) const override {
    if (property_columns_[label] == nullptr) {
      return RTAny(RTAnyType::kNull);
    }
    return TypedConverter<T>::from_typed(property_columns_[label]->get_view(v));
  }

  bool is_optional() const override {
    for (auto col : property_columns_) {
      if (col == nullptr) {
        return true;
      }
    }

    return false;
  }

 private:
  std::vector<const TypedColumn<elem_t>*> property_columns_;
};

class EdgeIdPathAccessor : public IAccessor {
 public:
  using elem_t = std::tuple<LabelTriplet, vid_t, vid_t, Any, Direction>;
  EdgeIdPathAccessor(const Context& ctx, int tag)
      : edge_col_(*std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag))) {}

  elem_t typed_eval_path(size_t idx) const { return edge_col_.get_edge(idx); }

  RTAny eval_path(size_t idx) const override {
    return RTAny::from_edge(typed_eval_path(idx));
  }

  bool is_optional() const override { return edge_col_.is_optional(); }

  RTAny eval_path(size_t idx, int) const override {
    if (!edge_col_.has_value(idx)) {
      return RTAny(RTAnyType::kNull);
    }
    return RTAny::from_edge(typed_eval_path(idx));
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return edge_col_.builder();
  }

 private:
  const IEdgeColumn& edge_col_;
};

template <typename T>
class EdgePropertyPathAccessor : public IAccessor {
 public:
  using elem_t = T;
  EdgePropertyPathAccessor(const ReadTransaction& txn,
                           const std::string& prop_name, const Context& ctx,
                           int tag)
      : col_(*std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag))) {}

  RTAny eval_path(size_t idx) const override {
    const auto& e = col_.get_edge(idx);
    return RTAny(std::get<3>(e));
  }

  elem_t typed_eval_path(size_t idx) const {
    const auto& e = col_.get_edge(idx);
    elem_t ret;
    ConvertAny<T>::to(std::get<3>(e), ret);
    return ret;
  }

  bool is_optional() const override { return col_.is_optional(); }

  RTAny eval_path(size_t idx, int) const override {
    if (!col_.has_value(idx)) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx);
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return col_.builder();
  }

 private:
  const IEdgeColumn& col_;
};

template <typename T>
class MultiPropsEdgePropertyPathAccessor : public IAccessor {
 public:
  using elem_t = T;
  MultiPropsEdgePropertyPathAccessor(const ReadTransaction& txn,
                                     const std::string& prop_name,
                                     const Context& ctx, int tag)
      : col_(*std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag))) {
    const auto& labels = col_.get_labels();
    vertex_label_num_ = txn.schema().vertex_label_num();
    edge_label_num_ = txn.schema().edge_label_num();
    prop_index_.resize(
        2 * vertex_label_num_ * vertex_label_num_ * edge_label_num_,
        std::numeric_limits<size_t>::max());
    for (auto& label : labels) {
      size_t idx = label.src_label * vertex_label_num_ * edge_label_num_ +
                   label.dst_label * edge_label_num_ + label.edge_label;
      const auto& names = txn.schema().get_edge_property_names(
          label.src_label, label.dst_label, label.edge_label);
      for (size_t i = 0; i < names.size(); ++i) {
        if (names[i] == prop_name) {
          prop_index_[idx] = i;
          break;
        }
      }
    }
  }

  RTAny eval_path(size_t idx) const override {
    const auto& e = col_.get_edge(idx);
    auto val = std::get<3>(e);
    auto id = get_index(std::get<0>(e));
    if (std::get<3>(e).type != PropertyType::RecordView()) {
      CHECK(id == 0);
      return RTAny(val);
    } else {
      auto rv = val.AsRecordView();
      CHECK(id != std::numeric_limits<size_t>::max());
      return RTAny(rv[id]);
    }
  }

  elem_t typed_eval_path(size_t idx) const {
    const auto& e = col_.get_edge(idx);
    auto val = std::get<3>(e);
    auto id = get_index(std::get<0>(e));
    if (std::get<3>(e).type != PropertyType::RecordView()) {
      CHECK(id == 0);
      elem_t ret;
      ConvertAny<T>::to(val, ret);
      return ret;

    } else {
      auto rv = val.AsRecordView();
      CHECK(id != std::numeric_limits<size_t>::max());
      auto tmp = rv[id];
      elem_t ret;
      ConvertAny<T>::to(tmp, ret);
      return ret;
    }
  }

  bool is_optional() const override { return col_.is_optional(); }

  size_t get_index(const LabelTriplet& label) const {
    size_t idx = label.src_label * vertex_label_num_ * edge_label_num_ +
                 label.dst_label * edge_label_num_ + label.edge_label;
    return prop_index_[idx];
  }

  RTAny eval_path(size_t idx, int) const override {
    if (!col_.has_value(idx)) {
      return RTAny(RTAnyType::kNull);
    }
    return eval_path(idx);
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return col_.builder();
  }

 private:
  const IEdgeColumn& col_;
  std::vector<size_t> prop_index_;
  size_t vertex_label_num_;
  size_t edge_label_num_;
};

class EdgeLabelPathAccessor : public IAccessor {
 public:
  using elem_t = int32_t;
  EdgeLabelPathAccessor(const Context& ctx, int tag)
      : col_(*std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag))) {}

  RTAny eval_path(size_t idx) const override {
    const auto& e = col_.get_edge(idx);
    return RTAny(static_cast<int32_t>(std::get<0>(e).edge_label));
  }

  elem_t typed_eval_path(size_t idx) const {
    const auto& e = col_.get_edge(idx);
    return static_cast<int32_t>(std::get<0>(e).edge_label);
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return col_.builder();
  }

 private:
  const IEdgeColumn& col_;
};

class EdgeLabelEdgeAccessor : public IAccessor {
 public:
  using elem_t = int32_t;
  EdgeLabelEdgeAccessor() {}

  elem_t typed_eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& data, size_t idx) const {
    return static_cast<elem_t>(label.edge_label);
  }

  RTAny eval_path(size_t idx) const override {
    LOG(FATAL) << "not supposed to reach here...";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    return RTAny::from_int32(typed_eval_edge(label, src, dst, data, idx));
  }
};

template <typename T>
class EdgePropertyEdgeAccessor : public IAccessor {
 public:
  using elem_t = T;
  EdgePropertyEdgeAccessor(const ReadTransaction& txn,
                           const std::string& name) {}

  elem_t typed_eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& data, size_t idx) const {
    T ret;
    ConvertAny<T>::to(data, ret);
    return ret;
  }

  RTAny eval_path(size_t idx) const override {
    LOG(FATAL) << "not supposed to reach here...";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    return RTAny(data);
  }
};

// Access the global edge id of an edge in a path
// Currently we have no unique id for a edge.
// We construct the id from the edge's src, dst and label.
class EdgeGlobalIdPathAccessor : public IAccessor {
 public:
  using elem_t = int64_t;  // edge global id
  EdgeGlobalIdPathAccessor(const Context& ctx, int tag)
      : edge_col_(*std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag))) {}

  static uint32_t generate_edge_label_id(label_t src_label_id,
                                         label_t dst_label_id,
                                         label_t edge_label_id) {
    uint32_t unique_edge_label_id = src_label_id;
    static constexpr int num_bits = sizeof(label_t) * 8;
    unique_edge_label_id = unique_edge_label_id << num_bits;
    unique_edge_label_id = unique_edge_label_id | dst_label_id;
    unique_edge_label_id = unique_edge_label_id << num_bits;
    unique_edge_label_id = unique_edge_label_id | edge_label_id;
    return unique_edge_label_id;
  }

  static int64_t encode_unique_edge_id(uint32_t label_id, vid_t src,
                                       vid_t dst) {
    // We assume label_id is only used by 24 bits.
    int64_t unique_edge_id = label_id;
    unique_edge_id = unique_edge_id << 40;
    // bitmask for top 40 bits set to 1
    int64_t bitmask = 0xFFFFFFFFFF000000;
    // 24 bit | 20 bit | 20 bit
    if (bitmask & (int64_t) src || bitmask & (int64_t) dst) {
      LOG(ERROR) << "src or dst is too large to be encoded in 20 bits: " << src
                 << " " << dst;
    }
    unique_edge_id = unique_edge_id | (src << 20);
    unique_edge_id = unique_edge_id | dst;
    return unique_edge_id;
  }

  elem_t typed_eval_path(size_t idx) const {
    const auto& e = edge_col_.get_edge(idx);
    auto label_id = generate_edge_label_id(std::get<0>(e).src_label,
                                           std::get<0>(e).dst_label,
                                           std::get<0>(e).edge_label);
    return encode_unique_edge_id(label_id, std::get<1>(e), std::get<2>(e));
  }

  RTAny eval_path(size_t idx) const override {
    return RTAny::from_int64(typed_eval_path(idx));
  }

  bool is_optional() const override { return edge_col_.is_optional(); }

  RTAny eval_path(size_t idx, int) const override {
    if (!edge_col_.has_value(idx)) {
      return RTAny(RTAnyType::kNull);
    }
    return RTAny::from_int64(typed_eval_path(idx));
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return edge_col_.builder();
  }

 private:
  const IEdgeColumn& edge_col_;
};

class EdgeGlobalIdEdgeAccessor : public IAccessor {
 public:
  using elem_t = int64_t;  // edge global id
  EdgeGlobalIdEdgeAccessor() {}

  elem_t typed_eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& data, size_t idx) const {
    auto label_id = EdgeGlobalIdPathAccessor::generate_edge_label_id(
        label.src_label, label.dst_label, label.edge_label);
    return EdgeGlobalIdPathAccessor::encode_unique_edge_id(label_id, src, dst);
  }

  RTAny eval_path(size_t idx) const override {
    LOG(FATAL) << "not supposed to reach here...";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    return RTAny::from_int64(typed_eval_edge(label, src, dst, data, idx));
  }
};

template <typename T>
class MultiPropsEdgePropertyEdgeAccessor : public IAccessor {
 public:
  using elem_t = T;
  MultiPropsEdgePropertyEdgeAccessor(const ReadTransaction& txn,
                                     const std::string& name) {
    edge_label_num_ = txn.schema().edge_label_num();
    vertex_label_num_ = txn.schema().vertex_label_num();
    indexs.resize(2 * vertex_label_num_ * vertex_label_num_ * edge_label_num_,
                  std::numeric_limits<size_t>::max());
    for (label_t src_label = 0; src_label < vertex_label_num_; ++src_label) {
      auto src = txn.schema().get_vertex_label_name(src_label);
      for (label_t dst_label = 0; dst_label < vertex_label_num_; ++dst_label) {
        auto dst = txn.schema().get_vertex_label_name(dst_label);
        for (label_t edge_label = 0; edge_label < edge_label_num_;
             ++edge_label) {
          auto edge = txn.schema().get_edge_label_name(edge_label);
          if (!txn.schema().exist(src, dst, edge)) {
            continue;
          }
          size_t idx = src_label * vertex_label_num_ * edge_label_num_ +
                       dst_label * edge_label_num_ + edge_label;
          const std::vector<std::string>& names =
              txn.schema().get_edge_property_names(src_label, dst_label,
                                                   edge_label);
          for (size_t i = 0; i < names.size(); ++i) {
            if (names[i] == name) {
              indexs[idx] = i;
              break;
            }
          }
        }
      }
    }
  }

  elem_t typed_eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                         const Any& data, size_t idx) const {
    T ret;
    if (data.type != PropertyType::RecordView()) {
      CHECK(get_index(label) == 0);
      ConvertAny<T>::to(data, ret);
    } else {
      auto id = get_index(label);
      CHECK(id != std::numeric_limits<size_t>::max());
      auto view = data.AsRecordView();
      ConvertAny<T>::to(view[id], ret);
    }
    return ret;
  }

  RTAny eval_path(size_t idx) const override {
    LOG(FATAL) << "not supposed to reach here...";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const Any& data, size_t idx) const override {
    return RTAny(typed_eval_edge(label, src, dst, data, idx));
  }

  size_t get_index(const LabelTriplet& label) const {
    size_t idx = label.src_label * vertex_label_num_ * edge_label_num_ +
                 label.dst_label * edge_label_num_ + label.edge_label;
    return indexs[idx];
  }

 private:
  std::vector<size_t> indexs;
  size_t vertex_label_num_;
  size_t edge_label_num_;
};

template <typename T>
class ParamAccessor : public IAccessor {
 public:
  using elem_t = T;
  ParamAccessor(const std::map<std::string, std::string>& params,
                const std::string& key) {
    val_ = TypedConverter<T>::typed_from_string(params.at(key));
  }

  T typed_eval_path(size_t) const { return val_; }
  T typed_eval_vertex(label_t, vid_t, size_t) const { return val_; }
  T typed_eval_edge(const LabelTriplet&, vid_t, vid_t, const Any&,
                    size_t) const {
    return val_;
  }

  RTAny eval_path(size_t) const override {
    return TypedConverter<T>::from_typed(val_);
  }
  RTAny eval_vertex(label_t, vid_t, size_t) const override {
    return TypedConverter<T>::from_typed(val_);
  }
  RTAny eval_edge(const LabelTriplet&, vid_t, vid_t, const Any&,
                  size_t) const override {
    return TypedConverter<T>::from_typed(val_);
  }

 private:
  T val_;
};

class PathIdPathAccessor : public IAccessor {
 public:
  using elem_t = Path;
  PathIdPathAccessor(const Context& ctx, int tag)
      : path_col_(*std::dynamic_pointer_cast<IPathColumn>(ctx.get(tag))) {}

  elem_t typed_eval_path(size_t idx) const { return path_col_.get_path(idx); }

  RTAny eval_path(size_t idx) const override { return path_col_.get_elem(idx); }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return path_col_.builder();
  }

 private:
  const IPathColumn& path_col_;
};

class PathLenPathAccessor : public IAccessor {
 public:
  using elem_t = int32_t;
  PathLenPathAccessor(const Context& ctx, int tag)
      : path_col_(*std::dynamic_pointer_cast<IPathColumn>(ctx.get(tag))) {}

  elem_t typed_eval_path(size_t idx) const {
    return static_cast<int32_t>(path_col_.get_path(idx).len());
  }

  RTAny eval_path(size_t idx) const override {
    return RTAny(static_cast<int32_t>(typed_eval_path(idx)));
  }

 private:
  const IPathColumn& path_col_;
};

template <typename T>
class ConstAccessor : public IAccessor {
 public:
  using elem_t = T;
  ConstAccessor(const T& val) : val_(val) {}

  T typed_eval_path(size_t) const { return val_; }
  T typed_eval_vertex(label_t, vid_t, size_t) const { return val_; }
  T typed_eval_edge(const LabelTriplet&, vid_t, vid_t, const Any&,
                    size_t) const {
    return val_;
  }

  RTAny eval_path(size_t) const override {
    return TypedConverter<T>::from_typed(val_);
  }

  RTAny eval_vertex(label_t, vid_t, size_t) const override {
    return TypedConverter<T>::from_typed(val_);
  }

  RTAny eval_edge(const LabelTriplet&, vid_t, vid_t, const Any&,
                  size_t) const override {
    return TypedConverter<T>::from_typed(val_);
  }

 private:
  T val_;
};

std::shared_ptr<IAccessor> create_context_value_accessor(const Context& ctx,
                                                         int tag,
                                                         RTAnyType type);

std::shared_ptr<IAccessor> create_vertex_property_path_accessor(
    const ReadTransaction& txn, const Context& ctx, int tag, RTAnyType type,
    const std::string& prop_name);

std::shared_ptr<IAccessor> create_vertex_property_vertex_accessor(
    const ReadTransaction& txn, RTAnyType type, const std::string& prop_name);

std::shared_ptr<IAccessor> create_vertex_label_path_accessor(const Context& ctx,
                                                             int tag);

std::shared_ptr<IAccessor> create_edge_property_path_accessor(
    const ReadTransaction& txn, const std::string& prop_name,
    const Context& ctx, int tag, RTAnyType type);

std::shared_ptr<IAccessor> create_edge_label_path_accessor(const Context& ctx,
                                                           int tag);

std::shared_ptr<IAccessor> create_edge_label_edge_accessor();

std::shared_ptr<IAccessor> create_edge_global_id_path_accessor(
    const Context& ctx, int tag);

std::shared_ptr<IAccessor> create_edge_global_id_edge_accessor();

std::shared_ptr<IAccessor> create_edge_property_edge_accessor(
    const ReadTransaction& txn, const std::string& prop_name, RTAnyType type);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_ACCESSORS_H_
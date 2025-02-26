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
#ifndef RUNTIME_COMMON_COLUMNS_EDGE_COLUMNS_H_
#define RUNTIME_COMMON_COLUMNS_EDGE_COLUMNS_H_

#include "flex/engines/graph_db/runtime/common/columns/columns_utils.h"
#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/utils/property/column.h"

namespace gs {

namespace runtime {

enum class EdgeColumnType { kSDSL, kSDML, kBDSL, kBDML, kUnKnown };
static inline void get_edge_data(EdgePropVecBase* prop, size_t idx,
                                 EdgeData& edge_data) {
  if (prop->type() == PropertyType::kEmpty) {
    edge_data.type = RTAnyType::kEmpty;
  } else if (prop->type() == PropertyType::kInt64) {
    edge_data.type = RTAnyType::kI64Value;
    edge_data.value.i64_val =
        dynamic_cast<EdgePropVec<int64_t>*>(prop)->get_view(idx);
  } else if (prop->type() == PropertyType::kInt32) {
    edge_data.type = RTAnyType::kI32Value;
    edge_data.value.i32_val =
        dynamic_cast<EdgePropVec<int32_t>*>(prop)->get_view(idx);
  } else if (prop->type() == PropertyType::kDouble) {
    edge_data.type = RTAnyType::kF64Value;
    edge_data.value.f64_val =
        dynamic_cast<EdgePropVec<double>*>(prop)->get_view(idx);
  } else if (prop->type() == PropertyType::kBool) {
    edge_data.type = RTAnyType::kBoolValue;
    edge_data.value.b_val =
        dynamic_cast<EdgePropVec<bool>*>(prop)->get_view(idx);
  } else if (prop->type() == PropertyType::kString) {
    edge_data.type = RTAnyType::kStringValue;
    edge_data.value.str_val =
        dynamic_cast<EdgePropVec<std::string_view>*>(prop)->get_view(idx);

  } else if (prop->type() == PropertyType::kDate) {
    edge_data.type = RTAnyType::kTimestamp;
    edge_data.value.date_val =
        dynamic_cast<EdgePropVec<Date>*>(prop)->get_view(idx);
  } else if (prop->type() == PropertyType::kDay) {
    edge_data.type = RTAnyType::kDate32;
    edge_data.value.day_val =
        dynamic_cast<EdgePropVec<Day>*>(prop)->get_view(idx);
  } else if (prop->type() == PropertyType::kRecordView) {
    edge_data.type = RTAnyType::kRecordView;
    edge_data.value.record_view =
        dynamic_cast<EdgePropVec<RecordView>*>(prop)->get_view(idx);
  } else {
    edge_data.type = RTAnyType::kUnknown;
  }
}

static inline void set_edge_data(EdgePropVecBase* col, size_t idx,
                                 const EdgeData& edge_data) {
  if (edge_data.type == RTAnyType::kEmpty) {
    return;
  } else if (edge_data.type == RTAnyType::kI64Value) {
    dynamic_cast<EdgePropVec<int64_t>*>(col)->set(idx, edge_data.value.i64_val);
  } else if (edge_data.type == RTAnyType::kI32Value) {
    dynamic_cast<EdgePropVec<int32_t>*>(col)->set(idx, edge_data.value.i32_val);
  } else if (edge_data.type == RTAnyType::kF64Value) {
    dynamic_cast<EdgePropVec<double>*>(col)->set(idx, edge_data.value.f64_val);
  } else if (edge_data.type == RTAnyType::kBoolValue) {
    dynamic_cast<EdgePropVec<bool>*>(col)->set(idx, edge_data.value.b_val);
  } else if (edge_data.type == RTAnyType::kStringValue) {
    dynamic_cast<EdgePropVec<std::string_view>*>(col)->set(
        idx, std::string_view(edge_data.value.str_val.data(),
                              edge_data.value.str_val.size()));
  } else if (edge_data.type == RTAnyType::kTimestamp) {
    dynamic_cast<EdgePropVec<Date>*>(col)->set(idx, edge_data.value.date_val);
  } else if (edge_data.type == RTAnyType::kDate32) {
    dynamic_cast<EdgePropVec<Day>*>(col)->set(idx, edge_data.value.day_val);
  } else if (edge_data.type == RTAnyType::kRecordView) {
    dynamic_cast<EdgePropVec<RecordView>*>(col)->set(
        idx, edge_data.value.record_view);
  } else {
    // LOG(FATAL) << "not support for " << edge_data.type;
  }
}

class IEdgeColumn : public IContextColumn {
 public:
  IEdgeColumn() = default;
  virtual ~IEdgeColumn() = default;

  ContextColumnType column_type() const override {
    return ContextColumnType::kEdge;
  }

  virtual EdgeRecord get_edge(size_t idx) const = 0;

  inline RTAny get_elem(size_t idx) const override {
    return RTAny::from_edge(this->get_edge(idx));
  }

  inline RTAnyType elem_type() const override { return RTAnyType::kEdge; }
  virtual std::vector<LabelTriplet> get_labels() const = 0;
  virtual EdgeColumnType edge_column_type() const = 0;
};

class SDSLEdgeColumnBuilder;

template <typename T>
class SDSLEdgeColumnBuilderBeta;

class SDSLEdgeColumn : public IEdgeColumn {
 public:
  SDSLEdgeColumn(Direction dir, const LabelTriplet& label,
                 PropertyType prop_type,
                 const std::vector<PropertyType>& sub_types = {})
      : dir_(dir),
        label_(label),
        prop_type_(prop_type),
        prop_col_(EdgePropVecBase::make_edge_prop_vec(prop_type)) {}

  inline EdgeRecord get_edge(size_t idx) const override {
    EdgeRecord ret;
    ret.label_triplet_ = label_;
    ret.src_ = edges_[idx].first;
    ret.dst_ = edges_[idx].second;
    get_edge_data(prop_col_.get(), idx, ret.prop_);
    ret.dir_ = dir_;
    return ret;
  }

  inline size_t size() const override { return edges_.size(); }

  inline Direction dir() const { return dir_; }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    // TODO: dedup with property value
    ColumnsUtils::generate_dedup_offset(edges_, size(), offsets);
  }

  ISigColumn* generate_signature() const override {
    // TODO: dedup with property value
    std::map<std::pair<vid_t, vid_t>, size_t> edge_map;
    std::vector<size_t> sigs;
    for (size_t i = 0; i < size(); ++i) {
      if (edge_map.find(edges_[i]) == edge_map.end()) {
        edge_map[edges_[i]] = i;
      }
      sigs.push_back(edge_map[edges_[i]]);
    }
    return new SigColumn<size_t>(sigs);
  }

  std::string column_info() const override {
    return "SDSLEdgeColumn: label = " + label_.to_string() +
           ", dir = " + std::to_string((int) dir_) +
           ", size = " + std::to_string(edges_.size());
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  template <typename FUNC_T>
  void foreach_edge(const FUNC_T& func) const {
    size_t idx = 0;
    for (auto& e : edges_) {
      func(idx, label_, e.first, e.second, prop_col_->get(idx), dir_);
      ++idx;
    }
  }

  std::vector<LabelTriplet> get_labels() const override { return {label_}; }

  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kSDSL;
  }

 private:
  friend class SDSLEdgeColumnBuilder;
  template <typename _T>
  friend class SDSLEdgeColumnBuilderBeta;
  friend class OptionalSDSLEdgeColumn;
  Direction dir_;
  LabelTriplet label_;
  std::vector<std::pair<vid_t, vid_t>> edges_;
  PropertyType prop_type_;
  std::shared_ptr<EdgePropVecBase> prop_col_;
};

class OptionalSDSLEdgeColumn : public IEdgeColumn {
 public:
  OptionalSDSLEdgeColumn(Direction dir, const LabelTriplet& label,
                         PropertyType prop_type)
      : column_(dir, label, prop_type) {}

  inline EdgeRecord get_edge(size_t idx) const override {
    return column_.get_edge(idx);
  }

  inline size_t size() const override { return column_.size(); }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    column_.generate_dedup_offset(offsets);
  }

  ISigColumn* generate_signature() const override {
    return column_.generate_signature();
  }

  std::string column_info() const override {
    return "OptionalSDSLEdgeColumn: label = " + column_.label_.to_string() +
           ", dir = " + std::to_string((int) column_.dir_) +
           ", size = " + std::to_string(column_.edges_.size());
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  template <typename FUNC_T>
  void foreach_edge(const FUNC_T& func) const {
    size_t idx = 0;
    for (auto& e : column_.edges_) {
      func(idx, column_.label_, e.first, e.second, column_.prop_col_->get(idx),
           column_.dir_);
      ++idx;
    }
  }

  inline bool is_optional() const override { return true; }

  inline bool has_value(size_t idx) const override {
    return column_.edges_[idx].first != std::numeric_limits<vid_t>::max() &&
           column_.edges_[idx].second != std::numeric_limits<vid_t>::max();
  }

  std::vector<LabelTriplet> get_labels() const override {
    return {column_.label_};
  }

  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kSDSL;
  }

 private:
  friend class SDSLEdgeColumnBuilder;
  SDSLEdgeColumn column_;
};

class BDSLEdgeColumnBuilder;

class OptionalBDSLEdgeColumnBuilder;
class BDSLEdgeColumn : public IEdgeColumn {
 public:
  BDSLEdgeColumn(const LabelTriplet& label, PropertyType prop_type)
      : label_(label),
        prop_type_(prop_type),
        prop_col_(EdgePropVecBase::make_edge_prop_vec(prop_type)) {}

  inline EdgeRecord get_edge(size_t idx) const override {
    auto src = std::get<0>(edges_[idx]);
    auto dst = std::get<1>(edges_[idx]);
    auto dir = std::get<2>(edges_[idx]);
    EdgeRecord ret;
    ret.label_triplet_ = label_;
    ret.src_ = src;
    ret.dst_ = dst;
    get_edge_data(prop_col_.get(), idx, ret.prop_);
    ret.dir_ = (dir ? Direction::kOut : Direction::kIn);
    return ret;
  }

  inline size_t size() const override { return edges_.size(); }

  std::string column_info() const override {
    return "BDSLEdgeColumn: label = " + label_.to_string() +
           ", size = " + std::to_string(edges_.size());
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;
  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  template <typename FUNC_T>
  void foreach_edge(const FUNC_T& func) const {
    size_t idx = 0;
    for (auto& e : edges_) {
      func(idx, label_, std::get<0>(e), std::get<1>(e), prop_col_->get(idx),
           (std::get<2>(e) ? Direction::kOut : Direction::kIn));
      ++idx;
    }
  }

  std::vector<LabelTriplet> get_labels() const override { return {label_}; }

  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kBDSL;
  }

 private:
  friend class BDSLEdgeColumnBuilder;
  friend class OptionalBDSLEdgeColumn;
  LabelTriplet label_;
  std::vector<std::tuple<vid_t, vid_t, bool>> edges_;
  PropertyType prop_type_;
  std::shared_ptr<EdgePropVecBase> prop_col_;
};

class OptionalBDSLEdgeColumn : public IEdgeColumn {
 public:
  OptionalBDSLEdgeColumn(const LabelTriplet& label, PropertyType prop_type)
      : column_(label, prop_type) {}

  inline EdgeRecord get_edge(size_t idx) const override {
    return column_.get_edge(idx);
  }

  inline size_t size() const override { return column_.edges_.size(); }

  std::string column_info() const override {
    return "OptionalBDSLEdgeColumn: label = " + column_.label_.to_string() +
           ", size = " + std::to_string(column_.edges_.size());
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;
  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  template <typename FUNC_T>
  void foreach_edge(const FUNC_T& func) const {
    size_t idx = 0;
    for (auto& e : column_.edges_) {
      func(idx, column_.label_, std::get<0>(e), std::get<1>(e),
           column_.prop_col_->get(idx),
           (std::get<2>(e) ? Direction::kOut : Direction::kIn));
      ++idx;
    }
  }

  inline bool is_optional() const override { return true; }

  inline bool has_value(size_t idx) const override {
    return std::get<0>(column_.edges_[idx]) !=
               std::numeric_limits<vid_t>::max() &&
           std::get<1>(column_.edges_[idx]) !=
               std::numeric_limits<vid_t>::max();
  }

  std::vector<LabelTriplet> get_labels() const override {
    return {column_.label_};
  }

  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kBDSL;
  }

 private:
  friend class BDSLEdgeColumnBuilder;
  BDSLEdgeColumn column_;
};

class SDMLEdgeColumnBuilder;

class SDMLEdgeColumn : public IEdgeColumn {
 public:
  SDMLEdgeColumn(
      Direction dir,
      const std::vector<std::pair<LabelTriplet, PropertyType>>& labels)
      : dir_(dir) {
    size_t idx = 0;
    prop_cols_.resize(labels.size());
    for (const auto& label : labels) {
      edge_labels_.emplace_back(label);
      index_[label.first] = idx++;
      prop_cols_[index_[label.first]] =
          EdgePropVecBase::make_edge_prop_vec(label.second);
    }
  }

  inline EdgeRecord get_edge(size_t idx) const override {
    auto& e = edges_[idx];
    auto index = std::get<0>(e);
    auto label = edge_labels_[index].first;
    auto offset = std::get<3>(e);
    EdgeRecord ret;
    ret.label_triplet_ = label;
    ret.src_ = std::get<1>(e);
    ret.dst_ = std::get<2>(e);
    get_edge_data(prop_cols_[index].get(), offset, ret.prop_);
    ret.dir_ = dir_;
    return ret;
  }

  inline size_t size() const override { return edges_.size(); }

  std::string column_info() const override {
    std::stringstream ss{};

    for (size_t idx = 0; idx < edge_labels_.size(); ++idx) {
      auto label = edge_labels_[idx];
      if (idx != 0) {
        ss << ", ";
      }
      ss << label.first.to_string();
    }
    return "SDMLEdgeColumn: label = {" + ss.str() +
           "}, dir = " + std::to_string((int) dir_) +
           ", size = " + std::to_string(edges_.size());
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  template <typename FUNC_T>
  void foreach_edge(const FUNC_T& func) const {
    size_t idx = 0;
    for (auto& e : edges_) {
      auto index = std::get<0>(e);
      auto label = edge_labels_[index].first;
      auto offset = std::get<3>(e);
      func(idx, label, std::get<1>(e), std::get<2>(e),
           prop_cols_[index]->get(offset), dir_);
      ++idx;
    }
  }

  std::vector<LabelTriplet> get_labels() const override {
    std::vector<LabelTriplet> labels;
    for (auto& label : edge_labels_) {
      labels.push_back(label.first);
    }
    return labels;
  }

  inline Direction dir() const { return dir_; }
  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kSDML;
  }

 private:
  friend class SDMLEdgeColumnBuilder;
  friend class OptionalSDMLEdgeColumn;
  Direction dir_;
  std::map<LabelTriplet, label_t> index_;
  std::vector<std::pair<LabelTriplet, PropertyType>> edge_labels_;
  std::vector<std::tuple<label_t, vid_t, vid_t, size_t>> edges_;
  std::vector<std::shared_ptr<EdgePropVecBase>> prop_cols_;
};

class OptionalSDMLEdgeColumn : public IEdgeColumn {
 public:
  OptionalSDMLEdgeColumn(
      Direction dir,
      const std::vector<std::pair<LabelTriplet, PropertyType>>& labels)
      : column_(dir, labels) {}

  inline EdgeRecord get_edge(size_t idx) const override {
    return column_.get_edge(idx);
  }

  inline size_t size() const override { return column_.edges_.size(); }

  std::string column_info() const override {
    std::stringstream ss{};

    for (size_t idx = 0; idx < column_.edge_labels_.size(); ++idx) {
      auto label = column_.edge_labels_[idx];
      if (idx != 0) {
        ss << ", ";
      }
      ss << label.first.to_string();
    }
    return "SDMLEdgeColumn: label = {" + ss.str() +
           "}, dir = " + std::to_string((int) column_.dir_) +
           ", size = " + std::to_string(column_.edges_.size());
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  template <typename FUNC_T>
  void foreach_edge(const FUNC_T& func) const {
    column_.foreach_edge(func);
  }

  std::vector<LabelTriplet> get_labels() const override {
    return column_.get_labels();
  }

  inline Direction dir() const { return column_.dir(); }
  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kSDML;
  }

  inline bool is_optional() const override { return true; }

  inline bool has_value(size_t idx) const override {
    return std::get<1>(column_.edges_[idx]) !=
               std::numeric_limits<vid_t>::max() &&
           std::get<2>(column_.edges_[idx]) !=
               std::numeric_limits<vid_t>::max();
  }

 private:
  friend class SDMLEdgeColumnBuilder;
  SDMLEdgeColumn column_;
};

class BDMLEdgeColumnBuilder;

class BDMLEdgeColumn : public IEdgeColumn {
 public:
  BDMLEdgeColumn(
      const std::vector<std::pair<LabelTriplet, PropertyType>>& labels)
      : labels_(labels) {
    size_t idx = 0;
    prop_cols_.resize(labels.size());
    for (const auto& label : labels) {
      index_[label.first] = idx++;
      prop_cols_[index_[label.first]] =
          EdgePropVecBase::make_edge_prop_vec(label.second);
    }
  }

  inline EdgeRecord get_edge(size_t idx) const override {
    auto& e = edges_[idx];
    auto index = std::get<0>(e);
    auto label = labels_[index].first;
    auto offset = std::get<3>(e);
    EdgeRecord ret;
    ret.label_triplet_ = label;
    ret.src_ = std::get<1>(e);
    ret.dst_ = std::get<2>(e);
    get_edge_data(prop_cols_[index].get(), offset, ret.prop_);
    ret.dir_ = (std::get<4>(e) ? Direction::kOut : Direction::kIn);
    return ret;
  }

  inline size_t size() const override { return edges_.size(); }

  std::string column_info() const override {
    std::stringstream ss{};

    for (size_t idx = 0; idx < labels_.size(); ++idx) {
      auto label = labels_[idx];
      if (idx != 0) {
        ss << ", ";
      }
      ss << label.first.to_string();
    }
    return "BDMLEdgeColumn: label = {" + ss.str() +
           "}, size = " + std::to_string(edges_.size());
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  template <typename FUNC_T>
  void foreach_edge(const FUNC_T& func) const {
    size_t idx = 0;
    for (auto& e : edges_) {
      auto index = std::get<0>(e);
      auto label = labels_[index].first;
      auto offset = std::get<3>(e);
      func(idx, label, std::get<1>(e), std::get<2>(e),
           prop_cols_[index]->get(offset),
           (std::get<4>(e) ? Direction::kOut : Direction::kIn));
      ++idx;
    }
  }

  std::vector<LabelTriplet> get_labels() const override {
    std::vector<LabelTriplet> labels;
    for (auto& label : labels_) {
      labels.push_back(label.first);
    }
    return labels;
  }

  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kBDML;
  }

 private:
  friend class BDMLEdgeColumnBuilder;
  friend class OptionalBDMLEdgeColumn;
  std::map<LabelTriplet, label_t> index_;
  std::vector<std::pair<LabelTriplet, PropertyType>> labels_;
  std::vector<std::tuple<label_t, vid_t, vid_t, size_t, bool>> edges_;
  std::vector<std::shared_ptr<EdgePropVecBase>> prop_cols_;
};

class OptionalBDMLEdgeColumn : public IEdgeColumn {
 public:
  OptionalBDMLEdgeColumn(
      const std::vector<std::pair<LabelTriplet, PropertyType>>& labels)
      : column_(labels) {}

  inline EdgeRecord get_edge(size_t idx) const override {
    return column_.get_edge(idx);
  }

  inline size_t size() const override { return column_.size(); }

  std::string column_info() const override {
    std::stringstream ss{};

    for (size_t idx = 0; idx < column_.labels_.size(); ++idx) {
      auto label = column_.labels_[idx];
      if (idx != 0) {
        ss << ", ";
      }
      ss << label.first.to_string();
    }
    return "BDMLEdgeColumn: label = {" + ss.str() +
           "}, size = " + std::to_string(column_.edges_.size());
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  template <typename FUNC_T>
  void foreach_edge(const FUNC_T& func) const {
    column_.foreach_edge(func);
  }

  std::vector<LabelTriplet> get_labels() const override {
    return column_.get_labels();
  }

  inline EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kBDML;
  }

  inline bool is_optional() const override { return true; }

  inline bool has_value(size_t idx) const override {
    return std::get<1>(column_.edges_[idx]) !=
               std::numeric_limits<vid_t>::max() &&
           std::get<2>(column_.edges_[idx]) !=
               std::numeric_limits<vid_t>::max();
  }

 private:
  friend class BDMLEdgeColumnBuilder;
  BDMLEdgeColumn column_;
};

class SDSLEdgeColumnBuilder : public IContextColumnBuilder {
 public:
  static SDSLEdgeColumnBuilder builder(Direction dir, const LabelTriplet& label,
                                       PropertyType prop_type) {
    return SDSLEdgeColumnBuilder(dir, label, prop_type);
  }

  static SDSLEdgeColumnBuilder optional_builder(Direction dir,
                                                const LabelTriplet& label,
                                                PropertyType prop_type) {
    auto builder = SDSLEdgeColumnBuilder(dir, label, prop_type);
    builder.is_optional_ = true;
    return builder;
  }
  ~SDSLEdgeColumnBuilder() = default;

  void reserve(size_t size) override { edges_.reserve(size); }
  inline void push_back_elem(const RTAny& val) override {
    const auto& e = val.as_edge();
    push_back_opt(e.src_, e.dst_, e.prop_);
  }
  inline void push_back_opt(vid_t src, vid_t dst, const EdgeData& data) {
    edges_.emplace_back(src, dst);

    size_t len = edges_.size();

    set_edge_data(prop_col_.get(), len - 1, data);
  }
  inline void push_back_endpoints(vid_t src, vid_t dst) {
    edges_.emplace_back(src, dst);
  }

  inline void push_back_null() {
    assert(is_optional_);
    edges_.emplace_back(std::numeric_limits<vid_t>::max(),
                        std::numeric_limits<vid_t>::max());
  }

  std::shared_ptr<IContextColumn> finish(
      const std::shared_ptr<Arena>&) override;

 private:
  SDSLEdgeColumnBuilder(Direction dir, const LabelTriplet& label,
                        PropertyType prop_type)
      : dir_(dir),
        label_(label),
        prop_type_(prop_type),
        prop_col_(EdgePropVecBase::make_edge_prop_vec(prop_type)),
        is_optional_(false) {}
  friend class SDSLEdgeColumn;
  Direction dir_;
  LabelTriplet label_;
  std::vector<std::pair<vid_t, vid_t>> edges_;
  PropertyType prop_type_;
  std::shared_ptr<EdgePropVecBase> prop_col_;
  bool is_optional_;
};

template <typename T>
class SDSLEdgeColumnBuilderBeta : public IContextColumnBuilder {
 public:
  SDSLEdgeColumnBuilderBeta(Direction dir, const LabelTriplet& label,
                            PropertyType prop_type)
      : dir_(dir),
        label_(label),
        prop_type_(prop_type),
        prop_col_(std::make_shared<EdgePropVec<T>>()),
        prop_col_ptr_(prop_col_.get()) {}
  ~SDSLEdgeColumnBuilderBeta() = default;

  void reserve(size_t size) override { edges_.reserve(size); }
  inline void push_back_elem(const RTAny& val) override {
    const auto& e = val.as_edge();

    push_back_opt(e.src_, e.dst_, e.prop_.as<T>());
  }
  inline void push_back_opt(vid_t src, vid_t dst, const T& data) {
    size_t len = edges_.size();
    edges_.emplace_back(src, dst);
    prop_col_ptr_->set(len, data);
  }

  std::shared_ptr<IContextColumn> finish(
      const std::shared_ptr<Arena>&) override {
    auto ret = std::make_shared<SDSLEdgeColumn>(dir_, label_, prop_type_,
                                                std::vector<PropertyType>());
    ret->edges_.swap(edges_);
    prop_col_->resize(edges_.size());
    ret->prop_col_ = prop_col_;
    return ret;
  }

 private:
  Direction dir_;
  LabelTriplet label_;
  std::vector<std::pair<vid_t, vid_t>> edges_;
  PropertyType prop_type_;
  std::shared_ptr<EdgePropVec<T>> prop_col_;
  EdgePropVec<T>* prop_col_ptr_;
};

class BDSLEdgeColumnBuilder : public IContextColumnBuilder {
 public:
  static BDSLEdgeColumnBuilder builder(const LabelTriplet& label,
                                       PropertyType prop_type) {
    return BDSLEdgeColumnBuilder(label, prop_type);
  }

  static BDSLEdgeColumnBuilder optional_builder(const LabelTriplet& label,
                                                PropertyType prop_type) {
    auto builder = BDSLEdgeColumnBuilder(label, prop_type);
    builder.is_optional_ = true;
    return builder;
  }
  ~BDSLEdgeColumnBuilder() = default;

  void reserve(size_t size) override { edges_.reserve(size); }
  inline void push_back_elem(const RTAny& val) override {
    const auto& e = val.as_edge();
    push_back_opt(e.src_, e.dst_, e.prop_, e.dir_);
  }
  inline void push_back_opt(vid_t src, vid_t dst, const EdgeData& data,
                            Direction dir) {
    edges_.emplace_back(src, dst, dir == Direction::kOut);
    size_t len = edges_.size();
    set_edge_data(prop_col_.get(), len - 1, data);
  }
  inline void push_back_endpoints(vid_t src, vid_t dst, Direction dir) {
    edges_.emplace_back(src, dst, dir == Direction::kOut);
  }

  inline void push_back_endpoints(vid_t src, vid_t dst, bool dir) {
    edges_.emplace_back(src, dst, dir);
  }

  inline void push_back_null() {
    assert(is_optional_);
    edges_.emplace_back(std::numeric_limits<vid_t>::max(),
                        std::numeric_limits<vid_t>::max(), false);
  }

  std::shared_ptr<IContextColumn> finish(
      const std::shared_ptr<Arena>&) override;

 private:
  friend class BDSLEdgeColumn;
  LabelTriplet label_;
  std::vector<std::tuple<vid_t, vid_t, bool>> edges_;
  PropertyType prop_type_;
  std::shared_ptr<EdgePropVecBase> prop_col_;
  bool is_optional_;

  BDSLEdgeColumnBuilder(const LabelTriplet& label, PropertyType prop_type)
      : label_(label),
        prop_type_(prop_type),
        prop_col_(EdgePropVecBase::make_edge_prop_vec(prop_type)),
        is_optional_(false) {}
};
class SDMLEdgeColumnBuilder : public IContextColumnBuilder {
 public:
  static SDMLEdgeColumnBuilder builder(
      Direction dir,
      const std::vector<std::pair<LabelTriplet, PropertyType>>& labels) {
    return SDMLEdgeColumnBuilder(dir, labels);
  }

  static SDMLEdgeColumnBuilder optional_builder(
      Direction dir,
      const std::vector<std::pair<LabelTriplet, PropertyType>>& labels) {
    auto builder = SDMLEdgeColumnBuilder(dir, labels);
    builder.is_optional_ = true;
    if (builder.prop_cols_.empty()) {
      builder.prop_cols_.emplace_back(EdgePropVecBase::make_edge_prop_vec(
          PropertyType::kEmpty));  // for null edge
    }
    return builder;
  }
  ~SDMLEdgeColumnBuilder() = default;

  void reserve(size_t size) override { edges_.reserve(size); }
  inline void push_back_elem(const RTAny& val) override {
    const auto& e = val.as_edge();
    auto label = e.label_triplet_;
    auto index = index_[label];
    push_back_opt(index, e.src_, e.dst_, e.prop_);
  }
  inline void push_back_opt(label_t index, vid_t src, vid_t dst,
                            const EdgeData& data) {
    edges_.emplace_back(index, src, dst, prop_cols_[index]->size());
    set_edge_data(prop_cols_[index].get(), prop_cols_[index]->size(), data);
  }

  inline void push_back_opt(LabelTriplet label, vid_t src, vid_t dst,
                            const EdgeData& data) {
    auto index = index_[label];
    push_back_opt(index, src, dst, data);
  }

  inline void push_back_null() {
    assert(is_optional_);
    edges_.emplace_back(0, std::numeric_limits<vid_t>::max(),
                        std::numeric_limits<vid_t>::max(),
                        prop_cols_[0]->size());
    prop_cols_[0]->resize(prop_cols_[0]->size() + 1);
  }

  inline void push_back_endpoints(label_t index, vid_t src, vid_t dst) {
    LOG(FATAL) << "Not implemented";
  }

  std::shared_ptr<IContextColumn> finish(
      const std::shared_ptr<Arena>&) override;

 private:
  SDMLEdgeColumnBuilder(
      Direction dir,
      const std::vector<std::pair<LabelTriplet, PropertyType>>& labels)
      : dir_(dir), is_optional_(false) {
    size_t idx = 0;
    prop_cols_.resize(labels.size());
    for (const auto& label : labels) {
      edge_labels_.emplace_back(label);
      index_[label.first] = idx++;
      prop_cols_[index_[label.first]] =
          EdgePropVecBase::make_edge_prop_vec(label.second);
    }
  }
  friend class SDMLEdgeColumn;
  Direction dir_;
  bool is_optional_;
  std::map<LabelTriplet, label_t> index_;
  std::vector<std::pair<LabelTriplet, PropertyType>> edge_labels_;
  std::vector<std::tuple<label_t, vid_t, vid_t, size_t>> edges_;
  std::vector<std::shared_ptr<EdgePropVecBase>> prop_cols_;
};

class BDMLEdgeColumnBuilder : public IContextColumnBuilder {
 public:
  static BDMLEdgeColumnBuilder builder(
      const std::vector<std::pair<LabelTriplet, PropertyType>>& labels) {
    return BDMLEdgeColumnBuilder(labels);
  }

  static BDMLEdgeColumnBuilder optional_builder(
      const std::vector<std::pair<LabelTriplet, PropertyType>>& labels) {
    auto builder = BDMLEdgeColumnBuilder(labels);
    builder.is_optional_ = true;
    if (builder.prop_cols_.empty()) {
      builder.prop_cols_.emplace_back(EdgePropVecBase::make_edge_prop_vec(
          PropertyType::kEmpty));  // for null edge
    }
    return builder;
  }
  static BDMLEdgeColumnBuilder builder() { return BDMLEdgeColumnBuilder(); }
  static BDMLEdgeColumnBuilder optional_builder() {
    auto builder = BDMLEdgeColumnBuilder();
    builder.is_optional_ = true;
    if (builder.prop_cols_.empty()) {
      builder.prop_cols_.emplace_back(EdgePropVecBase::make_edge_prop_vec(
          PropertyType::kEmpty));  // for null edge
    }
    return builder;
  }
  ~BDMLEdgeColumnBuilder() = default;

  void reserve(size_t size) override { edges_.reserve(size); }
  inline void push_back_elem(const RTAny& val) override {
    const auto& e = val.as_edge();
    auto label = e.label_triplet_;
    if (index_.find(label) == index_.end()) {
      index_[label] = labels_.size();
      auto data = e.prop_;
      auto type = rt_type_to_property_type(data.type);
      labels_.emplace_back(label, type);
      prop_cols_.emplace_back(EdgePropVecBase::make_edge_prop_vec(type));
    }
    auto index = index_[label];
    push_back_opt(index, e.src_, e.dst_, e.prop_, e.dir_);
  }
  inline void push_back_opt(label_t index, vid_t src, vid_t dst,
                            const EdgeData& data, Direction dir) {
    edges_.emplace_back(index, src, dst, prop_cols_[index]->size(),
                        dir == Direction::kOut);
    set_edge_data(prop_cols_[index].get(), prop_cols_[index]->size(), data);
  }

  inline void push_back_opt(LabelTriplet label, vid_t src, vid_t dst,
                            const EdgeData& data, Direction dir) {
    auto index = index_[label];
    push_back_opt(index, src, dst, data, dir);
  }

  inline void push_back_endpoints(label_t index, vid_t src, vid_t dst,
                                  Direction dir) {
    edges_.emplace_back(index, src, dst, prop_cols_[index]->size(),
                        dir == Direction::kOut);
  }

  inline void push_back_endpoints(label_t index, vid_t src, vid_t dst,
                                  bool dir) {
    edges_.emplace_back(index, src, dst, prop_cols_[index]->size(), dir);
  }

  inline void push_back_null() {
    assert(is_optional_);
    edges_.emplace_back(0, std::numeric_limits<vid_t>::max(),
                        std::numeric_limits<vid_t>::max(),
                        prop_cols_[0]->size(), false);
    prop_cols_[0]->resize(prop_cols_[0]->size() + 1);
  }

  std::shared_ptr<IContextColumn> finish(
      const std::shared_ptr<Arena>&) override;

 private:
  BDMLEdgeColumnBuilder() : index_(), labels_(), is_optional_(false) {}
  BDMLEdgeColumnBuilder(
      const std::vector<std::pair<LabelTriplet, PropertyType>>& labels)
      : labels_(labels), is_optional_(false) {
    size_t idx = 0;
    prop_cols_.resize(labels.size());
    for (const auto& label : labels) {
      index_[label.first] = idx++;
      prop_cols_[index_[label.first]] =
          EdgePropVecBase::make_edge_prop_vec(label.second);
    }
  }
  friend class BDMLEdgeColumn;

  std::map<LabelTriplet, label_t> index_;
  std::vector<std::pair<LabelTriplet, PropertyType>> labels_;
  std::vector<std::tuple<label_t, vid_t, vid_t, size_t, bool>> edges_;
  std::vector<std::shared_ptr<EdgePropVecBase>> prop_cols_;
  bool is_optional_;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_COLUMNS_EDGE_COLUMNS_H_
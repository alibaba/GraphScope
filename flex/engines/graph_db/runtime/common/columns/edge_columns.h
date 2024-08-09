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

#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/utils/property/column.h"

namespace gs {

namespace runtime {

enum class EdgeColumnType { kSDSL, kSDML, kBDSL, kBDML, kUnKnown };
class IEdgeColumn : public IContextColumn {
 public:
  IEdgeColumn() = default;
  virtual ~IEdgeColumn() = default;

  ContextColumnType column_type() const override {
    return ContextColumnType::kEdge;
  }

  virtual std::tuple<LabelTriplet, vid_t, vid_t, Any, Direction> get_edge(
      size_t idx) const = 0;

  RTAny get_elem(size_t idx) const override {
    return RTAny::from_edge(this->get_edge(idx));
  }

  RTAnyType elem_type() const override { return RTAnyType::kEdge; }
  virtual std::vector<LabelTriplet> get_labels() const = 0;
  virtual EdgeColumnType edge_column_type() const = 0;
};

class SDSLEdgeColumnBuilder;
class OptionalSDSLEdgeColumnBuilder;

class SDSLEdgeColumn : public IEdgeColumn {
 public:
  SDSLEdgeColumn(Direction dir, const LabelTriplet& label,
                 PropertyType prop_type,
                 const std::vector<PropertyType>& sub_types = {})
      : dir_(dir),
        label_(label),
        prop_type_(prop_type),
        prop_col_(CreateColumn(prop_type, StorageStrategy::kMem, sub_types)) {
    prop_col_->open_in_memory("");
  }

  std::tuple<LabelTriplet, vid_t, vid_t, Any, Direction> get_edge(
      size_t idx) const override {
    return std::make_tuple(label_, edges_[idx].first, edges_[idx].second,
                           prop_col_->get(idx), dir_);
  }

  size_t size() const override { return edges_.size(); }

  Direction dir() const { return dir_; }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return std::dynamic_pointer_cast<IContextColumnBuilder>(
        std::make_shared<SDSLEdgeColumnBuilder>(dir_, label_, prop_type_));
  }

  std::shared_ptr<IOptionalContextColumnBuilder> optional_builder()
      const override {
    return std::dynamic_pointer_cast<IOptionalContextColumnBuilder>(
        std::make_shared<OptionalSDSLEdgeColumnBuilder>(dir_, label_,
                                                        prop_type_));
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    // TODO: dedup with property value
    std::vector<size_t> origin_offsets(size());
    for (size_t i = 0; i < size(); ++i) {
      origin_offsets[i] = i;
    }
    std::sort(origin_offsets.begin(), origin_offsets.end(),
              [this](size_t a, size_t b) {
                auto& e1 = edges_[a];
                auto& e2 = edges_[b];
                if (e1.first == e2.first) {
                  return e1.second < e2.second;
                }
                return e1.first < e2.first;
              });

    for (size_t i = 0; i < size(); ++i) {
      if (i == 0 ||
          edges_[origin_offsets[i]] != edges_[origin_offsets[i - 1]]) {
        offsets.push_back(origin_offsets[i]);
      }
    }
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

  std::shared_ptr<IContextColumn> dup() const override;

  template <typename FUNC_T>
  void foreach_edge(const FUNC_T& func) const {
    if (prop_type_ == PropertyType::kEmpty) {
      size_t idx = 0;
      for (auto& e : edges_) {
        func(idx++, label_, e.first, e.second, grape::EmptyType(), dir_);
      }
    } else {
      size_t idx = 0;
      for (auto& e : edges_) {
        func(idx, label_, e.first, e.second, prop_col_->get(idx), dir_);
        ++idx;
      }
    }
  }

  std::vector<LabelTriplet> get_labels() const override { return {label_}; }

  EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kSDSL;
  }

 private:
  friend class SDSLEdgeColumnBuilder;
  Direction dir_;
  LabelTriplet label_;
  std::vector<std::pair<vid_t, vid_t>> edges_;
  PropertyType prop_type_;
  std::shared_ptr<ColumnBase> prop_col_;
};

class OptionalSDSLEdgeColumn : public IEdgeColumn {
 public:
  OptionalSDSLEdgeColumn(Direction dir, const LabelTriplet& label,
                         PropertyType prop_type)
      : dir_(dir),
        label_(label),
        prop_type_(prop_type),
        prop_col_(CreateColumn(prop_type, StorageStrategy::kMem)) {
    prop_col_->open_in_memory("");
  }

  std::tuple<LabelTriplet, vid_t, vid_t, Any, Direction> get_edge(
      size_t idx) const override {
    return std::make_tuple(label_, edges_[idx].first, edges_[idx].second,
                           prop_col_->get(idx), dir_);
  }

  size_t size() const override { return edges_.size(); }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override {
    std::vector<size_t> origin_offsets(size());
    for (size_t i = 0; i < size(); ++i) {
      origin_offsets[i] = i;
    }
    std::sort(origin_offsets.begin(), origin_offsets.end(),
              [this](size_t a, size_t b) {
                auto& e1 = edges_[a];
                auto& e2 = edges_[b];
                if (e1.first == e2.first) {
                  return e1.second < e2.second;
                }
                return e1.first < e2.first;
              });

    for (size_t i = 0; i < size(); ++i) {
      if (i == 0 ||
          edges_[origin_offsets[i]] != edges_[origin_offsets[i - 1]]) {
        offsets.push_back(origin_offsets[i]);
      }
    }
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return std::dynamic_pointer_cast<IContextColumnBuilder>(
        std::make_shared<OptionalSDSLEdgeColumnBuilder>(dir_, label_,
                                                        prop_type_));
  }

  ISigColumn* generate_signature() const override {
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
    return "OptionalSDSLEdgeColumn: label = " + label_.to_string() +
           ", dir = " + std::to_string((int) dir_) +
           ", size = " + std::to_string(edges_.size());
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> dup() const override;

  template <typename FUNC_T>
  void foreach_edge(const FUNC_T& func) const {
    if (prop_type_ == PropertyType::kEmpty) {
      size_t idx = 0;
      for (auto& e : edges_) {
        func(idx++, label_, e.first, e.second, grape::EmptyType(), dir_, 0);
      }
    } else {
      size_t idx = 0;
      for (auto& e : edges_) {
        func(idx, label_, e.first, e.second, prop_col_->get(idx), dir_, 0);
        ++idx;
      }
    }
  }

  bool is_optional() const override { return true; }

  bool has_value(size_t idx) const override {
    return edges_[idx].first != std::numeric_limits<vid_t>::max() &&
           edges_[idx].second != std::numeric_limits<vid_t>::max();
  }

  std::vector<LabelTriplet> get_labels() const override {
    LOG(INFO) << "get_labels: " << label_.to_string() << std::endl;
    return {label_};
  }

  EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kSDSL;
  }

 private:
  friend class OptionalSDSLEdgeColumnBuilder;
  Direction dir_;
  LabelTriplet label_;
  std::vector<std::pair<vid_t, vid_t>> edges_;
  PropertyType prop_type_;
  std::shared_ptr<ColumnBase> prop_col_;
};

class OptionalSDSLEdgeColumnBuilder : public IOptionalContextColumnBuilder {
 public:
  OptionalSDSLEdgeColumnBuilder(Direction dir, const LabelTriplet& label,
                                PropertyType prop_type)
      : dir_(dir),
        label_(label),
        prop_type_(prop_type),
        prop_col_(CreateColumn(prop_type, StorageStrategy::kMem)) {
    prop_col_->open_in_memory("");
  }
  ~OptionalSDSLEdgeColumnBuilder() = default;

  void reserve(size_t size) override { edges_.reserve(size); }
  void push_back_elem(const RTAny& val) override {
    const auto& e = val.as_edge();
    push_back_opt(std::get<1>(e), std::get<2>(e), std::get<3>(e));
  }
  void push_back_opt(vid_t src, vid_t dst, const Any& data) {
    edges_.emplace_back(src, dst);
    size_t len = edges_.size();
    prop_col_->resize(len);
    prop_col_->set_any(len - 1, data);
  }

  void push_back_null() override {
    edges_.emplace_back(std::numeric_limits<vid_t>::max(),
                        std::numeric_limits<vid_t>::max());
  }
  void push_back_endpoints(vid_t src, vid_t dst) {
    edges_.emplace_back(src, dst);
  }

  std::shared_ptr<IContextColumn> finish() override;

 private:
  friend class OptionalSDSLEdgeColumn;
  Direction dir_;
  LabelTriplet label_;
  std::vector<std::pair<vid_t, vid_t>> edges_;
  PropertyType prop_type_;
  std::shared_ptr<ColumnBase> prop_col_;
};

class BDSLEdgeColumnBuilder;

class OptionalBDSLEdgeColumnBuilder;
class BDSLEdgeColumn : public IEdgeColumn {
 public:
  BDSLEdgeColumn(const LabelTriplet& label, PropertyType prop_type)
      : label_(label),
        prop_type_(prop_type),
        prop_col_(CreateColumn(prop_type, StorageStrategy::kMem)) {
    prop_col_->open_in_memory("");
  }

  std::tuple<LabelTriplet, vid_t, vid_t, Any, Direction> get_edge(
      size_t idx) const override {
    auto src = std::get<0>(edges_[idx]);
    auto dst = std::get<1>(edges_[idx]);
    auto dir = std::get<2>(edges_[idx]);
    return std::make_tuple(label_, src, dst, prop_col_->get(idx),
                           (dir ? Direction::kOut : Direction::kIn));
  }

  size_t size() const override { return edges_.size(); }

  std::string column_info() const override {
    return "BDSLEdgeColumn: label = " + label_.to_string() +
           ", size = " + std::to_string(edges_.size());
  }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return std::dynamic_pointer_cast<IContextColumnBuilder>(
        std::make_shared<BDSLEdgeColumnBuilder>(label_, prop_type_));
  }

  std::shared_ptr<IOptionalContextColumnBuilder> optional_builder()
      const override {
    return std::dynamic_pointer_cast<IOptionalContextColumnBuilder>(
        std::make_shared<OptionalBDSLEdgeColumnBuilder>(label_, prop_type_));
  }
  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> dup() const override;

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

  EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kBDSL;
  }

 private:
  friend class BDSLEdgeColumnBuilder;
  LabelTriplet label_;
  std::vector<std::tuple<vid_t, vid_t, bool>> edges_;
  PropertyType prop_type_;
  std::shared_ptr<ColumnBase> prop_col_;
};

class OptionalBDSLEdgeColumn : public IEdgeColumn {
 public:
  OptionalBDSLEdgeColumn(const LabelTriplet& label, PropertyType prop_type)
      : label_(label),
        prop_type_(prop_type),
        prop_col_(CreateColumn(prop_type, StorageStrategy::kMem)) {
    prop_col_->open_in_memory("");
  }

  std::tuple<LabelTriplet, vid_t, vid_t, Any, Direction> get_edge(
      size_t idx) const override {
    auto src = std::get<0>(edges_[idx]);
    auto dst = std::get<1>(edges_[idx]);
    auto dir = std::get<2>(edges_[idx]);
    return std::make_tuple(label_, src, dst, prop_col_->get(idx),
                           (dir ? Direction::kOut : Direction::kIn));
  }

  size_t size() const override { return edges_.size(); }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return std::dynamic_pointer_cast<IContextColumnBuilder>(
        std::make_shared<OptionalBDSLEdgeColumnBuilder>(label_, prop_type_));
  }

  std::string column_info() const override {
    return "OptionalBDSLEdgeColumn: label = " + label_.to_string() +
           ", size = " + std::to_string(edges_.size());
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> dup() const override;

  template <typename FUNC_T>
  void foreach_edge(const FUNC_T& func) const {
    size_t idx = 0;
    for (auto& e : edges_) {
      func(idx, label_, std::get<0>(e), std::get<1>(e), prop_col_->get(idx),
           (std::get<2>(e) ? Direction::kOut : Direction::kIn));
      ++idx;
    }
  }

  bool is_optional() const override { return true; }

  bool has_value(size_t idx) const override {
    return std::get<0>(edges_[idx]) != std::numeric_limits<vid_t>::max() &&
           std::get<1>(edges_[idx]) != std::numeric_limits<vid_t>::max();
  }

  std::vector<LabelTriplet> get_labels() const override {
    LOG(INFO) << "get_labels: " << label_.to_string() << std::endl;
    return {label_};
  }

  EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kBDSL;
  }

 private:
  friend class OptionalBDSLEdgeColumnBuilder;
  LabelTriplet label_;
  std::vector<std::tuple<vid_t, vid_t, bool>> edges_;
  PropertyType prop_type_;
  std::shared_ptr<ColumnBase> prop_col_;
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
          CreateColumn(label.second, StorageStrategy::kMem);
      prop_cols_[index_[label.first]]->open_in_memory("");
    }
  }

  std::tuple<LabelTriplet, vid_t, vid_t, Any, Direction> get_edge(
      size_t idx) const override {
    auto& e = edges_[idx];
    auto index = std::get<0>(e);
    auto label = edge_labels_[index].first;
    auto offset = std::get<3>(e);
    return std::tuple<LabelTriplet, vid_t, vid_t, Any, Direction>(
        label, std::get<1>(e), std::get<2>(e), prop_cols_[index]->get(offset),
        dir_);
  }

  size_t size() const override { return edges_.size(); }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return std::dynamic_pointer_cast<IContextColumnBuilder>(
        std::make_shared<SDMLEdgeColumnBuilder>(dir_, edge_labels_));
  }

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

  std::shared_ptr<IContextColumn> dup() const override;

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

  Direction dir() const { return dir_; }
  EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kSDML;
  }

 private:
  friend class SDMLEdgeColumnBuilder;
  Direction dir_;
  std::map<LabelTriplet, int8_t> index_;
  std::vector<std::pair<LabelTriplet, PropertyType>> edge_labels_;
  std::vector<std::tuple<int8_t, vid_t, vid_t, size_t>> edges_;
  std::vector<std::shared_ptr<ColumnBase>> prop_cols_;
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
          CreateColumn(label.second, StorageStrategy::kMem);
      prop_cols_[index_[label.first]]->open_in_memory("");
    }
  }

  std::tuple<LabelTriplet, vid_t, vid_t, Any, Direction> get_edge(
      size_t idx) const override {
    auto& e = edges_[idx];
    auto index = std::get<0>(e);
    auto label = labels_[index].first;
    auto offset = std::get<3>(e);
    return std::tuple<LabelTriplet, vid_t, vid_t, Any, Direction>(
        label, std::get<1>(e), std::get<2>(e), prop_cols_[index]->get(offset),
        (std::get<4>(e) ? Direction::kOut : Direction::kIn));
  }

  size_t size() const override { return edges_.size(); }

  std::shared_ptr<IContextColumnBuilder> builder() const override {
    return std::dynamic_pointer_cast<IContextColumnBuilder>(
        std::make_shared<BDMLEdgeColumnBuilder>(labels_));
  }

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

  std::shared_ptr<IContextColumn> dup() const override;

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

  EdgeColumnType edge_column_type() const override {
    return EdgeColumnType::kBDML;
  }

 private:
  friend class BDMLEdgeColumnBuilder;
  std::map<LabelTriplet, int8_t> index_;
  std::vector<std::pair<LabelTriplet, PropertyType>> labels_;
  std::vector<std::tuple<int8_t, vid_t, vid_t, size_t, bool>> edges_;
  std::vector<std::shared_ptr<ColumnBase>> prop_cols_;
};

class SDSLEdgeColumnBuilder : public IContextColumnBuilder {
 public:
  SDSLEdgeColumnBuilder(Direction dir, const LabelTriplet& label,
                        PropertyType prop_type,
                        const std::vector<PropertyType>& sub_types = {})
      : dir_(dir),
        label_(label),
        prop_type_(prop_type),
        prop_col_(CreateColumn(prop_type, StorageStrategy::kMem, sub_types)),
        sub_types_(sub_types),
        cap_(0) {
    prop_col_->open_in_memory("");
  }
  ~SDSLEdgeColumnBuilder() = default;

  void reserve(size_t size) override { edges_.reserve(size); }
  void push_back_elem(const RTAny& val) override {
    const auto& e = val.as_edge();
    push_back_opt(std::get<1>(e), std::get<2>(e), std::get<3>(e));
  }
  void push_back_opt(vid_t src, vid_t dst, const Any& data) {
    edges_.emplace_back(src, dst);

    size_t len = edges_.size();

    if (cap_ == 0) {
      prop_col_->resize(len);
      cap_ = len;
    } else if (len >= cap_) {
      prop_col_->resize(len * 2);
      cap_ = len * 2;
    }
    prop_col_->set_any(len - 1, data);
  }
  void push_back_endpoints(vid_t src, vid_t dst) {
    edges_.emplace_back(src, dst);
  }

  std::shared_ptr<IContextColumn> finish() override;

 private:
  friend class SDSLEdgeColumn;
  Direction dir_;
  LabelTriplet label_;
  std::vector<std::pair<vid_t, vid_t>> edges_;
  PropertyType prop_type_;
  std::shared_ptr<ColumnBase> prop_col_;
  std::vector<PropertyType> sub_types_;
  size_t cap_;
};

class BDSLEdgeColumnBuilder : public IContextColumnBuilder {
 public:
  BDSLEdgeColumnBuilder(const LabelTriplet& label, PropertyType prop_type)
      : label_(label),
        prop_type_(prop_type),
        prop_col_(CreateColumn(prop_type, StorageStrategy::kMem)) {
    prop_col_->open_in_memory("");
  }
  ~BDSLEdgeColumnBuilder() = default;

  void reserve(size_t size) override { edges_.reserve(size); }
  void push_back_elem(const RTAny& val) override {
    const auto& e = val.as_edge();
    push_back_opt(std::get<1>(e), std::get<2>(e), std::get<3>(e),
                  std::get<4>(e));
  }
  void push_back_opt(vid_t src, vid_t dst, const Any& data, Direction dir) {
    edges_.emplace_back(src, dst, dir == Direction::kOut);
    size_t len = edges_.size();
    prop_col_->resize(len);
    prop_col_->set_any(len - 1, data);
  }
  void push_back_endpoints(vid_t src, vid_t dst, Direction dir) {
    edges_.emplace_back(src, dst, dir == Direction::kOut);
  }

  void push_back_endpoints(vid_t src, vid_t dst, bool dir) {
    edges_.emplace_back(src, dst, dir);
  }

  std::shared_ptr<IContextColumn> finish() override;

 private:
  friend class BDSLEdgeColumn;
  LabelTriplet label_;
  std::vector<std::tuple<vid_t, vid_t, bool>> edges_;
  PropertyType prop_type_;
  std::shared_ptr<ColumnBase> prop_col_;
};
class SDMLEdgeColumnBuilder : public IContextColumnBuilder {
 public:
  SDMLEdgeColumnBuilder(
      Direction dir,
      const std::vector<std::pair<LabelTriplet, PropertyType>>& labels)
      : dir_(dir) {
    size_t idx = 0;
    prop_cols_.resize(labels.size());
    for (const auto& label : labels) {
      edge_labels_.emplace_back(label);
      index_[label.first] = idx++;
      prop_cols_[index_[label.first]] =
          CreateColumn(label.second, StorageStrategy::kMem);
      prop_cols_[index_[label.first]]->open_in_memory("");
    }
  }
  ~SDMLEdgeColumnBuilder() = default;

  void reserve(size_t size) override { edges_.reserve(size); }
  void push_back_elem(const RTAny& val) override {
    const auto& e = val.as_edge();
    auto label = std::get<0>(e);
    auto index = index_[label];
    push_back_opt(index, std::get<1>(e), std::get<2>(e), std::get<3>(e));
  }
  void push_back_opt(int8_t index, vid_t src, vid_t dst, const Any& data) {
    edges_.emplace_back(index, src, dst, prop_cols_[index]->size());
    prop_cols_[index]->resize(prop_cols_[index]->size() + 1);
    prop_cols_[index]->set_any(prop_cols_[index]->size() - 1, data);
  }

  void push_back_opt(LabelTriplet label, vid_t src, vid_t dst,
                     const Any& data) {
    auto index = index_[label];
    push_back_opt(index, src, dst, data);
  }

  void push_back_endpoints(int8_t index, vid_t src, vid_t dst) {
    edges_.emplace_back(index, src, dst, prop_cols_[index]->size());
  }

  std::shared_ptr<IContextColumn> finish() override;

 private:
  friend class SDMLEdgeColumn;
  Direction dir_;
  std::map<LabelTriplet, int8_t> index_;
  std::vector<std::pair<LabelTriplet, PropertyType>> edge_labels_;
  std::vector<std::tuple<int8_t, vid_t, vid_t, size_t>> edges_;
  std::vector<std::shared_ptr<ColumnBase>> prop_cols_;
};

class BDMLEdgeColumnBuilder : public IContextColumnBuilder {
 public:
  BDMLEdgeColumnBuilder() : index_(), labels_() {}
  BDMLEdgeColumnBuilder(
      const std::vector<std::pair<LabelTriplet, PropertyType>>& labels)
      : labels_(labels) {
    size_t idx = 0;
    prop_cols_.resize(labels.size());
    for (const auto& label : labels) {
      index_[label.first] = idx++;
      prop_cols_[index_[label.first]] =
          CreateColumn(label.second, StorageStrategy::kMem);
      prop_cols_[index_[label.first]]->open_in_memory("");
    }
  }
  ~BDMLEdgeColumnBuilder() = default;

  void reserve(size_t size) override { edges_.reserve(size); }
  void push_back_elem(const RTAny& val) override {
    const auto& e = val.as_edge();
    auto label = std::get<0>(e);
    if (index_.find(label) == index_.end()) {
      index_[label] = labels_.size();
      auto data = std::get<3>(e);
      labels_.emplace_back(label, data.type);
      prop_cols_.emplace_back(CreateColumn(data.type, StorageStrategy::kMem));
      prop_cols_.back()->open_in_memory("");
    }
    auto index = index_[label];
    push_back_opt(index, std::get<1>(e), std::get<2>(e), std::get<3>(e),
                  std::get<4>(e));
  }
  void push_back_opt(int8_t index, vid_t src, vid_t dst, const Any& data,
                     Direction dir) {
    edges_.emplace_back(index, src, dst, prop_cols_[index]->size(),
                        dir == Direction::kOut);
    prop_cols_[index]->resize(prop_cols_[index]->size() + 1);
    prop_cols_[index]->set_any(prop_cols_[index]->size() - 1, data);
  }

  void push_back_opt(LabelTriplet label, vid_t src, vid_t dst, const Any& data,
                     Direction dir) {
    auto index = index_[label];
    push_back_opt(index, src, dst, data, dir);
  }

  void push_back_endpoints(int8_t index, vid_t src, vid_t dst, Direction dir) {
    edges_.emplace_back(index, src, dst, prop_cols_[index]->size(),
                        dir == Direction::kOut);
  }

  void push_back_endpoints(int8_t index, vid_t src, vid_t dst, bool dir) {
    edges_.emplace_back(index, src, dst, prop_cols_[index]->size(), dir);
  }

  std::shared_ptr<IContextColumn> finish() override;

 private:
  friend class BDMLEdgeColumn;

  std::map<LabelTriplet, int8_t> index_;
  std::vector<std::pair<LabelTriplet, PropertyType>> labels_;
  std::vector<std::tuple<int8_t, vid_t, vid_t, size_t, bool>> edges_;
  std::vector<std::shared_ptr<ColumnBase>> prop_cols_;
};

class OptionalBDSLEdgeColumnBuilder : public IOptionalContextColumnBuilder {
 public:
  OptionalBDSLEdgeColumnBuilder(const LabelTriplet& label,
                                PropertyType prop_type)
      : label_(label),
        prop_type_(prop_type),
        prop_col_(CreateColumn(prop_type, StorageStrategy::kMem)) {
    prop_col_->open_in_memory("");
  }
  ~OptionalBDSLEdgeColumnBuilder() = default;

  void reserve(size_t size) override { edges_.reserve(size); }
  void push_back_elem(const RTAny& val) override {
    const auto& e = val.as_edge();
    push_back_opt(std::get<1>(e), std::get<2>(e), std::get<3>(e),
                  std::get<4>(e));
  }
  void push_back_opt(vid_t src, vid_t dst, const Any& data, Direction dir) {
    edges_.emplace_back(src, dst, dir == Direction::kOut);
    size_t len = edges_.size();
    prop_col_->resize(len);
    prop_col_->set_any(len - 1, data);
  }
  void push_back_endpoints(vid_t src, vid_t dst, Direction dir) {
    edges_.emplace_back(src, dst, dir == Direction::kOut);
  }

  void push_back_endpoints(vid_t src, vid_t dst, bool dir) {
    edges_.emplace_back(src, dst, dir);
  }

  void push_back_null() override {
    edges_.emplace_back(std::numeric_limits<vid_t>::max(),
                        std::numeric_limits<vid_t>::max(), false);
  }

  std::shared_ptr<IContextColumn> finish() override;

 private:
  friend class OptionalBDSLEdgeColumn;
  LabelTriplet label_;
  std::vector<std::tuple<vid_t, vid_t, bool>> edges_;
  PropertyType prop_type_;
  std::shared_ptr<ColumnBase> prop_col_;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_COLUMNS_EDGE_COLUMNS_H_
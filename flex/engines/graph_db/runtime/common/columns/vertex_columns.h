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

#ifndef RUNTIME_COMMON_COLUMNS_VERTEX_COLUMNS_H_
#define RUNTIME_COMMON_COLUMNS_VERTEX_COLUMNS_H_

#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"

namespace gs {

namespace runtime {

enum class VertexColumnType {
  kSingle,
  kMultiSegment,
  kMultiple,
};

class IVertexColumn : public IContextColumn {
 public:
  IVertexColumn() = default;
  virtual ~IVertexColumn() = default;

  ContextColumnType column_type() const override {
    return ContextColumnType::kVertex;
  }

  virtual VertexColumnType vertex_column_type() const = 0;
  virtual VertexRecord get_vertex(size_t idx) const = 0;

  RTAny get_elem(size_t idx) const override {
    return RTAny::from_vertex(this->get_vertex(idx));
  }

  RTAnyType elem_type() const override { return RTAnyType::kVertex; }

  virtual std::set<label_t> get_labels_set() const = 0;
};

class IVertexColumnBuilder : public IContextColumnBuilder {
 public:
  IVertexColumnBuilder() = default;
  virtual ~IVertexColumnBuilder() = default;

  virtual void push_back_vertex(VertexRecord v) = 0;

  void push_back_elem(const RTAny& val) override {
    this->push_back_vertex(val.as_vertex());
  }
};

class IOptionalVertexColumnBuilder : public IOptionalContextColumnBuilder {
 public:
  IOptionalVertexColumnBuilder() = default;
  virtual ~IOptionalVertexColumnBuilder() = default;

  virtual void push_back_vertex(VertexRecord v) = 0;

  void push_back_elem(const RTAny& val) override {
    this->push_back_vertex(val.as_vertex());
  }
};

class SLVertexColumnBuilder;
class OptionalSLVertexColumnBuilder;

class SLVertexColumnBase : public IVertexColumn {};

class MLVertexColumnBase : public IVertexColumn {};
class SLVertexColumn : public SLVertexColumnBase {
 public:
  SLVertexColumn(label_t label) : label_(label) {}
  ~SLVertexColumn() = default;

  inline size_t size() const override { return vertices_.size(); }

  std::string column_info() const override {
    return "SLVertexColumn(" + std::to_string(label_) + ")[" +
           std::to_string(size()) + "]";
  }

  inline VertexColumnType vertex_column_type() const override {
    return VertexColumnType::kSingle;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offset) const override;

  std::shared_ptr<IOptionalContextColumnBuilder> optional_builder()
      const override {
    auto ptr = std::make_shared<OptionalSLVertexColumnBuilder>(label_);
    return std::dynamic_pointer_cast<IOptionalContextColumnBuilder>(ptr);
  }

  inline VertexRecord get_vertex(size_t idx) const override {
    return {label_, vertices_[idx]};
  }

  std::shared_ptr<IContextColumn> union_col(
      std::shared_ptr<IContextColumn> other) const override;

  void generate_dedup_offset(std::vector<size_t>& offsets) const override;

  std::pair<std::shared_ptr<IContextColumn>, std::vector<std::vector<size_t>>>
  generate_aggregate_offset() const override;

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func) const {
    size_t num = vertices_.size();
    for (size_t k = 0; k < num; ++k) {
      func(k, label_, vertices_[k]);
    }
  }

  std::set<label_t> get_labels_set() const override {
    std::set<label_t> ret;
    ret.insert(label_);
    return ret;
  }

  inline label_t label() const { return label_; }

  ISigColumn* generate_signature() const override;

  inline const std::vector<vid_t>& vertices() const { return vertices_; }

 private:
  friend class SLVertexColumnBuilder;
  std::vector<vid_t> vertices_;
  label_t label_;
};

class SLVertexColumnBuilder : public IVertexColumnBuilder {
 public:
  SLVertexColumnBuilder(label_t label) : label_(label) {}
  SLVertexColumnBuilder(const std::set<label_t>& labels)
      : label_(*labels.begin()) {
    assert(labels.size() == 1);
  }
  ~SLVertexColumnBuilder() = default;

  void reserve(size_t size) override { vertices_.reserve(size); }

  inline void push_back_vertex(VertexRecord v) override {
    assert(v.label_ == label_);
    vertices_.push_back(v.vid_);
  }
  inline void push_back_opt(vid_t v) { vertices_.push_back(v); }

  std::shared_ptr<IContextColumn> finish(
      const std::shared_ptr<Arena>&) override;

 private:
  std::vector<vid_t> vertices_;
  label_t label_;
};

class OptionalSLVertexColumn : public SLVertexColumnBase {
 public:
  OptionalSLVertexColumn(label_t label) : label_(label) {}
  ~OptionalSLVertexColumn() = default;

  inline size_t size() const override { return vertices_.size(); }

  std::string column_info() const override {
    return "OptionalSLVertex[" + std::to_string(size()) + "]";
  }

  inline VertexColumnType vertex_column_type() const override {
    return VertexColumnType::kSingle;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;
  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offset) const override;

  inline VertexRecord get_vertex(size_t idx) const override {
    return {label_, vertices_[idx]};
  }

  inline bool is_optional() const override { return true; }

  inline bool has_value(size_t idx) const override {
    return vertices_[idx] != std::numeric_limits<vid_t>::max();
  }

  void generate_dedup_offset(std::vector<size_t>& offsets) const override;

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func) const {
    size_t num = vertices_.size();
    for (size_t k = 0; k < num; ++k) {
      func(k, label_, vertices_[k]);
    }
  }

  std::set<label_t> get_labels_set() const override { return {label_}; }

  ISigColumn* generate_signature() const override;

 private:
  friend class OptionalSLVertexColumnBuilder;
  label_t label_;
  std::vector<vid_t> vertices_;
};

class OptionalSLVertexColumnBuilder : public IOptionalVertexColumnBuilder {
 public:
  OptionalSLVertexColumnBuilder(label_t label) : label_(label) {}
  ~OptionalSLVertexColumnBuilder() = default;

  void reserve(size_t size) override { vertices_.reserve(size); }

  inline void push_back_vertex(VertexRecord v) override {
    vertices_.push_back(v.vid_);
  }

  inline void push_back_opt(vid_t v) { vertices_.push_back(v); }

  inline void push_back_null() override {
    vertices_.emplace_back(std::numeric_limits<vid_t>::max());
  }

  std::shared_ptr<IContextColumn> finish(
      const std::shared_ptr<Arena>&) override;

 private:
  label_t label_;
  std::vector<vid_t> vertices_;
};

class MSVertexColumnBuilder;
class MSVertexColumn : public IVertexColumn {
 public:
  MSVertexColumn() = default;
  ~MSVertexColumn() = default;

  size_t size() const override {
    size_t ret = 0;
    for (auto& pair : vertices_) {
      ret += pair.second.size();
    }
    return ret;
  }

  std::string column_info() const override {
    std::string labels;
    for (auto label : labels_) {
      labels += std::to_string(label);
      labels += ", ";
    }
    if (!labels.empty()) {
      labels.resize(labels.size() - 2);
    }
    return "MSVertexColumn(" + labels + ")[" + std::to_string(size()) + "]";
  }

  inline VertexColumnType vertex_column_type() const override {
    return VertexColumnType::kMultiSegment;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  inline VertexRecord get_vertex(size_t idx) const override {
    for (auto& pair : vertices_) {
      if (idx < pair.second.size()) {
        return {pair.first, pair.second[idx]};
      }
      idx -= pair.second.size();
    }
    LOG(FATAL) << "not found...";
    return {std::numeric_limits<label_t>::max(),
            std::numeric_limits<vid_t>::max()};
  }

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func) const {
    size_t index = 0;
    for (auto& pair : vertices_) {
      label_t label = pair.first;
      for (auto v : pair.second) {
        func(index++, label, v);
      }
    }
  }

  std::set<label_t> get_labels_set() const override { return labels_; }

  ISigColumn* generate_signature() const override;

  inline size_t seg_num() const { return vertices_.size(); }

  inline label_t seg_label(size_t seg_id) const {
    return vertices_[seg_id].first;
  }

  const std::vector<vid_t>& seg_vertices(size_t seg_id) const {
    return vertices_[seg_id].second;
  }

 private:
  friend class MSVertexColumnBuilder;
  std::vector<std::pair<label_t, std::vector<vid_t>>> vertices_;
  std::set<label_t> labels_;
};

class MSVertexColumnBuilder : public IVertexColumnBuilder {
 public:
  MSVertexColumnBuilder() = default;
  ~MSVertexColumnBuilder() = default;

  void reserve(size_t size) override {}

  inline void push_back_vertex(VertexRecord v) override {
    if (v.label_ == cur_label_) {
      cur_list_.push_back(v.vid_);
    } else {
      if (!cur_list_.empty()) {
        vertices_.emplace_back(cur_label_, std::move(cur_list_));
        cur_list_.clear();
      }
      cur_label_ = v.label_;
      cur_list_.push_back(v.vid_);
    }
  }

  void start_label(label_t label) {
    if (!cur_list_.empty() && cur_label_ != label) {
      vertices_.emplace_back(cur_label_, std::move(cur_list_));
      cur_list_.clear();
    }
    cur_label_ = label;
  }

  inline void push_back_opt(vid_t v) { cur_list_.push_back(v); }

  std::shared_ptr<IContextColumn> finish(
      const std::shared_ptr<Arena>&) override;

 private:
  label_t cur_label_;
  std::vector<vid_t> cur_list_;

  std::vector<std::pair<label_t, std::vector<vid_t>>> vertices_;
};

class MLVertexColumnBuilder;

class OptionalMLVertexColumnBuilder;
class MLVertexColumn : public MLVertexColumnBase {
 public:
  MLVertexColumn() = default;
  ~MLVertexColumn() = default;

  inline size_t size() const override { return vertices_.size(); }

  std::string column_info() const override {
    std::string labels;
    for (auto label : labels_) {
      labels += std::to_string(label);
      labels += ", ";
    }
    if (!labels.empty()) {
      labels.resize(labels.size() - 2);
    }
    return "MLVertexColumn(" + labels + ")[" + std::to_string(size()) + "]";
  }

  inline VertexColumnType vertex_column_type() const override {
    return VertexColumnType::kMultiple;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;
  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  inline VertexRecord get_vertex(size_t idx) const override {
    return vertices_[idx];
  }

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func) const {
    size_t index = 0;
    for (auto& pair : vertices_) {
      func(index++, pair.label_, pair.vid_);
    }
  }

  std::set<label_t> get_labels_set() const override { return labels_; }

  ISigColumn* generate_signature() const override;

  void generate_dedup_offset(std::vector<size_t>& offsets) const override;

 private:
  friend class MLVertexColumnBuilder;
  std::vector<VertexRecord> vertices_;
  std::set<label_t> labels_;
};

class MLVertexColumnBuilder : public IVertexColumnBuilder {
 public:
  MLVertexColumnBuilder() = default;
  MLVertexColumnBuilder(const std::set<label_t>& labels) : labels_(labels) {}
  ~MLVertexColumnBuilder() = default;

  void reserve(size_t size) override { vertices_.reserve(size); }

  inline void push_back_vertex(VertexRecord v) override {
    labels_.insert(v.label_);
    vertices_.push_back(v);
  }

  std::shared_ptr<IContextColumn> finish(
      const std::shared_ptr<Arena>&) override;

 private:
  std::vector<VertexRecord> vertices_;
  std::set<label_t> labels_;
};

class OptionalMLVertexColumn : public MLVertexColumnBase {
 public:
  OptionalMLVertexColumn() = default;
  ~OptionalMLVertexColumn() = default;

  inline size_t size() const override { return vertices_.size(); }

  std::string column_info() const override {
    std::string labels;
    for (auto label : labels_) {
      labels += std::to_string(label);
      labels += ", ";
    }
    if (!labels.empty()) {
      labels.resize(labels.size() - 2);
    }
    return "OptionalMLVertexColumn(" + labels + ")[" + std::to_string(size()) +
           "]";
  }

  inline VertexColumnType vertex_column_type() const override {
    return VertexColumnType::kMultiple;
  }

  std::shared_ptr<IContextColumn> shuffle(
      const std::vector<size_t>& offsets) const override;

  std::shared_ptr<IContextColumn> optional_shuffle(
      const std::vector<size_t>& offsets) const override;

  inline VertexRecord get_vertex(size_t idx) const override {
    return vertices_[idx];
  }

  inline bool is_optional() const override { return true; }

  inline bool has_value(size_t idx) const override {
    return vertices_[idx].vid_ != std::numeric_limits<vid_t>::max();
  }

  template <typename FUNC_T>
  void foreach_vertex(const FUNC_T& func) const {
    size_t index = 0;
    for (auto& pair : vertices_) {
      func(index++, pair.label_, pair.vid_);
    }
  }

  std::set<label_t> get_labels_set() const override { return labels_; }

 private:
  friend class OptionalMLVertexColumnBuilder;
  std::vector<VertexRecord> vertices_;
  std::set<label_t> labels_;
};

class OptionalMLVertexColumnBuilder : public IOptionalVertexColumnBuilder {
 public:
  OptionalMLVertexColumnBuilder() = default;
  ~OptionalMLVertexColumnBuilder() = default;

  void reserve(size_t size) override { vertices_.reserve(size); }

  inline void push_back_opt(VertexRecord v) {
    labels_.insert(v.label_);
    vertices_.push_back(v);
  }

  inline void push_back_vertex(VertexRecord v) override {
    labels_.insert(v.label_);
    vertices_.push_back(v);
  }

  inline void push_back_null() override {
    vertices_.emplace_back(VertexRecord{std::numeric_limits<label_t>::max(),
                                        std::numeric_limits<vid_t>::max()});
  }

  inline void push_back_elem(const RTAny& val) override {
    this->push_back_opt(val.as_vertex());
  }

  std::shared_ptr<IContextColumn> finish(
      const std::shared_ptr<Arena>& arena) override;

 private:
  std::vector<VertexRecord> vertices_;
  std::set<label_t> labels_;
};

template <typename FUNC_T>
void foreach_vertex(const IVertexColumn& col, const FUNC_T& func) {
  if (col.vertex_column_type() == VertexColumnType::kSingle) {
    if (!col.is_optional()) {
      const SLVertexColumn& ref = dynamic_cast<const SLVertexColumn&>(col);
      ref.foreach_vertex(func);
    } else {
      const OptionalSLVertexColumn& ref =
          dynamic_cast<const OptionalSLVertexColumn&>(col);
      ref.foreach_vertex(func);
    }
  } else if (col.vertex_column_type() == VertexColumnType::kMultiple) {
    if (!col.is_optional()) {
      const MLVertexColumn& ref = dynamic_cast<const MLVertexColumn&>(col);
      ref.foreach_vertex(func);
    } else {
      const OptionalMLVertexColumn& ref =
          dynamic_cast<const OptionalMLVertexColumn&>(col);
      ref.foreach_vertex(func);
    }
  } else {
    const MSVertexColumn& ref = dynamic_cast<const MSVertexColumn&>(col);
    ref.foreach_vertex(func);
  }
}

}  // namespace runtime

}  // namespace gs

#endif  // COMMON_COLUMNS_VERTEX_COLUMNS_H_
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

#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"

namespace gs {
namespace runtime {

std::shared_ptr<IContextColumn> SLVertexColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  SLVertexColumnBuilder builder(label_);
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    builder.push_back_opt(vertices_[offset]);
  }
  return builder.finish();
}

void SLVertexColumn::generate_dedup_offset(std::vector<size_t>& offsets) const {
  offsets.clear();
#if 0
  std::set<vid_t> vset;
  size_t n = vertices_.size();
  for (size_t i = 0; i != n; ++i) {
    vid_t cur = vertices_[i];
    if (vset.find(cur) == vset.end()) {
      offsets.push_back(i);
      vset.insert(cur);
    }
  }
#else
  std::vector<bool> bitset;
  size_t vnum = vertices_.size();
  bitset.resize(vnum);
  for (size_t i = 0; i < vnum; ++i) {
    vid_t v = vertices_[i];
    if (bitset.size() <= v) {
      bitset.resize(v + 1);
      bitset[v] = true;

      offsets.push_back(i);
    } else {
      if (!bitset[v]) {
        offsets.push_back(i);
        bitset[v] = true;
      }
    }
  }
#endif
}

std::shared_ptr<IContextColumn> SLVertexColumn::union_col(
    std::shared_ptr<IContextColumn> other) const {
  CHECK(other->column_type() == ContextColumnType::kVertex);
  const IVertexColumn& vertex_column =
      *std::dynamic_pointer_cast<IVertexColumn>(other);
  if (vertex_column.vertex_column_type() == VertexColumnType::kSingle) {
    const SLVertexColumn& col =
        dynamic_cast<const SLVertexColumn&>(vertex_column);
    if (label() == col.label()) {
      SLVertexColumnBuilder builder(label());
      for (auto v : vertices_) {
        builder.push_back_opt(v);
      }
      for (auto v : col.vertices_) {
        builder.push_back_opt(v);
      }
      return builder.finish();
    }
  }
  LOG(FATAL) << "not support...";
  return nullptr;
}

std::shared_ptr<IContextColumn> SLVertexColumn::dup() const {
  SLVertexColumnBuilder builder(label_);
  for (auto v : vertices_) {
    builder.push_back_opt(v);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> SLVertexColumnBuilder::finish() {
  auto ret = std::make_shared<SLVertexColumn>(label_);
  ret->vertices_.swap(vertices_);
  return ret;
}

std::shared_ptr<IContextColumn> MSVertexColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  MLVertexColumnBuilder builder;
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    builder.push_back_vertex(get_vertex(offset));
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> MSVertexColumn::dup() const {
  MSVertexColumnBuilder builder;
  for (auto& pair : vertices_) {
    for (auto v : pair.second) {
      builder.push_back_vertex(std::make_pair(pair.first, v));
    }
  }
  return builder.finish();
}

ISigColumn* SLVertexColumn::generate_signature() const {
  return new SigColumn<vid_t>(vertices_);
}

ISigColumn* MSVertexColumn::generate_signature() const {
  LOG(FATAL) << "not implemented...";
  return nullptr;
}

std::shared_ptr<IContextColumn> MSVertexColumnBuilder::finish() {
  if (!cur_list_.empty()) {
    vertices_.emplace_back(cur_label_, std::move(cur_list_));
    cur_list_.clear();
  }
  auto ret = std::make_shared<MSVertexColumn>();
  auto& label_set = ret->labels_;
  for (auto& pair : vertices_) {
    label_set.insert(pair.first);
  }
  ret->vertices_.swap(vertices_);
  return ret;
}

std::shared_ptr<IContextColumn> MLVertexColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  MLVertexColumnBuilder builder(labels_);
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    builder.push_back_vertex(vertices_[offset]);
  }
  return builder.finish();
}

ISigColumn* MLVertexColumn::generate_signature() const {
  return new SigColumn<std::pair<label_t, vid_t>>(vertices_);
}

void MLVertexColumn::generate_dedup_offset(std::vector<size_t>& offsets) const {
  offsets.clear();
  std::set<std::pair<label_t, vid_t>> vset;
  size_t n = vertices_.size();
  for (size_t i = 0; i != n; ++i) {
    auto cur = vertices_[i];
    if (vset.find(cur) == vset.end()) {
      offsets.push_back(i);
      vset.insert(cur);
    }
  }
}

std::shared_ptr<IContextColumn> MLVertexColumn::dup() const {
  MLVertexColumnBuilder builder(labels_);
  for (auto& pair : vertices_) {
    builder.push_back_vertex(pair);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> MLVertexColumnBuilder::finish() {
  auto ret = std::make_shared<MLVertexColumn>();
  ret->vertices_.swap(vertices_);
  ret->labels_.swap(labels_);
  return ret;
}

void OptionalSLVertexColumn::generate_dedup_offset(
    std::vector<size_t>& offsets) const {
  offsets.clear();
  std::vector<bool> bitset;
  size_t vnum = vertices_.size();
  bitset.resize(vnum);
  bool flag = false;
  size_t idx = 0;
  for (size_t i = 0; i < vnum; ++i) {
    vid_t v = vertices_[i];
    if (v == std::numeric_limits<vid_t>::max()) {
      if (!flag) {
        flag = true;
        idx = i;
        continue;
      }
    }
    if (bitset.size() <= v) {
      bitset.resize(v + 1);
      bitset[v] = true;
      offsets.push_back(i);
    } else {
      if (!bitset[v]) {
        offsets.push_back(i);
        bitset[v] = true;
      }
    }
  }
  if (flag) {
    offsets.push_back(idx);
  }
}

std::shared_ptr<IContextColumn> OptionalSLVertexColumn::dup() const {
  OptionalSLVertexColumnBuilder builder(label_);
  for (auto v : vertices_) {
    builder.push_back_opt(v);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> OptionalSLVertexColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  OptionalSLVertexColumnBuilder builder(label_);
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    builder.push_back_opt(vertices_[offset]);
  }
  return builder.finish();
}

ISigColumn* OptionalSLVertexColumn::generate_signature() const {
  return new SigColumn<vid_t>(vertices_);
}

std::shared_ptr<IContextColumn> OptionalSLVertexColumnBuilder::finish() {
  auto ret = std::make_shared<OptionalSLVertexColumn>(label_);
  ret->vertices_.swap(vertices_);
  return ret;
}

}  // namespace runtime

}  // namespace gs

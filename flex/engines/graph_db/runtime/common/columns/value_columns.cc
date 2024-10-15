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

#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"

namespace gs {

namespace runtime {

std::shared_ptr<IContextColumn> ValueColumn<std::string_view>::shuffle(
    const std::vector<size_t>& offsets) const {
  ValueColumnBuilder<std::string_view> builder;
  builder.reserve(offsets.size());
  for (auto offset : offsets) {
    builder.push_back_opt(data_[offset]);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> ValueColumn<std::string_view>::dup() const {
  ValueColumnBuilder<std::string_view> builder;
  for (auto v : data_) {
    builder.push_back_opt(v);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> OptionalValueColumn<std::string_view>::shuffle(
    const std::vector<size_t>& offsets) const {
  OptionalValueColumnBuilder<std::string_view> builder;
  for (size_t i : offsets) {
    builder.push_back_opt(data_[i], valid_[i]);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> OptionalValueColumn<std::string_view>::dup()
    const {
  OptionalValueColumnBuilder<std::string_view> builder;
  for (size_t i = 0; i < data_.size(); ++i) {
    builder.push_back_opt(data_[i], valid_[i]);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> MapValueColumn::dup() const {
  MapValueColumnBuilder builder;
  builder.set_keys(keys_);
  for (const auto& values : values_) {
    builder.push_back_opt(values);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumn> MapValueColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  MapValueColumnBuilder builder;
  builder.reserve(offsets.size());
  builder.set_keys(keys_);
  for (auto offset : offsets) {
    builder.push_back_opt(values_[offset]);
  }
  return builder.finish();
}

std::shared_ptr<IContextColumnBuilder> MapValueColumn::builder() const {
  auto builder = std::make_shared<MapValueColumnBuilder>();
  builder->set_keys(keys_);
  return builder;
}

template class ValueColumn<int>;
template class ValueColumn<std::set<std::string>>;
template class ValueColumn<std::vector<vid_t>>;

}  // namespace runtime

}  // namespace gs

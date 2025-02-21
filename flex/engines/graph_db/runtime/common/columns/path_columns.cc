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

#include "flex/engines/graph_db/runtime/common/columns/path_columns.h"

namespace gs {
namespace runtime {

std::shared_ptr<IContextColumn> GeneralPathColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  GeneralPathColumnBuilder builder;
  builder.reserve(offsets.size());
  for (auto& offset : offsets) {
    builder.push_back_opt(data_[offset]);
  }
  return builder.finish(this->get_arena());
}

std::shared_ptr<IContextColumn> GeneralPathColumn::optional_shuffle(
    const std::vector<size_t>& offsets) const {
  OptionalGeneralPathColumnBuilder builder;
  builder.reserve(offsets.size());
  for (auto& offset : offsets) {
    if (offset == std::numeric_limits<size_t>::max()) {
      builder.push_back_null();
    } else {
      builder.push_back_opt(data_[offset], true);
    }
  }
  return builder.finish(this->get_arena());
}

std::shared_ptr<IContextColumnBuilder> GeneralPathColumn::builder() const {
  auto builder = std::make_shared<GeneralPathColumnBuilder>();
  return std::dynamic_pointer_cast<IContextColumnBuilder>(builder);
}

std::shared_ptr<IContextColumn> OptionalGeneralPathColumn::shuffle(
    const std::vector<size_t>& offsets) const {
  OptionalGeneralPathColumnBuilder builder;
  builder.reserve(offsets.size());
  for (auto& offset : offsets) {
    builder.push_back_opt(data_[offset], valids_[offset]);
  }
  return builder.finish(this->get_arena());
}

std::shared_ptr<IContextColumnBuilder> OptionalGeneralPathColumn::builder()
    const {
  auto builder = std::make_shared<OptionalGeneralPathColumnBuilder>();
  return std::dynamic_pointer_cast<IContextColumnBuilder>(builder);
}

}  // namespace runtime
}  // namespace gs
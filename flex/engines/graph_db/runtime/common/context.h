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

#ifndef RUNTIME_COMMON_CONTEXT_H_
#define RUNTIME_COMMON_CONTEXT_H_

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"

namespace gs {

namespace runtime {

class Context {
 public:
  Context();
  ~Context() = default;

  Context dup() const;

  void clear();

  void update_tag_ids(const std::vector<size_t>& tag_ids);

  void append_tag_id(size_t tag_id);
  void set(int alias, std::shared_ptr<IContextColumn> col);

  void set_with_reshuffle(int alias, std::shared_ptr<IContextColumn> col,
                          const std::vector<size_t>& offsets);

  void set_with_reshuffle_beta(int alias, std::shared_ptr<IContextColumn> col,
                               const std::vector<size_t>& offsets,
                               const std::set<int>& keep_cols);

  void reshuffle(const std::vector<size_t>& offsets);

  std::shared_ptr<IContextColumn> get(int alias);

  const std::shared_ptr<IContextColumn> get(int alias) const;

  size_t row_num() const;

  void desc(const std::string& info = "") const;

  void show(const ReadTransaction& txn) const;

  void generate_idx_col(int idx);

  size_t col_num() const;

  void push_idx_col();

  const ValueColumn<size_t>& get_idx_col() const;

  void pop_idx_col();

  std::vector<std::shared_ptr<IContextColumn>> columns;
  std::shared_ptr<IContextColumn> head;
  std::vector<std::shared_ptr<ValueColumn<size_t>>> idx_columns;
  std::vector<size_t> tag_ids;
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_CONTEXT_H_
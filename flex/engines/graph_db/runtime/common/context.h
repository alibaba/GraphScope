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
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_CONTEXT_H_
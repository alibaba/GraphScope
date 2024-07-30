#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"

namespace gs {

namespace runtime {

Context::Context() : head(nullptr) {}

void Context::clear() {
  columns.clear();
  head.reset();
  idx_columns.clear();
}

Context Context::dup() const {
  Context new_ctx;
  new_ctx.head = nullptr;
  for (auto col : columns) {
    if (col != nullptr) {
      new_ctx.columns.push_back(col->dup());
      if (col == head) {
        new_ctx.head = new_ctx.columns.back();
      }
    } else {
      new_ctx.columns.push_back(nullptr);
    }
  }
  if (head != nullptr && new_ctx.head == nullptr) {
    new_ctx.head = head->dup();
  }
  for (auto& idx_col : idx_columns) {
    new_ctx.idx_columns.emplace_back(
        std::dynamic_pointer_cast<ValueColumn<size_t>>(idx_col->dup()));
  }
  return new_ctx;
}

void Context::set(int alias, std::shared_ptr<IContextColumn> col) {
  head = col;
  if (alias >= 0) {
    if (columns.size() <= static_cast<size_t>(alias)) {
      columns.resize(alias + 1, nullptr);
    }
    assert(columns[alias] == nullptr);
    columns[alias] = col;
  }
}

void Context::set_with_reshuffle(int alias, std::shared_ptr<IContextColumn> col,
                                 const std::vector<size_t>& offsets) {
  head.reset();
  head = nullptr;

  if (alias >= 0) {
    if (columns.size() > static_cast<size_t>(alias)) {
      columns[alias].reset();
      columns[alias] = nullptr;
    }
  }

  reshuffle(offsets);
  set(alias, col);
}

void Context::set_with_reshuffle_beta(int alias,
                                      std::shared_ptr<IContextColumn> col,
                                      const std::vector<size_t>& offsets,
                                      const std::set<int>& keep_cols) {
  LOG(INFO) << col->size();
  head.reset();
  head = nullptr;

  if (alias >= 0) {
    if (columns.size() > static_cast<size_t>(alias)) {
      columns[alias].reset();
      columns[alias] = nullptr;
    }
  }
  for (size_t k = 0; k < columns.size(); ++k) {
    if (keep_cols.find(k) == keep_cols.end() && columns[k] != nullptr) {
      columns[k].reset();
      columns[k] = nullptr;
    }
  }

  reshuffle(offsets);

  set(alias, col);
}

void Context::reshuffle(const std::vector<size_t>& offsets) {
  bool head_shuffled = false;
  std::vector<std::shared_ptr<IContextColumn>> new_cols;

  for (auto col : columns) {
    if (col == nullptr) {
      new_cols.push_back(nullptr);

      continue;
    }
    if (col == head) {
      head = col->shuffle(offsets);
      new_cols.push_back(head);
      head_shuffled = true;
    } else {
      new_cols.push_back(col->shuffle(offsets));
    }
  }
  if (!head_shuffled && head != nullptr) {
    head = head->shuffle(offsets);
  }
  std::swap(new_cols, columns);
  std::vector<std::shared_ptr<ValueColumn<size_t>>> new_idx_columns;
  for (auto& idx_col : idx_columns) {
    new_idx_columns.emplace_back(std::dynamic_pointer_cast<ValueColumn<size_t>>(
        idx_col->shuffle(offsets)));
  }
  std::swap(new_idx_columns, idx_columns);
}

std::shared_ptr<IContextColumn> Context::get(int alias) {
  assert(static_cast<size_t>(alias) < columns.size());
  if (alias == -1) {
    return head;
  } else {
    return columns[alias];
  }
}

const std::shared_ptr<IContextColumn> Context::get(int alias) const {
  assert(static_cast<size_t>(alias) < columns.size());
  if (alias == -1) {
    assert(head != nullptr);
    return head;
  } else {
    assert(columns[alias] != nullptr);
    return columns[alias];
  }
}

size_t Context::row_num() const {
  for (auto col : columns) {
    if (col != nullptr) {
      return col->size();
    }
  }
  if (head != nullptr) {
    return head->size();
  }
  return 0;
}

void Context::desc(const std::string& info) const {
  if (!info.empty()) {
    LOG(INFO) << info;
  }
  for (size_t col_i = 0; col_i < col_num(); ++col_i) {
    if (columns[col_i] != nullptr) {
      LOG(INFO) << "\tcol-" << col_i << ": " << columns[col_i]->column_info();
    }
  }
  LOG(INFO) << "\thead: " << ((head == nullptr) ? "NULL" : head->column_info());
}

void Context::show(const ReadTransaction& txn) const {
  size_t rn = row_num();
  size_t cn = col_num();
  for (size_t ri = 0; ri < rn; ++ri) {
    std::string line;
    for (size_t ci = 0; ci < cn; ++ci) {
      if (columns[ci] != nullptr &&
          columns[ci]->column_type() == ContextColumnType::kVertex) {
        auto v = std::dynamic_pointer_cast<IVertexColumn>(columns[ci])
                     ->get_vertex(ri);
        int64_t id = txn.GetVertexId(v.first, v.second).AsInt64();
        line += std::to_string(id);
        line += ", ";
      } else if (columns[ci] != nullptr) {
        line += columns[ci]->get_elem(ri).to_string();
        line += ", ";
      }
    }
    LOG(INFO) << line;
  }
}

void Context::generate_idx_col(int idx) {
  size_t n = row_num();
  ValueColumnBuilder<size_t> builder;
  builder.reserve(n);
  for (size_t k = 0; k < n; ++k) {
    builder.push_back_opt(k);
  }
  set(idx, builder.finish());
}

size_t Context::col_num() const { return columns.size(); }

void Context::push_idx_col() {
  ValueColumnBuilder<size_t> builder;
  builder.reserve(row_num());
  for (size_t k = 0; k < row_num(); ++k) {
    builder.push_back_opt(k);
  }
  idx_columns.emplace_back(
      std::dynamic_pointer_cast<ValueColumn<size_t>>(builder.finish()));
}

const ValueColumn<size_t>& Context::get_idx_col() const {
  return *idx_columns.back();
}

void Context::pop_idx_col() { idx_columns.pop_back(); }

}  // namespace runtime

}  // namespace gs

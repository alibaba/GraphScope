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

#ifndef RUNTIME_COMMON_OPERATORS_PROJECT_H_
#define RUNTIME_COMMON_OPERATORS_PROJECT_H_

#include <tuple>
#include <type_traits>

#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"

#include "flex/engines/graph_db/runtime/common/leaf_utils.h"

namespace gs {

namespace runtime {

template <typename Expr>
struct ProjectExpr {
  ProjectExpr(const Expr& expr_, int alias_) : expr(expr_), alias(alias_) {}

  const Expr& expr;
  int alias;
};

struct DummyGetter {
  DummyGetter(int from_, int to_) : from(from_), to(to_) {}

  int from;
  int to;
};

template <std::size_t... Ns, typename... Ts>
auto tail_impl(std::index_sequence<Ns...>, const std::tuple<Ts...>& t) {
  return std::make_tuple(std::get<Ns + 1u>(t)...);
}

template <typename... Ts>
auto tail(const std::tuple<Ts...>& t) {
  return tail_impl(std::make_index_sequence<sizeof...(Ts) - 1u>(), t);
}

void map_value_impl(Context& ctx, const std::tuple<>&, size_t row_num,
                    std::vector<std::shared_ptr<IContextColumn>>& output) {}

template <typename T, typename... Rest>
void map_value_impl(Context& ctx, const std::tuple<ProjectExpr<T>, Rest...>& t,
                    size_t row_num,
                    std::vector<std::shared_ptr<IContextColumn>>& output) {
  const ProjectExpr<T>& cur = std::get<0>(t);
  using ELEM_T = typename T::elem_t;

  ValueColumnBuilder<ELEM_T> builder;
  builder.reserve(row_num);
  for (size_t k = 0; k < row_num; ++k) {
    builder.push_back_elem(cur.expr.eval_path(k));
  }

  if (output.size() <= cur.alias) {
    output.resize(cur.alias + 1, nullptr);
  }
  output[cur.alias] = builder.finish();

  map_value_impl(ctx, tail(t), row_num, output);
}

template <typename... Rest>
void map_value_impl(Context& ctx, const std::tuple<DummyGetter, Rest...>& t,
                    size_t row_num,
                    std::vector<std::shared_ptr<IContextColumn>>& output) {
  const DummyGetter& getter = std::get<0>(t);
  if (output.size() <= getter.to) {
    output.resize(getter.to + 1, nullptr);
  }
  output[getter.to] = ctx.get(getter.from);

  map_value_impl(ctx, tail(t), row_num, output);
}

class Project {
 public:
  static Context select_column(
      Context&& ctx, const std::vector<std::pair<size_t, size_t>>& mappings) {
    Context new_ctx;
    for (auto& pair : mappings) {
      new_ctx.set(pair.second, ctx.get(pair.first));
    }
    new_ctx.head = ctx.head;
    return new_ctx;
  }

  template <typename... Args>
  static Context map_value(const ReadTransaction& txn, Context&& ctx,
                           const std::tuple<Args...>& exprs, bool is_append) {
    std::vector<std::shared_ptr<IContextColumn>> new_columns;
    map_value_impl(ctx, exprs, ctx.row_num(), new_columns);

    Context new_ctx;
    if (is_append) {
      size_t col_num = ctx.col_num();
      for (size_t k = 0; k < col_num; ++k) {
        auto col = ctx.get(k);
        if (col != nullptr) {
          new_ctx.set(k, col);
        }
      }
    }
    size_t new_col_num = new_columns.size();
    for (size_t k = 0; k < new_col_num; ++k) {
      auto col = new_columns[k];
      if (col != nullptr) {
        new_ctx.set(k, col);
      }
    }

    if (is_append) {
      new_ctx.head = ctx.head;
    } else {
      new_ctx.head = nullptr;
    }
    return new_ctx;
  }

  template <typename EXPR_T>
  static bl::result<Context> map_value_general(
      const ReadTransaction& txn, Context&& ctx,
      const std::vector<ProjectExpr<EXPR_T>>& expressions, bool is_append) {
    if (!is_append) {
      std::vector<std::shared_ptr<IContextColumn>> new_columns;
      for (auto& pr : expressions) {
        int alias = pr.alias;
        const auto& expr = pr.expr;
        if (new_columns.size() <= alias) {
          new_columns.resize(alias + 1, nullptr);
        }
      }
    }
    RETURN_UNSUPPORTED_ERROR(
        "Currently we don't support project with is_append=true");
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_PROJECT_H_
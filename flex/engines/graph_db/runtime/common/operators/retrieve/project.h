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

#ifndef RUNTIME_COMMON_OPERATORS_RETRIEVE_PROJECT_H_
#define RUNTIME_COMMON_OPERATORS_RETRIEVE_PROJECT_H_

#include <tuple>
#include <type_traits>

#include "flex/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/operators/retrieve/order_by.h"

namespace gs {

namespace runtime {

struct ProjectExprBase {
  virtual ~ProjectExprBase() = default;
  virtual Context evaluate(const Context& ctx, Context&& ret) = 0;
  virtual int alias() const = 0;

  virtual bool order_by_limit(const Context& ctx, bool asc, size_t limit,
                              std::vector<size_t>& offsets) const {
    return false;
  }
};

struct DummyGetter : public ProjectExprBase {
  DummyGetter(int from, int to) : from_(from), to_(to) {}
  Context evaluate(const Context& ctx, Context&& ret) override {
    ret.set(to_, ctx.get(from_));
    return ret;
  }
  int alias() const override { return to_; }

  int from_;
  int to_;
};

template <typename EXPR, typename COLLECTOR_T>
struct ProjectExpr : public ProjectExprBase {
  EXPR expr_;
  COLLECTOR_T collector_;
  int alias_;

  ProjectExpr(EXPR&& expr, const COLLECTOR_T& collector, int alias)
      : expr_(std::move(expr)), collector_(collector), alias_(alias) {}

  Context evaluate(const Context& ctx, Context&& ret) override {
    size_t row_num = ctx.row_num();
    for (size_t i = 0; i < row_num; ++i) {
      collector_.collect(expr_, i);
    }
    ret.set(alias_, collector_.get());
    return ret;
  }

  bool order_by_limit(const Context& ctx, bool asc, size_t limit,
                      std::vector<size_t>& offsets) const override {
    size_t size = ctx.row_num();
    if (size == 0) {
      return false;
    }
    using T = typename EXPR::V;
    if constexpr (std::is_same_v<T, Date> || std::is_same_v<T, Day> ||
                  std::is_same_v<T, std::string_view> ||
                  std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                  std::is_same_v<T, double>) {
      if (asc) {
        TopNGenerator<T, TopNAscCmp<T>> generator(limit);
        for (size_t i = 0; i < size; ++i) {
          generator.push(expr_(i), i);
        }
        generator.generate_indices(offsets);
      } else {
        TopNGenerator<T, TopNDescCmp<T>> generator(limit);
        for (size_t i = 0; i < size; ++i) {
          generator.push(expr_(i), i);
        }
        generator.generate_indices(offsets);
      }
      return true;
    } else {
      return false;
    }
  }

  int alias() const override { return alias_; }
};

class Project {
 public:
  static bl::result<Context> project(
      Context&& ctx, const std::vector<std::unique_ptr<ProjectExprBase>>& exprs,
      bool is_append = false) {
    Context ret = ctx.newContext();
    if (is_append) {
      ret = ctx;
    }
    for (size_t i = 0; i < exprs.size(); ++i) {
      ret = exprs[i]->evaluate(ctx, std::move(ret));
    }
    return ret;
  }

  template <typename Comparer>
  static bl::result<Context> project_order_by_fuse(
      const GraphReadInterface& graph,
      const std::map<std::string, std::string>& params, Context&& ctx,
      const std::vector<std::function<std::unique_ptr<ProjectExprBase>(
          const GraphReadInterface& graph,
          const std::map<std::string, std::string>& params,
          const Context& ctx)>>& exprs,
      const std::function<Comparer(const Context&)>& cmp, size_t lower,
      size_t upper, const std::set<int>& order_index,
      const std::tuple<int, int, bool>& first_key) {
    lower = std::max(lower, static_cast<size_t>(0));
    upper = std::min(upper, ctx.row_num());

    Context ret = ctx.newContext();

    Context tmp(ctx);
    std::vector<int> alias;
    auto [fst_key, fst_idx, fst_asc] = first_key;
    auto expr = exprs[fst_idx](graph, params, ctx);
    std::vector<size_t> indices;

    if (upper < ctx.row_num() &&
        expr->order_by_limit(ctx, fst_asc, upper, indices)) {
      ctx.reshuffle(indices);
      for (size_t i : order_index) {
        auto expr = exprs[i](graph, params, ctx);
        int alias_ = expr->alias();
        ctx = expr->evaluate(ctx, std::move(ctx));
        alias.push_back(alias_);
      }
      auto cmp_ = cmp(ctx);
      auto ctx_res = OrderBy::order_by_with_limit(graph, std::move(ctx), cmp_,
                                                  lower, upper);
      if (!ctx_res) {
        return ctx_res;
      }
      ctx = std::move(ctx_res.value());
    } else {
      for (size_t i : order_index) {
        auto expr = exprs[i](graph, params, ctx);
        int alias_ = expr->alias();
        ctx = expr->evaluate(ctx, std::move(ctx));
        alias.push_back(alias_);
      }
      auto cmp_ = cmp(ctx);
      auto ctx_res = OrderBy::order_by_with_limit(graph, std::move(ctx), cmp_,
                                                  lower, upper);
      if (!ctx_res) {
        return ctx_res;
      }
      ctx = std::move(ctx_res.value());
    }

    for (int i : alias) {
      ret.set(i, ctx.get(i));
    }
    for (size_t i = 0; i < exprs.size(); ++i) {
      if (order_index.find(i) == order_index.end()) {
        ret = exprs[i](graph, params, ctx)->evaluate(ctx, std::move(ret));
      }
    }

    return ret;
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_RETRIEVE_PROJECT_H_
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

#ifndef RUNTIME_COMMON_OPERATORS_UPDATE_PROJECT_H_
#define RUNTIME_COMMON_OPERATORS_UPDATE_PROJECT_H_

#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/leaf_utils.h"

namespace gs {
namespace runtime {
struct WriteProjectExprBase {
  virtual ~WriteProjectExprBase() = default;
  virtual WriteContext evaluate(WriteContext& ctx, WriteContext&& ret) = 0;
};
struct ParamsGetter : public WriteProjectExprBase {
  ParamsGetter(const std::string& val, int alias) : val_(val), alias_(alias) {}

  WriteContext evaluate(WriteContext& ctx, WriteContext&& ret) override {
    WriteContext::WriteParams p(val_);
    ret.set(alias_, WriteContext::WriteParamsColumn({p}));
    return ret;
  }

  std::string_view val_;
  int alias_;
};

struct DummyWGetter : public WriteProjectExprBase {
  DummyWGetter(int from, int to) : from_(from), to_(to) {}

  WriteContext evaluate(WriteContext& ctx, WriteContext&& ret) override {
    ret.set(to_, std::move(ctx.get(from_)));
    return ret;
  }

  int from_;
  int to_;
};

struct PairsGetter : public WriteProjectExprBase {
  PairsGetter(int from, int first, int second)
      : from_(from), first_(first), second_(second) {}
  WriteContext evaluate(WriteContext& ctx, WriteContext&& ret) override {
    auto col = ctx.get(from_);
    auto [first, second] = col.pairs();
    ret.set(first_, std::move(first));
    ret.set(second_, std::move(second));
    return ret;
  }
  int from_;
  int first_;
  int second_;
};

struct PairsFstGetter : public WriteProjectExprBase {
  PairsFstGetter(int from, int to) : from_(from), to_(to) {}
  WriteContext evaluate(WriteContext& ctx, WriteContext&& ret) override {
    auto col = ctx.get(from_);
    auto [first, second] = col.pairs();
    ret.set(to_, std::move(first));
    return ret;
  }
  int from_;
  int to_;
};

struct PairsSndGetter : public WriteProjectExprBase {
  PairsSndGetter(int from, int to) : from_(from), to_(to) {}
  WriteContext evaluate(WriteContext& ctx, WriteContext&& ret) override {
    auto col = ctx.get(from_);
    auto [first, second] = col.pairs();
    ret.set(to_, std::move(second));
    return ret;
  }
  int from_;
  int to_;
};

class Project {
 public:
  static bl::result<WriteContext> project(
      WriteContext&& ctx,
      const std::vector<std::unique_ptr<WriteProjectExprBase>>& exprs);
};

struct UProjectExprBase {
  virtual ~UProjectExprBase() = default;
  virtual Context evaluate(const Context& ctx, Context&& ret) = 0;
};

struct UDummyGetter : public UProjectExprBase {
  UDummyGetter(int from, int to) : from_(from), to_(to) {}
  Context evaluate(const Context& ctx, Context&& ret) override {
    ret.set(to_, ctx.get(from_));
    return ret;
  }

  int from_;
  int to_;
};

template <typename EXPR, typename COLLECTOR_T>
struct UProjectExpr : public UProjectExprBase {
  EXPR expr_;
  COLLECTOR_T collector_;
  int alias_;

  UProjectExpr(EXPR&& expr, const COLLECTOR_T& collector, int alias)
      : expr_(std::move(expr)), collector_(collector), alias_(alias) {}

  Context evaluate(const Context& ctx, Context&& ret) override {
    size_t row_num = ctx.row_num();
    for (size_t i = 0; i < row_num; ++i) {
      collector_.collect(expr_, i);
    }
    ret.set(alias_, collector_.get());
    return ret;
  }
};

class UProject {
 public:
  static bl::result<Context> project(
      Context&& ctx,
      const std::vector<std::unique_ptr<UProjectExprBase>>& exprs,
      bool is_append = false);
};

}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_UPDATE_PROJECT_H_
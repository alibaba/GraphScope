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
  static WriteContext project(
      WriteContext&& ctx,
      const std::vector<std::unique_ptr<WriteProjectExprBase>>& exprs);
};
}  // namespace runtime
}  // namespace gs
#endif  // RUNTIME_COMMON_OPERATORS_UPDATE_PROJECT_H_
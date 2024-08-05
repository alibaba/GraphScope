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
#ifndef RUNTIME_COMMON_OPERATORS_ORDER_BY_H_
#define RUNTIME_COMMON_OPERATORS_ORDER_BY_H_

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/operators/project.h"

#include <queue>

namespace gs {

namespace runtime {

template <typename T>
class AscValue {
 public:
  AscValue() : val_() {}
  AscValue(const T& val) : val_(val) {}

  bool operator<(const AscValue& rhs) const { return val_ < rhs.val_; }

  const T& value() const { return val_; }

 private:
  T val_;
};

template <typename VAR_T>
class AscWrapper {
 public:
  using elem_t = typename VAR_T::elem_t;
  using value_t = AscValue<elem_t>;

  AscWrapper(VAR_T&& var) : var_(std::move(var)) {}
  AscValue<elem_t> get(size_t idx) const {
    return AscValue<elem_t>(var_.typed_eval_path(idx));
  }

 private:
  VAR_T var_;
};

template <typename T>
class DescValue {
 public:
  DescValue() : val_() {}
  DescValue(const T& val) : val_(val) {}

  bool operator<(const DescValue& rhs) const { return rhs.val_ < val_; }

  const T& value() const { return val_; }

 private:
  T val_;
};

template <typename VAR_T>
class DescWrapper {
 public:
  using elem_t = typename VAR_T::elem_t;
  using value_t = DescValue<elem_t>;

  DescWrapper(VAR_T&& var) : var_(std::move(var)) {}
  DescValue<elem_t> get(size_t idx) const {
    return DescValue<elem_t>(var_.typed_eval_path(idx));
  }

 private:
  VAR_T var_;
};

bool apply_compare(const std::tuple<>&, size_t lhs, size_t rhs) {
  return lhs < rhs;
}

template <typename T, typename... Rest>
bool apply_compare(const std::tuple<T, Rest...>& keys, size_t lhs, size_t rhs) {
  const T& key = std::get<0>(keys);

  auto lhs_value = key.get(lhs);
  auto rhs_value = key.get(rhs);

  if (lhs_value < rhs_value) {
    return true;
  } else if (rhs_value < lhs_value) {
    return false;
  } else {
    return apply_compare(tail(keys), lhs, rhs);
  }
}

template <typename... Types>
struct GeneralTemplatedComparer {
 public:
  GeneralTemplatedComparer(std::tuple<Types...>&& keys)
      : keys_(std::move(keys)) {}

  bool operator()(size_t lhs, size_t rhs) const {
    return apply_compare(keys_, lhs, rhs);
  }

 private:
  std::tuple<Types...> keys_;
};

template <typename T>
struct ValueTypeExtractor;

template <typename... Types>
struct ValueTypeExtractor<std::tuple<Types...>> {
  using type = std::tuple<typename Types::elem_t...>;
};

template <std::size_t I, std::size_t N>
struct TupleInvokeHelper {
  template <typename... Ts, typename... Us>
  static void apply(const std::tuple<Ts...>& input_tuple,
                    std::tuple<Us...>& output_tuple, size_t idx) {
    std::get<I>(output_tuple) = std::get<I>(input_tuple).typed_eval_path(idx);
    TupleInvokeHelper<I + 1, N>::apply(input_tuple, output_tuple, idx);
  }
};

// Specialization to end recursion
template <std::size_t N>
struct TupleInvokeHelper<N, N> {
  template <typename... Ts, typename... Us>
  static void apply(const std::tuple<Ts...>&, std::tuple<Us...>&, size_t) {
    // Do nothing, end of recursion
  }
};

// Function to start the tuple invocation process
template <typename... Ts>
typename ValueTypeExtractor<std::tuple<Ts...>>::type invokeTuple(
    const std::tuple<Ts...>& input_tuple, size_t idx) {
  typename ValueTypeExtractor<std::tuple<Ts...>>::type output_tuple;
  TupleInvokeHelper<0, sizeof...(Ts)>::apply(input_tuple, output_tuple, idx);
  return output_tuple;
}

class OrderBy {
 public:
  template <typename Comparer>
  static void order_by_with_limit(const ReadTransaction& txn, Context& ctx,
                                  const Comparer& cmp, size_t low,
                                  size_t high) {
    if (low == 0 && high >= ctx.row_num()) {
      std::vector<size_t> offsets;
      for (size_t i = 0; i < ctx.row_num(); ++i) {
        offsets.push_back(i);
      }
      std::sort(offsets.begin(), offsets.end(),
                [&](size_t lhs, size_t rhs) { return cmp(lhs, rhs); });
      ctx.reshuffle(offsets);
      return;
    }
    size_t row_num = ctx.row_num();
    std::priority_queue<size_t, std::vector<size_t>, Comparer> queue(cmp);
    for (size_t i = 0; i < row_num; ++i) {
      queue.push(i);
      if (queue.size() > high) {
        queue.pop();
      }
    }
    std::vector<size_t> offsets;
    for (size_t k = 0; k < low; ++k) {
      queue.pop();
    }
    offsets.resize(queue.size());
    size_t idx = queue.size();

    while (!queue.empty()) {
      offsets[--idx] = queue.top();
      queue.pop();
    }

    ctx.reshuffle(offsets);
  }

  template <typename... Args>
  static void order_by_with_limit_beta(const ReadTransaction& txn, Context& ctx,
                                       const std::tuple<Args...>& keys,
                                       size_t low, size_t high) {
    size_t row_num = ctx.row_num();
    using value_t = typename ValueTypeExtractor<std::tuple<Args...>>::type;
    std::priority_queue<std::pair<value_t, size_t>,
                        std::vector<std::pair<value_t, size_t>>>
        queue;

    for (size_t i = 0; i < row_num; ++i) {
      auto cur = invokeTuple(keys, i);
      queue.emplace(std::move(cur), i);
      if (queue.size() > high) {
        queue.pop();
      }
    }

    for (size_t k = 0; k < low; ++k) {
      queue.pop();
    }
    std::vector<size_t> offsets(queue.size());
    size_t idx = queue.size();
    while (!queue.empty()) {
      offsets[--idx] = queue.top().second;
      queue.pop();
    }

    ctx.reshuffle(offsets);
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_ORDER_BY_H_
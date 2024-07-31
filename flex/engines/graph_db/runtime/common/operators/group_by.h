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

#ifndef RUNTIME_COMMON_OPERATORS_GROUP_BY_H_
#define RUNTIME_COMMON_OPERATORS_GROUP_BY_H_

#include <map>
#include <unordered_map>
#include <vector>

#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "flex/engines/graph_db/runtime/common/operators/project.h"

namespace gs {

namespace runtime {

void aggregate_value_impl(
    const std::tuple<>&, const std::vector<std::vector<size_t>>& to_aggregate,
    std::vector<std::shared_ptr<IContextColumn>>& output) {}

template <typename T, typename... Rest>
void aggregate_value_impl(
    const std::tuple<ProjectExpr<T>, Rest...>& t,
    const std::vector<std::vector<size_t>>& to_aggregate,
    std::vector<std::shared_ptr<IContextColumn>>& output) {
  const ProjectExpr<T>& cur = std::get<0>(t);
  using ELEM_T = typename T::elem_t;

  ValueColumnBuilder<ELEM_T> builder;
  builder.reserve(to_aggregate.size());
  for (size_t k = 0; k < to_aggregate.size(); ++k) {
    builder.push_back_opt(cur.expr.reduce(to_aggregate[k]));
  }

  if (output.size() <= cur.alias) {
    output.resize(cur.alias + 1, nullptr);
  }
  output[cur.alias] = builder.finish();

  aggregate_value_impl(tail(t), to_aggregate, output);
}

class GroupBy {
 public:
  template <typename... Args>
  static Context group_by(Context&& ctx, const std::vector<size_t>& keys,
                          const std::tuple<Args...>& funcs) {
    size_t row_num = ctx.row_num();
    std::vector<size_t> offsets;
    std::vector<std::vector<size_t>> to_aggregate;

    if (keys.size() == 0) {
      return ctx;
    } else if (keys.size() == 1) {
      ISigColumn* sig = ctx.get(keys[0])->generate_signature();
#if 1
      std::unordered_map<size_t, size_t> sig_to_root;
      for (size_t r_i = 0; r_i < row_num; ++r_i) {
        size_t cur = sig->get_sig(r_i);
        auto iter = sig_to_root.find(cur);
        if (iter == sig_to_root.end()) {
          sig_to_root.emplace(cur, offsets.size());
          offsets.push_back(r_i);
          std::vector<size_t> list;
          list.push_back(r_i);
          to_aggregate.emplace_back(std::move(list));
        } else {
          to_aggregate[iter->second].push_back(r_i);
        }
      }
#else
      std::vector<std::pair<size_t, size_t>> vec;
      vec.reserve(row_num);
      for (size_t r_i = 0; r_i < row_num; ++r_i) {
        size_t cur = sig->get_sig(r_i);
        vec.emplace_back(cur, r_i);
      }
      std::sort(vec.begin(), vec.end());
      if (row_num > 0) {
        std::vector<size_t> ta;
        size_t cur = vec[0].first;
        ta.push_back(vec[0].second);
        offsets.push_back(vec[0].second);
        for (size_t k = 1; k < row_num; ++k) {
          if (vec[k].first != cur) {
            to_aggregate.emplace_back(std::move(ta));
            ta.clear();
            cur = vec[k].first;
            ta.push_back(vec[k].second);
            offsets.push_back(vec[k].second);
          } else {
            ta.push_back(vec[k].second);
          }
        }
        if (!ta.empty()) {
          to_aggregate.emplace_back(std::move(ta));
        }
      }
#endif
      delete sig;
    } else if (keys.size() == 2) {
      std::map<std::pair<size_t, size_t>, size_t> sig_to_root;
      ISigColumn* sig0 = ctx.get(keys[0])->generate_signature();
      ISigColumn* sig1 = ctx.get(keys[1])->generate_signature();
      for (size_t r_i = 0; r_i < row_num; ++r_i) {
        auto cur = std::make_pair(sig0->get_sig(r_i), sig1->get_sig(r_i));
        auto iter = sig_to_root.find(cur);
        if (iter == sig_to_root.end()) {
          sig_to_root.emplace(cur, offsets.size());
          offsets.push_back(r_i);
          std::vector<size_t> list;
          list.push_back(r_i);
          to_aggregate.emplace_back(std::move(list));
        } else {
          to_aggregate[iter->second].push_back(r_i);
        }
      }
      delete sig0;
      delete sig1;
    } else {
      std::set<std::string> set;
      for (size_t r_i = 0; r_i < row_num; ++r_i) {
        std::vector<char> bytes;
        Encoder encoder(bytes);
        for (size_t k = 0; k < keys.size(); ++k) {
          auto val = ctx.get(keys[k])->get_elem(k);
          val.encode_sig(val.type(), encoder);
          encoder.put_byte('#');
        }
        std::string sv(bytes.data(), bytes.size());
        if (set.find(sv) == set.end()) {
          offsets.push_back(r_i);
          set.insert(sv);
        }
      }
    }

    std::vector<std::shared_ptr<IContextColumn>> new_columns;
    aggregate_value_impl(funcs, to_aggregate, new_columns);

    Context new_ctx;
    for (auto col : keys) {
      new_ctx.set(col, ctx.get(col));
    }
    new_ctx.head = nullptr;

    new_ctx.reshuffle(offsets);
    for (size_t k = 0; k < new_columns.size(); ++k) {
      auto col = new_columns[k];
      if (col != nullptr) {
        new_ctx.set(k, col);
      }
    }

    new_ctx.head = nullptr;
    return new_ctx;
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_GROUP_BY_H_
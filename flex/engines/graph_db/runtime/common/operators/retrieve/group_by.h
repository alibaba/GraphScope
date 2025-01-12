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

#ifndef RUNTIME_COMMON_OPERATORS_RETRIEVE_GROUP_BY_H_
#define RUNTIME_COMMON_OPERATORS_RETRIEVE_GROUP_BY_H_

#include <map>
#include <unordered_map>
#include <vector>

#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"
#include "flex/engines/graph_db/runtime/common/context.h"
#include "parallel_hashmap/phmap.h"
namespace std {
template <>
struct hash<gs::runtime::VertexRecord> {
  size_t operator()(const gs::runtime::VertexRecord& record) const {
    return std::hash<int64_t>()(1ll * record.vid_ << 4 | record.label_);
  }
};

template <>
struct hash<gs::Date> {
  size_t operator()(const gs::Date& date) const {
    return std::hash<int64_t>()(date.milli_second);
  }
};

};  // namespace std
namespace gs {

namespace runtime {

enum class AggrKind {
  kSum,
  kMin,
  kMax,
  kCount,
  kCountDistinct,
  kToSet,
  kFirst,
  kToList,
  kAvg,
};

struct KeyBase {
  virtual ~KeyBase() = default;
  virtual std::pair<std::vector<size_t>, std::vector<std::vector<size_t>>>
  group(const Context& ctx) = 0;
  virtual const std::vector<std::pair<int, int>>& tag_alias() const = 0;
};
template <typename EXPR>
struct Key : public KeyBase {
  Key(EXPR&& expr, const std::vector<std::pair<int, int>>& tag_alias)
      : expr(std::move(expr)), tag_alias_(tag_alias) {}
  std::pair<std::vector<size_t>, std::vector<std::vector<size_t>>> group(
      const Context& ctx) {
    size_t row_num = ctx.row_num();
    std::vector<std::vector<size_t>> groups;
    std::vector<size_t> offsets;
    phmap::flat_hash_map<typename EXPR::V, size_t> group_map;
    for (size_t i = 0; i < row_num; ++i) {
      auto val = expr(i);
      auto iter = group_map.find(val);
      if (iter == group_map.end()) {
        size_t idx = groups.size();
        group_map[val] = idx;
        groups.emplace_back();
        groups.back().push_back(i);
        offsets.push_back(i);
      } else {
        groups[iter->second].push_back(i);
      }
    }
    return std::make_pair(std::move(offsets), std::move(groups));
  }
  const std::vector<std::pair<int, int>>& tag_alias() const override {
    return tag_alias_;
  }
  EXPR expr;
  std::vector<std::pair<int, int>> tag_alias_;
};

template <typename EXPR>
struct GKey : public KeyBase {
  GKey(std::vector<EXPR>&& exprs,
       const std::vector<std::pair<int, int>>& tag_alias)
      : exprs(std::move(exprs)), tag_alias_(tag_alias) {}
  std::pair<std::vector<size_t>, std::vector<std::vector<size_t>>> group(
      const Context& ctx) {
    size_t row_num = ctx.row_num();
    std::vector<std::vector<size_t>> groups;
    std::vector<size_t> offsets;
    std::unordered_map<std::string_view, size_t> sig_to_root;
    std::vector<std::vector<char>> root_list;
    for (size_t i = 0; i < row_num; ++i) {
      std::vector<char> buf;
      ::gs::Encoder encoder(buf);
      for (size_t k_i = 0; k_i < exprs.size(); ++k_i) {
        auto val = exprs[k_i](i);
        val.encode_sig(val.type(), encoder);
      }
      std::string_view sv(buf.data(), buf.size());
      auto iter = sig_to_root.find(sv);
      if (iter != sig_to_root.end()) {
        groups[iter->second].push_back(i);
      } else {
        sig_to_root.emplace(sv, groups.size());
        root_list.emplace_back(std::move(buf));
        offsets.push_back(i);
        std::vector<size_t> ret_elem;
        ret_elem.push_back(i);
        groups.emplace_back(std::move(ret_elem));
      }
    }
    return std::make_pair(std::move(offsets), std::move(groups));
  }
  const std::vector<std::pair<int, int>>& tag_alias() const override {
    return tag_alias_;
  }
  std::vector<EXPR> exprs;
  std::vector<std::pair<int, int>> tag_alias_;
};

struct ReducerBase {
  virtual ~ReducerBase() = default;
  virtual Context reduce(const Context& ctx, Context&& ret,
                         const std::vector<std::vector<size_t>>& groups,
                         std::set<size_t>& filter) = 0;
};

template <typename REDUCER_T, typename COLLECTOR_T>
struct Reducer : public ReducerBase {
  Reducer(REDUCER_T&& reducer, COLLECTOR_T&& collector, int alias)
      : reducer_(std::move(reducer)),
        collector_(std::move(collector)),
        alias_(alias) {}

  Context reduce(const Context& ctx, Context&& ret,
                 const std::vector<std::vector<size_t>>& groups,
                 std::set<size_t>& filter) {
    using T = typename REDUCER_T::V;
    collector_.init(groups.size());
    for (size_t i = 0; i < groups.size(); ++i) {
      const auto& group = groups[i];
      T val{};
      if (!reducer_(group, val)) {
        filter.insert(i);
      }
      collector_.collect(std::move(val));
    }
    ret.set(alias_, collector_.get());
    return ret;
  }

  REDUCER_T reducer_;
  COLLECTOR_T collector_;
  int alias_;
};

class GroupBy {
 public:
  static Context group_by(const GraphReadInterface& graph, Context&& ctx,
                          std::unique_ptr<KeyBase>&& key,
                          std::vector<std::unique_ptr<ReducerBase>>&& aggrs) {
    auto [offsets, groups] = key->group(ctx);
    Context ret;
    const auto& tag_alias = key->tag_alias();
    for (size_t i = 0; i < tag_alias.size(); ++i) {
      ret.set(tag_alias[i].second, ctx.get(tag_alias[i].first));
    }
    ret.reshuffle(offsets);
    std::set<size_t> filter;
    for (auto& aggr : aggrs) {
      ret = aggr->reduce(ctx, std::move(ret), groups, filter);
    }
    if (filter.empty()) {
      return ret;
    } else {
      std::vector<size_t> new_offsets;
      for (size_t i = 0; i < ret.row_num(); ++i) {
        if (filter.find(i) == filter.end()) {
          new_offsets.push_back(i);
        }
      }
      ret.reshuffle(new_offsets);
      return ret;
    }
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_RETRIEVE_GROUP_BY_H_
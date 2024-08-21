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

#ifndef RUNTIME_COMMON_OPERATORS_JOIN_H_
#define RUNTIME_COMMON_OPERATORS_JOIN_H_

#include <vector>
#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {
namespace runtime {
template <typename Expr>
struct JoinKey {
  JoinKey(const Expr& expr, int tag) : expr_(expr), tag_(tag) {}
  auto operator()(size_t i) { return expr_(i); }
  Expr expr_;
  int tag_;
};
struct JoinParams {
  std::vector<int> left_columns;
  std::vector<int> right_columns;
  JoinKind join_type;
};

class Join {
 public:
  static Context join(Context&& ctx, Context&& ctx2, const JoinParams& params);

  template <typename... Left_Keys, typename... Right_Keys>
  static Context join(Context&& ctx, Context&& ctx2, JoinKind join_type,
                      std::tuple<Left_Keys...> left_keys,
                      std::tuple<Right_Keys...> right_keys) {
    size_t left_row_num = ctx.row_num();
    std::vector<decltype(std::declval<Left_Keys>()(0))...> left_keys_vec(
        left_row_num);
    for (size_t i = 0; i < left_row_num; ++i) {
      left_keys_vec[i] = std::apply(
          [&left_keys_vec, i](auto&&... keys) {
            return std::make_tuple(keys(i)...);
          },
          left_keys);
    }
    size_t right_row_num = ctx2.row_num();
    std::vector<decltype(std::declval<Right_Keys>()(0))...> right_keys_vec(
        right_row_num);
    for (size_t i = 0; i < right_row_num; ++i) {
      right_keys_vec[i] = std::apply(
          [i](auto&&... keys) { return std::make_tuple(keys(i)...); },
          right_keys);
    }
    std::vector<size_t> left_offsets(left_row_num);
    std::iota(left_offsets.begin(), left_offsets.end(), 0);
    std::vector<size_t> right_offsets(right_row_num);
    std::iota(right_offsets.begin(), right_offsets.end(), 0);
    std::sort(left_offsets.begin(), left_offsets.end(),
              [&left_keys_vec](size_t a, size_t b) {
                return left_keys_vec[a] < left_keys_vec[b];
              });
    std::sort(right_offsets.begin(), right_offsets.end(),
              [&right_keys_vec](size_t a, size_t b) {
                return right_keys_vec[a] < right_keys_vec[b];
              });
    if (join_type == JoinKind::kInnerJoin) {
      std::vector<size_t> offset1;
      std::vector<size_t> offset2;
      size_t i = 0, j = 0;
      while (i < left_row_num && j < right_row_num) {
        if (left_keys_vec[left_offsets[i]] < right_keys_vec[right_offsets[j]]) {
          ++i;
        } else if (left_keys_vec[left_offsets[i]] >
                   right_keys_vec[right_offsets[j]]) {
          ++j;
        } else {
          auto val = left_keys_vec[left_offsets[i]];
          int l1 = i;
          while (i < left_row_num && left_keys_vec[left_offsets[i]] == val) {
            ++i;
          }
          int l2 = j;
          while (j < right_row_num && right_keys_vec[right_offsets[j]] == val) {
            ++j;
          }
          for (int k = l1; k < i; ++k) {
            for (int l = l2; l < j; ++l) {
              offset1.push_back(left_offsets[k]);
              offset2.push_back(right_offsets[l]);
            }
          }
        }
      }

      ctx.reshuffle(offset1);
      ctx2.reshuffle(offset2);
      for (size_t i = 0; i < ctx.col_num(); ++i) {
        if (i >= ctx.col_num() || ctx.get(i) == nullptr) {
          ctx.set(i, ctx2.get(i));
        }
      }
      return ctx;

    } else if ((join_type == JoinKind::kAntiJoin) ||
               (join_type == JoinKind::kSemiJoin)) {
      if (join_type == JoinKind::kAntiJoin) {
        std::vector<size_t> offset;
        size_t j = 0;
        for (size_t i = 0; i < left_row_num; ++i) {
          auto val = left_offsets[i];
          while (j < right_row_num &&
                 right_keys_vec[right_offsets[j]] < left_keys_vec[val]) {
            ++j;
          }
          if (j == right_row_num ||
              left_keys_vec[val] < right_keys_vec[right_offsets[j]]) {
            offset.push_back(val);
            if (j > 0) {
              --j;
            }
          }
        }

        ctx.reshuffle(offset);
      } else {
        std::vector<size_t> offset;

        size_t j = 0;
        for (size_t i = 0; i < left_row_num; ++i) {
          auto val = left_offsets[i];
          while (j < right_row_num &&
                 right_keys_vec[right_offsets[j]] < left_keys_vec[val]) {
            ++j;
          }
          if (j < right_row_num &&
              left_keys_vec[val] == right_keys_vec[right_offsets[j]]) {
            offset.push_back(val);
          }
        }
        ctx.reshuffle(offset);
      }
      return ctx;

    } else if (join_type == JoinKind::kLeftOuterJoin) {
      std::vector<std::shared_ptr<IOptionalContextColumnBuilder>> builders;
      std::set<int> right_columns;
      std::apply(
          [&right_columns](auto&&... keys) {
            (right_columns.insert(keys.tag_), ...);
          },
          right_keys);
      for (size_t i = 0; i < ctx2.col_num(); ++i) {
        if (right_columns.find(i) == right_columns.end() &&
            ctx2.get(i) != nullptr) {
          builders.emplace_back(ctx2.get(i)->optional_builder());
        } else {
          builders.emplace_back(nullptr);
        }
      }
      std::vector<size_t> offsets;
      std::vector<size_t> offsets2;
      size_t i = 0, j = 0;
      while (i < left_row_num) {
        auto val = left_keys_vec[left_offsets[i]];
        while (j < right_row_num && right_keys_vec[right_offsets[j]] < val) {
          ++j;
        }
        int k = j;
        if (j < right_row_num && right_keys_vec[right_offsets[j]] == val) {
          while (j < right_row_num && right_keys_vec[right_offsets[j]] == val) {
            offsets.push_back(left_offsets[i]);
            offsets2.push_back(right_offsets[j]);
            ++j;
          }
        } else {
          offsets.push_back(left_offsets[i]);
          offsets2.push_back(std::numeric_limits<size_t>::max());
        }
        j = k;
      }
      ctx.reshuffle(offsets);
      for (size_t i = 0; i < ctx2.col_num(); ++i) {
        if (builders[i] != nullptr) {
          for (size_t j = 0; j < offsets2.size(); ++j) {
            if (offsets2[j] == std::numeric_limits<size_t>::max()) {
              builders[i]->push_back_null();
            } else {
              builders[i]->push_back_elem(ctx2.get(i)->get_elem(offsets2[j]));
            }
          }
        }
      }
      for (size_t i = 0; i < ctx2.col_num(); ++i) {
        if (builders[i] != nullptr) {
          ctx.set(i, builders[i]->finish());
        }
      }
      return ctx;
    }

    LOG(FATAL) << "Unsupported join type: " << static_cast<int>(join_type);
  }
};
}  // namespace runtime
}  // namespace gs

#endif  // COMMON_OPERATORS_JOIN_H_
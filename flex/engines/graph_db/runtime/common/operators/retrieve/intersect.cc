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

#include "flex/engines/graph_db/runtime/common/operators/retrieve/intersect.h"
#include "flex/engines/graph_db/runtime/common/columns/edge_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/value_columns.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "parallel_hashmap/phmap.h"

namespace gs {

namespace runtime {

static Context left_outer_intersect(Context&& ctx, Context&& ctx0,
                                    Context&& ctx1, int key) {
  // specifically, this function is called when the first context is not
  // optional and the second context is optional
  auto& idx_col0 = ctx0.get_offsets();
  auto& idx_col1 = ctx1.get_offsets();
  auto& vlist0 = *(std::dynamic_pointer_cast<IVertexColumn>(ctx0.get(key)));
  auto& vlist1 = *(std::dynamic_pointer_cast<IVertexColumn>(ctx1.get(key)));

  std::vector<size_t> offset0, offset1;
  if (ctx0.row_num() == ctx.row_num()) {
    bool flag = true;
    for (size_t i = 0; i < idx_col0.size(); ++i) {
      if (idx_col0.get_value(i) != i) {
        flag = false;
        break;
      }
    }
    if (flag) {
      size_t j = 0;
      for (size_t i = 0; i < ctx0.row_num(); i++) {
        bool exist = false;
        for (; j < ctx1.row_num(); ++j) {
          if (idx_col1.get_value(j) != idx_col0.get_value(i)) {
            break;
          }
          if (vlist1.has_value(j) &&
              vlist0.get_vertex(i) == vlist1.get_vertex(j)) {
            exist = true;
            offset0.emplace_back(i);
            offset1.emplace_back(j);
          }
        }
        if (!exist) {
          offset0.emplace_back(i);
          offset1.emplace_back(std::numeric_limits<size_t>::max());
        }
      }
      ctx0.reshuffle(offset0);
      ctx1.optional_reshuffle(offset1);
      ctx.reshuffle(ctx0.get_offsets().data());
      for (size_t i = 0; i < ctx0.col_num() || i < ctx1.col_num(); ++i) {
        if (i < ctx0.col_num()) {
          if (ctx0.get(i) != nullptr) {
            ctx.set(i, ctx0.get(i));
          }
        }
        if (i < ctx1.col_num()) {
          if ((i >= ctx.col_num() || ctx.get(i) == nullptr) &&
              ctx1.get(i) != nullptr) {
            ctx.set(i, ctx1.get(i));
          }
        } else if (i >= ctx.col_num()) {
          ctx.set(i, nullptr);
        }
      }
      return ctx;
    }
  }
  std::vector<size_t> shuffle_offsets, shuffle_offsets_1;
  std::vector<std::vector<size_t>> vec0(ctx.row_num() + 1),
      vec1(ctx.row_num() + 1);
  for (size_t i = 0; i < idx_col0.size(); ++i) {
    vec0[idx_col0.get_value(i)].push_back(i);
  }
  for (size_t i = 0; i < idx_col1.size(); ++i) {
    vec1[idx_col1.get_value(i)].push_back(i);
  }
  size_t len = vec0.size();
  for (size_t i = 0; i < len; ++i) {
    if (vec1[i].empty()) {
      if (!vec0[i].empty()) {
        for (auto& j : vec0[i]) {
          shuffle_offsets.push_back(j);
          shuffle_offsets_1.push_back(std::numeric_limits<size_t>::max());
        }
      }
      continue;
    }

    if (vec0.size() < vec1.size()) {
      phmap::flat_hash_map<VertexRecord, std::vector<size_t>, VertexRecordHash>
          left_map;
      for (auto& j : vec0[i]) {
        left_map[vlist0.get_vertex(j)].push_back(j);
      }
      for (auto& k : vec1[i]) {
        auto iter = left_map.find(vlist1.get_vertex(k));
        if (iter != left_map.end()) {
          for (auto& idx : iter->second) {
            shuffle_offsets.push_back(idx);
            shuffle_offsets_1.push_back(k);
          }
        }
      }
    } else {
      phmap::flat_hash_map<VertexRecord, std::vector<size_t>, VertexRecordHash>
          right_map;
      for (auto& k : vec1[i]) {
        right_map[vlist1.get_vertex(k)].push_back(k);
      }
      for (auto& j : vec0[i]) {
        auto iter = right_map.find(vlist0.get_vertex(j));
        if (iter != right_map.end()) {
          for (auto& idx : iter->second) {
            shuffle_offsets.push_back(j);
            shuffle_offsets_1.push_back(idx);
          }
        } else {
          shuffle_offsets.push_back(j);
          shuffle_offsets_1.push_back(std::numeric_limits<size_t>::max());
        }
      }
    }
  }
  ctx0.reshuffle(shuffle_offsets);
  ctx1.optional_reshuffle(shuffle_offsets_1);
  ctx.reshuffle(ctx0.get_offsets().data());
  for (size_t i = 0; i < ctx0.col_num() || i < ctx1.col_num(); ++i) {
    if (i >= ctx.col_num() || ctx.get(i) == nullptr) {
      std::shared_ptr<IContextColumn> col(nullptr);
      if (i < ctx0.col_num()) {
        if (ctx0.get(i) != nullptr) {
          col = ctx0.get(i);
        }
      }
      if (col == nullptr && i < ctx1.col_num()) {
        if (ctx1.get(i) != nullptr) {
          col = ctx1.get(i);
        }
      }
      ctx.set(i, col);
    }
  }
  return ctx;
}
static Context intersect_impl(Context&& ctx, std::vector<Context>&& ctxs,
                              int key) {
  if (ctxs[0].get(key)->column_type() == ContextColumnType::kVertex) {
    if (ctxs.size() == 2) {
      auto& vlist0 =
          *(std::dynamic_pointer_cast<IVertexColumn>(ctxs[0].get(key)));
      auto& vlist1 =
          *(std::dynamic_pointer_cast<IVertexColumn>(ctxs[1].get(key)));
      if (!vlist0.is_optional() && vlist1.is_optional()) {
        return left_outer_intersect(std::move(ctx), std::move(ctxs[0]),
                                    std::move(ctxs[1]), key);
      } else if (vlist0.is_optional() && !vlist1.is_optional()) {
        return left_outer_intersect(std::move(ctx), std::move(ctxs[1]),
                                    std::move(ctxs[0]), key);
      } else if (vlist0.is_optional() && vlist1.is_optional()) {
        //        LOG(INFO) << "both optional" << vlist0.size() << " " <<
        //        vlist1.size();
      }
      auto& idx_col0 = ctxs[0].get_offsets();
      auto& idx_col1 = ctxs[1].get_offsets();
      std::vector<size_t> offsets0(idx_col0.size()), offsets1(idx_col1.size());
      size_t maxi = ctx.row_num();
      std::vector<std::vector<size_t>> vec0(maxi + 1), vec1(maxi + 1);

      std::vector<size_t> shuffle_offsets;
      std::vector<size_t> shuffle_offsets_1;

      for (size_t i = 0; i < idx_col0.size(); ++i) {
        vec0[idx_col0.get_value(i)].push_back(i);
      }
      for (size_t i = 0; i < idx_col1.size(); ++i) {
        vec1[idx_col1.get_value(i)].push_back(i);
      }
      size_t len = vec0.size();
      for (size_t i = 0; i < len; ++i) {
        if (vec1[i].empty() || vec0[i].empty()) {
          continue;
        }

        if (vec0.size() < vec1.size()) {
          phmap::flat_hash_map<VertexRecord, std::vector<size_t>,
                               VertexRecordHash>
              left_map;
          for (auto& j : vec0[i]) {
            left_map[vlist0.get_vertex(j)].push_back(j);
          }
          for (auto& k : vec1[i]) {
            auto iter = left_map.find(vlist1.get_vertex(k));
            if (iter != left_map.end()) {
              for (auto& idx : iter->second) {
                shuffle_offsets.push_back(idx);
                shuffle_offsets_1.push_back(k);
              }
            }
          }
        } else {
          phmap::flat_hash_map<VertexRecord, std::vector<size_t>,
                               VertexRecordHash>
              right_map;
          for (auto& k : vec1[i]) {
            right_map[vlist1.get_vertex(k)].push_back(k);
          }
          for (auto& j : vec0[i]) {
            auto iter = right_map.find(vlist0.get_vertex(j));
            if (iter != right_map.end()) {
              for (auto& idx : iter->second) {
                shuffle_offsets.push_back(j);
                shuffle_offsets_1.push_back(idx);
              }
            }
          }
        }
      }

      ctxs[0].reshuffle(shuffle_offsets);
      ctxs[1].reshuffle(shuffle_offsets_1);
      ctx.reshuffle(ctxs[0].get_offsets().data());

      for (size_t i = 0; i < ctxs[0].col_num() || i < ctxs[1].col_num(); ++i) {
        if (i >= ctx.col_num() || ctx.get(i) == nullptr) {
          std::shared_ptr<IContextColumn> col(nullptr);
          if (i < ctxs[0].col_num()) {
            if (ctxs[0].get(i) != nullptr) {
              col = ctxs[0].get(i);
            }
          }
          if (col == nullptr && i < ctxs[1].col_num()) {
            if (ctxs[1].get(i) != nullptr) {
              col = ctxs[1].get(i);
            }
          }
          ctx.set(i, col);
        }
      }
      return ctx;
    }
  }
  LOG(FATAL) << "not support";
  return Context();
}

Context Intersect::intersect(Context&& ctx, std::vector<Context>&& ctxs,
                             int key) {
  return intersect_impl(std::move(ctx), std::move(ctxs), key);
}

}  // namespace runtime

}  // namespace gs

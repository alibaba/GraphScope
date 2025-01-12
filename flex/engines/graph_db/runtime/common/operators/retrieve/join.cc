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

#include "flex/engines/graph_db/runtime/common/operators/retrieve/join.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "parallel_hashmap/phmap.h"

// #define DEBUG_JOIN

namespace gs {

namespace runtime {

using vertex_pair = std::pair<VertexRecord, VertexRecord>;

static Context default_semi_join(Context&& ctx, Context&& ctx2,
                                 const JoinParams& params) {
  size_t right_size = ctx2.row_num();
  std::set<std::string> right_set;
  std::vector<size_t> offset;

  for (size_t r_i = 0; r_i < right_size; ++r_i) {
    std::vector<char> bytes;
    Encoder encoder(bytes);
    for (size_t i = 0; i < params.right_columns.size(); i++) {
      auto val = ctx2.get(params.right_columns[i])->get_elem(r_i);
      val.encode_sig(val.type(), encoder);
      encoder.put_byte('#');
    }
    std::string cur(bytes.begin(), bytes.end());
    right_set.insert(cur);
  }

  size_t left_size = ctx.row_num();
  for (size_t r_i = 0; r_i < left_size; ++r_i) {
    std::vector<char> bytes;
    Encoder encoder(bytes);
    for (size_t i = 0; i < params.left_columns.size(); i++) {
      auto val = ctx.get(params.left_columns[i])->get_elem(r_i);
      val.encode_sig(val.type(), encoder);
      encoder.put_byte('#');
    }
    std::string cur(bytes.begin(), bytes.end());
    if (params.join_type == JoinKind::kSemiJoin) {
      if (right_set.find(cur) != right_set.end()) {
        offset.push_back(r_i);
      }
    } else {
      if (right_set.find(cur) == right_set.end()) {
        offset.push_back(r_i);
      }
    }
  }
  ctx.reshuffle(offset);
  return ctx;
}

static Context single_vertex_column_inner_join(Context&& ctx, Context&& ctx2,
                                               const JoinParams& params) {
  std::vector<size_t> left_offset, right_offset;
  auto casted_left_col =
      std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.left_columns[0]));
  auto casted_right_col = std::dynamic_pointer_cast<IVertexColumn>(
      ctx2.get(params.right_columns[0]));

  size_t left_size = casted_left_col->size();
  size_t right_size = casted_right_col->size();

  if (left_size < right_size) {
    phmap::flat_hash_set<VertexRecord, VertexRecordHash> left_set;
    phmap::flat_hash_map<VertexRecord, std::vector<size_t>, VertexRecordHash>
        right_map;
    for (size_t r_i = 0; r_i < left_size; ++r_i) {
      left_set.emplace(casted_left_col->get_vertex(r_i));
    }
    for (size_t r_i = 0; r_i < right_size; ++r_i) {
      auto cur = casted_right_col->get_vertex(r_i);
      if (left_set.find(cur) != left_set.end()) {
        right_map[cur].emplace_back(r_i);
      }
    }
    for (size_t r_i = 0; r_i < left_size; ++r_i) {
      auto iter = right_map.find(casted_left_col->get_vertex(r_i));
      if (iter != right_map.end()) {
        for (auto idx : iter->second) {
          left_offset.emplace_back(r_i);
          right_offset.emplace_back(idx);
        }
      }
    }
  } else {
    phmap::flat_hash_set<VertexRecord, VertexRecordHash> right_set;
    phmap::flat_hash_map<VertexRecord, std::vector<size_t>, VertexRecordHash>
        left_map;
    if (right_size != 0) {
      for (size_t r_i = 0; r_i < right_size; ++r_i) {
        right_set.emplace(casted_right_col->get_vertex(r_i));
      }
      for (size_t r_i = 0; r_i < left_size; ++r_i) {
        auto cur = casted_left_col->get_vertex(r_i);
        if (right_set.find(cur) != right_set.end()) {
          left_map[cur].emplace_back(r_i);
        }
      }
      for (size_t r_i = 0; r_i < right_size; ++r_i) {
        auto iter = left_map.find(casted_right_col->get_vertex(r_i));
        if (iter != left_map.end()) {
          for (auto idx : iter->second) {
            right_offset.emplace_back(r_i);
            left_offset.emplace_back(idx);
          }
        }
      }
    }
  }
  ctx.reshuffle(left_offset);
  ctx2.reshuffle(right_offset);
  Context ret;
  for (size_t i = 0; i < ctx.col_num(); i++) {
    ret.set(i, ctx.get(i));
  }
  for (size_t i = 0; i < ctx2.col_num(); i++) {
    if (i >= ret.col_num() || ret.get(i) == nullptr) {
      ret.set(i, ctx2.get(i));
    }
  }
  return ret;
}

static Context dual_vertex_column_inner_join(Context&& ctx, Context&& ctx2,
                                             const JoinParams& params) {
  std::vector<size_t> left_offset, right_offset;
  auto casted_left_col =
      std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.left_columns[0]));
  auto casted_left_col2 =
      std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.left_columns[1]));
  auto casted_right_col = std::dynamic_pointer_cast<IVertexColumn>(
      ctx2.get(params.right_columns[0]));
  auto casted_right_col2 = std::dynamic_pointer_cast<IVertexColumn>(
      ctx2.get(params.right_columns[1]));

  size_t left_size = casted_left_col->size();
  size_t right_size = casted_right_col->size();

  if (left_size < right_size) {
    phmap::flat_hash_set<vertex_pair, VertexRecordHash> left_set;
    phmap::flat_hash_map<vertex_pair, std::vector<size_t>, VertexRecordHash>
        right_map;
    for (size_t r_i = 0; r_i < left_size; ++r_i) {
      left_set.emplace(casted_left_col->get_vertex(r_i),
                       casted_left_col2->get_vertex(r_i));
    }
    for (size_t r_i = 0; r_i < right_size; ++r_i) {
      auto cur1 = casted_right_col->get_vertex(r_i);
      auto cur2 = casted_right_col2->get_vertex(r_i);
      auto cur = std::make_pair(cur1, cur2);
      if (left_set.find(cur) != left_set.end()) {
        right_map[cur].emplace_back(r_i);
      }
    }
    for (size_t r_i = 0; r_i < left_size; ++r_i) {
      auto cur1 = casted_left_col->get_vertex(r_i);
      auto cur2 = casted_left_col2->get_vertex(r_i);
      auto cur = std::make_pair(cur1, cur2);
      auto iter = right_map.find(cur);
      if (iter != right_map.end()) {
        for (auto idx : iter->second) {
          left_offset.emplace_back(r_i);
          right_offset.emplace_back(idx);
        }
      }
    }
  } else {
    phmap::flat_hash_map<vertex_pair, std::vector<size_t>, VertexRecordHash>
        right_map;
    for (size_t r_i = 0; r_i < right_size; ++r_i) {
      auto cur1 = casted_right_col->get_vertex(r_i);
      auto cur2 = casted_right_col2->get_vertex(r_i);
      auto cur = std::make_pair(cur1, cur2);
      right_map[cur].emplace_back(r_i);
    }
    for (size_t r_i = 0; r_i < left_size; ++r_i) {
      auto cur1 = casted_left_col->get_vertex(r_i);
      auto cur2 = casted_left_col2->get_vertex(r_i);
      auto cur = std::make_pair(cur1, cur2);

      auto iter = right_map.find(cur);
      if (iter != right_map.end()) {
        for (auto idx : iter->second) {
          left_offset.emplace_back(r_i);
          right_offset.emplace_back(idx);
        }
      }
    }
  }
  ctx.reshuffle(left_offset);
  ctx2.reshuffle(right_offset);
  Context ret;
  for (size_t i = 0; i < ctx.col_num(); i++) {
    ret.set(i, ctx.get(i));
  }
  for (size_t i = 0; i < ctx2.col_num(); i++) {
    if (i >= ret.col_num() || ret.get(i) == nullptr) {
      ret.set(i, ctx2.get(i));
    }
  }
  return ret;
}

static Context default_inner_join(Context&& ctx, Context&& ctx2,
                                  const JoinParams& params) {
  std::vector<size_t> left_offset, right_offset;
  size_t right_size = ctx2.row_num();
  std::map<std::string, std::vector<size_t>> right_set;

  for (size_t r_i = 0; r_i < right_size; ++r_i) {
    std::vector<char> bytes;
    Encoder encoder(bytes);
    for (size_t i = 0; i < params.right_columns.size(); i++) {
      auto val = ctx2.get(params.right_columns[i])->get_elem(r_i);
      val.encode_sig(val.type(), encoder);
      encoder.put_byte('#');
    }
    std::string cur(bytes.begin(), bytes.end());
    right_set[cur].emplace_back(r_i);
  }

  size_t left_size = ctx.row_num();
  for (size_t r_i = 0; r_i < left_size; ++r_i) {
    std::vector<char> bytes;
    Encoder encoder(bytes);
    for (size_t i = 0; i < params.left_columns.size(); i++) {
      auto val = ctx.get(params.left_columns[i])->get_elem(r_i);
      val.encode_sig(val.type(), encoder);
      encoder.put_byte('#');
    }
    std::string cur(bytes.begin(), bytes.end());
    if (right_set.find(cur) != right_set.end()) {
      for (auto right : right_set[cur]) {
        left_offset.push_back(r_i);
        right_offset.push_back(right);
      }
    }
  }
  ctx.reshuffle(left_offset);
  ctx2.reshuffle(right_offset);
  Context ret;
  for (size_t i = 0; i < ctx.col_num(); i++) {
    ret.set(i, ctx.get(i));
  }
  for (size_t i = 0; i < ctx2.col_num(); i++) {
    if (i >= ret.col_num() || ret.get(i) == nullptr) {
      ret.set(i, ctx2.get(i));
    }
  }
  return ret;
}

static Context single_vertex_column_left_outer_join(Context&& ctx,
                                                    Context&& ctx2,
                                                    const JoinParams& params) {
  std::vector<size_t> left_offset, right_offset;
  auto casted_left_col =
      std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.left_columns[0]));
  auto casted_right_col = std::dynamic_pointer_cast<IVertexColumn>(
      ctx2.get(params.right_columns[0]));

  std::vector<size_t> left_offsets;
  std::vector<size_t> right_offsets;

  size_t left_size = casted_left_col->size();
  size_t right_size = casted_right_col->size();
  if (left_size < right_size) {
    phmap::flat_hash_set<VertexRecord, VertexRecordHash> left_set;
    phmap::flat_hash_map<VertexRecord, std::vector<size_t>, VertexRecordHash>
        right_map;
    for (size_t r_i = 0; r_i < left_size; ++r_i) {
      left_set.emplace(casted_left_col->get_vertex(r_i));
    }
    for (size_t r_i = 0; r_i < right_size; ++r_i) {
      auto cur = casted_right_col->get_vertex(r_i);
      if (left_set.find(cur) != left_set.end()) {
        right_map[cur].emplace_back(r_i);
      }
    }
    for (size_t r_i = 0; r_i < left_size; ++r_i) {
      auto cur = casted_left_col->get_vertex(r_i);
      auto iter = right_map.find(cur);
      if (iter == right_map.end()) {
        left_offsets.emplace_back(r_i);
        right_offsets.emplace_back(std::numeric_limits<size_t>::max());
      } else {
        for (auto idx : iter->second) {
          left_offsets.emplace_back(r_i);
          right_offsets.emplace_back(idx);
        }
      }
    }
  } else {
    phmap::flat_hash_map<VertexRecord, std::vector<vid_t>, VertexRecordHash>
        right_map;
    if (left_size > 0) {
      for (size_t r_i = 0; r_i < right_size; ++r_i) {
        right_map[casted_right_col->get_vertex(r_i)].emplace_back(r_i);
      }
    }
    for (size_t r_i = 0; r_i < left_size; ++r_i) {
      auto cur = casted_left_col->get_vertex(r_i);
      auto iter = right_map.find(cur);
      if (iter == right_map.end()) {
        left_offsets.emplace_back(r_i);
        right_offsets.emplace_back(std::numeric_limits<size_t>::max());
      } else {
        for (auto idx : iter->second) {
          left_offsets.emplace_back(r_i);
          right_offsets.emplace_back(idx);
        }
      }
    }
  }
  ctx.reshuffle(left_offsets);
  ctx2.remove(params.right_columns[0]);
  ctx2.optional_reshuffle(right_offsets);
  for (size_t i = 0; i < ctx2.col_num(); ++i) {
    if (ctx2.get(i) != nullptr) {
      ctx.set(i, ctx2.get(i));
    }
  }
  return ctx;
}

static Context dual_vertex_column_left_outer_join(Context&& ctx, Context&& ctx2,
                                                  const JoinParams& params) {
  auto left_col0 = ctx.get(params.left_columns[0]);
  auto left_col1 = ctx.get(params.left_columns[1]);
  auto right_col0 = ctx2.get(params.right_columns[0]);
  auto right_col1 = ctx2.get(params.right_columns[1]);
  auto casted_left_col0 = std::dynamic_pointer_cast<IVertexColumn>(left_col0);
  auto casted_left_col1 = std::dynamic_pointer_cast<IVertexColumn>(left_col1);
  auto casted_right_col0 = std::dynamic_pointer_cast<IVertexColumn>(right_col0);
  auto casted_right_col1 = std::dynamic_pointer_cast<IVertexColumn>(right_col1);

  std::vector<size_t> left_offsets;
  std::vector<size_t> right_offsets;
  size_t left_size = casted_left_col0->size();
  size_t right_size = casted_right_col0->size();

  if (left_size < right_size) {
    phmap::flat_hash_set<vertex_pair, VertexRecordHash> left_set;
    phmap::flat_hash_map<vertex_pair, std::vector<size_t>, VertexRecordHash>
        right_map;
    for (size_t r_i = 0; r_i < left_size; ++r_i) {
      vertex_pair cur(casted_left_col0->get_vertex(r_i),
                      casted_left_col1->get_vertex(r_i));
      left_set.emplace(cur);
    }
    for (size_t r_i = 0; r_i < right_size; ++r_i) {
      vertex_pair cur(casted_right_col0->get_vertex(r_i),
                      casted_right_col1->get_vertex(r_i));
      if (left_set.find(cur) != left_set.end()) {
        right_map[cur].emplace_back(r_i);
      }
    }
    for (size_t r_i = 0; r_i < left_size; ++r_i) {
      vertex_pair cur(casted_left_col0->get_vertex(r_i),
                      casted_left_col1->get_vertex(r_i));
      auto iter = right_map.find(cur);
      if (iter == right_map.end()) {
        left_offsets.emplace_back(r_i);
        right_offsets.emplace_back(std::numeric_limits<size_t>::max());
      } else {
        for (auto idx : iter->second) {
          left_offsets.emplace_back(r_i);
          right_offsets.emplace_back(idx);
        }
      }
    }
  } else {
    phmap::flat_hash_map<vertex_pair, std::vector<vid_t>, VertexRecordHash>
        right_map;
    if (left_size > 0) {
      for (size_t r_i = 0; r_i < right_size; ++r_i) {
        vertex_pair cur(casted_right_col0->get_vertex(r_i),
                        casted_right_col1->get_vertex(r_i));
        right_map[cur].emplace_back(r_i);
      }
    }
    for (size_t r_i = 0; r_i < left_size; ++r_i) {
      vertex_pair cur(casted_left_col0->get_vertex(r_i),
                      casted_left_col1->get_vertex(r_i));
      auto iter = right_map.find(cur);
      if (iter == right_map.end()) {
        left_offsets.emplace_back(r_i);
        right_offsets.emplace_back(std::numeric_limits<size_t>::max());
      } else {
        for (auto idx : iter->second) {
          left_offsets.emplace_back(r_i);
          right_offsets.emplace_back(idx);
        }
      }
    }
  }
  ctx.reshuffle(left_offsets);
  ctx2.remove(params.right_columns[0]);
  ctx2.remove(params.right_columns[1]);
  ctx2.optional_reshuffle(right_offsets);
  for (size_t i = 0; i < ctx2.col_num(); ++i) {
    if (ctx2.get(i) != nullptr) {
      ctx.set(i, ctx2.get(i));
    }
  }
  return ctx;
}

static Context default_left_outer_join(Context&& ctx, Context&& ctx2,
                                       const JoinParams& params) {
  size_t right_size = ctx2.row_num();
  std::map<std::string, std::vector<vid_t>> right_map;
  if (ctx.row_num() > 0) {
    for (size_t r_i = 0; r_i < right_size; r_i++) {
      std::vector<char> bytes;
      Encoder encoder(bytes);
      for (size_t i = 0; i < params.right_columns.size(); i++) {
        auto val = ctx2.get(params.right_columns[i])->get_elem(r_i);
        val.encode_sig(val.type(), encoder);
        encoder.put_byte('#');
      }
      std::string cur(bytes.begin(), bytes.end());
      right_map[cur].emplace_back(r_i);
    }
  }

  std::vector<std::shared_ptr<IOptionalContextColumnBuilder>> builders;
  for (size_t i = 0; i < ctx2.col_num(); i++) {
    if (std::find(params.right_columns.begin(), params.right_columns.end(),
                  i) == params.right_columns.end() &&
        ctx2.get(i) != nullptr) {
      builders.emplace_back(ctx2.get(i)->optional_builder());
    } else {
      builders.emplace_back(nullptr);
    }
  }

  std::vector<size_t> offsets;
  size_t left_size = ctx.row_num();
  for (size_t r_i = 0; r_i < left_size; r_i++) {
    std::vector<char> bytes;
    Encoder encoder(bytes);
    for (size_t i = 0; i < params.left_columns.size(); i++) {
      auto val = ctx.get(params.left_columns[i])->get_elem(r_i);
      val.encode_sig(val.type(), encoder);
      encoder.put_byte('#');
    }
    std::string cur(bytes.begin(), bytes.end());
    if (right_map.find(cur) == right_map.end()) {
      for (size_t i = 0; i < ctx2.col_num(); i++) {
        if (builders[i] != nullptr) {
          builders[i]->push_back_null();
        }
      }
      offsets.emplace_back(r_i);
    } else {
      for (auto idx : right_map[cur]) {
        for (size_t i = 0; i < ctx2.col_num(); i++) {
          if (builders[i] != nullptr) {
            builders[i]->push_back_elem(ctx2.get(i)->get_elem(idx));
          }
        }
        offsets.emplace_back(r_i);
      }
    }
  }
  ctx.reshuffle(offsets);
  for (size_t i = 0; i < ctx2.col_num(); i++) {
    if (builders[i] != nullptr) {
      ctx.set(i, builders[i]->finish());
    } else if (i >= ctx.col_num()) {
      ctx.set(i, nullptr);
    }
  }

  return ctx;
}

Context Join::join(Context&& ctx, Context&& ctx2, const JoinParams& params) {
  CHECK(params.left_columns.size() == params.right_columns.size())
      << "Join columns size mismatch";
#ifdef DEBUG_JOIN
  LOG(INFO) << params.join_type
            << ": keys size: " << params.right_columns.size()
            << ", left size = " << ctx.row_num()
            << ", right size = " << ctx2.row_num();
  for (size_t k = 0; k < params.left_columns.size(); ++k) {
    LOG(INFO) << "left key - " << k << ": "
              << ctx.get(params.left_columns[k])->column_info();
  }
  for (size_t k = 0; k < params.right_columns.size(); ++k) {
    LOG(INFO) << "right key - " << k << ": "
              << ctx2.get(params.right_columns[k])->column_info();
  }
#endif
  if (params.join_type == JoinKind::kSemiJoin ||
      params.join_type == JoinKind::kAntiJoin) {
    return default_semi_join(std::move(ctx), std::move(ctx2), params);
  } else if (params.join_type == JoinKind::kInnerJoin) {
    if (params.right_columns.size() == 1 &&
        ctx.get(params.right_columns[0])->column_type() ==
            ContextColumnType::kVertex) {
      return single_vertex_column_inner_join(std::move(ctx), std::move(ctx2),
                                             params);
    } else if (params.right_columns.size() == 2 &&
               ctx.get(params.right_columns[0])->column_type() ==
                   ContextColumnType::kVertex &&
               ctx.get(params.right_columns[1])->column_type() ==
                   ContextColumnType::kVertex) {
      return dual_vertex_column_inner_join(std::move(ctx), std::move(ctx2),
                                           params);
    } else {
      return default_inner_join(std::move(ctx), std::move(ctx2), params);
    }
  } else if (params.join_type == JoinKind::kLeftOuterJoin) {
    if (params.right_columns.size() == 1 &&
        ctx.get(params.right_columns[0])->column_type() ==
            ContextColumnType::kVertex) {
      return single_vertex_column_left_outer_join(std::move(ctx),
                                                  std::move(ctx2), params);
    } else if (params.right_columns.size() == 2 &&
               ctx.get(params.right_columns[0])->column_type() ==
                   ContextColumnType::kVertex &&
               ctx.get(params.right_columns[1])->column_type() ==
                   ContextColumnType::kVertex) {
      return dual_vertex_column_left_outer_join(std::move(ctx), std::move(ctx2),
                                                params);
    } else {
      return default_left_outer_join(std::move(ctx), std::move(ctx2), params);
    }
  }
  LOG(FATAL) << "Unsupported join type" << params.join_type;
  return Context();
}

}  // namespace runtime
}  // namespace gs
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

#include "flex/engines/graph_db/runtime/common/operators/join.h"
#include "flex/engines/graph_db/runtime/common/columns/vertex_columns.h"

#include "flex/engines/graph_db/runtime/common/leaf_utils.h"

namespace gs {

namespace runtime {
bl::result<Context> Join::join(Context&& ctx, Context&& ctx2,
                               const JoinParams& params) {
  CHECK(params.left_columns.size() == params.right_columns.size())
      << "Join columns size mismatch";
  if (params.join_type == JoinKind::kSemiJoin ||
      params.join_type == JoinKind::kAntiJoin) {
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
  } else if (params.join_type == JoinKind::kInnerJoin) {
    size_t right_size = ctx2.row_num();
    std::map<std::string, std::vector<size_t>> right_set;
    std::vector<size_t> left_offset, right_offset;

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
  } else if (params.join_type == JoinKind::kLeftOuterJoin) {
    size_t right_size = ctx2.row_num();
    auto right_col = ctx2.get(params.right_columns[0]);
    //    CHECK(right_col->column_type() == ContextColumnType::kVertex);

    std::map<std::string, std::vector<vid_t>> right_map;
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
  } else {
    LOG(ERROR) << "Unsupported join type: "
               << static_cast<int>(params.join_type);
    RETURN_NOT_IMPLEMENTED_ERROR(
        "Join of type " + std::to_string(static_cast<int>(params.join_type)) +
        " is not supported");
  }

  return Context();
}
}  // namespace runtime
}  // namespace gs
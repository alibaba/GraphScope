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

#include "flex/engines/graph_db/runtime/common/operators/dedup.h"

namespace gs {

namespace runtime {

void Dedup::dedup(const ReadTransaction& txn, Context& ctx,
                  const std::vector<size_t>& cols) {
  size_t row_num = ctx.row_num();
  std::vector<size_t> offsets;
  if (cols.size() == 0) {
    return;
  } else if (cols.size() == 1) {
    ctx.get(cols[0])->generate_dedup_offset(offsets);
  } else if (cols.size() == 2) {
    ISigColumn* sig0 = ctx.get(cols[0])->generate_signature();
    ISigColumn* sig1 = ctx.get(cols[1])->generate_signature();
#if 0
      std::set<std::pair<size_t, size_t>> sigset;
      for (size_t r_i = 0; r_i < row_num; ++r_i) {
        auto cur = std::make_pair(sig0->get_sig(r_i), sig1->get_sig(r_i));
        if (sigset.find(cur) == sigset.end()) {
          offsets.push_back(r_i);
          sigset.insert(cur);
        }
      }
#else
    std::vector<std::tuple<size_t, size_t, size_t>> list;
    for (size_t r_i = 0; r_i < row_num; ++r_i) {
      list.emplace_back(sig0->get_sig(r_i), sig1->get_sig(r_i), r_i);
    }
    std::sort(list.begin(), list.end());
    size_t list_size = list.size();
    if (list_size > 0) {
      offsets.push_back(std::get<2>(list[0]));
      for (size_t k = 1; k < list_size; ++k) {
        if (std::get<0>(list[k]) != std::get<0>(list[k - 1]) ||
            std::get<1>(list[k]) != std::get<1>(list[k - 1])) {
          offsets.push_back(std::get<2>(list[k]));
        }
      }
      std::sort(offsets.begin(), offsets.end());
    }
#endif
    delete sig0;
    delete sig1;
  } else if (cols.size() == 3) {
    std::set<std::tuple<size_t, size_t, size_t>> sigset;
    ISigColumn* sig0 = ctx.get(cols[0])->generate_signature();
    ISigColumn* sig1 = ctx.get(cols[1])->generate_signature();
    ISigColumn* sig2 = ctx.get(cols[2])->generate_signature();
    for (size_t r_i = 0; r_i < row_num; ++r_i) {
      auto cur = std::make_tuple(sig0->get_sig(r_i), sig1->get_sig(r_i),
                                 sig2->get_sig(r_i));
      if (sigset.find(cur) == sigset.end()) {
        offsets.push_back(r_i);
        sigset.insert(cur);
      }
    }
    delete sig0;
    delete sig1;
    delete sig2;
  } else {
    std::set<std::string> set;
    for (size_t r_i = 0; r_i < row_num; ++r_i) {
      std::vector<char> bytes;
      Encoder encoder(bytes);
      for (size_t c_i = 0; c_i < cols.size(); ++c_i) {
        auto val = ctx.get(cols[c_i])->get_elem(r_i);
        val.encode_sig(val.type(), encoder);
        encoder.put_byte('#');
      }
      std::string cur(bytes.begin(), bytes.end());
      if (set.find(cur) == set.end()) {
        offsets.push_back(r_i);
        set.insert(cur);
      }
    }
  }
  ctx.reshuffle(offsets);
}

void Dedup::dedup(const ReadTransaction& txn, Context& ctx,
                  const std::vector<size_t>& cols,
                  const std::vector<std::function<RTAny(size_t)>>& vars) {
  std::set<std::string> set;
  size_t row_num = ctx.row_num();
  std::vector<size_t> offsets;
  for (size_t r_i = 0; r_i < row_num; ++r_i) {
    std::vector<char> bytes;
    Encoder encoder(bytes);
    for (size_t c_i = 0; c_i < cols.size(); ++c_i) {
      auto val = ctx.get(cols[c_i])->get_elem(r_i);
      val.encode_sig(val.type(), encoder);
      encoder.put_byte('#');
    }
    for (auto& var : vars) {
      auto val = var(r_i);
      val.encode_sig(val.type(), encoder);
      encoder.put_byte('#');
    }
    std::string cur(bytes.begin(), bytes.end());
    if (set.find(cur) == set.end()) {
      offsets.push_back(r_i);
      set.insert(cur);
    }
  }
  ctx.reshuffle(offsets);
}

}  // namespace runtime

}  // namespace gs

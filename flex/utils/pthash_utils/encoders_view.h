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

#ifndef GRAPHSCOPE_PTHASH_UTILS_ENCODERS_VIEW_VIEW_H_
#define GRAPHSCOPE_PTHASH_UTILS_ENCODERS_VIEW_VIEW_H_

#ifdef USE_PTHASH
#include "flex/utils/pthash_utils/ef_sequence_view.h"

namespace gs {

// This code is an adaptation from
// https://github.com/jermp/pthash/blob/master/include/encoders/encoders.hpp
struct dictionary_view {
  size_t size() const { return m_ranks.size(); }
  uint64_t access(uint64_t i) const {
    uint64_t rank = m_ranks.access(i);
    return m_dict.access(rank);
  }

  template <typename Visitor>
  void visit(Visitor& visitor) {
    visitor.visit(m_ranks);
    visitor.visit(m_dict);
  }

  compact_vector_view m_ranks;
  compact_vector_view m_dict;
};

struct dual_dictionary_view {
  uint64_t access(uint64_t i) const {
    if (i < m_front.size()) {
      return m_front.access(i);
    }
    return m_back.access(i - m_front.size());
  }

  template <typename Visitor>
  void visit(Visitor& visitor) {
    visitor.visit(m_front);
    visitor.visit(m_back);
  }

  dictionary_view m_front;
  dictionary_view m_back;
};

}  // namespace gs

#endif  // USE_PTHASH

#endif  // GRAPHSCOPE_PTHASH_UTILS_ENCODERS_VIEW_VIEW_H_
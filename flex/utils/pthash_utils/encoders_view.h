#ifndef GRAPHSCOPE_PTHASH_UTILS_ENCODERS_VIEW_VIEW_H_
#define GRAPHSCOPE_PTHASH_UTILS_ENCODERS_VIEW_VIEW_H_

#include "flex/utils/pthash_utils/ef_sequence_view.h"

namespace gs {

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

#endif  // GRAPHSCOPE_PTHASH_UTILS_ENCODERS_VIEW_VIEW_H_
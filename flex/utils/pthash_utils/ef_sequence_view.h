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

#ifndef GRAPHSCOPE_PTHASH_UTILS_EF_SEQUENCE_VIEW_VIEW_H_
#define GRAPHSCOPE_PTHASH_UTILS_EF_SEQUENCE_VIEW_VIEW_H_

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include <type_traits>

#ifdef USE_PTHASH
#include "encoders/util.hpp"

namespace gs {

template <typename T>
struct ref_vector {
  static_assert(std::is_pod<T>::value, "T must be POD type");
  ref_vector() : buffer_(nullptr), size_(0) {}
  ~ref_vector() {}

  void init(const T* buffer, size_t size) {
    buffer_ = buffer;
    size_ = size;
  }

  const T* data() const { return buffer_; }
  const T& operator[](size_t idx) const { return buffer_[idx]; }

  const T* buffer_;
  size_t size_;
};

// This code is an adaptation from
// https://github.com/jermp/pthash/blob/master/include/encoders/bit_vector.hpp
struct bit_vector_view {
  const uint64_t* data() const { return m_bits.data(); }

  template <typename Visitor>
  void visit(Visitor& visitor) {
    visitor.visit(m_size);
    visitor.visit_vec(m_bits);
  }

  size_t m_size;
  ref_vector<uint64_t> m_bits;
};

// This code is an adaptation from
// https://github.com/jermp/pthash/blob/master/include/encoders/darray.hpp
struct darray1_view {
  inline uint64_t select(const bit_vector_view& bv, uint64_t idx) const {
    assert(idx < m_positions);
    uint64_t block = idx / block_size;
    int64_t block_pos = m_block_inventory[block];
    if (block_pos < 0) {  // sparse super-block
      uint64_t overflow_pos = uint64_t(-block_pos - 1);
      return m_overflow_positions[overflow_pos + (idx & (block_size - 1))];
    }

    size_t subblock = idx / subblock_size;
    size_t start_pos = uint64_t(block_pos) + m_subblock_inventory[subblock];
    size_t reminder = idx & (subblock_size - 1);
    if (!reminder) {
      return start_pos;
    }

    const uint64_t* data = bv.data();
    size_t word_idx = start_pos >> 6;
    size_t word_shift = start_pos & 63;
    uint64_t word = data[word_idx] & (uint64_t(-1) << word_shift);

    while (true) {
      size_t popcnt = pthash::util::popcount(word);
      if (reminder < popcnt) {
        break;
      }
      reminder -= popcnt;
      word = data[++word_idx];
    }

    return (word_idx << 6) + pthash::util::select_in_word(word, reminder);
  }

  template <typename Visitor>
  void visit(Visitor& visitor) {
    visitor.visit(m_positions);
    visitor.visit_vec(m_block_inventory);
    visitor.visit_vec(m_subblock_inventory);
    visitor.visit_vec(m_overflow_positions);
  }

  static const size_t block_size = 1024;  // 2048
  static const size_t subblock_size = 32;
  static const size_t max_in_block_distance = 1 << 16;

  size_t m_positions;
  ref_vector<int64_t> m_block_inventory;
  ref_vector<uint16_t> m_subblock_inventory;
  ref_vector<uint64_t> m_overflow_positions;
};

// This code is an adaptation from
// https://github.com/jermp/pthash/blob/master/include/encoders/compact_vector.hpp
struct compact_vector_view {
  inline uint64_t size() const { return m_size; }
  inline uint64_t width() const { return m_width; }
  inline uint64_t access(uint64_t pos) const {
    assert(pos < size());
    uint64_t i = pos * m_width;
    const char* ptr = reinterpret_cast<const char*>(m_bits.data());
    return (*(reinterpret_cast<uint64_t const*>(ptr + (i >> 3))) >> (i & 7)) &
           m_mask;
  }

  template <typename Visitor>
  void visit(Visitor& visitor) {
    visitor.visit(m_size);
    visitor.visit(m_width);
    visitor.visit(m_mask);
    visitor.visit_vec(m_bits);
  }

  uint64_t m_size;
  uint64_t m_width;
  uint64_t m_mask;
  ref_vector<uint64_t> m_bits;
};

// This code is an adaptation from
// https://github.com/jermp/pthash/blob/master/include/encoders/ef_sequence.hpp
struct ef_sequence_view {
  uint64_t access(uint64_t i) const {
    assert(i < m_low_bits.size());
    return ((m_high_bits_d1.select(m_high_bits, i) - i) << m_low_bits.width()) |
           m_low_bits.access(i);
  }

  template <typename Visitor>
  void visit(Visitor& visitor) {
    visitor.visit(m_high_bits);
    visitor.visit(m_high_bits_d1);
    visitor.visit(m_low_bits);
  }

  bit_vector_view m_high_bits;
  darray1_view m_high_bits_d1;
  compact_vector_view m_low_bits;
};

}  // namespace gs

#endif  // USE_PTHASH

#endif  // GRAPHSCOPE_PTHASH_UTILS_EF_SEQUENCE_VIEW_VIEW_H_
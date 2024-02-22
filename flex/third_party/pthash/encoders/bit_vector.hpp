/** Copyright 2020-2024 Giulio Ermanno Pibiri and Roberto Trani
 *
 * The following sets forth attribution notices for third party software.
 *
 * PTHash:
 * The software includes components licensed by Giulio Ermanno Pibiri and
 * Roberto Trani, available at https://github.com/jermp/pthash
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstddef>
#include <vector>

#include "encoders/util.hpp"

namespace pthash {

struct bit_vector_builder {
  bit_vector_builder(uint64_t size = 0, bool init = 0) : m_size(size) {
    m_bits.resize(essentials::words_for(size), uint64_t(-init));
    if (size) {
      m_cur_word = &m_bits.back();
      // clear padding bits
      if (init && (size & 63)) {
        *m_cur_word >>= 64 - (size & 63);
      }
    }
  }

  void reserve(uint64_t num_bits) {
    m_bits.reserve(essentials::words_for(num_bits));
  }

  inline void push_back(bool b) {
    uint64_t pos_in_word = m_size % 64;
    if (pos_in_word == 0) {
      m_bits.push_back(0);
      m_cur_word = &m_bits.back();
    }
    *m_cur_word |= (uint64_t) b << pos_in_word;
    ++m_size;
  }

  inline void zero_extend(uint64_t n) {
    m_size += n;
    uint64_t needed = essentials::words_for(m_size) - m_bits.size();
    if (needed) {
      m_bits.insert(m_bits.end(), needed, 0);
      m_cur_word = &m_bits.back();
    }
  }

  inline void set(uint64_t pos, bool b = true) {
    assert(pos < size());
    uint64_t word = pos >> 6;
    uint64_t pos_in_word = pos & 63;
    m_bits[word] &= ~(uint64_t(1) << pos_in_word);
    m_bits[word] |= uint64_t(b) << pos_in_word;
  }

  inline uint64_t get(uint64_t pos) const {
    assert(pos < size());
    uint64_t word = pos >> 6;
    uint64_t pos_in_word = pos & 63;
    return m_bits[word] >> pos_in_word & uint64_t(1);
  }

  inline void set_bits(uint64_t pos, uint64_t bits, size_t len) {
    assert(pos + len <= size());
    // check there are no spurious bits
    assert(len == 64 || (bits >> len) == 0);
    if (!len)
      return;
    uint64_t mask = (len == 64) ? uint64_t(-1) : ((uint64_t(1) << len) - 1);
    uint64_t word = pos >> 6;
    uint64_t pos_in_word = pos & 63;

    m_bits[word] &= ~(mask << pos_in_word);
    m_bits[word] |= bits << pos_in_word;

    uint64_t stored = 64 - pos_in_word;
    if (stored < len) {
      m_bits[word + 1] &= ~(mask >> stored);
      m_bits[word + 1] |= bits >> stored;
    }
  }

  inline void append_bits(uint64_t bits, size_t len) {
    // check there are no spurious bits
    assert(len == 64 || (bits >> len) == 0);
    if (!len)
      return;
    uint64_t pos_in_word = m_size & 63;
    m_size += len;
    if (pos_in_word == 0) {
      m_bits.push_back(bits);
    } else {
      *m_cur_word |= bits << pos_in_word;
      if (len > 64 - pos_in_word) {
        m_bits.push_back(bits >> (64 - pos_in_word));
      }
    }
    m_cur_word = &m_bits.back();
  }

  inline uint64_t get_word64(uint64_t pos) const {
    assert(pos < size());
    uint64_t block = pos >> 6;
    uint64_t shift = pos & 63;
    uint64_t word = m_bits[block] >> shift;
    if (shift && block + 1 < m_bits.size()) {
      word |= m_bits[block + 1] << (64 - shift);
    }
    return word;
  }

  void append(bit_vector_builder const& rhs) {
    if (!rhs.size())
      return;

    uint64_t pos = m_bits.size();
    uint64_t shift = size() % 64;
    m_size = size() + rhs.size();
    m_bits.resize(essentials::words_for(m_size));

    if (shift == 0) {  // word-aligned, easy case
      std::copy(rhs.m_bits.begin(), rhs.m_bits.end(),
                m_bits.begin() + ptrdiff_t(pos));
    } else {
      uint64_t* cur_word = &m_bits.front() + pos - 1;
      for (size_t i = 0; i < rhs.m_bits.size() - 1; ++i) {
        uint64_t w = rhs.m_bits[i];
        *cur_word |= w << shift;
        *++cur_word = w >> (64 - shift);
      }
      *cur_word |= rhs.m_bits.back() << shift;
      if (cur_word < &m_bits.back()) {
        *++cur_word = rhs.m_bits.back() >> (64 - shift);
      }
    }
    m_cur_word = &m_bits.back();
  }

  void resize(uint64_t size) {
    m_size = size;
    m_bits.resize(essentials::words_for(m_size));
  }

  void swap(bit_vector_builder& other) {
    m_bits.swap(other.m_bits);
    std::swap(m_size, other.m_size);
    std::swap(m_cur_word, other.m_cur_word);
  }

  std::vector<uint64_t>& data() { return m_bits; }

  uint64_t size() const { return m_size; }

 private:
  std::vector<uint64_t> m_bits;
  uint64_t m_size;
  uint64_t* m_cur_word;
};

struct bit_vector {
  bit_vector() : m_size(0) {}

  void build(bit_vector_builder* in) {
    m_size = in->size();
    m_bits.swap(in->data());
  }

  bit_vector(bit_vector_builder* in) { build(in); }

  void swap(bit_vector& other) {
    std::swap(other.m_size, m_size);
    other.m_bits.swap(m_bits);
  }

  inline size_t size() const { return m_size; }

  uint64_t bytes() const {
    return sizeof(m_size) + essentials::vec_bytes(m_bits);
  }

  // get i-th bit
  inline uint64_t operator[](uint64_t i) const {
    assert(i < size());
    uint64_t block = i >> 6;
    uint64_t shift = i & 63;
    return m_bits[block] >> shift & uint64_t(1);
  }

  inline uint64_t get_bits(uint64_t pos, uint64_t len) const {
    assert(pos + len <= size());
    if (!len)
      return 0;
    uint64_t block = pos >> 6;
    uint64_t shift = pos & 63;
    uint64_t mask = -(len == 64) | ((1ULL << len) - 1);
    if (shift + len <= 64) {
      return m_bits[block] >> shift & mask;
    } else {
      return (m_bits[block] >> shift) |
             (m_bits[block + 1] << (64 - shift) & mask);
    }
  }

  // fast and unsafe version: it retrieves at least 56 bits
  inline uint64_t get_word56(uint64_t pos) const {
    const char* base_ptr = reinterpret_cast<const char*>(m_bits.data());
    return *(reinterpret_cast<uint64_t const*>(base_ptr + (pos >> 3))) >>
           (pos & 7);
  }

  // pad with zeros if extension further size is needed
  inline uint64_t get_word64(uint64_t pos) const {
    assert(pos < size());
    uint64_t block = pos >> 6;
    uint64_t shift = pos & 63;
    uint64_t word = m_bits[block] >> shift;
    if (shift && block + 1 < m_bits.size()) {
      word |= m_bits[block + 1] << (64 - shift);
    }
    return word;
  }

  inline uint64_t predecessor1(uint64_t pos) const {
    assert(pos < m_size);
    uint64_t block = pos / 64;
    uint64_t shift = 64 - pos % 64 - 1;
    uint64_t word = m_bits[block];
    word = (word << shift) >> shift;

    unsigned long ret;
    while (!util::msb(word, ret)) {
      assert(block);
      word = m_bits[--block];
    };
    return block * 64 + ret;
  }

  std::vector<uint64_t> const& data() const { return m_bits; }

  struct unary_iterator {
    unary_iterator() : m_data(0), m_position(0), m_buf(0) {}

    unary_iterator(bit_vector const& bv, uint64_t pos = 0) {
      m_data = bv.data().data();
      m_position = pos;
      m_buf = m_data[pos >> 6];
      // clear low bits
      m_buf &= uint64_t(-1) << (pos & 63);
    }

    uint64_t position() const { return m_position; }

    uint64_t next() {
      unsigned long pos_in_word;
      uint64_t buf = m_buf;
      while (!util::lsb(buf, pos_in_word)) {
        m_position += 64;
        buf = m_data[m_position >> 6];
      }

      m_buf = buf & (buf - 1);  // clear LSB
      m_position = (m_position & ~uint64_t(63)) + pos_in_word;
      return m_position;
    }

    // skip to the k-th one after the current position
    void skip(uint64_t k) {
      uint64_t skipped = 0;
      uint64_t buf = m_buf;
      uint64_t w = 0;
      while (skipped + (w = util::popcount(buf)) <= k) {
        skipped += w;
        m_position += 64;
        buf = m_data[m_position / 64];
      }
      assert(buf);
      uint64_t pos_in_word = util::select_in_word(buf, k - skipped);
      m_buf = buf & (uint64_t(-1) << pos_in_word);
      m_position = (m_position & ~uint64_t(63)) + pos_in_word;
    }

    // skip to the k-th zero after the current position
    void skip0(uint64_t k) {
      uint64_t skipped = 0;
      uint64_t pos_in_word = m_position % 64;
      uint64_t buf = ~m_buf & (uint64_t(-1) << pos_in_word);
      uint64_t w = 0;
      while (skipped + (w = util::popcount(buf)) <= k) {
        skipped += w;
        m_position += 64;
        buf = ~m_data[m_position / 64];
      }
      assert(buf);
      pos_in_word = util::select_in_word(buf, k - skipped);
      m_buf = ~buf & (uint64_t(-1) << pos_in_word);
      m_position = (m_position & ~uint64_t(63)) + pos_in_word;
    }

   private:
    uint64_t const* m_data;
    uint64_t m_position;
    uint64_t m_buf;
  };

  template <typename Visitor>
  void visit(Visitor& visitor) {
    visitor.visit(m_size);
    visitor.visit(m_bits);
  }

 protected:
  size_t m_size;
  std::vector<uint64_t> m_bits;
};

}  // namespace pthash

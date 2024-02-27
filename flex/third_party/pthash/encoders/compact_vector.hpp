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

#include <cmath>
#include <vector>

namespace pthash {

struct compact_vector {
  template <typename Data>
  struct enumerator {
    enumerator() {}

    enumerator(Data const* data, uint64_t i = 0)
        : m_i(i),
          m_cur_val(0),
          m_cur_block((i * data->m_width) >> 6),
          m_cur_shift((i * data->m_width) & 63),
          m_data(data) {}

    uint64_t operator*() {
      read();
      return m_cur_val;
    }

    enumerator& operator++() {
      ++m_i;
      return *this;
    }

    inline uint64_t value() {
      read();
      return m_cur_val;
    }

    inline void next() { ++m_i; }

    bool operator==(enumerator const& other) const { return m_i == other.m_i; }

    bool operator!=(enumerator const& other) const { return !(*this == other); }

   private:
    uint64_t m_i;
    uint64_t m_cur_val;
    uint64_t m_cur_block;
    int64_t m_cur_shift;
    Data const* m_data;

    void read() {
      if (m_cur_shift + m_data->m_width <= 64) {
        m_cur_val = m_data->m_bits[m_cur_block] >> m_cur_shift & m_data->m_mask;
      } else {
        uint64_t res_shift = 64 - m_cur_shift;
        m_cur_val =
            (m_data->m_bits[m_cur_block] >> m_cur_shift) |
            (m_data->m_bits[m_cur_block + 1] << res_shift & m_data->m_mask);
        ++m_cur_block;
        m_cur_shift = -res_shift;
      }

      m_cur_shift += m_data->m_width;

      if (m_cur_shift == 64) {
        m_cur_shift = 0;
        ++m_cur_block;
      }
    }
  };

  struct builder {
    builder()
        : m_size(0),
          m_width(0),
          m_mask(0),
          m_back(0),
          m_cur_block(0),
          m_cur_shift(0) {}

    builder(uint64_t n, uint64_t w) { resize(n, w); }

    void resize(size_t n, uint64_t w) {
      m_size = n;
      m_width = w;
      m_mask = -(w == 64) | ((uint64_t(1) << w) - 1);
      m_back = 0;
      m_cur_block = 0;
      m_cur_shift = 0;
      m_bits.resize(
          /* use 1 word more for safe access() */
          essentials::words_for(m_size * m_width) + 1, 0);
    }

    template <typename Iterator>
    builder(Iterator begin, uint64_t n, uint64_t w) : builder(n, w) {
      fill(begin, n);
    }

    template <typename Iterator>
    void fill(Iterator begin, uint64_t n) {
      if (!m_width)
        throw std::runtime_error("width must be greater than 0");
      for (uint64_t i = 0; i != n; ++i, ++begin)
        push_back(*begin);
    }

    void set(uint64_t i, uint64_t v) {
      assert(m_width);
      assert(i < m_size);
      if (i == m_size - 1)
        m_back = v;

      uint64_t pos = i * m_width;
      uint64_t block = pos >> 6;
      uint64_t shift = pos & 63;

      m_bits[block] &= ~(m_mask << shift);
      m_bits[block] |= v << shift;

      uint64_t res_shift = 64 - shift;
      if (res_shift < m_width) {
        m_bits[block + 1] &= ~(m_mask >> res_shift);
        m_bits[block + 1] |= v >> res_shift;
      }
    }

    void push_back(uint64_t v) {
      assert(m_width);
      m_back = v;
      m_bits[m_cur_block] &= ~(m_mask << m_cur_shift);
      m_bits[m_cur_block] |= v << m_cur_shift;

      uint64_t res_shift = 64 - m_cur_shift;
      if (res_shift < m_width) {
        ++m_cur_block;
        m_bits[m_cur_block] &= ~(m_mask >> res_shift);
        m_bits[m_cur_block] |= v >> res_shift;
        m_cur_shift = -res_shift;
      }

      m_cur_shift += m_width;

      if (m_cur_shift == 64) {
        m_cur_shift = 0;
        ++m_cur_block;
      }
    }

    friend struct enumerator<builder>;

    typedef enumerator<builder> iterator;

    iterator begin() const { return iterator(this); }

    iterator end() const { return iterator(this, size()); }

    void build(compact_vector& cv) {
      cv.m_size = m_size;
      cv.m_width = m_width;
      cv.m_mask = m_mask;
      cv.m_bits.swap(m_bits);
      builder().swap(*this);
    }

    void swap(compact_vector::builder& other) {
      std::swap(m_size, other.m_size);
      std::swap(m_width, other.m_width);
      std::swap(m_mask, other.m_mask);
      std::swap(m_cur_block, other.m_cur_block);
      std::swap(m_cur_shift, other.m_cur_shift);
      m_bits.swap(other.m_bits);
    }

    uint64_t back() const { return m_back; }

    uint64_t size() const { return m_size; }

    uint64_t width() const { return m_width; }

    std::vector<uint64_t>& bits() { return m_bits; }

   private:
    uint64_t m_size;
    uint64_t m_width;
    uint64_t m_mask;
    uint64_t m_back;
    uint64_t m_cur_block;
    int64_t m_cur_shift;
    std::vector<uint64_t> m_bits;
  };

  compact_vector() : m_size(0), m_width(0), m_mask(0) {}

  template <typename Iterator>
  void build(Iterator begin, uint64_t n) {
    assert(n > 0);
    uint64_t max = *std::max_element(begin, begin + n);
    uint64_t width = max == 0 ? 1 : std::ceil(std::log2(max + 1));
    build(begin, n, width);
  }

  template <typename Iterator>
  void build(Iterator begin, uint64_t n, uint64_t w) {
    compact_vector::builder builder(begin, n, w);
    builder.build(*this);
  }

  inline uint64_t operator[](uint64_t i) const {
    assert(i < size());
    uint64_t pos = i * m_width;
    uint64_t block = pos >> 6;
    uint64_t shift = pos & 63;
    return shift + m_width <= 64
               ? m_bits[block] >> shift & m_mask
               : (m_bits[block] >> shift) |
                     (m_bits[block + 1] << (64 - shift) & m_mask);
  }

  // it retrieves at least 57 bits
  inline uint64_t access(uint64_t pos) const {
    assert(pos < size());
    uint64_t i = pos * m_width;
    const char* ptr = reinterpret_cast<const char*>(m_bits.data());
    return (*(reinterpret_cast<uint64_t const*>(ptr + (i >> 3))) >> (i & 7)) &
           m_mask;
  }

  uint64_t back() const { return operator[](size() - 1); }

  inline uint64_t size() const { return m_size; }

  inline uint64_t width() const { return m_width; }

  typedef enumerator<compact_vector> iterator;

  iterator begin() const { return iterator(this); }

  iterator end() const { return iterator(this, size()); }

  iterator at(uint64_t pos) const { return iterator(this, pos); }

  std::vector<uint64_t> const& bits() const { return m_bits; }

  size_t bytes() const {
    return sizeof(m_size) + sizeof(m_width) + sizeof(m_mask) +
           essentials::vec_bytes(m_bits);
  }

  void swap(compact_vector& other) {
    std::swap(m_size, other.m_size);
    std::swap(m_width, other.m_width);
    std::swap(m_mask, other.m_mask);
    m_bits.swap(other.m_bits);
  }

  template <typename Visitor>
  void visit(Visitor& visitor) {
    visitor.visit(m_size);
    visitor.visit(m_width);
    visitor.visit(m_mask);
    visitor.visit(m_bits);
  }

 private:
  uint64_t m_size;
  uint64_t m_width;
  uint64_t m_mask;
  std::vector<uint64_t> m_bits;
};

}  // namespace pthash
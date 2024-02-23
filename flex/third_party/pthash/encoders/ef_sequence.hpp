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

#include "encoders/bit_vector.hpp"
#include "encoders/compact_vector.hpp"
#include "encoders/darray.hpp"

namespace pthash {

template <bool encode_prefix_sum = false>
struct ef_sequence {
  ef_sequence() {}

  template <typename Iterator>
  void encode(Iterator begin, uint64_t n) {
    if (n == 0)
      return;
    uint64_t u;
    if constexpr (encode_prefix_sum) {
      u = std::accumulate(begin, begin + n, static_cast<uint64_t>(0));
      n = n + 1;  // because I will add a zero at the beginning
    } else {
      u = *(begin + n - 1);
    };

    uint64_t l = uint64_t((n && u / n) ? util::msb(u / n) : 0);
    bit_vector_builder bvb_high_bits(n + (u >> l) + 1);
    compact_vector::builder cv_builder_low_bits(n, l);

    uint64_t low_mask = (uint64_t(1) << l) - 1;
    uint64_t last = 0;
    // I add a zero at the beginning
    if constexpr (encode_prefix_sum) {
      if (l)
        cv_builder_low_bits.push_back(0);
      bvb_high_bits.set(0, 1);
      n = n - 1;  // restore n
    }
    for (size_t i = 0; i < n; ++i, ++begin) {
      auto v = *begin;
      if constexpr (encode_prefix_sum) {
        v = v + last;               // prefix sum
      } else if (i and v < last) {  // check the order
        std::cerr << "error at " << i << "/" << n << ":\n";
        std::cerr << "last " << last << "\n";
        std::cerr << "current " << v << "\n";
        throw std::runtime_error("ef_sequence is not sorted");
      }
      if (l)
        cv_builder_low_bits.push_back(v & low_mask);
      bvb_high_bits.set((v >> l) + i + encode_prefix_sum, 1);
      last = v;
    }

    bit_vector(&bvb_high_bits).swap(m_high_bits);
    cv_builder_low_bits.build(m_low_bits);
    darray1(m_high_bits).swap(m_high_bits_d1);
  }

  inline uint64_t access(uint64_t i) const {
    assert(i < size());
    return ((m_high_bits_d1.select(m_high_bits, i) - i) << m_low_bits.width()) |
           m_low_bits.access(i);
  }

  inline uint64_t diff(uint64_t i) const {
    assert(i < size() && encode_prefix_sum);
    uint64_t low1 = m_low_bits.access(i);
    uint64_t low2 = m_low_bits.access(i + 1);
    uint64_t l = m_low_bits.width();
    uint64_t pos = m_high_bits_d1.select(m_high_bits, i);
    uint64_t h1 = pos - i;
    uint64_t h2 =
        bit_vector::unary_iterator(m_high_bits, pos + 1).next() - i - 1;
    uint64_t val1 = (h1 << l) | low1;
    uint64_t val2 = (h2 << l) | low2;
    return val2 - val1;
  }

  inline uint64_t size() const { return m_low_bits.size(); }

  uint64_t num_bits() const {
    return 8 *
           (m_high_bits.bytes() + m_high_bits_d1.bytes() + m_low_bits.bytes());
  }

  template <typename Visitor>
  void visit(Visitor& visitor) {
    visitor.visit(m_high_bits);
    visitor.visit(m_high_bits_d1);
    visitor.visit(m_low_bits);
  }

 private:
  bit_vector m_high_bits;
  darray1 m_high_bits_d1;
  compact_vector m_low_bits;
};

}  // namespace pthash

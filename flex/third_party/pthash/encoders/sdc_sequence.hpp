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
#include "encoders/ef_sequence.hpp"

namespace pthash {

struct sdc_sequence {
  sdc_sequence() : m_size(0) {}

  template <typename Iterator>
  void build(Iterator begin, uint64_t n) {
    m_size = n;
    auto start = begin;
    uint64_t bits = 0;
    for (uint64_t i = 0; i < n; ++i, ++start)
      bits += std::floor(std::log2(*start + 1));
    bit_vector_builder bvb_codewords(bits);
    std::vector<uint64_t> lengths;
    lengths.reserve(n + 1);
    uint64_t pos = 0;
    for (uint64_t i = 0; i < n; ++i, ++begin) {
      auto v = *begin;
      uint64_t len = std::floor(std::log2(v + 1));
      assert(len <= 64);
      uint64_t cw = v + 1 - (uint64_t(1) << len);
      if (len > 0)
        bvb_codewords.set_bits(pos, cw, len);
      lengths.push_back(pos);
      pos += len;
    }
    assert(pos == bits);
    lengths.push_back(pos);
    bit_vector(&bvb_codewords).swap(m_codewords);
    m_index.encode(lengths.data(), lengths.size());
  }

  inline uint64_t access(uint64_t i) const {
    assert(i < size());
    uint64_t pos = m_index.access(i);
    uint64_t len = m_index.access(i + 1) - pos;
    assert(len < 64);
    uint64_t cw = m_codewords.get_bits(pos, len);
    uint64_t value = cw + (uint64_t(1) << len) - 1;
    return value;
  }

  uint64_t size() const { return m_size; }

  uint64_t bytes() const {
    return sizeof(m_size) + m_codewords.bytes() + m_index.num_bits() / 8;
  }

  template <typename Visitor>
  void visit(Visitor& visitor) {
    visitor.visit(m_size);
    visitor.visit(m_codewords);
    visitor.visit(m_index);
  }

 private:
  uint64_t m_size;
  bit_vector m_codewords;
  ef_sequence<false> m_index;
};

}  // namespace pthash

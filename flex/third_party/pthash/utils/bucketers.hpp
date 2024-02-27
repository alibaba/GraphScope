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

#include "util.hpp"

namespace pthash {

struct skew_bucketer {
  skew_bucketer() {}

  void init(uint64_t num_buckets) {
    m_num_dense_buckets = 0.3 * num_buckets;
    m_num_sparse_buckets = num_buckets - m_num_dense_buckets;
    m_M_num_dense_buckets = fastmod::computeM_u64(m_num_dense_buckets);
    m_M_num_sparse_buckets = fastmod::computeM_u64(m_num_sparse_buckets);
  }

  inline uint64_t bucket(uint64_t hash) const {
    static const uint64_t T = UINT64_MAX / 5 * 3;
    return (hash < T) ? fastmod::fastmod_u64(hash, m_M_num_dense_buckets,
                                             m_num_dense_buckets)
                      : m_num_dense_buckets +
                            fastmod::fastmod_u64(hash, m_M_num_sparse_buckets,
                                                 m_num_sparse_buckets);
  }

  uint64_t num_buckets() const {
    return m_num_dense_buckets + m_num_sparse_buckets;
  }

  size_t num_bits() const {
    return 8 * (sizeof(m_num_dense_buckets) + sizeof(m_num_sparse_buckets) +
                sizeof(m_M_num_dense_buckets) + sizeof(m_M_num_sparse_buckets));
  }

  void swap(skew_bucketer& other) {
    std::swap(m_num_dense_buckets, other.m_num_dense_buckets);
    std::swap(m_num_sparse_buckets, other.m_num_sparse_buckets);
    std::swap(m_M_num_dense_buckets, other.m_M_num_dense_buckets);
    std::swap(m_M_num_sparse_buckets, other.m_M_num_sparse_buckets);
  }

  template <typename Visitor>
  void visit(Visitor& visitor) {
    visitor.visit(m_num_dense_buckets);
    visitor.visit(m_num_sparse_buckets);
    visitor.visit(m_M_num_dense_buckets);
    visitor.visit(m_M_num_sparse_buckets);
  }

 private:
  uint64_t m_num_dense_buckets, m_num_sparse_buckets;
  __uint128_t m_M_num_dense_buckets, m_M_num_sparse_buckets;
};

struct uniform_bucketer {
  uniform_bucketer() {}

  void init(uint64_t num_buckets) {
    m_num_buckets = num_buckets;
    m_M_num_buckets = fastmod::computeM_u64(m_num_buckets);
  }

  inline uint64_t bucket(uint64_t hash) const {
    return fastmod::fastmod_u64(hash, m_M_num_buckets, m_num_buckets);
  }

  uint64_t num_buckets() const { return m_num_buckets; }

  size_t num_bits() const {
    return 8 * (sizeof(m_num_buckets) + sizeof(m_M_num_buckets));
  }

  template <typename Visitor>
  void visit(Visitor& visitor) {
    visitor.visit(m_num_buckets);
    visitor.visit(m_M_num_buckets);
  }

 private:
  uint64_t m_num_buckets;
  __uint128_t m_M_num_buckets;
};

}  // namespace pthash
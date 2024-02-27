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

#include <thread>

#include "builders/external_memory_builder_partitioned_phf.hpp"
#include "builders/internal_memory_builder_partitioned_phf.hpp"
#include "single_phf.hpp"

namespace pthash {

template <typename Hasher, typename Encoder, bool Minimal>
struct partitioned_phf {
 private:
  struct partition {
    template <typename Visitor>
    void visit(Visitor& visitor) {
      visitor.visit(offset);
      visitor.visit(f);
    }

    uint64_t offset;
    single_phf<Hasher, Encoder, Minimal> f;
  };

 public:
  typedef Encoder encoder_type;
  static constexpr bool minimal = Minimal;

  template <typename Iterator>
  build_timings build_in_internal_memory(Iterator keys, uint64_t num_keys,
                                         build_configuration const& config) {
    internal_memory_builder_partitioned_phf<Hasher> builder;
    auto timings = builder.build_from_keys(keys, num_keys, config);
    timings.encoding_seconds = build(builder, config);
    return timings;
  }

  template <typename Iterator>
  build_timings build_in_external_memory(Iterator keys, uint64_t num_keys,
                                         build_configuration const& config) {
    external_memory_builder_partitioned_phf<Hasher> builder;
    auto timings = builder.build_from_keys(keys, num_keys, config);
    timings.encoding_seconds = build(builder, config);
    return timings;
  }

  template <typename Builder>
  double build(Builder& builder, build_configuration const& config) {
    auto start = clock_type::now();
    uint64_t num_partitions = builder.num_partitions();

    m_seed = builder.seed();
    m_num_keys = builder.num_keys();
    m_table_size = builder.table_size();
    m_bucketer = builder.bucketer();
    m_partitions.resize(num_partitions);

    auto const& offsets = builder.offsets();
    auto const& builders = builder.builders();
    uint64_t num_threads = config.num_threads;

    if (num_threads > 1) {
      std::vector<std::thread> threads(num_threads);
      auto exe = [&](uint64_t begin, uint64_t end) {
        for (; begin != end; ++begin) {
          m_partitions[begin].offset = offsets[begin];
          m_partitions[begin].f.build(builders[begin], config);
        }
      };

      uint64_t num_partitions_per_thread =
          (num_partitions + num_threads - 1) / num_threads;
      for (uint64_t t = 0, begin = 0; t != num_threads; ++t) {
        uint64_t end = begin + num_partitions_per_thread;
        if (end > num_partitions)
          end = num_partitions;
        threads[t] = std::thread(exe, begin, end);
        begin = end;
      }

      for (auto& t : threads) {
        if (t.joinable())
          t.join();
      }
    } else {
      for (uint64_t i = 0; i != num_partitions; ++i) {
        m_partitions[i].offset = offsets[i];
        m_partitions[i].f.build(builders[i], config);
      }
    }

    auto stop = clock_type::now();
    return seconds(stop - start);
  }

  template <typename T>
  uint64_t operator()(T const& key) const {
    auto hash = Hasher::hash(key, m_seed);
    auto b = m_bucketer.bucket(hash.mix());
    auto const& p = m_partitions[b];
    return p.offset + p.f.position(hash);
  }

  size_t num_bits_for_pilots() const {
    size_t bits =
        8 * (sizeof(m_seed) + sizeof(m_num_keys)) + m_bucketer.num_bits();
    for (auto const& p : m_partitions)
      bits += 8 * sizeof(p.offset) + p.f.num_bits_for_pilots();
    return bits;
  }

  size_t num_bits_for_mapper() const {
    size_t bits = 0;
    for (auto const& p : m_partitions)
      bits += p.f.num_bits_for_mapper();
    return bits;
  }

  size_t num_bits() const {
    return num_bits_for_pilots() + num_bits_for_mapper();
  }

  inline uint64_t num_keys() const { return m_num_keys; }

  inline uint64_t table_size() const { return m_table_size; }

  template <typename Visitor>
  void visit(Visitor& visitor) {
    visitor.visit(m_seed);
    visitor.visit(m_num_keys);
    visitor.visit(m_table_size);
    visitor.visit(m_bucketer);
    visitor.visit(m_partitions);
  }

 private:
  uint64_t m_seed;
  uint64_t m_num_keys;
  uint64_t m_table_size;
  uniform_bucketer m_bucketer;
  std::vector<partition> m_partitions;
};

}  // namespace pthash

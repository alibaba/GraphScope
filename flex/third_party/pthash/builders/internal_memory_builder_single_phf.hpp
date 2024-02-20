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

#include "builders/search.hpp"
#include "builders/util.hpp"
#include "utils/bucketers.hpp"
#include "utils/hasher.hpp"
#include "utils/logger.hpp"

namespace pthash {

template <typename Hasher>
struct internal_memory_builder_single_phf {
  typedef Hasher hasher_type;

  template <typename RandomAccessIterator>
  build_timings build_from_keys(RandomAccessIterator keys, uint64_t num_keys,
                                build_configuration const& config) {
    if (config.seed == constants::invalid_seed) {
      for (auto attempt = 0; attempt < 10; ++attempt) {
        m_seed = random_value();
        try {
          return build_from_hashes(
              hash_generator<RandomAccessIterator>(keys, m_seed), num_keys,
              config);
        } catch (seed_runtime_error const& error) {
          std::cout << "attempt " << attempt + 1 << " failed" << std::endl;
        }
      }
      throw seed_runtime_error();
    }
    m_seed = config.seed;
    return build_from_hashes(hash_generator<RandomAccessIterator>(keys, m_seed),
                             num_keys, config);
  }

  template <typename RandomAccessIterator>
  build_timings build_from_hashes(RandomAccessIterator hashes,
                                  uint64_t num_keys,
                                  build_configuration const& config) {
    assert(num_keys > 1);
    if (config.alpha == 0 or config.alpha > 1.0) {
      throw std::invalid_argument("load factor must be > 0 and <= 1.0");
    }

    clock_type::time_point start;

    start = clock_type::now();

    build_timings time;

    uint64_t table_size = static_cast<double>(num_keys) / config.alpha;
    if ((table_size & (table_size - 1)) == 0)
      table_size += 1;
    uint64_t num_buckets =
        (config.num_buckets == constants::invalid_num_buckets)
            ? (std::ceil((config.c * num_keys) / std::log2(num_keys)))
            : config.num_buckets;

    m_num_keys = num_keys;
    m_table_size = table_size;
    m_num_buckets = num_buckets;
    m_bucketer.init(m_num_buckets);

    if (config.verbose_output) {
      std::cout << "c = " << config.c << std::endl;
      std::cout << "alpha = " << config.alpha << std::endl;
      std::cout << "num_keys = " << num_keys << std::endl;
      std::cout << "table_size = " << table_size << std::endl;
      std::cout << "num_buckets = " << num_buckets << std::endl;
    }

    buckets_t buckets;
    {
      auto start = clock_type::now();
      std::vector<pairs_t> pairs_blocks;
      map(hashes, num_keys, pairs_blocks, config);
      auto elapsed = seconds(clock_type::now() - start);
      if (config.verbose_output) {
        std::cout << " == map+sort took: " << elapsed << " seconds"
                  << std::endl;
      }

      start = clock_type::now();
      merge(pairs_blocks, buckets, config.verbose_output);
      elapsed = seconds(clock_type::now() - start);
      if (config.verbose_output) {
        std::cout << " == merge+check took: " << elapsed << " seconds"
                  << std::endl;
      }
    }
    auto buckets_iterator = buckets.begin();
    time.mapping_ordering_seconds = seconds(clock_type::now() - start);
    if (config.verbose_output) {
      std::cout << " == mapping+ordering took " << time.mapping_ordering_seconds
                << " seconds " << std::endl;
      std::cout << " == max bucket size = " << int((*buckets_iterator).size())
                << std::endl;
    }

    start = clock_type::now();
    {
      m_pilots.resize(num_buckets);
      std::fill(m_pilots.begin(), m_pilots.end(), 0);
      bit_vector_builder taken(m_table_size);
      uint64_t num_non_empty_buckets = buckets.num_buckets();
      pilots_wrapper_t pilots_wrapper(m_pilots);
      search(m_num_keys, m_num_buckets, num_non_empty_buckets, m_seed, config,
             buckets_iterator, taken, pilots_wrapper);
      if (config.minimal_output) {
        m_free_slots.clear();
        m_free_slots.reserve(taken.size() - num_keys);
        fill_free_slots(taken, num_keys, m_free_slots);
      }
    }
    time.searching_seconds = seconds(clock_type::now() - start);
    if (config.verbose_output) {
      std::cout << " == search took " << time.searching_seconds << " seconds"
                << std::endl;
    }

    return time;
  }

  uint64_t seed() const { return m_seed; }

  uint64_t num_keys() const { return m_num_keys; }

  uint64_t table_size() const { return m_table_size; }

  skew_bucketer bucketer() const { return m_bucketer; }

  std::vector<uint64_t> const& pilots() const { return m_pilots; }

  std::vector<uint64_t> const& free_slots() const { return m_free_slots; }

  void swap(internal_memory_builder_single_phf& other) {
    std::swap(m_seed, other.m_seed);
    std::swap(m_num_keys, other.m_num_keys);
    std::swap(m_num_buckets, other.m_num_buckets);
    std::swap(m_table_size, other.m_table_size);
    std::swap(m_bucketer, other.m_bucketer);
    m_pilots.swap(other.m_pilots);
    m_free_slots.swap(other.m_free_slots);
  }

  template <typename Visitor>
  void visit(Visitor& visitor) {
    visitor.visit(m_seed);
    visitor.visit(m_num_keys);
    visitor.visit(m_num_buckets);
    visitor.visit(m_table_size);
    visitor.visit(m_bucketer);
    visitor.visit(m_pilots);
    visitor.visit(m_free_slots);
  }

  static size_t estimate_num_bytes_for_construction(
      uint64_t num_keys, build_configuration const& config) {
    uint64_t table_size = static_cast<double>(num_keys) / config.alpha;
    if ((table_size & (table_size - 1)) == 0)
      table_size += 1;
    uint64_t num_buckets =
        (config.num_buckets == constants::invalid_num_buckets)
            ? (std::ceil((config.c * num_keys) / std::log2(num_keys)))
            : config.num_buckets;

    size_t mapping_bytes =
        num_keys * sizeof(bucket_payload_pair)          // pairs
        + (num_keys + num_buckets) * sizeof(uint64_t);  // buckets

    size_t search_bytes =
        num_buckets * sizeof(uint64_t)    // pilots
        + num_buckets * sizeof(uint64_t)  // buckets
        + (config.minimal_output ? (table_size - num_keys) * sizeof(uint64_t)
                                 : 0)  // free_slots
        + num_keys * sizeof(uint64_t)  // hashes
        + table_size / 8;              // bitmap taken
    return std::max<size_t>(mapping_bytes, search_bytes);
  }

 private:
  uint64_t m_seed;
  uint64_t m_num_keys;
  uint64_t m_num_buckets;
  uint64_t m_table_size;
  skew_bucketer m_bucketer;
  std::vector<uint64_t> m_pilots;
  std::vector<uint64_t> m_free_slots;

  template <typename RandomAccessIterator>
  struct hash_generator {
    hash_generator(RandomAccessIterator keys, uint64_t seed)
        : m_iterator(keys), m_seed(seed) {}

    inline auto operator*() { return hasher_type::hash(*m_iterator, m_seed); }

    inline void operator++() { ++m_iterator; }

    inline hash_generator operator+(uint64_t offset) const {
      return hash_generator(m_iterator + offset, m_seed);
    }

   private:
    RandomAccessIterator m_iterator;
    uint64_t m_seed;
  };

  typedef std::vector<bucket_payload_pair> pairs_t;

  struct buckets_iterator_t {
    buckets_iterator_t(std::vector<std::vector<uint64_t>> const& buffers)
        : m_buffers_it(buffers.end() - 1), m_bucket_size(buffers.size()) {
      m_bucket.init(m_buffers_it->data(), m_bucket_size);
      skip_empty_buckets();
    }

    inline void operator++() {
      uint64_t const* begin = m_bucket.begin() + m_bucket_size;
      uint64_t const* end = m_buffers_it->data() + m_buffers_it->size();
      m_bucket.init(begin, m_bucket_size);
      if ((m_bucket.begin() - 1) == end and m_bucket_size != 0) {
        --m_bucket_size;
        --m_buffers_it;
        skip_empty_buckets();
      }
    }

    inline bucket_t operator*() const { return m_bucket; }

   private:
    std::vector<std::vector<uint64_t>>::const_iterator m_buffers_it;
    bucket_size_type m_bucket_size;
    bucket_t m_bucket;

    void skip_empty_buckets() {
      while (m_bucket_size != 0 and m_buffers_it->empty()) {
        --m_bucket_size;
        --m_buffers_it;
      }
      if (m_bucket_size != 0)
        m_bucket.init(m_buffers_it->data(), m_bucket_size);
    }
  };

  struct buckets_t {
    buckets_t() : m_buffers(MAX_BUCKET_SIZE), m_num_buckets(0) {}

    template <typename HashIterator>
    void add(bucket_id_type bucket_id, bucket_size_type bucket_size,
             HashIterator hashes) {
      assert(bucket_size > 0);
      uint64_t i = bucket_size - 1;
      m_buffers[i].push_back(bucket_id);
      for (uint64_t k = 0; k != bucket_size; ++k, ++hashes)
        m_buffers[i].push_back(*hashes);
      ++m_num_buckets;
    }

    uint64_t num_buckets() const { return m_num_buckets; };

    buckets_iterator_t begin() const { return buckets_iterator_t(m_buffers); }

   private:
    std::vector<std::vector<uint64_t>> m_buffers;
    uint64_t m_num_buckets;
  };

  struct pilots_wrapper_t {
    pilots_wrapper_t(std::vector<uint64_t>& pilots) : m_pilots(pilots) {}

    inline void emplace_back(bucket_id_type bucket_id, uint64_t pilot) {
      m_pilots[bucket_id] = pilot;
    }

   private:
    std::vector<uint64_t>& m_pilots;
  };

  template <typename RandomAccessIterator>
  void map_sequential(RandomAccessIterator hashes, uint64_t num_keys,
                      std::vector<pairs_t>& pairs_blocks,
                      build_configuration const&) const {
    pairs_t pairs(num_keys);
    RandomAccessIterator begin = hashes;
    for (uint64_t i = 0; i != num_keys; ++i, ++begin) {
      auto hash = *begin;
      auto bucket_id = m_bucketer.bucket(hash.first());
      pairs[i] = {static_cast<bucket_id_type>(bucket_id), hash.second()};
    }
    std::sort(pairs.begin(), pairs.end());
    pairs_blocks.resize(1);
    pairs_blocks.front().swap(pairs);
  }

  template <typename RandomAccessIterator>
  void map_parallel(RandomAccessIterator hashes, uint64_t num_keys,
                    std::vector<pairs_t>& pairs_blocks,
                    build_configuration const& config) const {
    pairs_blocks.resize(config.num_threads);
    uint64_t num_keys_per_thread =
        (num_keys + config.num_threads - 1) / config.num_threads;

    auto exe = [&](uint64_t tid) {
      auto& local_pairs = pairs_blocks[tid];
      RandomAccessIterator begin = hashes + tid * num_keys_per_thread;
      uint64_t local_num_keys = (tid != config.num_threads - 1)
                                    ? num_keys_per_thread
                                    : (num_keys - tid * num_keys_per_thread);
      local_pairs.resize(local_num_keys);

      for (uint64_t local_i = 0; local_i != local_num_keys;
           ++local_i, ++begin) {
        auto hash = *begin;
        auto bucket_id = m_bucketer.bucket(hash.first());
        local_pairs[local_i] = {static_cast<bucket_id_type>(bucket_id),
                                hash.second()};
      }
      std::sort(local_pairs.begin(), local_pairs.end());
    };

    std::vector<std::thread> threads(config.num_threads);
    for (uint64_t i = 0; i != config.num_threads; ++i)
      threads[i] = std::thread(exe, i);
    for (auto& t : threads) {
      if (t.joinable())
        t.join();
    }
  }

  template <typename RandomAccessIterator>
  void map(RandomAccessIterator hashes, uint64_t num_keys,
           std::vector<pairs_t>& pairs_blocks,
           build_configuration const& config) const {
    if (config.num_threads > 1) {
      map_parallel(hashes, num_keys, pairs_blocks, config);
    } else {
      map_sequential(hashes, num_keys, pairs_blocks, config);
    }
  }
};

}  // namespace pthash

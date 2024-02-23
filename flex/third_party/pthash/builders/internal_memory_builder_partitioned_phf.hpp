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

#include "builders/internal_memory_builder_single_phf.hpp"
#include "builders/util.hpp"

namespace pthash {

template <typename Hasher>
struct internal_memory_builder_partitioned_phf {
  typedef Hasher hasher_type;

  template <typename Iterator>
  build_timings build_from_keys(Iterator keys, uint64_t num_keys,
                                build_configuration const& config) {
    assert(num_keys > 1);
    if (config.num_partitions == 0) {
      throw std::invalid_argument("number of partitions must be > 0");
    }

    auto start = clock_type::now();

    build_timings timings;
    uint64_t num_partitions = config.num_partitions;
    if (config.verbose_output)
      std::cout << "num_partitions " << num_partitions << std::endl;

    m_seed =
        config.seed == constants::invalid_seed ? random_value() : config.seed;
    m_num_keys = num_keys;
    m_table_size = 0;
    m_num_partitions = num_partitions;
    m_bucketer.init(num_partitions);
    m_offsets.resize(num_partitions);
    m_builders.resize(num_partitions);

    double average_partition_size =
        static_cast<double>(num_keys) / num_partitions;
    if (average_partition_size < 10000) {
      throw std::runtime_error(
          "average partition size is too small: use less partitions");
    }
    std::vector<std::vector<typename hasher_type::hash_type>> partitions(
        num_partitions);
    for (auto& partition : partitions)
      partition.reserve(1.5 * average_partition_size);

    progress_logger logger(num_keys, " == partitioned ", " keys",
                           config.verbose_output);
    for (uint64_t i = 0; i != num_keys; ++i, ++keys) {
      auto const& key = *keys;
      auto hash = hasher_type::hash(key, m_seed);
      auto b = m_bucketer.bucket(hash.mix());
      partitions[b].push_back(hash);
      logger.log();
    }
    logger.finalize();

    for (uint64_t i = 0, cumulative_size = 0; i != num_partitions; ++i) {
      auto const& partition = partitions[i];

      uint64_t table_size =
          static_cast<double>(partition.size()) / config.alpha;
      if ((table_size & (table_size - 1)) == 0)
        table_size += 1;
      m_table_size += table_size;

      if (partition.size() <= 1) {
        throw std::runtime_error(
            "each partition must contain more than one key: use less "
            "partitions");
      }
      m_offsets[i] = cumulative_size;
      cumulative_size += config.minimal_output ? partition.size() : table_size;
    }

    auto partition_config = config;
    partition_config.seed = m_seed;
    uint64_t num_buckets_single_phf =
        std::ceil((config.c * num_keys) / std::log2(num_keys));
    partition_config.num_buckets =
        static_cast<double>(num_buckets_single_phf) / num_partitions;
    partition_config.verbose_output = false;
    partition_config.num_threads = 1;

    timings.partitioning_seconds = seconds(clock_type::now() - start);

    auto t = build_partitions(partitions.begin(), m_builders.begin(),
                              partition_config, config.num_threads);
    timings.mapping_ordering_seconds = t.mapping_ordering_seconds;
    timings.searching_seconds = t.searching_seconds;

    return timings;
  }

  template <typename PartitionsIterator, typename BuildersIterator>
  static build_timings build_partitions(PartitionsIterator partitions,
                                        BuildersIterator builders,
                                        build_configuration const& config,
                                        uint64_t num_threads) {
    build_timings timings;
    uint64_t num_partitions = config.num_partitions;
    assert(config.num_threads == 1);

    if (num_threads > 1) {  // parallel
      std::vector<std::thread> threads(num_threads);
      std::vector<build_timings> thread_timings(num_threads);

      auto exe = [&](uint64_t i, uint64_t begin, uint64_t end) {
        for (; begin != end; ++begin) {
          auto const& partition = partitions[begin];
          auto t = builders[begin].build_from_hashes(partition.begin(),
                                                     partition.size(), config);
          thread_timings[i].mapping_ordering_seconds +=
              t.mapping_ordering_seconds;
          thread_timings[i].searching_seconds += t.searching_seconds;
        }
      };

      uint64_t num_partitions_per_thread =
          (num_partitions + num_threads - 1) / num_threads;
      for (uint64_t i = 0, begin = 0; i != num_threads; ++i) {
        uint64_t end = begin + num_partitions_per_thread;
        if (end > num_partitions)
          end = num_partitions;
        threads[i] = std::thread(exe, i, begin, end);
        begin = end;
      }

      for (auto& t : threads) {
        if (t.joinable())
          t.join();
      }

      for (auto const& t : thread_timings) {
        if (t.mapping_ordering_seconds > timings.mapping_ordering_seconds)
          timings.mapping_ordering_seconds = t.mapping_ordering_seconds;
        if (t.searching_seconds > timings.searching_seconds)
          timings.searching_seconds = t.searching_seconds;
      }
    } else {  // sequential
      for (uint64_t i = 0; i != num_partitions; ++i) {
        auto const& partition = partitions[i];
        auto t = builders[i].build_from_hashes(partition.begin(),
                                               partition.size(), config);
        timings.mapping_ordering_seconds += t.mapping_ordering_seconds;
        timings.searching_seconds += t.searching_seconds;
      }
    }
    return timings;
  }

  uint64_t seed() const { return m_seed; }

  uint64_t num_keys() const { return m_num_keys; }

  uint64_t table_size() const { return m_table_size; }

  uint64_t num_partitions() const { return m_num_partitions; }

  uniform_bucketer bucketer() const { return m_bucketer; }

  std::vector<uint64_t> const& offsets() const { return m_offsets; }

  std::vector<internal_memory_builder_single_phf<hasher_type>> const& builders()
      const {
    return m_builders;
  }

 private:
  uint64_t m_seed;
  uint64_t m_num_keys;
  uint64_t m_table_size;
  uint64_t m_num_partitions;
  uniform_bucketer m_bucketer;
  std::vector<uint64_t> m_offsets;
  std::vector<internal_memory_builder_single_phf<hasher_type>> m_builders;
};

}  // namespace pthash

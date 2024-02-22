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

#include "builders/util.hpp"
#include "mm_file/mm_file.hpp"

#include "builders/internal_memory_builder_partitioned_phf.hpp"
#include "builders/internal_memory_builder_single_phf.hpp"

namespace pthash {

template <typename Hasher>
struct external_memory_builder_partitioned_phf {
  typedef Hasher hasher_type;
  typedef typename hasher_type::hash_type hash_type;

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
    if (config.verbose_output) {
      std::cout << "num_partitions " << num_partitions << std::endl;
      std::cout << "using " << static_cast<double>(config.ram) / 1000000000
                << " GB of RAM" << std::endl;
    }

    m_seed =
        config.seed == constants::invalid_seed ? random_value() : config.seed;
    m_num_keys = num_keys;
    m_table_size = 0;
    m_num_partitions = num_partitions;
    m_bucketer.init(num_partitions);
    m_offsets.resize(num_partitions);
    m_builders.init(
        config.tmp_dir,
        static_cast<uint64_t>(clock_type::now().time_since_epoch().count()),
        num_partitions);

    std::vector<meta_partition> partitions;
    partitions.reserve(num_partitions);
    double average_partition_size =
        static_cast<double>(num_keys) / num_partitions;
    if (average_partition_size < 10000) {
      throw std::runtime_error(
          "average partition size is too small: use less partitions");
    }
    for (uint64_t id = 0; id != num_partitions; ++id) {
      partitions.emplace_back(config.tmp_dir, id);
      partitions.back().reserve(1.5 * average_partition_size);
    }

    size_t bytes = num_partitions * sizeof(meta_partition);
    if (bytes >= config.ram)
      throw std::runtime_error("not enough RAM available");

    progress_logger logger(num_keys, " == partitioned ", " keys",
                           config.verbose_output);
    for (uint64_t i = 0; i != num_keys; ++i, ++keys) {
      auto const& key = *keys;
      auto hash = hasher_type::hash(key, m_seed);
      auto b = m_bucketer.bucket(hash.mix());
      partitions[b].push_back(hash);
      bytes += sizeof(hash_type);
      if (bytes >= config.ram) {
        for (auto& partition : partitions)
          partition.flush();
        bytes = num_partitions * sizeof(meta_partition);
      }
      logger.log();
    }
    logger.finalize();

    for (auto& partition : partitions)
      partition.release();

    bool failure = false;
    for (uint64_t i = 0, cumulative_size = 0; i != num_partitions; ++i) {
      auto const& partition = partitions[i];

      uint64_t table_size =
          static_cast<double>(partition.size()) / config.alpha;
      if ((table_size & (table_size - 1)) == 0)
        table_size += 1;
      m_table_size += table_size;

      if (partition.size() <= 1) {
        failure = true;
        break;
      }
      m_offsets[i] = cumulative_size;
      cumulative_size += config.minimal_output ? partition.size() : table_size;
    }

    if (failure) {
      for (uint64_t i = 0; i != num_partitions; ++i) {
        std::remove(partitions[i].filename().c_str());
      }
      throw std::runtime_error(
          "each partition must contain more than one key: use less partitions");
    }

    auto partition_config = config;
    partition_config.seed = m_seed;
    uint64_t num_buckets_single_phf =
        std::ceil((config.c * num_keys) / std::log2(num_keys));
    partition_config.num_buckets =
        static_cast<double>(num_buckets_single_phf) / num_partitions;
    partition_config.num_threads = 1;
    partition_config.verbose_output = false;

    timings.partitioning_seconds += seconds(clock_type::now() - start);

    if (config.num_threads > 1) {  // parallel
      start = clock_type::now();

      bytes = num_partitions * sizeof(meta_partition);
      std::vector<std::vector<hash_type>> in_memory_partitions;
      uint64_t i = 0;

      auto build_partitions = [&]() {
        if (config.verbose_output) {
          std::cout << "processing " << in_memory_partitions.size() << "/"
                    << num_partitions << " partitions..." << std::endl;
        }
        std::vector<internal_memory_builder_single_phf<hasher_type>>
            in_memory_builders(in_memory_partitions.size());
        partition_config.num_partitions = in_memory_partitions.size();
        auto t = internal_memory_builder_partitioned_phf<
            hasher_type>::build_partitions(in_memory_partitions.begin(),
                                           in_memory_builders.begin(),
                                           partition_config,
                                           config.num_threads);
        timings.mapping_ordering_seconds += t.mapping_ordering_seconds;
        timings.searching_seconds += t.searching_seconds;
        in_memory_partitions.clear();
        bytes = num_partitions * sizeof(meta_partition);

        if (config.verbose_output) {
          std::cout << "writing builders to disk..." << std::endl;
        }
        start = clock_type::now();
        uint64_t id = i - partition_config.num_partitions;
        for (auto& builder : in_memory_builders) {
          m_builders.save(builder, id);
          internal_memory_builder_single_phf<hasher_type>().swap(builder);
          ++id;
        }
        timings.partitioning_seconds += seconds(clock_type::now() - start);
        assert(id == i);
      };

      for (; i != num_partitions; ++i) {
        uint64_t size = partitions[i].size();
        uint64_t partition_bytes = internal_memory_builder_single_phf<
            hasher_type>::estimate_num_bytes_for_construction(size,
                                                              partition_config);
        if (bytes + partition_bytes >= config.ram) {
          timings.partitioning_seconds += seconds(clock_type::now() - start);
          build_partitions();
          start = clock_type::now();
        }
        std::vector<hash_type> p(size);
        std::ifstream in(partitions[i].filename().c_str(),
                         std::ifstream::binary);
        if (!in.is_open())
          throw std::runtime_error("cannot open file");
        in.read(reinterpret_cast<char*>(p.data()),
                static_cast<std::streamsize>(size * sizeof(hash_type)));
        in.close();
        std::remove(partitions[i].filename().c_str());
        in_memory_partitions.push_back(std::move(p));
        bytes += partition_bytes;
      }
      timings.partitioning_seconds += seconds(clock_type::now() - start);
      if (!in_memory_partitions.empty())
        build_partitions();
      std::vector<std::vector<hash_type>>().swap(in_memory_partitions);

    } else {  // sequential
      internal_memory_builder_single_phf<hasher_type> b;
      for (uint64_t i = 0; i != num_partitions; ++i) {
        if (config.verbose_output) {
          std::cout << "processing partition " << i << "/" << num_partitions
                    << " partitions..." << std::endl;
        }
        mm::file_source<hash_type> partition(partitions[i].filename(),
                                             mm::advice::sequential);
        auto t = b.build_from_hashes(partition.data(), partition.size(),
                                     partition_config);
        partition.close();
        start = clock_type::now();
        std::remove(partitions[i].filename().c_str());
        m_builders.save(b, i);
        timings.partitioning_seconds += seconds(clock_type::now() - start);
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

 private:
  template <typename Builder>
  struct builders_files_manager {
    builders_files_manager() {}

    void init(std::string const& dir_name, uint64_t run_identifier,
              uint64_t num_partitions) {
      m_dir_name = dir_name;
      m_run_identifier = run_identifier;
      m_num_partitions = num_partitions;
    }

    ~builders_files_manager() { close(); }

    void close() {
      for (uint64_t i = 0; i != m_num_partitions; ++i) {
        std::remove(get_partition_filename(i).c_str());
      }
    }

    void save(Builder& builder, uint64_t partition) {
      essentials::save(builder, get_partition_filename(partition).c_str());
    }

    Builder operator[](uint64_t partition) const {
      assert(partition < m_num_partitions);
      Builder builder;
      essentials::load(builder, get_partition_filename(partition).c_str());
      return builder;
    }

    inline uint64_t size() const { return m_num_partitions; }

   private:
    std::string get_partition_filename(uint64_t partition) const {
      std::stringstream filename;
      filename << m_dir_name << "/pthash.tmp.run" << m_run_identifier
               << ".partition" << partition << ".bin";
      return filename.str();
    }

    std::string m_dir_name;
    uint64_t m_run_identifier;
    uint64_t m_num_partitions;
  };

 public:
  builders_files_manager<internal_memory_builder_single_phf<hasher_type>> const&
  builders() const {
    return m_builders;
  }

 private:
  uint64_t m_seed;
  uint64_t m_num_keys;
  uint64_t m_table_size;
  uint64_t m_num_partitions;
  uniform_bucketer m_bucketer;
  std::vector<uint64_t> m_offsets;
  builders_files_manager<internal_memory_builder_single_phf<hasher_type>>
      m_builders;

  struct meta_partition {
    meta_partition(std::string const& dir_name, uint64_t id)
        : m_filename(dir_name + "/pthash.temp." + std::to_string(id)),
          m_size(0) {}

    void push_back(hash_type hash) { m_hashes.push_back(hash); }

    std::string const& filename() const { return m_filename; }

    void flush() {
      if (m_hashes.empty())
        return;
      m_size += m_hashes.size();
      std::ofstream out(m_filename.c_str(),
                        std::ofstream::binary | std::ofstream::app);
      if (!out.is_open())
        throw std::runtime_error("cannot open file");
      out.write(reinterpret_cast<char const*>(m_hashes.data()),
                m_hashes.size() * sizeof(hash_type));
      out.close();
      m_hashes.clear();
    }

    void reserve(uint64_t n) { m_hashes.reserve(n); }

    void release() {
      flush();
      std::vector<hash_type>().swap(m_hashes);
    }

    uint64_t size() const { return m_size; }

   private:
    std::string m_filename;
    std::vector<hash_type> m_hashes;
    uint64_t m_size;
  };
};

}  // namespace pthash

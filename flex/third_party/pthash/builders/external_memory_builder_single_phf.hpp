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
#include "mm_file/mm_file.hpp"

#include "utils/bucketers.hpp"
#include "utils/hasher.hpp"
#include "utils/logger.hpp"

namespace pthash {

template <typename Hasher>
struct external_memory_builder_single_phf {
  typedef Hasher hasher_type;

  external_memory_builder_single_phf()
      : m_pilots_filename(""), m_free_slots_filename("") {}
  // non construction-copyable
  external_memory_builder_single_phf(
      external_memory_builder_single_phf const&) = delete;
  // non copyable
  external_memory_builder_single_phf& operator=(
      external_memory_builder_single_phf const&) = delete;

  ~external_memory_builder_single_phf() {
    if (m_pilots_filename != "")
      std::remove(m_pilots_filename.c_str());
    m_pilots_filename = "";
    if (m_free_slots_filename != "")
      std::remove(m_free_slots_filename.c_str());
    m_free_slots_filename = "";
  }

  template <typename Iterator>
  build_timings build_from_keys(Iterator keys, uint64_t num_keys,
                                build_configuration const& config) {
    assert(num_keys > 1);
    if (config.alpha == 0 or config.alpha > 1.0) {
      throw std::invalid_argument("load factor must be > 0 and <= 1.0");
    }

    build_timings time;
    uint64_t table_size = static_cast<double>(num_keys) / config.alpha;
    if ((table_size & (table_size - 1)) == 0)
      table_size += 1;
    uint64_t num_buckets =
        std::ceil((config.c * num_keys) / std::log2(num_keys));

    if (sizeof(bucket_id_type) != sizeof(uint64_t) and
        num_buckets > (1ULL << (sizeof(bucket_id_type) * 8))) {
      throw std::runtime_error(
          "using too many buckets: change bucket_id_type to uint64_t or use a "
          "smaller c");
    }

    m_num_keys = num_keys;
    m_table_size = table_size;
    m_num_buckets = num_buckets;
    m_seed =
        config.seed == constants::invalid_seed ? random_value() : config.seed;
    m_bucketer.init(num_buckets);

    uint64_t ram = config.ram;

    uint64_t bitmap_taken_bytes = 8 * ((table_size + 63) / 64);
    uint64_t hashed_pilots_cache_bytes = search_cache_size * sizeof(uint64_t);
    if (bitmap_taken_bytes + hashed_pilots_cache_bytes >= ram) {
      std::stringstream ss;
      ss << "not enough RAM available, the bitmap alone takes "
         << static_cast<double>(bitmap_taken_bytes) / 1000000000
         << " GB of space.";
      throw std::runtime_error(ss.str());
    }

    if (config.verbose_output) {
      constexpr uint64_t GB = 1000000000;
      uint64_t peak =
          num_keys * (sizeof(bucket_payload_pair) + sizeof(uint64_t)) +
          (num_keys + num_buckets) * sizeof(uint64_t);
      std::cout << "c = " << config.c << std::endl;
      std::cout << "alpha = " << config.alpha << std::endl;
      std::cout << "num_keys = " << num_keys << std::endl;
      std::cout << "table_size = " << table_size << std::endl;
      std::cout << "num_buckets = " << num_buckets << std::endl;
      std::cout << "using " << static_cast<double>(ram) / GB << " GB of RAM"
                << " (" << static_cast<double>(bitmap_taken_bytes) / GB
                << " GB occupied by the bitmap)" << std::endl;
      std::cout << "using a peak of " << static_cast<double>(peak) / GB
                << " GB of disk space" << std::endl;
    }

    uint64_t run_identifier = clock_type::now().time_since_epoch().count();
    temporary_files_manager tfm(config.tmp_dir, run_identifier);

    uint64_t num_non_empty_buckets = 0;

    try {
      auto start = clock_type::now();
      {
        auto start = clock_type::now();
        std::vector<pairs_t> pairs_blocks;
        map(keys, num_keys, pairs_blocks, tfm, config);
        auto stop = clock_type::now();
        if (config.verbose_output) {
          std::cout << " == map+sort " << tfm.get_num_pairs_files()
                    << " files(s) took: " << seconds(stop - start) << " seconds"
                    << std::endl;
        }
        start = clock_type::now();
        buckets_t buckets = tfm.buckets(config);
        merge(pairs_blocks, buckets, config.verbose_output);
        buckets.flush();
        for (auto& pairs_block : pairs_blocks)
          pairs_block.close();
        num_non_empty_buckets = buckets.num_buckets();
        tfm.remove_all_pairs_files();
        stop = clock_type::now();
        if (config.verbose_output) {
          std::cout << " == merge+check took: " << seconds(stop - start)
                    << " seconds" << std::endl;
          std::cout << " == max bucket size = " << int(tfm.max_bucket_size())
                    << std::endl;
        }
      }
      auto stop = clock_type::now();
      time.mapping_ordering_seconds = seconds(stop - start);
      if (config.verbose_output) {
        std::cout << " == map+ordering took " << time.mapping_ordering_seconds
                  << " seconds" << std::endl;
      }
    } catch (...) {
      tfm.remove_all_pairs_files();
      tfm.remove_all_merge_files();
      throw;
    }

    try {
      auto start = clock_type::now();
      bit_vector_builder taken(m_table_size);

      {  // search
        auto buckets_iterator = tfm.buckets_iterator();

        // write all bucket-pilot pairs to files
        uint64_t ram_for_pilots =
            ram - bitmap_taken_bytes - hashed_pilots_cache_bytes;
        auto pilots = tfm.get_multifile_pairs_writer(num_non_empty_buckets,
                                                     ram_for_pilots, 1, 0);

        search(m_num_keys, m_num_buckets, num_non_empty_buckets, m_seed, config,
               buckets_iterator, taken, pilots);

        pilots.flush();
        buckets_iterator.close();
        // merge all sorted bucket-pilot pairs on a single file, saving only the
        // pilot
        pilots_merger_t pilots_merger(tfm.get_pilots_filename(), ram);
        merge(tfm.pairs_blocks(), pilots_merger, false);
        pilots_merger.finalize_and_close(m_num_buckets);

        if (m_pilots_filename != "")
          std::remove(m_pilots_filename.c_str());
        m_pilots_filename = tfm.get_pilots_filename();

        // remove unused temporary files
        tfm.remove_all_pairs_files();
        tfm.remove_all_merge_files();
      }

      if (config.minimal_output) {  // fill free slots
        // write all free slots to file
        buffered_file_t<uint64_t> writer(tfm.get_free_slots_filename(),
                                         ram - bitmap_taken_bytes);
        fill_free_slots(taken, num_keys, writer);
        writer.close();
        if (m_free_slots_filename != "")
          std::remove(m_free_slots_filename.c_str());
        m_free_slots_filename = tfm.get_free_slots_filename();
      }

      auto stop = clock_type::now();
      time.searching_seconds = seconds(stop - start);
      if (config.verbose_output) {
        std::cout << " == search took " << time.searching_seconds << " seconds"
                  << std::endl;
      }
    } catch (...) {
      tfm.remove_all_pairs_files();
      tfm.remove_all_merge_files();
      throw;
    }

    return time;
  }

  uint64_t seed() const { return m_seed; }

  uint64_t num_keys() const { return m_num_keys; }

  uint64_t table_size() const { return m_table_size; }

  skew_bucketer bucketer() const { return m_bucketer; }

  mm::file_source<uint64_t> pilots() const {
    return mm::file_source<uint64_t>(m_pilots_filename);
  }

  mm::file_source<uint64_t> free_slots() const {
    return mm::file_source<uint64_t>(m_free_slots_filename);
  }

 private:
  uint64_t m_seed;
  uint64_t m_num_keys;
  uint64_t m_table_size;
  uint64_t m_num_buckets;
  skew_bucketer m_bucketer;
  std::string m_pilots_filename;
  std::string m_free_slots_filename;

  template <typename T>
  struct buffer_t {
    buffer_t(uint64_t ram) : m_buffer_capacity(ram / sizeof(T)) {
      m_buffer.reserve(m_buffer_capacity);
      assert(m_buffer_capacity > 0);
    }

    template <class... _Args>
    void emplace_back(_Args&&... __args) {
      m_buffer.emplace_back(std::forward<_Args>(__args)...);
      if (--m_buffer_capacity == 0)
        flush();
    }

    void flush() {
      if (!m_buffer.empty()) {
        uint64_t buffer_size = m_buffer.size();
        flush_impl(m_buffer);
        m_buffer_capacity += buffer_size;
        m_buffer.clear();
      }
    }

   protected:
    virtual void flush_impl(std::vector<T>& buffer) = 0;

   private:
    uint64_t m_buffer_capacity;
    std::vector<T> m_buffer;
  };

  template <typename T>
  struct buffered_file_t : buffer_t<T> {
    buffered_file_t(std::string const& filename, uint64_t ram)
        : buffer_t<T>(ram) {
      m_out.open(filename, std::ofstream::out | std::ofstream::binary);
      if (!m_out.is_open())
        throw std::runtime_error("cannot open binary file in write mode");
    }

    void close() {
      buffer_t<T>::flush();
      m_out.close();
    }

   protected:
    void flush_impl(std::vector<T>& buffer) {
      m_out.write(reinterpret_cast<char const*>(buffer.data()),
                  buffer.size() * sizeof(T));
    }

   private:
    std::ofstream m_out;
  };

  template <typename T>
  struct memory_view {
    typedef T* iterator;
    typedef const T* const_iterator;

    memory_view() : m_begin(nullptr), m_end(nullptr){};
    memory_view(T* begin, uint64_t size)
        : m_begin(begin), m_end(begin + size) {}

    inline T* begin() const { return m_begin; }
    inline T* end() const { return m_end; }
    inline T& operator[](uint64_t pos) const { return *(m_begin + pos); }
    inline uint64_t size() const { return std::distance(m_begin, m_end); }

   protected:
    T *m_begin, *m_end;
  };

  template <typename T>
  struct reader_t : memory_view<const T> {
    void open(std::string const& filename) {
      if (m_is.is_open())
        m_is.close();
      m_is.open(filename, mm::advice::sequential);
      if (!m_is.is_open())
        throw std::runtime_error("cannot open temporary file (read)");
      memory_view<const T>::m_begin = m_is.data();
      memory_view<const T>::m_end = m_is.data() + m_is.size();
    }

    void close() { m_is.close(); }

   private:
    mm::file_source<T> m_is;
  };

  typedef reader_t<bucket_payload_pair> pairs_t;

  struct pairs_merger_t {
    pairs_merger_t(std::string const& filename, uint64_t ram)
        : m_buffer(filename, ram) {}

    template <typename HashIterator>
    void add(bucket_id_type bucket_id, bucket_size_type bucket_size,
             HashIterator hashes) {
      for (uint64_t k = 0; k != bucket_size; ++k, ++hashes) {
        m_buffer.emplace_back(bucket_id, *hashes);
      }
    }

    void close() { m_buffer.close(); }

   private:
    buffered_file_t<bucket_payload_pair> m_buffer;
  };

  struct buckets_t {  // merger
    buckets_t(std::vector<std::string> const& filenames, uint64_t ram,
              std::vector<bool>& used_bucket_sizes)
        : m_filenames(filenames),
          m_buffers(filenames.size()),
          m_buffer_capacity(ram / (sizeof(uint64_t) * 2)),
          m_ram(ram / (sizeof(uint64_t) * 2)),
          m_used_bucket_sizes(used_bucket_sizes),
          m_outs(filenames.size()),
          m_num_buckets(0) {
      assert(m_filenames.size() == m_used_bucket_sizes.size());
      m_non_empty_buckets.reserve(filenames.size());
      for (uint64_t i = 0; i != filenames.size(); ++i) {
        if (m_used_bucket_sizes[i]) {
          throw std::runtime_error("One of the output files is already open");
        }
      }
    }

    template <typename HashIterator>
    void add(bucket_id_type bucket_id, bucket_size_type bucket_size,
             HashIterator hashes) {
      assert(bucket_size > 0 and bucket_size <= MAX_BUCKET_SIZE);
      ensure_capacity(bucket_size);
      uint64_t i = bucket_size - 1;
      if (m_buffers[i].empty())
        m_non_empty_buckets.push_back(bucket_size - 1);
      m_buffers[i].push_back(bucket_id);
      for (uint64_t k = 0; k != bucket_size; ++k, ++hashes)
        m_buffers[i].push_back(*hashes);
      m_buffer_capacity -= bucket_size + 1;
      ++m_num_buckets;
    }

    uint64_t num_buckets() const { return m_num_buckets; };

    void flush() {
      for (uint64_t i = 0; i != m_buffers.size(); ++i)
        flush_i(i);
      m_non_empty_buckets.clear();
    }

   private:
    void ensure_capacity(uint64_t bucket_size) {
      if (bucket_size + 1 > m_buffer_capacity) {
        std::sort(m_non_empty_buckets.begin(), m_non_empty_buckets.end(),
                  [&](uint64_t i, uint64_t j) {
                    return m_buffers[i].size() < m_buffers[j].size();
                  });

        uint64_t target =
            std::max((uint64_t) std::ceil(0.999 * m_ram), bucket_size + 1);
        while (m_buffer_capacity < target) {
          flush_i(m_non_empty_buckets.back());
          m_non_empty_buckets.pop_back();
        }
      }
    }

    void flush_i(uint64_t i) {
      if (m_buffers[i].size() == 0)
        return;
      if (!m_used_bucket_sizes[i]) {
        m_outs[i].open(m_filenames[i].c_str(),
                       std::ofstream::out | std::ofstream::binary);
        if (!m_outs[i].is_open()) {
          throw std::runtime_error("cannot open temporary file (write)");
        }
        m_used_bucket_sizes[i] = true;
      }
      m_outs[i].write(reinterpret_cast<char const*>(m_buffers[i].data()),
                      m_buffers[i].size() * sizeof(uint64_t));
      m_buffer_capacity += m_buffers[i].size();
      std::vector<uint64_t>().swap(m_buffers[i]);
    }

    std::vector<std::string> m_filenames;
    std::vector<std::vector<uint64_t>> m_buffers;
    uint64_t m_buffer_capacity;
    uint64_t m_ram;
    std::vector<uint64_t> m_non_empty_buckets;
    std::vector<bool>& m_used_bucket_sizes;
    std::vector<std::ofstream> m_outs;
    uint64_t m_num_buckets;
  };

  struct buckets_iterator_t {
    buckets_iterator_t(
        std::vector<std::pair<bucket_size_type, std::string>> const&
            sizes_filenames)
        : m_sizes(sizes_filenames.size()), m_sources(sizes_filenames.size()) {
      m_pos = sizes_filenames.size();
      for (uint64_t i = 0, i_end = m_pos; i < i_end; ++i) {
        m_sizes[i] = sizes_filenames[i].first;
        m_sources[i].open(sizes_filenames[i].second, mm::advice::sequential);
        assert(i == 0 or m_sizes[i - 1] < m_sizes[i]);
      }
      read_next_file();
    }

    void close() {
      for (auto& is : m_sources)
        is.close();
    }

    inline bucket_t operator*() {
      bucket_t bucket;
      bucket.init(m_it, m_bucket_size);
      return bucket;
    }

    void operator++() {
      m_it += m_bucket_size + 1;
      if (m_it >= m_end)
        read_next_file();
    }

   private:
    void read_next_file() {
      if (m_pos == 0) {
        m_it = m_end;
        return;
      }
      --m_pos;
      m_bucket_size = m_sizes[m_pos];
      m_it = m_sources[m_pos].data();
      m_end = m_it + m_sources[m_pos].size();
    }

    uint64_t m_pos;
    std::vector<bucket_size_type> m_sizes;
    std::vector<mm::file_source<uint64_t>> m_sources;
    bucket_size_type m_bucket_size;
    uint64_t const* m_it;
    uint64_t const* m_end;
  };

  struct pilots_merger_t {
    pilots_merger_t(std::string const& filename, uint64_t ram)
        : m_buffer(filename, ram), m_next_bucket_id(0) {}

    template <typename HashIterator>
    void add(bucket_id_type bucket_id, bucket_size_type bucket_size,
             HashIterator hashes) {
      assert(bucket_size == 1);
      (void) bucket_size;  // avoid unused warning in release mode
      emplace_back_and_fill(bucket_id, *hashes);
    }

    void finalize_and_close(uint64_t num_buckets) {
      if (m_next_bucket_id < num_buckets)
        emplace_back_and_fill(num_buckets - 1, 0);
      m_buffer.close();
    }

   private:
    inline void emplace_back_and_fill(bucket_id_type bucket_id,
                                      uint64_t pilot) {
      assert(m_next_bucket_id <= bucket_id);

      while (m_next_bucket_id++ < bucket_id) {
        m_buffer.emplace_back(0);
      }
      m_buffer.emplace_back(pilot);
    }

    buffered_file_t<uint64_t> m_buffer;
    uint64_t m_next_bucket_id;
  };

  struct multifile_pairs_writer : buffer_t<bucket_payload_pair> {
    multifile_pairs_writer(std::vector<std::string> const& filenames,
                           uint64_t& num_pairs_files, uint64_t num_pairs,
                           uint64_t ram, uint64_t num_threads_sort = 1,
                           uint64_t ram_parallel_merge = 0)
        : buffer_t<bucket_payload_pair>(get_balanced_ram(num_pairs, ram)),
          m_filenames(filenames),
          m_num_pairs_files(num_pairs_files),
          m_num_threads_sort(num_threads_sort),
          m_ram_parallel_merge(ram_parallel_merge) {
      assert(num_threads_sort > 1 or ram_parallel_merge == 0);
    }

   protected:
    void flush_impl(std::vector<bucket_payload_pair>& buffer) {
      const uint64_t size = buffer.size();

      if (m_num_threads_sort > 1) {  // parallel
        std::vector<memory_view<bucket_payload_pair>> blocks;
        uint64_t num_keys_per_thread =
            (size + m_num_threads_sort - 1) / m_num_threads_sort;
        auto exe = [&](uint64_t tid) {
          std::sort(blocks[tid].begin(), blocks[tid].end());
        };

        std::vector<std::thread> threads(m_num_threads_sort);
        for (uint64_t i = 0; i != m_num_threads_sort; ++i) {
          auto begin = buffer.data() + i * num_keys_per_thread;
          auto end =
              buffer.data() + std::min((i + 1) * num_keys_per_thread, size);
          uint64_t block_size = std::distance(begin, end);

          blocks.emplace_back(begin, block_size);
          threads[i] = std::thread(exe, i);
        }
        for (uint64_t i = 0; i != m_num_threads_sort; ++i) {
          if (threads[i].joinable())
            threads[i].join();
        }
        pairs_merger_t pairs_merger(m_filenames[m_num_pairs_files],
                                    m_ram_parallel_merge);
        ++m_num_pairs_files;
        merge(blocks, pairs_merger, false);
        pairs_merger.close();
      } else {  // sequential
        std::ofstream out(m_filenames[m_num_pairs_files],
                          std::ofstream::out | std::ofstream::binary);
        if (!out.is_open())
          throw std::runtime_error("cannot open temporary file (write)");
        ++m_num_pairs_files;
        std::sort(buffer.begin(), buffer.end());
        out.write(reinterpret_cast<char const*>(buffer.data()),
                  size * sizeof(bucket_payload_pair));
        out.close();
      }
    }

   private:
    std::vector<std::string> m_filenames;
    uint64_t& m_num_pairs_files;
    uint64_t m_num_threads_sort;
    uint64_t m_ram_parallel_merge;

    static uint64_t get_balanced_ram(uint64_t num_pairs, uint64_t ram) {
      uint64_t num_pairs_per_file = ram / sizeof(bucket_payload_pair);
      uint64_t num_temporary_files =
          (num_pairs + num_pairs_per_file - 1) / num_pairs_per_file;
      uint64_t balanced_num_pairs_per_temporary_file =
          (num_pairs + num_temporary_files - 1) / num_temporary_files;
      uint64_t balanced_ram =
          balanced_num_pairs_per_temporary_file * sizeof(bucket_payload_pair);
      assert(balanced_ram <= ram);

      return balanced_ram;
    }
  };

  struct temporary_files_manager {
    temporary_files_manager(std::string const& dir_name,
                            uint64_t run_identifier)
        : m_dir_name(dir_name),
          m_run_identifier(run_identifier),
          m_num_pairs_files(0),
          m_used_bucket_sizes(MAX_BUCKET_SIZE) {
      std::fill(m_used_bucket_sizes.begin(), m_used_bucket_sizes.end(), false);
    }

    multifile_pairs_writer get_multifile_pairs_writer(
        uint64_t num_pairs, uint64_t ram, uint64_t num_threads_sort = 1,
        uint64_t ram_parallel_merge = 0) {
      uint64_t num_pairs_per_file = ram / sizeof(bucket_payload_pair);
      uint64_t num_temporary_files =
          (num_pairs + num_pairs_per_file - 1) / num_pairs_per_file;
      std::vector<std::string> filenames;
      filenames.reserve(num_temporary_files);
      for (uint64_t i = 0; i < num_temporary_files; ++i) {
        filenames.emplace_back(get_pairs_filename(m_num_pairs_files + i));
      }
      return multifile_pairs_writer(filenames, m_num_pairs_files, num_pairs,
                                    ram, num_threads_sort, ram_parallel_merge);
    }

    uint64_t get_num_pairs_files() const { return m_num_pairs_files; }

    void remove_all_pairs_files() {
      while (m_num_pairs_files > 0) {
        std::remove(get_pairs_filename(--m_num_pairs_files).c_str());
      }
    }

    void remove_all_merge_files() {
      for (uint64_t i = 0; i != MAX_BUCKET_SIZE; ++i) {
        if (m_used_bucket_sizes[i]) {
          std::remove(get_buckets_filename(i + 1).c_str());
          m_used_bucket_sizes[i] = false;
        }
      }
    }

    std::vector<pairs_t> pairs_blocks() const {
      std::vector<pairs_t> result(m_num_pairs_files);
      for (uint64_t i = 0; i != m_num_pairs_files; ++i)
        result[i].open(get_pairs_filename(i));
      return result;
    };

    buckets_t buckets(build_configuration const& config) {
      std::vector<std::string> filenames;
      filenames.reserve(MAX_BUCKET_SIZE);
      for (uint64_t bucket_size = 1; bucket_size <= MAX_BUCKET_SIZE;
           ++bucket_size) {
        filenames.emplace_back(get_buckets_filename(bucket_size));
      }
      return buckets_t(filenames, config.ram, m_used_bucket_sizes);
    }

    buckets_iterator_t buckets_iterator() {
      std::vector<std::pair<bucket_size_type, std::string>> sizes_filenames;
      for (uint64_t i = 0; i != MAX_BUCKET_SIZE; ++i) {
        if (m_used_bucket_sizes[i]) {
          uint64_t bucket_size = i + 1;
          sizes_filenames.emplace_back(bucket_size,
                                       get_buckets_filename(bucket_size));
        }
      }
      assert(sizes_filenames.size() > 0);
      return buckets_iterator_t(sizes_filenames);
    }

    bucket_size_type max_bucket_size() {
      bucket_size_type bucket_size = 0;
      for (uint64_t i = 0, i_end = m_used_bucket_sizes.size(); i < i_end; ++i) {
        if (m_used_bucket_sizes[i])
          bucket_size = i;
      }
      return bucket_size + 1;
    }

    std::string get_pilots_filename() const {
      std::stringstream filename;
      filename << m_dir_name << "/pthash.tmp.run" << m_run_identifier
               << ".pilots"
               << ".bin";
      return filename.str();
    }

    std::string get_free_slots_filename() const {
      std::stringstream filename;
      filename << m_dir_name << "/pthash.tmp.run" << m_run_identifier
               << ".free_slots"
               << ".bin";
      return filename.str();
    }

   private:
    std::string get_pairs_filename(uint32_t file_id) const {
      std::stringstream filename;
      filename << m_dir_name << "/pthash.tmp.run" << m_run_identifier
               << ".pairs" << file_id << ".bin";
      return filename.str();
    }

    std::string get_buckets_filename(bucket_size_type bucket_size) const {
      std::stringstream filename;
      filename << m_dir_name << "/pthash.tmp.run" << m_run_identifier << ".size"
               << static_cast<uint32_t>(bucket_size) << ".bin";
      return filename.str();
    }

    std::string m_dir_name;
    uint64_t m_run_identifier;
    uint64_t m_num_pairs_files;
    std::vector<bool> m_used_bucket_sizes;
  };

  template <typename Iterator>
  void map(Iterator keys, uint64_t num_keys, std::vector<pairs_t>& pairs_blocks,
           temporary_files_manager& tfm, build_configuration const& config) {
    progress_logger logger(num_keys, " == processed ", " keys from input",
                           config.verbose_output);

    uint64_t ram = config.ram;
    uint64_t ram_parallel_merge = 0;
    if (config.num_threads > 1) {
      ram_parallel_merge = ram * 0.01;
      assert(ram_parallel_merge >=
             MAX_BUCKET_SIZE * sizeof(bucket_payload_pair));
    }

    auto writer =
        tfm.get_multifile_pairs_writer(num_keys, ram - ram_parallel_merge,
                                       config.num_threads, ram_parallel_merge);
    try {
      for (uint64_t i = 0; i != num_keys; ++i, ++keys) {
        auto const& key = *keys;
        auto hash = hasher_type::hash(key, m_seed);
        bucket_id_type bucket_id = m_bucketer.bucket(hash.first());
        writer.emplace_back(bucket_id, hash.second());
        logger.log();
      }
      writer.flush();
      logger.finalize();
    } catch (std::runtime_error const& e) { throw e; }

    auto tmp = tfm.pairs_blocks();
    pairs_blocks.swap(tmp);
  }
};

}  // namespace pthash

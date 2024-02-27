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

#include <fstream>
#include <thread>

#include "utils/logger.hpp"

namespace pthash {

typedef uint32_t bucket_id_type;
typedef uint8_t bucket_size_type;
#define MAX_BUCKET_SIZE static_cast<bucket_size_type>(100)

static inline std::string get_tmp_builder_filename(std::string const& dir_name,
                                                   uint64_t id) {
  return dir_name + "/pthash.temp." + std::to_string(id) + ".builder";
}

struct build_timings {
  build_timings()
      : partitioning_seconds(0.0),
        mapping_ordering_seconds(0.0),
        searching_seconds(0.0),
        encoding_seconds(0.0) {}

  double partitioning_seconds;
  double mapping_ordering_seconds;
  double searching_seconds;
  double encoding_seconds;
};

struct build_configuration {
  build_configuration()
      : c(4.5),
        alpha(0.98),
        num_partitions(1),
        num_buckets(constants::invalid_num_buckets),
        num_threads(1),
        seed(constants::invalid_seed),
        ram(static_cast<double>(constants::available_ram) * 0.75),
        tmp_dir(constants::default_tmp_dirname),
        minimal_output(false),
        verbose_output(true) {}

  double c;
  double alpha;
  uint64_t num_partitions;
  uint64_t num_buckets;
  uint64_t num_threads;
  uint64_t seed;
  uint64_t ram;
  std::string tmp_dir;
  bool minimal_output;
  bool verbose_output;
};

struct seed_runtime_error : public std::runtime_error {
  seed_runtime_error() : std::runtime_error("seed did not work") {}
};

#pragma pack(push, 4)
struct bucket_payload_pair {
  bucket_id_type bucket_id;
  uint64_t payload;

  bucket_payload_pair() {}
  bucket_payload_pair(bucket_id_type bucket_id, uint64_t payload)
      : bucket_id(bucket_id), payload(payload) {}

  bool operator<(bucket_payload_pair const& other) const {
    return (bucket_id < other.bucket_id) or
           (bucket_id == other.bucket_id and payload < other.payload);
  }
};
#pragma pack(pop)

struct bucket_t {
  bucket_t() : m_begin(nullptr), m_size(0) {}

  void init(uint64_t const* begin, bucket_size_type size) {
    m_begin = begin;
    m_size = size;
  }

  inline bucket_id_type id() const { return *m_begin; }

  inline uint64_t const* begin() const { return m_begin + 1; }

  inline uint64_t const* end() const { return m_begin + 1 + m_size; }

  inline bucket_size_type size() const { return m_size; }

 private:
  uint64_t const* m_begin;
  bucket_size_type m_size;
};

template <typename PairsRandomAccessIterator>
struct payload_iterator {
  payload_iterator(PairsRandomAccessIterator const& iterator)
      : m_iterator(iterator) {}

  uint64_t operator*() const { return (*m_iterator).payload; }

  void operator++() { ++m_iterator; }

 private:
  PairsRandomAccessIterator m_iterator;
};

template <typename Pairs, typename Merger>
void merge_single_block(Pairs const& pairs, Merger& merger, bool verbose) {
  progress_logger logger(pairs.size(), " == merged ", " pairs", verbose);

  bucket_size_type bucket_size = 1;
  uint64_t num_pairs = pairs.size();
  logger.log();
  for (uint64_t i = 1; i != num_pairs; ++i) {
    if (pairs[i].bucket_id == pairs[i - 1].bucket_id) {
      if (PTHASH_LIKELY(pairs[i].payload != pairs[i - 1].payload)) {
        ++bucket_size;
      } else {
        throw seed_runtime_error();
      }
    } else {
      merger.add(pairs[i - 1].bucket_id, bucket_size,
                 payload_iterator(pairs.begin() + i - bucket_size));
      bucket_size = 1;
    }
    logger.log();
  }

  // add the last bucket
  merger.add(pairs[num_pairs - 1].bucket_id, bucket_size,
             payload_iterator(pairs.end() - bucket_size));
  logger.finalize();
}

template <typename Pairs, typename Merger>
void merge_multiple_blocks(std::vector<Pairs> const& pairs_blocks,
                           Merger& merger, bool verbose) {
  uint64_t num_pairs = std::accumulate(
      pairs_blocks.begin(), pairs_blocks.end(), static_cast<uint64_t>(0),
      [](uint64_t sum, Pairs const& pairs) { return sum + pairs.size(); });
  progress_logger logger(num_pairs, " == merged ", " pairs", verbose);

  // input iterators and heap
  std::vector<typename Pairs::const_iterator> iterators;
  std::vector<uint32_t> idx_heap;
  iterators.reserve(pairs_blocks.size());
  idx_heap.reserve(pairs_blocks.size());

  // heap functions
  auto stdheap_idx_comparator = [&](uint32_t idxa, uint32_t idxb) {
    return !((*iterators[idxa]) < (*iterators[idxb]));
  };
  auto advance_heap_head = [&]() {
    auto idx = idx_heap[0];
    ++iterators[idx];
    if (PTHASH_LIKELY(iterators[idx] != pairs_blocks[idx].end())) {
      // percolate down the head
      uint64_t pos = 0;
      uint64_t size = idx_heap.size();
      while (2 * pos + 1 < size) {
        uint64_t i = 2 * pos + 1;
        if (i + 1 < size and
            stdheap_idx_comparator(idx_heap[i], idx_heap[i + 1]))
          ++i;
        if (stdheap_idx_comparator(idx_heap[i], idx_heap[pos]))
          break;
        std::swap(idx_heap[pos], idx_heap[i]);
        pos = i;
      }
    } else {
      std::pop_heap(idx_heap.begin(), idx_heap.end(), stdheap_idx_comparator);
      idx_heap.pop_back();
    }
  };

  // create the input iterators and the heap
  for (uint64_t i = 0; i != pairs_blocks.size(); ++i) {
    iterators.push_back(pairs_blocks[i].begin());
    idx_heap.push_back(i);
  }
  std::make_heap(idx_heap.begin(), idx_heap.end(), stdheap_idx_comparator);

  bucket_id_type bucket_id;
  std::vector<uint64_t> bucket_payloads;
  bucket_payloads.reserve(MAX_BUCKET_SIZE);

  // read the first pair
  {
    bucket_payload_pair pair = (*iterators[idx_heap[0]]);
    bucket_id = pair.bucket_id;
    bucket_payloads.push_back(pair.payload);
    advance_heap_head();
    logger.log();
  }

  // merge
  for (uint64_t i = 0; (PTHASH_LIKELY(idx_heap.size()));
       ++i, advance_heap_head()) {
    bucket_payload_pair pair = (*iterators[idx_heap[0]]);

    if (pair.bucket_id == bucket_id) {
      if (PTHASH_LIKELY(pair.payload != bucket_payloads.back())) {
        bucket_payloads.push_back(pair.payload);
      } else {
        throw seed_runtime_error();
      }
    } else {
      merger.add(bucket_id, bucket_payloads.size(), bucket_payloads.begin());
      bucket_id = pair.bucket_id;
      bucket_payloads.clear();
      bucket_payloads.push_back(pair.payload);
    }
    logger.log();
  }

  // add the last bucket
  merger.add(bucket_id, bucket_payloads.size(), bucket_payloads.begin());
  logger.finalize();
}

template <typename Pairs, typename Merger>
void merge(std::vector<Pairs> const& pairs_blocks, Merger& merger,
           bool verbose) {
  if (pairs_blocks.size() == 1) {
    merge_single_block(pairs_blocks[0], merger, verbose);
  } else {
    merge_multiple_blocks(pairs_blocks, merger, verbose);
  }
}

template <typename FreeSlots>
void fill_free_slots(bit_vector_builder const& taken, uint64_t num_keys,
                     FreeSlots& free_slots) {
  uint64_t table_size = taken.size();
  if (table_size <= num_keys)
    return;

  uint64_t next_used_slot = num_keys;
  uint64_t last_free_slot = 0, last_valid_free_slot = 0;

  while (true) {
    // find the next free slot (on the left)
    while (last_free_slot < num_keys && taken.get(last_free_slot))
      ++last_free_slot;
    // exit condition
    if (last_free_slot == num_keys)
      break;
    // fill with the last free slot (on the left) until I find a new used slot
    // (on the right) note: since I found a free slot on the left, there must be
    // an used slot on the right
    assert(next_used_slot < table_size);
    while (!taken.get(next_used_slot)) {
      free_slots.emplace_back(last_free_slot);
      ++next_used_slot;
    }
    assert(next_used_slot < table_size);
    // fill the used slot (on the right) with the last free slot and advance all
    // cursors
    free_slots.emplace_back(last_free_slot);
    last_valid_free_slot = last_free_slot;
    ++next_used_slot;
    ++last_free_slot;
  }
  // fill the tail with the last valid slot that I found
  while (next_used_slot != table_size) {
    free_slots.emplace_back(last_valid_free_slot);
    ++next_used_slot;
  }
  assert(next_used_slot == table_size);
}

}  // namespace pthash

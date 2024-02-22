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

#include <math.h>   // for pow, round, log2
#include <sstream>  // for stringbuf
#include <vector>
#include "essentials/essentials.hpp"

#include "builders/util.hpp"
#include "encoders/bit_vector.hpp"
#include "utils/hasher.hpp"

namespace pthash {

constexpr uint64_t search_cache_size = 1000;

struct search_logger {
  search_logger(uint64_t num_keys, uint64_t table_size, uint64_t num_buckets)
      : m_num_keys(num_keys),
        m_table_size(table_size),
        m_num_buckets(num_buckets),
        m_step(m_num_buckets > 20 ? m_num_buckets / 20 : 1),
        m_bucket(0),
        m_placed_keys(0),
        m_trials(0),
        m_total_trials(0),
        m_expected_trials(0.0),
        m_total_expected_trials(0.0) {}

  void init() {
    essentials::logger("search starts");
    m_timer.start();
  }

  /* If X_i is the random variable counting the number of trials
   for bucket i, then Pr(X_i <= N - 1) = 1 - (1 - p_i)^N,
   where p_i is the success probability for bucket i.
   By solving 1 - (1 - p_i)^N >= T wrt N and for a given target
   probability T < 1, we obtain N <= log_{1-p_i}(1-T), that is:
   we get a pilot <= N with probability T.
   Of course, the closer T is to 1, the higher N becomes.
   In practice T = 0.65 suffices to have
      N > # trials per bucket, for all buckets.
   */
  double pilot_wp_T(double T, double p) {
    assert(T > 0 and p > 0);
    double x = std::log2(1.0 - T) / std::log2(1.0 - p);
    return round(x);
  }

  void update(uint64_t bucket, uint64_t bucket_size, uint64_t pilot) {
    if (bucket > 0) {
      double base =
          static_cast<double>(m_table_size - m_placed_keys) / m_table_size;
      double p = pow(base, bucket_size);
      double e = 1.0 / p;
      m_expected_trials += e;
      m_total_expected_trials += e;
    }

    m_placed_keys += bucket_size;
    m_trials += pilot + 1;
    m_total_trials += pilot + 1;

    if (bucket > 0 and bucket % m_step == 0)
      print(bucket);
  }

  void finalize(uint64_t bucket) {
    m_step = bucket - m_bucket;
    print(bucket);
    essentials::logger("search ends");
    std::cout << " == " << m_num_buckets - bucket << " empty buckets ("
              << ((m_num_buckets - bucket) * 100.0) / m_num_buckets << "%)"
              << std::endl;
    std::cout << " == total trials = " << m_total_trials << std::endl;
    std::cout << " == total expected trials = "
              << uint64_t(m_total_expected_trials) << std::endl;
  }

 private:
  uint64_t m_num_keys;
  uint64_t m_table_size;
  uint64_t m_num_buckets;
  uint64_t m_step;
  uint64_t m_bucket;
  uint64_t m_placed_keys;

  uint64_t m_trials;
  uint64_t m_total_trials;
  double m_expected_trials;
  double m_total_expected_trials;

  essentials::timer<std::chrono::high_resolution_clock, std::chrono::seconds>
      m_timer;

  void print(uint64_t bucket) {
    m_timer.stop();
    std::stringbuf buffer;
    std::ostream os(&buffer);
    os << m_step << " buckets done in " << m_timer.elapsed() << " seconds ("
       << (m_placed_keys * 100.0) / m_num_keys << "% of keys, "
       << (bucket * 100.0) / m_num_buckets << "% of buckets, "
       << static_cast<double>(m_trials) / m_step << " trials per bucket, "
       << m_expected_trials / m_step << " expected trials per bucket)";
    essentials::logger(buffer.str());
    m_bucket = bucket;
    m_trials = 0;
    m_expected_trials = 0.0;
    m_timer.reset();
    m_timer.start();
  }
};

template <typename BucketsIterator, typename PilotsBuffer>
void search_sequential(uint64_t num_keys, uint64_t num_buckets,
                       uint64_t num_non_empty_buckets, uint64_t seed,
                       build_configuration const& config,
                       BucketsIterator& buckets, bit_vector_builder& taken,
                       PilotsBuffer& pilots) {
  uint64_t max_bucket_size = (*buckets).size();
  uint64_t table_size = taken.size();
  std::vector<uint64_t> positions;
  positions.reserve(max_bucket_size);
  __uint128_t M = fastmod::computeM_u64(table_size);

  std::vector<uint64_t> hashed_pilots_cache(search_cache_size);
  for (uint64_t pilot = 0; pilot != search_cache_size; ++pilot) {
    hashed_pilots_cache[pilot] = default_hash64(pilot, seed);
  }

  search_logger log(num_keys, table_size, num_buckets);
  if (config.verbose_output)
    log.init();

  uint64_t processed_buckets = 0;
  for (; processed_buckets < num_non_empty_buckets;
       ++processed_buckets, ++buckets) {
    auto const& bucket = *buckets;
    assert(bucket.size() > 0);

    for (uint64_t pilot = 0; true; ++pilot) {
      uint64_t hashed_pilot = PTHASH_LIKELY(pilot < search_cache_size)
                                  ? hashed_pilots_cache[pilot]
                                  : default_hash64(pilot, seed);

      positions.clear();

      auto bucket_begin = bucket.begin(), bucket_end = bucket.end();
      for (; bucket_begin != bucket_end; ++bucket_begin) {
        uint64_t hash = *bucket_begin;
        uint64_t p = fastmod::fastmod_u64(hash ^ hashed_pilot, M, table_size);
        if (taken.get(p))
          break;
        positions.push_back(p);
      }

      if (bucket_begin ==
          bucket_end) {  // all keys do not have collisions with taken

        // check for in-bucket collisions
        std::sort(positions.begin(), positions.end());
        auto it = std::adjacent_find(positions.begin(), positions.end());
        if (it != positions.end())
          continue;  // in-bucket collision detected, try next pilot

        pilots.emplace_back(bucket.id(), pilot);
        for (auto p : positions) {
          assert(taken.get(p) == false);
          taken.set(p, true);
        }
        if (config.verbose_output)
          log.update(processed_buckets, bucket.size(), pilot);
        break;
      }
    }
  }

  if (config.verbose_output)
    log.finalize(processed_buckets);
}

template <typename BucketsIterator, typename PilotsBuffer>
void search_parallel(uint64_t num_keys, uint64_t num_buckets,
                     uint64_t num_non_empty_buckets, uint64_t seed,
                     build_configuration const& config,
                     BucketsIterator& buckets, bit_vector_builder& taken,
                     PilotsBuffer& pilots) {
  uint64_t max_bucket_size = (*buckets).size();
  uint64_t table_size = taken.size();
  __uint128_t M = fastmod::computeM_u64(table_size);

  const uint64_t num_threads = config.num_threads;
  std::vector<uint64_t> hashed_pilots_cache(search_cache_size);
  for (uint64_t pilot = 0; pilot != search_cache_size; ++pilot) {
    hashed_pilots_cache[pilot] = default_hash64(pilot, seed);
  }

  search_logger log(num_keys, table_size, num_buckets);
  if (config.verbose_output)
    log.init();

  volatile uint64_t next_bucket_idx = 0;

  auto exe = [&](uint64_t local_bucket_idx, bucket_t bucket) {
    std::vector<uint64_t> positions;
    positions.reserve(max_bucket_size);

    while (true) {
      uint64_t pilot = 0;
      bool pilot_checked = false;

      while (true) {
        uint64_t local_next_bucket_idx = next_bucket_idx;

        for (; true; ++pilot) {
          if (PTHASH_LIKELY(!pilot_checked)) {
            uint64_t hashed_pilot = PTHASH_LIKELY(pilot < search_cache_size)
                                        ? hashed_pilots_cache[pilot]
                                        : default_hash64(pilot, seed);

            positions.clear();

            auto bucket_begin = bucket.begin(), bucket_end = bucket.end();
            for (; bucket_begin != bucket_end; ++bucket_begin) {
              uint64_t hash = *bucket_begin;
              uint64_t p =
                  fastmod::fastmod_u64(hash ^ hashed_pilot, M, table_size);
              if (taken.get(p))
                break;
              positions.push_back(p);
            }

            if (bucket_begin == bucket_end) {
              std::sort(positions.begin(), positions.end());
              auto it = std::adjacent_find(positions.begin(), positions.end());
              if (it != positions.end())
                continue;

              // I can stop the pilot search as there are not collisions
              pilot_checked = true;
              break;
            }
          } else {
            // I already computed the positions and checked the in-bucket
            // collisions I must only check the bitmap again
            for (auto p : positions) {
              if (taken.get(p)) {
                pilot_checked = false;
                break;
              }
            }
            // I can stop the pilot search as there are not collisions
            if (pilot_checked)
              break;
          }
        }

        // I am the first thread: this is the only condition that can stop the
        // loop
        if (local_next_bucket_idx == local_bucket_idx)
          break;

        // active wait until another thread pushes a change in the bitmap
        while (local_next_bucket_idx == next_bucket_idx)
          ;
      }
      assert(local_bucket_idx == next_bucket_idx);

      /* thread-safe from now on */

      pilots.emplace_back(bucket.id(), pilot);
      for (auto p : positions) {
        assert(taken.get(p) == false);
        taken.set(p, true);
      }
      if (config.verbose_output)
        log.update(local_bucket_idx, bucket.size(), pilot);

      // update (local) local_bucket_idx
      local_bucket_idx = next_bucket_idx + num_threads;

      if (local_bucket_idx >= num_non_empty_buckets) {  // stop the thread
        // update (global) next_bucket_idx, which may unlock other threads
        ++next_bucket_idx;
        break;
      }

      // read the next bucket and advance the iterator
      bucket = (*buckets);
      ++buckets;

      // update (global) next_bucket_idx, which may unlock other threads
      ++next_bucket_idx;
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(num_threads);
  next_bucket_idx = static_cast<uint64_t>(
      -1);  // avoid that some thread advances the iterator
  for (uint64_t i = 0; i != num_threads and i < num_non_empty_buckets;
       ++i, ++buckets) {
    bucket_t bucket = *buckets;
    threads.emplace_back(exe, i, bucket);
  }

  next_bucket_idx = 0;  // notify the first thread
  for (auto& t : threads) {
    if (t.joinable())
      t.join();
  }
  assert(next_bucket_idx == num_non_empty_buckets);

  if (config.verbose_output)
    log.finalize(next_bucket_idx);
}

template <typename BucketsIterator, typename PilotsBuffer>
void search(uint64_t num_keys, uint64_t num_buckets,
            uint64_t num_non_empty_buckets, uint64_t seed,
            build_configuration const& config, BucketsIterator& buckets,
            bit_vector_builder& taken, PilotsBuffer& pilots) {
  if (config.num_threads > 1) {
    if (config.num_threads > std::thread::hardware_concurrency()) {
      throw std::invalid_argument(
          "parallel search should use at most " +
          std::to_string(std::thread::hardware_concurrency()) + " threads");
    }
    search_parallel(num_keys, num_buckets, num_non_empty_buckets, seed, config,
                    buckets, taken, pilots);
  } else {
    search_sequential(num_keys, num_buckets, num_non_empty_buckets, seed,
                      config, buckets, taken, pilots);
  }
}

}  // namespace pthash

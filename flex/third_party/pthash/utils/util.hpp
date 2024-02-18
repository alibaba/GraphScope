#pragma once

#include <chrono>
#include <string>

#include "pthash/essentials/essentials.hpp"
#include "pthash/fastmod/fastmod.h"

#define PTHASH_LIKELY(expr) __builtin_expect((bool) (expr), true)

namespace pthash {

typedef std::chrono::high_resolution_clock clock_type;

namespace constants {
static const uint64_t available_ram =
    sysconf(_SC_PAGESIZE) * sysconf(_SC_PHYS_PAGES);
static const uint64_t invalid_seed = uint64_t(-1);
static const uint64_t invalid_num_buckets = uint64_t(-1);
static const std::string default_tmp_dirname(".");
}  // namespace constants

inline uint64_t random_value() {
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::mt19937_64 rng(seed);
  return rng();
}

template <typename DurationType>
double seconds(DurationType const& d) {
  return static_cast<double>(
             std::chrono::duration_cast<std::chrono::milliseconds>(d).count()) /
         1000;  // better resolution than std::chrono::seconds
}

}  // namespace pthash

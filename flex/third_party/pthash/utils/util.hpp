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

#include <chrono>
#include <string>

#include "essentials/essentials.hpp"
#include "fastmod/fastmod.h"

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

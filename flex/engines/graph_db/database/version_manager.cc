/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flex/engines/graph_db/database/version_manager.h"

#include "flex/engines/graph_db/app/app_base.h"

#define likely(x) __builtin_expect(!!(x), 1)

namespace gs {

constexpr static uint32_t ring_buf_size = 1024 * 1024;
constexpr static uint32_t ring_index_mask = ring_buf_size - 1;

VersionManager::VersionManager() { buf_.init(ring_buf_size); }

VersionManager::~VersionManager() {}

void VersionManager::init_ts(uint32_t ts) {
  write_ts_.store(ts + 1);
  read_ts_.store(ts);
}

uint32_t VersionManager::acquire_read_timestamp() {
  auto pr = pending_reqs_.fetch_add(1);
  if (likely(pr >= 0)) {
    return read_ts_.load();
  } else {
    while (true) {
      while (pending_reqs_.load() < 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
      pr = pending_reqs_.fetch_add(1);
      if (pr >= 0) {
        return read_ts_.load();
      }
    }
  }
}

void VersionManager::release_read_timestamp() { pending_reqs_.fetch_sub(1); }

uint32_t VersionManager::acquire_insert_timestamp() {
  auto pr = pending_reqs_.fetch_add(1);
  if (likely(pr >= 0)) {
    return write_ts_.fetch_add(1);
  } else {
    while (true) {
      while (pending_reqs_.load() < 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
      pr = pending_reqs_.fetch_add(1);
      if (pr >= 0) {
        return write_ts_.fetch_add(1);
      }
    }
  }
}
void VersionManager::release_insert_timestamp(uint32_t ts) {
  lock_.lock();
  if (ts == read_ts_.load() + 1) {
    while (buf_.reset_bit_with_ret((ts + 1) & ring_index_mask)) {
      ++ts;
    }
    read_ts_.store(ts);
  } else {
    buf_.set_bit(ts & ring_index_mask);
  }
  lock_.unlock();

  pending_reqs_.fetch_sub(1);
}

uint32_t VersionManager::acquire_update_timestamp() {
  int expected = 0;
  while (!pending_reqs_.compare_exchange_strong(
      expected, std::numeric_limits<int>::min())) {
    expected = 0;
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  return write_ts_.fetch_add(1);
}
void VersionManager::release_update_timestamp(uint32_t ts) {
  buf_.set_bit(ts & ring_index_mask);
  pending_reqs_.store(0);
}

}  // namespace gs

#undef likely

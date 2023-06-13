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

#ifndef GRAPHSCOPE_DATABASE_VERSION_MANAGER_H_
#define GRAPHSCOPE_DATABASE_VERSION_MANAGER_H_

#include <assert.h>
#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <bitset>
#include <thread>

#include "glog/logging.h"
#include "grape/utils/bitset.h"
#include "grape/utils/concurrent_queue.h"

namespace gs {

class VersionManager {
 public:
  VersionManager();
  ~VersionManager();

  void init_ts(uint32_t ts);

  uint32_t acquire_read_timestamp();

  void release_read_timestamp();

  uint32_t acquire_insert_timestamp();
  void release_insert_timestamp(uint32_t ts);

  uint32_t acquire_update_timestamp();
  void release_update_timestamp(uint32_t ts);

 private:
  std::atomic<uint32_t> write_ts_{1};
  std::atomic<uint32_t> read_ts_{0};

  std::atomic<int> pending_reqs_{0};

  grape::Bitset buf_;
  grape::SpinLock lock_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_VERSION_MANAGER_H_

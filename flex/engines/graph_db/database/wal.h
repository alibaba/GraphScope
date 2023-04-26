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

#ifndef GRAPHSCOPE_DATABASE_WAL_H_
#define GRAPHSCOPE_DATABASE_WAL_H_

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <queue>
#include <string>
#include <thread>

#include "flex/utils/mmap_array.h"
#include "glog/logging.h"

namespace gs {

struct WalHeader {
  uint32_t timestamp;
  uint8_t type : 1;
  int32_t length : 31;
};

struct WalContentUnit {
  char* ptr{NULL};
  size_t size{0};
};

struct UpdateWalUnit {
  uint32_t timestamp{0};
  char* ptr{NULL};
  size_t size{0};
};

class WalWriter {
  static constexpr size_t TRUNC_SIZE = 1ul << 30;

 public:
  WalWriter() : fd_(-1), file_size_(0), file_used_(0) {}
  ~WalWriter() { close(); }

  void open(const std::string& prefix, int thread_id);

  void close();

  void append(const char* data, size_t length);

 private:
  int fd_;
  size_t file_size_;
  size_t file_used_;
};

class WalsParser {
 public:
  WalsParser(const std::vector<std::string>& paths);
  ~WalsParser();

  uint32_t last_ts() const;
  const WalContentUnit& get_insert_wal(uint32_t ts) const;
  const std::vector<UpdateWalUnit>& update_wals() const;

 private:
  std::vector<int> fds_;
  std::vector<void*> mmapped_ptrs_;
  std::vector<size_t> mmapped_size_;

  mmap_array<WalContentUnit> insert_wal_list_;
  uint32_t last_ts_{0};

  std::vector<UpdateWalUnit> update_wal_list_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_WAL_H_

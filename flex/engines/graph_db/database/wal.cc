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

#include "flex/engines/graph_db/database/wal.h"

#include <chrono>
#include <filesystem>

namespace gs {

void WalWriter::open(const std::string& prefix, int thread_id) {
  const int max_version = 65536;
  for (int version = 0; version != max_version; ++version) {
    std::string path = prefix + "/thread_" + std::to_string(thread_id) + "_" +
                       std::to_string(version) + ".wal";
    if (std::filesystem::exists(path)) {
      continue;
    }
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    break;
  }
  if (fd_ == -1) {
    LOG(FATAL) << "Failed to open wal file";
  }
  if (ftruncate(fd_, TRUNC_SIZE) != 0) {
    LOG(FATAL) << "Failed to truncate wal file";
  }
  file_size_ = TRUNC_SIZE;
  file_used_ = 0;
}

void WalWriter::close() {
  if (fd_ != -1) {
    ::close(fd_);
    fd_ = -1;
    file_size_ = 0;
    file_used_ = 0;
  }
}

#define unlikely(x) __builtin_expect(!!(x), 0)

void WalWriter::append(const char* data, size_t length) {
  if (unlikely(fd_ == -1)) {
    return;
  }
  size_t expected_size = file_used_ + length;
  if (expected_size > file_size_) {
    size_t new_file_size = (expected_size / TRUNC_SIZE + 1) * TRUNC_SIZE;
    if (ftruncate(fd_, new_file_size) != 0) {
      LOG(FATAL) << "Failed to truncate wal file";
    }
    file_size_ = new_file_size;
  }

  file_used_ += length;

  if (static_cast<size_t>(write(fd_, data, length)) != length) {
    LOG(FATAL) << "Failed to write wal file";
  }

#if 1
#ifdef F_FULLFSYNC
  if (fcntl(fd_, F_FULLFSYNC) != 0) {
    LOG(FATAL) << "Failed to fcntl sync wal file";
  }
#else
  // if (fsync(fd_) != 0) {
  if (fdatasync(fd_) != 0) {
    LOG(FATAL) << "Failed to fsync wal file";
  }
#endif
#endif
}

#undef unlikely

static constexpr size_t MAX_WALS_NUM = 134217728;

WalsParser::WalsParser(const std::vector<std::string>& paths) {
  for (auto path : paths) {
    size_t file_size = std::filesystem::file_size(path);
    if (file_size == 0) {
      continue;
    }
    int fd = open(path.c_str(), O_RDONLY);
    void* mmapped_buffer = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mmapped_buffer == MAP_FAILED) {
      LOG(FATAL) << "mmap failed...";
    }

    fds_.push_back(fd);
    mmapped_ptrs_.push_back(mmapped_buffer);
    mmapped_size_.push_back(file_size);
  }

  insert_wal_list_.resize(MAX_WALS_NUM);
  for (size_t i = 0; i < mmapped_ptrs_.size(); ++i) {
    char* ptr = static_cast<char*>(mmapped_ptrs_[i]);
    while (true) {
      const WalHeader* header = reinterpret_cast<const WalHeader*>(ptr);
      ptr += sizeof(WalHeader);
      uint32_t ts = header->timestamp;
      if (ts == 0) {
        break;
      }
      int length = header->length;
      if (header->type) {
        UpdateWalUnit unit;
        unit.timestamp = ts;
        unit.ptr = ptr;
        unit.size = length;
        update_wal_list_.push_back(unit);
      } else {
        insert_wal_list_[ts].ptr = ptr;
        insert_wal_list_[ts].size = length;
      }
      ptr += length;
      last_ts_ = std::max(ts, last_ts_);
    }
  }

  if (!update_wal_list_.empty()) {
    std::sort(update_wal_list_.begin(), update_wal_list_.end(),
              [](const UpdateWalUnit& lhs, const UpdateWalUnit& rhs) {
                return lhs.timestamp < rhs.timestamp;
              });
  }
}

WalsParser::~WalsParser() {
  size_t ptr_num = mmapped_ptrs_.size();
  for (size_t i = 0; i < ptr_num; ++i) {
    munmap(mmapped_ptrs_[i], mmapped_size_[i]);
  }
  for (auto fd : fds_) {
    close(fd);
  }
}

uint32_t WalsParser::last_ts() const { return last_ts_; }

const WalContentUnit& WalsParser::get_insert_wal(uint32_t ts) const {
  return insert_wal_list_[ts];
}

const std::vector<UpdateWalUnit>& WalsParser::update_wals() const {
  return update_wal_list_;
}

}  // namespace gs

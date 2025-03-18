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

#include "flex/engines/graph_db/database/wal/local_wal_parser.h"
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <filesystem>
#include "flex/engines/graph_db/database/wal/wal.h"

namespace gs {

LocalWalParser::LocalWalParser(const std::string& wal_uri) {
  LocalWalParser::open(wal_uri);
}

void LocalWalParser::open(const std::string& wal_uri) {
  auto wal_dir = get_uri_path(wal_uri);
  if (!std::filesystem::exists(wal_dir)) {
    std::filesystem::create_directory(wal_dir);
  }

  std::vector<std::string> paths;
  for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
    paths.push_back(entry.path().string());
  }
  for (auto path : paths) {
    LOG(INFO) << "Start to ingest WALs from file: " << path;
    size_t file_size = std::filesystem::file_size(path);
    if (file_size == 0) {
      continue;
    }
    int fd = ::open(path.c_str(), O_RDONLY);
    void* mmapped_buffer = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mmapped_buffer == MAP_FAILED) {
      LOG(FATAL) << "mmap failed...";
    }

    fds_.push_back(fd);
    mmapped_ptrs_.push_back(mmapped_buffer);
    mmapped_size_.push_back(file_size);
  }

  insert_wal_list_.resize(4096);
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
        if (ts >= insert_wal_list_.size()) {
          insert_wal_list_.resize(ts + 1);
        }
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

void LocalWalParser::close() {
  insert_wal_list_.clear();
  size_t ptr_num = mmapped_ptrs_.size();
  for (size_t i = 0; i < ptr_num; ++i) {
    munmap(mmapped_ptrs_[i], mmapped_size_[i]);
  }
  for (auto fd : fds_) {
    ::close(fd);
  }
}

uint32_t LocalWalParser::last_ts() const { return last_ts_; }

const WalContentUnit& LocalWalParser::get_insert_wal(uint32_t ts) const {
  return insert_wal_list_[ts];
}

const std::vector<UpdateWalUnit>& LocalWalParser::get_update_wals() const {
  return update_wal_list_;
}

const bool LocalWalParser::registered_ = WalParserFactory::RegisterWalParser(
    "file", static_cast<WalParserFactory::wal_parser_initializer_t>(
                &LocalWalParser::Make));

}  // namespace gs
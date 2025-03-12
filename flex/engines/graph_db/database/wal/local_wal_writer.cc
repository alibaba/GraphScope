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

#include "flex/engines/graph_db/database/wal/local_wal_writer.h"
#include "flex/engines/graph_db/database/wal/wal.h"

#include <chrono>
#include <filesystem>

namespace gs {

std::unique_ptr<IWalWriter> LocalWalWriter::Make() {
  return std::unique_ptr<IWalWriter>(new LocalWalWriter());
}

void LocalWalWriter::open(const std::string& wal_uri, int thread_id) {
  auto prefix = get_uri_path(wal_uri);
  if (!std::filesystem::exists(prefix)) {
    std::filesystem::create_directories(prefix);
  }
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
    LOG(FATAL) << "Failed to open wal file " << strerror(errno);
  }
  if (ftruncate(fd_, TRUNC_SIZE) != 0) {
    LOG(FATAL) << "Failed to truncate wal file " << strerror(errno);
  }
  file_size_ = TRUNC_SIZE;
  file_used_ = 0;
}

void LocalWalWriter::close() {
  if (fd_ != -1) {
    if (::close(fd_) != 0) {
      LOG(FATAL) << "Failed to close file" << strerror(errno);
    }
    fd_ = -1;
    file_size_ = 0;
    file_used_ = 0;
  }
}

#define unlikely(x) __builtin_expect(!!(x), 0)

bool LocalWalWriter::append(const char* data, size_t length) {
  if (unlikely(fd_ == -1)) {
    return false;
  }
  size_t expected_size = file_used_ + length;
  if (expected_size > file_size_) {
    size_t new_file_size = (expected_size / TRUNC_SIZE + 1) * TRUNC_SIZE;
    if (ftruncate(fd_, new_file_size) != 0) {
      LOG(FATAL) << "Failed to truncate wal file " << strerror(errno);
    }
    file_size_ = new_file_size;
  }

  file_used_ += length;

  if (static_cast<size_t>(write(fd_, data, length)) != length) {
    LOG(FATAL) << "Failed to write wal file " << strerror(errno);
  }

#if 1
#ifdef F_FULLFSYNC
  if (fcntl(fd_, F_FULLFSYNC) != 0) {
    LOG(FATAL) << "Failed to fcntl sync wal file " << strerrno(errno);
  }
#else
  // if (fsync(fd_) != 0) {
  if (fdatasync(fd_) != 0) {
    LOG(FATAL) << "Failed to fsync wal file " << strerror(errno);
  }
#endif
#endif
  return true;
}

#undef unlikely

const bool LocalWalWriter::registered_ = WalWriterFactory::RegisterWalWriter(
    "file", static_cast<WalWriterFactory::wal_writer_initializer_t>(
                &LocalWalWriter::Make));

}  // namespace gs
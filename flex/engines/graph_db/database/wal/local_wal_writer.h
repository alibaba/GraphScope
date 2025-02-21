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

#ifndef ENGINES_GRAPH_DB_DATABASE_WAL_LOCAL_WAL_WRITER_H_
#define ENGINES_GRAPH_DB_DATABASE_WAL_LOCAL_WAL_WRITER_H_

#include <memory>
#include <unordered_map>
#include "flex/engines/graph_db/database/wal/wal.h"

namespace gs {

class LocalWalWriter : public IWalWriter {
 public:
  static std::unique_ptr<IWalWriter> Make();

  static constexpr size_t TRUNC_SIZE = 1ul << 30;
  LocalWalWriter() = default;
  ~LocalWalWriter() { close(); }

  void open(const std::string& wal_uri, int thread_id) override;
  void close() override;
  bool append(const char* data, size_t length) override;
  std::string type() const override { return "file"; }

 private:
  int fd_;
  size_t file_size_;
  size_t file_used_;

  static const bool registered_;
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_DATABASE_WAL_LOCAL_WAL_WRITER_H_
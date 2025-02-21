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

#ifndef ENGINES_GRAPH_DB_DATABASE_WAL_LOCAL_WAL_PARSER_H_
#define ENGINES_GRAPH_DB_DATABASE_WAL_LOCAL_WAL_PARSER_H_

#include <vector>
#include "flex/engines/graph_db/database/wal/wal.h"

namespace gs {

class LocalWalParser : public IWalParser {
 public:
  static std::unique_ptr<IWalParser> Make(const std::string& wal_dir) {
    return std::unique_ptr<IWalParser>(new LocalWalParser());
  }

  LocalWalParser();
  ~LocalWalParser() { close(); }

  void open(const std::string& wal_uri) override;
  void close() override;

  uint32_t last_ts() const override;
  const WalContentUnit& get_insert_wal(uint32_t ts) const override;
  const std::vector<UpdateWalUnit>& get_update_wals() const override;

 private:
  std::vector<int> fds_;
  std::vector<void*> mmapped_ptrs_;
  std::vector<size_t> mmapped_size_;
  WalContentUnit* insert_wal_list_;
  size_t insert_wal_list_size_;
  uint32_t last_ts_{0};

  std::vector<UpdateWalUnit> update_wal_list_;

  static const bool registered_;
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_DATABASE_WAL_LOCAL_WAL_PARSER_H_
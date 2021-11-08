/**
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <fstream>
#include "common/schema.h"
#include "db/snapshot.h"

namespace LGRAPH_NAMESPACE {

class ReadonlyDB {
public:
  static ReadonlyDB Open(const char *store_path);

  // Move Only!
  // Avoid copy construction and assignment.
  ReadonlyDB(const ReadonlyDB &) = delete;
  ReadonlyDB &operator=(const ReadonlyDB &) = delete;
  ReadonlyDB(ReadonlyDB &&rd) noexcept;
  ReadonlyDB &operator=(ReadonlyDB &&rd) noexcept;

  ~ReadonlyDB();

  Snapshot GetSnapshot(SnapshotId snapshot_id);

  static Schema LoadSchema(const char *schema_proto_bytes_file);

private:
  PartitionGraphHandle handle_;

  ReadonlyDB();
  explicit ReadonlyDB(const char *store_path);
};

inline ReadonlyDB ReadonlyDB::Open(const char *store_path) {
  return ReadonlyDB{store_path};
}

inline ReadonlyDB::ReadonlyDB() : handle_(nullptr) {}

inline ReadonlyDB::ReadonlyDB(ReadonlyDB &&rd) noexcept: ReadonlyDB() {
  *this = std::move(rd);
}

inline ReadonlyDB &ReadonlyDB::operator=(ReadonlyDB &&rd) noexcept {
  if (this != &rd) {
    this->~ReadonlyDB();
    handle_ = rd.handle_;
    rd.handle_ = nullptr;
  }
  return *this;
}

Schema ReadonlyDB::LoadSchema(const char *schema_proto_bytes_file) {
  std::ifstream infile(schema_proto_bytes_file);
  std::vector<char> buffer;
  infile.seekg(0, infile.end);
  long length = infile.tellg();
  Check(length > 0, "Loading empty schema file!");
  buffer.resize(length);
  infile.seekg(0, infile.beg);
  infile.read(&buffer[0], length);
  infile.close();
  return Schema::FromProto(buffer.data(), buffer.size());
}

}

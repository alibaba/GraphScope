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

#include "db/readonly_db.h"
#include "store_ffi/store_ffi.h"

namespace LGRAPH_NAMESPACE {

ReadonlyDB::ReadonlyDB(const char *store_path)
    : handle_(ffi::OpenPartitionGraph(store_path)) {}

ReadonlyDB::~ReadonlyDB() {
  if (handle_ != nullptr) {
    ffi::ReleasePartitionGraphHandle(handle_);
  }
}

Snapshot ReadonlyDB::GetSnapshot(SnapshotId snapshot_id) {
  auto snapshot_handle = ffi::GetSnapshot(handle_, snapshot_id);
  return Snapshot{snapshot_handle};
}

Schema ReadonlyDB::GetGraphSchema() {
  auto response = ffi::GetGraphDef(handle_);
  Check(response.success, "Get graph schema failed!");
  GraphDefPb proto;
  Check(proto.ParseFromArray(response.data, response.len), "Parse graph schema failed!");
  ffi::DropFfiResponse(response);
  return Schema::FromProto(proto);
}

}

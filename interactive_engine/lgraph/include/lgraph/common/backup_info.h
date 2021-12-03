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

#include "lgraph/common/schema.h"
#include "lgraph/proto/model.pb.h"

namespace LGRAPH_NAMESPACE {

class BackupInfo {
public:
  BackupInfo(BackupId backup_id, SnapshotId snapshot_id, Schema &&graph_schema,
             std::unordered_map<int32_t, BackupId> &&partition_backup_id_map, std::vector<int64_t> &&wal_offsets)
      : backup_id_(backup_id), snapshot_id_(snapshot_id), graph_schema_(std::move(graph_schema))
      , partition_backup_id_map_(std::move(partition_backup_id_map)), wal_offsets_(std::move(wal_offsets)) {}
  ~BackupInfo() = default;

  static BackupInfo FromProto(const BackupInfoPb& proto);

  BackupInfo(const BackupInfo &) = default;
  BackupInfo(BackupInfo &&) = default;
  BackupInfo &operator=(const BackupInfo &) = default;
  BackupInfo &operator=(BackupInfo &&) = default;

  BackupId GetBackupId() const {
    return backup_id_;
  }

  SnapshotId GetSnapshotId() const {
    return snapshot_id_;
  }

  const Schema &GetSchema() const {
    return graph_schema_;
  }

  const std::unordered_map<int32_t, BackupId> &GetPartitionBackupIdMap() const {
    return partition_backup_id_map_;
  }

  const std::vector<int64_t> &GetWalOffsets() const {
    return wal_offsets_;
  }

private:
  BackupId backup_id_;
  SnapshotId snapshot_id_;
  Schema graph_schema_;
  std::unordered_map<int32_t, BackupId> partition_backup_id_map_;
  std::vector<int64_t> wal_offsets_;
};

inline BackupInfo BackupInfo::FromProto(const BackupInfoPb &proto) {
  auto schema = Schema::FromProto(proto.graphdef());
  std::unordered_map<int32_t, BackupId> partition_backup_id_map;
  partition_backup_id_map.reserve(proto.partitiontobackupid_size());
  for (auto &entry : proto.partitiontobackupid()) {
    partition_backup_id_map.emplace(entry.first, entry.second);
  }
  std::vector<int64_t> wal_offsets;
  wal_offsets.reserve(proto.waloffsets_size());
  for (auto& offset : proto.waloffsets()) {
    wal_offsets.push_back(offset);
  }
  return BackupInfo{proto.globalbackupid(), static_cast<SnapshotId>(proto.snapshotid()),
                    std::move(schema), std::move(partition_backup_id_map), std::move(wal_offsets)};
}

}

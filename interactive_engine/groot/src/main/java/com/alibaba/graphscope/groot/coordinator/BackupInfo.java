/**
 * Copyright 2020 Alibaba Group Holding Limited.
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
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.maxgraph.proto.groot.BackupInfoPb;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class BackupInfo {

    private int globalBackupId;
    private long snapshotId;
    private long ddlSnapshotId;
    private Map<Integer, Integer> partitionToBackupId;
    private List<Long> walOffsets;

    public BackupInfo(int globalBackupId, long snapshotId, long ddlSnapshotId,
                      Map<Integer, Integer> partitionToBackupId, List<Long> walOffsets) {
        this.globalBackupId = globalBackupId;
        this.snapshotId = snapshotId;
        this.ddlSnapshotId = ddlSnapshotId;
        this.partitionToBackupId = partitionToBackupId;
        this.walOffsets = walOffsets;
    }

    public int getGlobalBackupId() {
        return globalBackupId;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long getDdlSnapshotId() {
        return ddlSnapshotId;
    }

    public Map<Integer, Integer> getPartitionToBackupId() {
        return partitionToBackupId;
    }

    public List<Long> getWalOffsets() {
        return walOffsets;
    }

    public static BackupInfo parseProto(BackupInfoPb proto) {
        int globalBackupId = proto.getGlobalBackupId();
        long snapshotId = proto.getSnapshotId();
        long ddlSnapshotId = proto.getDdlSnapshotId();
        Map<Integer, Integer> partitionToBackupId = proto.getPartitionToBackupIdMap();
        List<Long> walOffsets = proto.getWalOffsetsList();
        return new BackupInfo(globalBackupId, snapshotId, ddlSnapshotId, partitionToBackupId, walOffsets);
    }

    public BackupInfoPb toProto() {
        return BackupInfoPb.newBuilder()
                .setGlobalBackupId(globalBackupId)
                .setSnapshotId(snapshotId)
                .setDdlSnapshotId(ddlSnapshotId)
                .putAllPartitionToBackupId(partitionToBackupId)
                .addAllWalOffsets(walOffsets)
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BackupInfo backupInfo = (BackupInfo) o;
        if (!Objects.equals(partitionToBackupId, backupInfo.partitionToBackupId)) {
            return false;
        }
        if (!Objects.equals(walOffsets, backupInfo.walOffsets)) {
            return false;
        }
        return (globalBackupId == backupInfo.globalBackupId) &&
                (snapshotId == backupInfo.snapshotId) &&
                (ddlSnapshotId == backupInfo.ddlSnapshotId);
    }
}

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
package com.alibaba.graphscope.groot.backup;

import com.alibaba.maxgraph.proto.v2.BackupInfoPb;

import java.util.List;

public class BackupInfo {

    private int globalBackupId;
    private long snapshotId;
    private long ddlSnapshotId;
    private List<Integer> partitionBackupIds;
    private List<Long> walOffsets;

    public BackupInfo(int globalBackupId, long snapshotId, long ddlSnapshotId,
                      List<Integer> partitionBackupIds, List<Long> walOffsets) {
        this.globalBackupId = globalBackupId;
        this.snapshotId = snapshotId;
        this.ddlSnapshotId = ddlSnapshotId;
        this.partitionBackupIds = partitionBackupIds;
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

    public List<Integer> getPartitionBackupIds() {
        return partitionBackupIds;
    }

    public List<Long> getWalOffsets() {
        return walOffsets;
    }

    public static BackupInfo parseProto(BackupInfoPb proto) {
        int globalBackupId = proto.getGlobalBackupId();
        long snapshotId = proto.getSnapshotId();
        long ddlSnapshotId = proto.getDdlSnapshotId();
        List<Integer> partitionBackupIds = proto.getPartitionBackupIdsList();
        List<Long> walOffsets = proto.getWalOffsetsList();
        return new BackupInfo(globalBackupId, snapshotId, ddlSnapshotId, partitionBackupIds, walOffsets);
    }

    public BackupInfoPb toProto() {
        return BackupInfoPb.newBuilder()
                .setGlobalBackupId(globalBackupId)
                .setSnapshotId(snapshotId)
                .setDdlSnapshotId(ddlSnapshotId)
                .addAllPartitionBackupIds(partitionBackupIds)
                .addAllWalOffsets(walOffsets)
                .build();
    }
}

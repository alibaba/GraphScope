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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class BackupInfo {

    private int globalBackupId;
    private long querySnapshotId;
    private long queryDdlSnapshotId;
    private long writeSnapshotId;
    private long allocatedTailId;
    private List<Long> walOffsets;
    private Map<Integer, Integer> partitionToBackupId;

    @JsonCreator
    public BackupInfo(
            @JsonProperty("globalBackupId") int globalBackupId,
            @JsonProperty("querySnapshotId") long querySnapshotId,
            @JsonProperty("queryDdlSnapshotId") long queryDdlSnapshotId,
            @JsonProperty("writeSnapshotId") long writeSnapshotId,
            @JsonProperty("allocatedTailId") long allocatedTailId,
            @JsonProperty("walOffsets") List<Long> walOffsets,
            @JsonProperty("partitionToBackupId") Map<Integer, Integer> partitionToBackupId) {
        this.globalBackupId = globalBackupId;
        this.querySnapshotId = querySnapshotId;
        this.queryDdlSnapshotId = queryDdlSnapshotId;
        this.writeSnapshotId = writeSnapshotId;
        this.allocatedTailId = allocatedTailId;
        this.walOffsets = walOffsets;
        this.partitionToBackupId = partitionToBackupId;
    }

    public int getGlobalBackupId() {
        return globalBackupId;
    }

    public long getQuerySnapshotId() {
        return querySnapshotId;
    }

    public long getQueryDdlSnapshotId() {
        return queryDdlSnapshotId;
    }

    public long getWriteSnapshotId() {
        return writeSnapshotId;
    }

    public long getAllocatedTailId() {
        return allocatedTailId;
    }

    public List<Long> getWalOffsets() {
        return walOffsets;
    }

    public Map<Integer, Integer> getPartitionToBackupId() {
        return partitionToBackupId;
    }

    public static BackupInfo parseProto(BackupInfoPb proto) {
        int globalBackupId = proto.getGlobalBackupId();
        long querySnapshotId = proto.getQuerySnapshotId();
        long queryDdlSnapshotId = proto.getQueryDdlSnapshotId();
        long writeSnapshotId = proto.getWriteSnapshotId();
        long allocatedTailId = proto.getAllocatedTailId();
        List<Long> walOffsets = proto.getWalOffsetsList();
        Map<Integer, Integer> partitionToBackupId = proto.getPartitionToBackupIdMap();
        return new BackupInfo(
                globalBackupId, querySnapshotId, queryDdlSnapshotId,
                writeSnapshotId, allocatedTailId, walOffsets, partitionToBackupId);
    }

    public BackupInfoPb toProto() {
        return BackupInfoPb.newBuilder()
                .setGlobalBackupId(globalBackupId)
                .setQuerySnapshotId(querySnapshotId)
                .setQueryDdlSnapshotId(queryDdlSnapshotId)
                .setWriteSnapshotId(writeSnapshotId)
                .setAllocatedTailId(allocatedTailId)
                .addAllWalOffsets(walOffsets)
                .putAllPartitionToBackupId(partitionToBackupId)
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
        if (!walOffsets.equals(backupInfo.walOffsets)) {
            return false;
        }
        if (!partitionToBackupId.equals(backupInfo.partitionToBackupId)) {
            return false;
        }
        return (globalBackupId == backupInfo.globalBackupId) &&
                (querySnapshotId == backupInfo.querySnapshotId) &&
                (queryDdlSnapshotId == backupInfo.queryDdlSnapshotId) &&
                (writeSnapshotId == backupInfo.writeSnapshotId) &&
                (allocatedTailId == backupInfo.allocatedTailId);
    }

    @Override
    public int hashCode() {
        return globalBackupId;
    }
}

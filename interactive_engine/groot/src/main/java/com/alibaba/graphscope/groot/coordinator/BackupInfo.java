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
import com.alibaba.maxgraph.proto.groot.GraphDefPb;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BackupInfo {

    private int globalBackupId;
    private long snapshotId;
    private byte[] graphDefBytes;
    private List<Long> walOffsets;
    private Map<Integer, Integer> partitionToBackupId;

    @JsonCreator
    public BackupInfo(
            @JsonProperty("globalBackupId") int globalBackupId,
            @JsonProperty("snapshotId") long snapshotId,
            @JsonProperty("graphDefBytes") byte[] graphDefBytes,
            @JsonProperty("walOffsets") List<Long> walOffsets,
            @JsonProperty("partitionToBackupId") Map<Integer, Integer> partitionToBackupId) {
        this.globalBackupId = globalBackupId;
        this.snapshotId = snapshotId;
        this.graphDefBytes = graphDefBytes;
        this.walOffsets = walOffsets;
        this.partitionToBackupId = partitionToBackupId;
    }

    public int getGlobalBackupId() {
        return globalBackupId;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public byte[] getGraphDefBytes() {
        return graphDefBytes;
    }

    public List<Long> getWalOffsets() {
        return walOffsets;
    }

    public Map<Integer, Integer> getPartitionToBackupId() {
        return partitionToBackupId;
    }

    public static BackupInfo parseProto(BackupInfoPb proto) {
        int globalBackupId = proto.getGlobalBackupId();
        long snapshotId = proto.getSnapshotId();
        List<Long> walOffsets = proto.getWalOffsetsList();
        Map<Integer, Integer> partitionToBackupId = proto.getPartitionToBackupIdMap();
        return new BackupInfo(
                globalBackupId, snapshotId, proto.getGraphDef().toByteArray(), walOffsets, partitionToBackupId);
    }

    public BackupInfoPb toProto() throws IOException {
        return BackupInfoPb.newBuilder()
                .setGlobalBackupId(globalBackupId)
                .setSnapshotId(snapshotId)
                .setGraphDef(GraphDefPb.parseFrom(graphDefBytes))
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
        if (!Arrays.equals(graphDefBytes, backupInfo.graphDefBytes)) {
            return false;
        }
        if (!walOffsets.equals(backupInfo.walOffsets)) {
            return false;
        }
        if (!partitionToBackupId.equals(backupInfo.partitionToBackupId)) {
            return false;
        }
        return (globalBackupId == backupInfo.globalBackupId) &&
                (snapshotId == backupInfo.snapshotId);
    }

    @Override
    public int hashCode() {
        return globalBackupId;
    }
}

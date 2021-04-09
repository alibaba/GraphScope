package com.alibaba.maxgraph.v2.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SnapshotInfo implements Comparable<SnapshotInfo> {

    private long snapshotId;

    private long ddlSnapshotId;

    @JsonCreator
    public SnapshotInfo(@JsonProperty("snapshotId") long snapshotId,
                        @JsonProperty("ddlSnapshotId") long ddlSnapshotId) {
        this.snapshotId = snapshotId;
        this.ddlSnapshotId = ddlSnapshotId;
    }
    public long getSnapshotId() {
        return snapshotId;
    }

    public long getDdlSnapshotId() {
        return ddlSnapshotId;
    }

    @Override
    public int compareTo(SnapshotInfo o) {
        return Long.compare(snapshotId, o.snapshotId);
    }

    @Override
    public String toString() {
        return "SnapshotInfo{" +
                "snapshotId=" + snapshotId +
                ", ddlSnapshotId=" + ddlSnapshotId +
                '}';
    }
}

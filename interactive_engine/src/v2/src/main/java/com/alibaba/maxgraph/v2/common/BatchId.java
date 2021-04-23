package com.alibaba.maxgraph.v2.common;

public class BatchId {

    private long snapshotId;

    public BatchId(long snapshotId) {
        this.snapshotId = snapshotId;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    @Override
    public String toString() {
        return "BatchId{" +
                "snapshotId=" + snapshotId +
                '}';
    }
}

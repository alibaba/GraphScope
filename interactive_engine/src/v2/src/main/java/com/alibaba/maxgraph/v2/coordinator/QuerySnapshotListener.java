package com.alibaba.maxgraph.v2.coordinator;

public interface QuerySnapshotListener {
    void snapshotAdvanced(long snapshotId, long ddlSnapshotId);
}

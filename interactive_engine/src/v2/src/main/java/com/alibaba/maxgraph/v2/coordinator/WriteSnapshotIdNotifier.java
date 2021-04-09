package com.alibaba.maxgraph.v2.coordinator;

public interface WriteSnapshotIdNotifier {
    void notifyWriteSnapshotIdChanged(long snapshotId);
}

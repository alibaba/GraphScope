package com.alibaba.maxgraph.v2.store;

import java.util.List;

public interface SnapshotCommitter {
    void commitSnapshotId(int storeId, long snapshotId, long ddlSnapshotId, List<Long> offsets);
}

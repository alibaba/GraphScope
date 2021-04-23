package com.alibaba.maxgraph.v2.common.wal;

import com.alibaba.maxgraph.proto.v2.LogEntryPb;
import com.alibaba.maxgraph.v2.common.OperationBatch;

public class LogEntry {
    private long snapshotId;
    private OperationBatch operationBatch;

    public LogEntry(long snapshotId, OperationBatch operationBatch) {
        this.snapshotId = snapshotId;
        this.operationBatch = operationBatch;
    }

    public static LogEntry parseProto(LogEntryPb proto) {
        long snapshotId = proto.getSnapshotId();
        OperationBatch operationBatch = OperationBatch.parseProto(proto.getOperations());
        return new LogEntry(snapshotId, operationBatch);
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public OperationBatch getOperationBatch() {
        return operationBatch;
    }

    public LogEntryPb toProto() {
        return LogEntryPb.newBuilder()
                .setSnapshotId(snapshotId)
                .setOperations(operationBatch.toProto())
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogEntry logEntry = (LogEntry) o;

        if (snapshotId != logEntry.snapshotId) return false;
        return operationBatch != null ? operationBatch.equals(logEntry.operationBatch) : logEntry.operationBatch == null;
    }

}

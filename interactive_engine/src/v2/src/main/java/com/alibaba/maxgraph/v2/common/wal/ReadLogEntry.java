package com.alibaba.maxgraph.v2.common.wal;

import com.alibaba.maxgraph.v2.common.OperationBatch;

public class ReadLogEntry {

    private long offset;
    private LogEntry logEntry;

    public ReadLogEntry(long offset, long snapshotId, OperationBatch operationBatch) {
        this.offset = offset;
        this.logEntry = new LogEntry(snapshotId, operationBatch);
    }

    public ReadLogEntry(long offset, LogEntry logEntry) {
        this.offset = offset;
        this.logEntry = logEntry;
    }

    public LogEntry getLogEntry() {
        return logEntry;
    }

    public long getOffset() {
        return offset;
    }
}

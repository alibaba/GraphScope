/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.wal;

import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.maxgraph.proto.groot.LogEntryPb;

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
        return operationBatch != null
                ? operationBatch.equals(logEntry.operationBatch)
                : logEntry.operationBatch == null;
    }
}

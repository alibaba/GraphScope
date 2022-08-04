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

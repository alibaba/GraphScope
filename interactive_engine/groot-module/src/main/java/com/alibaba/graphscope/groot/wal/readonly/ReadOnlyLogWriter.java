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
package com.alibaba.graphscope.groot.wal.readonly;

import com.alibaba.graphscope.groot.wal.LogEntry;
import com.alibaba.graphscope.groot.wal.LogWriter;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.concurrent.Future;

public class ReadOnlyLogWriter implements LogWriter {
    private long offset = 0;

    public ReadOnlyLogWriter() {}

    @Override
    public long append(LogEntry logEntry) throws IOException {
        return append(0, logEntry);
    }

    public long append(int partition, LogEntry logEntry) throws IOException {
        offset += 1;
        return offset;
    }

    public Future<RecordMetadata> appendAsync(int partition, LogEntry logEntry) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {}
}

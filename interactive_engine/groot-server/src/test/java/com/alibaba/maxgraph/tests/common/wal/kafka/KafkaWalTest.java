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
package com.alibaba.maxgraph.tests.common.wal.kafka;

import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.OperationBlob;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.KafkaConfig;
import com.alibaba.graphscope.groot.wal.LogEntry;
import com.alibaba.graphscope.groot.wal.LogReader;
import com.alibaba.graphscope.groot.wal.LogService;
import com.alibaba.graphscope.groot.wal.LogWriter;
import com.alibaba.graphscope.groot.wal.ReadLogEntry;
import com.alibaba.graphscope.groot.wal.kafka.KafkaLogService;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaWalTest {

    @RegisterExtension
    static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();

    @Test
    void testDoubleDestroy() {
        Configs configs =
                Configs.newBuilder()
                        .put(
                                KafkaConfig.KAFKA_SERVERS.getKey(),
                                sharedKafkaTestResource.getKafkaConnectString())
                        .put(KafkaConfig.KAKFA_TOPIC.getKey(), "test_double_destroy")
                        .put(CommonConfig.INGESTOR_QUEUE_COUNT.getKey(), "1")
                        .build();
        LogService logService = new KafkaLogService(configs);
        logService.init();
        logService.destroy();
        assertThrows(Exception.class, () -> logService.destroy());
    }

    @Test
    void testDoubleInit() {
        Configs configs =
                Configs.newBuilder()
                        .put(
                                KafkaConfig.KAFKA_SERVERS.getKey(),
                                sharedKafkaTestResource.getKafkaConnectString())
                        .put(KafkaConfig.KAKFA_TOPIC.getKey(), "test_double_init")
                        .put(CommonConfig.INGESTOR_QUEUE_COUNT.getKey(), "1")
                        .build();
        LogService logService = new KafkaLogService(configs);
        logService.init();
        assertThrows(Exception.class, () -> logService.init());
        logService.destroy();
    }

    @Test
    void testLogService() throws IOException {
        Configs configs =
                Configs.newBuilder()
                        .put(
                                KafkaConfig.KAFKA_SERVERS.getKey(),
                                sharedKafkaTestResource.getKafkaConnectString())
                        .put(KafkaConfig.KAKFA_TOPIC.getKey(), "test_logservice")
                        .put(CommonConfig.INGESTOR_QUEUE_COUNT.getKey(), "1")
                        .build();
        LogService logService = new KafkaLogService(configs);
        logService.init();
        int queueId = 0;
        long snapshotId = 1L;
        LogWriter writer = logService.createWriter(queueId);
        LogEntry logEntry =
                new LogEntry(
                        snapshotId,
                        OperationBatch.newBuilder()
                                .addOperationBlob(OperationBlob.MARKER_OPERATION_BLOB)
                                .build());
        assertEquals(writer.append(logEntry), 0);

        LogReader reader = logService.createReader(queueId, 0);
        ReadLogEntry readLogEntry = reader.readNext();
        reader.close();

        assertAll(
                () -> assertEquals(readLogEntry.getOffset(), 0),
                () -> assertEquals(readLogEntry.getLogEntry().getSnapshotId(), snapshotId));

        OperationBatch operationBatch = readLogEntry.getLogEntry().getOperationBatch();
        assertEquals(operationBatch.getOperationCount(), 1);
        assertEquals(operationBatch.getOperationBlob(0), OperationBlob.MARKER_OPERATION_BLOB);

        assertEquals(writer.append(logEntry), 1);
        assertEquals(writer.append(logEntry), 2);
        assertEquals(writer.append(logEntry), 3);

        LogReader readerTail = logService.createReader(queueId, 4);
        assertNull(readerTail.readNext());
        readerTail.close();

        assertThrows(IllegalArgumentException.class, () -> logService.createReader(queueId, 5));
        logService.deleteBeforeOffset(queueId, 2);
        assertThrows(IllegalArgumentException.class, () -> logService.createReader(queueId, 1));
        writer.close();
        logService.destroy();
    }
}

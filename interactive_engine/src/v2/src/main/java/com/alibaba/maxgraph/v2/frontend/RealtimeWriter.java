/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.v2.frontend;

import com.alibaba.maxgraph.v2.common.BatchId;
import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.OperationBatch;
import com.alibaba.maxgraph.v2.common.metrics.MetricsAgent;
import com.alibaba.maxgraph.v2.common.metrics.MetricsCollector;
import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is for compiler to perform realtime write operations.
 */
@Deprecated
public class RealtimeWriter implements MetricsAgent {
    private static final Logger logger = LoggerFactory.getLogger(RealtimeWriter.class);

    public static final String WRITE_RECORDS_PER_SECOND = "write.records.per.second";
    public static final String WRITE_RECORDS_TOTAL = "write.records.total";

    private MetaService metaService;
    private SnapshotCache snapshotCache;
    private RoleClients<IngestorWriteClient> ingestWriteClients;

    // For metrics
    private AtomicLong totalProcessed;
    private volatile long lastUpdateTime;
    private volatile long lastUpdateProcessed;
    private volatile long writeRecordsPerSecond;

    public RealtimeWriter(MetaService metaService, SnapshotCache snapshotCache,
                          RoleClients<IngestorWriteClient> ingestWriteClients, MetricsCollector metricsCollector) {
        this.metaService = metaService;
        this.snapshotCache = snapshotCache;
        this.ingestWriteClients = ingestWriteClients;
        initMetrics();
        metricsCollector.register(this, () -> updateMetrics());
    }

    /**
     * Compiler use this method to write operations to the LogService.
     *
     * @param
     * @return BatchId of the data
     */
    public BatchId writeOperations(String requestId, String sessionId, int queueId, OperationBatch operationBatch) {
        logger.info("writeOperations requestId [" + requestId + "], sessionId [" + sessionId + "]");

        int ingestorId = this.metaService.getIngestorIdForQueue(queueId);
        BatchId batchId = this.ingestWriteClients.getClient(ingestorId).writeIngestor(requestId, queueId,
                operationBatch);
        totalProcessed.addAndGet(operationBatch.getOperationCount());
        return batchId;
    }

    public BatchId writeOperations(String requestId, String sessionId, OperationBatch operationBatch) {
        int queueId = getTargetQueueId(sessionId);
        return writeOperations(requestId, sessionId, queueId, operationBatch);
    }

    private int getTargetQueueId(String sessionId) {
        int queueCount = this.metaService.getQueueCount();
        if (queueCount <= 1) {
            throw new IllegalStateException("expect queueCount > 1, but was [" + queueCount + "]");
        }
        return sessionId.hashCode() % (queueCount - 1) + 1;
    }

    /**
     * Block until the specific snapshotId is available for query.
     *
     * @param snapshotId
     */
    public void waitForSnapshotCompletion(long snapshotId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.snapshotCache.addListener(snapshotId, () -> future.complete(null));
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new MaxGraphException(e);
        }
    }

    @Override
    public void initMetrics() {
        this.totalProcessed = new AtomicLong(0L);
        this.lastUpdateTime = System.nanoTime();
        this.lastUpdateProcessed = 0L;
    }

    private void updateMetrics() {
        long currentTime = System.nanoTime();
        long propcessed = this.totalProcessed.get();
        this.writeRecordsPerSecond = 1000000000 * (propcessed - this.lastUpdateProcessed) /
                (currentTime - this.lastUpdateTime);
        this.lastUpdateProcessed = propcessed;
        this.lastUpdateTime = currentTime;
    }

    @Override
    public Map<String, String> getMetrics() {
        return new HashMap<String, String>() {{
            put(WRITE_RECORDS_PER_SECOND, String.valueOf(writeRecordsPerSecond));
            put(WRITE_RECORDS_TOTAL, String.valueOf(totalProcessed));
        }};
    }

    @Override
    public String[] getMetricKeys() {
        return new String[] {
                WRITE_RECORDS_PER_SECOND,
                WRITE_RECORDS_TOTAL
        };
    }
}

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
package com.alibaba.graphscope.groot.ingestor;

import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.IngestorConfig;
import com.alibaba.graphscope.groot.common.exception.IngestRejectException;
import com.alibaba.graphscope.groot.metrics.MetricsAgent;
import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.OperationBlob;
import com.alibaba.graphscope.groot.operation.OperationType;
import com.alibaba.graphscope.groot.wal.LogEntry;
import com.alibaba.graphscope.groot.wal.LogReader;
import com.alibaba.graphscope.groot.wal.LogService;
import com.alibaba.graphscope.groot.wal.LogWriter;
import com.alibaba.graphscope.groot.wal.ReadLogEntry;
import com.alibaba.graphscope.groot.wal.readonly.ReadOnlyLogReader;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** A IngestProcessor handles one ingest queue */
public class IngestProcessor implements MetricsAgent {
    private static final Logger logger = LoggerFactory.getLogger(IngestProcessor.class);

    public static final String WRITE_RECORDS_PER_SECOND = "write.records.per.second";
    public static final String WRITE_RECORDS_TOTAL = "write.records.total";
    public static final String WAL_BLOCK_PER_SECOND_MS = "wal.block.per.second.ms";
    public static final String STORE_BLOCK_PER_SECOND_MS = "store.block.per.second.ms";
    public static final String INGESTOR_REJECT_COUNT = "ingestor.reject.count";
    public static final String INGEST_BUFFER_TASKS_COUNT = "ingest.buffer.tasks.count";

    private volatile boolean shouldStop = true;
    private volatile long tailOffset;

    private final int queueId;
    private final int bufferSize;
    private BlockingQueue<IngestTask> ingestBuffer;
    private Thread ingestThread;
    private Thread tailWALThread;
    private final AtomicLong ingestSnapshotId;

    private final LogService logService;
    private final BatchSender batchSender;
    private volatile boolean started;

    // For metrics
    private volatile long totalProcessed;
    private volatile long lastUpdateTime;
    private volatile long lastUpdateProcessed;
    private volatile long writeRecordsPerSecond;
    private volatile long walBlockTimeNano;
    private volatile long storeBlockTimeNano;
    private AtomicLong ingestorRejectCount;
    private volatile long lastUpdateWalBlockTimeNano;
    private volatile long walBlockPerSecondMs;
    private volatile long lastUpdateStoreBlockTimeNano;
    private volatile long storeBlockPerSecondMs;
    private boolean isSecondary;

    public IngestProcessor(
            Configs configs,
            LogService logService,
            BatchSender batchSender,
            int queueId,
            AtomicLong ingestSnapshotId,
            MetricsCollector metricsCollector) {
        this.logService = logService;
        this.batchSender = batchSender;
        this.queueId = queueId;
        this.ingestSnapshotId = ingestSnapshotId;

        this.bufferSize = IngestorConfig.INGESTOR_QUEUE_BUFFER_MAX_COUNT.get(configs);
        this.isSecondary = CommonConfig.SECONDARY_INSTANCE_ENABLED.get(configs);

        initMetrics();
        metricsCollector.register(this, this::updateMetrics);
    }

    public void start() {
        logger.info("staring ingestProcessor queue#[" + queueId + "]");
        this.ingestBuffer = new ArrayBlockingQueue<>(this.bufferSize);

        this.shouldStop = false;
        this.batchSender.start();
        this.ingestThread =
                new Thread(
                        () -> {
                            while (!shouldStop) {
                                try {
                                    replayWAL(this.tailOffset);
                                    break;
                                } catch (Exception e) {
                                    logger.error("error occurred before ingest, retrying", e);
                                    try {
                                        Thread.sleep(1000L);
                                    } catch (InterruptedException ie) {
                                        // Ignore
                                    }
                                }
                            }
                            LogWriter logWriter = this.logService.createWriter(this.queueId);
                            while (!shouldStop) {
                                try {
                                    process(logWriter);
                                } catch (Exception e) {
                                    logger.warn("error occurred in ingest process", e);
                                }
                            }
                            try {
                                logWriter.close();
                            } catch (IOException e) {
                                logger.warn("close logWriter failed", e);
                            }
                        });
        this.ingestThread.setDaemon(true);
        this.ingestThread.start();
        if (isSecondary) {
            this.tailWALThread =
                    new Thread(
                            () -> {
                                try {
                                    tailWAL();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            this.tailWALThread.setDaemon(true);
            this.tailWALThread.start();
        }

        started = true;
        logger.info("ingestProcessor queue#[" + queueId + "] started");
    }

    public void stop() {
        logger.info("stopping ingestProcessor queue#[" + queueId + "]");
        this.shouldStop = true;
        this.started = false;
        if (this.ingestThread != null && this.ingestThread.isAlive()) {
            try {
                this.ingestThread.interrupt();
                this.ingestThread.join();
            } catch (InterruptedException e) {
                logger.warn("stop ingestProcessor queue#[" + queueId + "] interrupted");
            }
            this.ingestThread = null;
        }
        if (tailWALThread != null && tailWALThread.isAlive()) {
            try {
                this.tailWALThread.interrupt();
                this.tailWALThread.join();
            } catch (InterruptedException e) {
                logger.warn("stop ingestProcessor queue#[" + queueId + "] interrupted");
            }
            this.tailWALThread = null;
        }
        this.batchSender.stop();
        logger.debug("ingestProcessor queue#[" + queueId + "] stopped");
    }

    private void checkStarted() {
        if (!started) {
            throw new IllegalStateException("IngestProcessor queue#[" + queueId + "] not started");
        }
    }

    public boolean isStarted() {
        return started;
    }

    public void ingestBatch(
            String requestId, OperationBatch operationBatch, IngestCallback callback) {
        checkStarted();
        logger.debug("ingestBatch requestId [{}], queueId [{}]", requestId, queueId);
        if (this.ingestSnapshotId.get() == -1L) {
            throw new IllegalStateException("ingestor has no valid ingestSnapshotId");
        }

        boolean suc = this.ingestBuffer.offer(new IngestTask(requestId, operationBatch, callback));
        if (!suc) {
            logger.warn("ingest buffer is full");
            this.ingestorRejectCount.incrementAndGet();
            throw new IngestRejectException("add ingestTask to buffer failed");
        }
    }

    private void process(LogWriter logWriter) {
        IngestTask task;
        try {
            task = this.ingestBuffer.poll(1000L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("polling ingestBuffer interrupted");
            return;
        }
        if (task == null) {
            return;
        }
        try {
            long batchSnapshotId = processTask(logWriter, task);
            task.callback.onSuccess(batchSnapshotId);
        } catch (Exception e) {
            task.callback.onFailure(e);
        }
        logger.debug("batch ingested. requestId [" + task.requestId + "]");
    }

    private long processTask(LogWriter logWriter, IngestTask task) throws IOException {
        long batchSnapshotId = this.ingestSnapshotId.get();
        if (batchSnapshotId == -1L) {
            throw new IllegalStateException("invalid ingestSnapshotId [" + batchSnapshotId + "]");
        }
        logger.debug(
                "append batch to WAL. requestId [{}], snapshotId [{}]",
                task.requestId,
                batchSnapshotId);
        long latestSnapshotId = task.operationBatch.getLatestSnapshotId();
        if (latestSnapshotId > 0 && latestSnapshotId < batchSnapshotId) {
            throw new IllegalStateException(
                    "latestSnapshotId ["
                            + latestSnapshotId
                            + "] must >= batchSnapshotId ["
                            + batchSnapshotId
                            + "]");
        }
        long startTimeNano = System.nanoTime();
        long walOffset = -1L;
        if (!shouldStop) {
            try {
                walOffset = logWriter.append(new LogEntry(batchSnapshotId, task.operationBatch));
            } catch (Exception e) {
                // write failed, just throw out to fail this task
                logger.error("write WAL failed. requestId [" + task.requestId + "]", e);
                throw e;
            }
        }
        long walCompleteTimeNano = System.nanoTime();
        if (shouldStop) {
            throw new IllegalStateException("ingestProcessor queue#[" + this.queueId + "] stopped");
        }
        this.batchSender.asyncSendWithRetry(
                task.requestId, this.queueId, batchSnapshotId, walOffset, task.operationBatch);
        long storeCompleteTimeNano = System.nanoTime();
        if (!task.operationBatch.equals(IngestService.MARKER_BATCH)) {
            this.walBlockTimeNano += (walCompleteTimeNano - startTimeNano);
            this.storeBlockTimeNano += (storeCompleteTimeNano - walCompleteTimeNano);
            this.totalProcessed += task.operationBatch.getOperationCount();
        }
        return batchSnapshotId;
    }

    class IngestTask {
        String requestId;
        OperationBatch operationBatch;
        IngestCallback callback;

        public IngestTask(
                String requestId, OperationBatch operationBatch, IngestCallback callback) {
            this.requestId = requestId;
            this.operationBatch = operationBatch;
            this.callback = callback;
        }
    }

    public void setTailOffset(long offset) {
        logger.info("IngestProcessor of queue #[{}] set tail offset to [{}]", queueId, offset);
        this.tailOffset = offset;
    }

    public void tailWAL() throws IOException {
        List<OperationType> types = new ArrayList<>();
        types.add(OperationType.CREATE_VERTEX_TYPE);
        types.add(OperationType.CREATE_EDGE_TYPE);
        types.add(OperationType.ADD_EDGE_KIND);
        types.add(OperationType.DROP_VERTEX_TYPE);
        types.add(OperationType.DROP_EDGE_TYPE);
        types.add(OperationType.REMOVE_EDGE_KIND);
        types.add(OperationType.PREPARE_DATA_LOAD);
        types.add(OperationType.COMMIT_DATA_LOAD);
        try (ReadOnlyLogReader reader = (ReadOnlyLogReader) logService.createReader(queueId, 0)) {
            while (!shouldStop) {
                ConsumerRecords<LogEntry, LogEntry> records = reader.getLatestUpdates();
                for (ConsumerRecord<LogEntry, LogEntry> record : records) {
                    long offset = record.offset();
                    LogEntry logEntry = record.value();
                    OperationBatch batch = extractOperations(logEntry.getOperationBatch(), types);
                    long snapshotId = logEntry.getSnapshotId();
                    if (batch.getOperationCount() > 0) {
                        long batchSnapshotId = this.ingestSnapshotId.get();
                        this.batchSender.asyncSendWithRetry(
                                "", queueId, batchSnapshotId, offset, batch);
                        logger.info(
                                "Sent logEntry snapshot Id {}, SnapshotId {}, batch {}",
                                snapshotId,
                                batchSnapshotId,
                                batch.toProto());
                    }
                }
            }
        }
    }

    public void replayWAL(long tailOffset) throws IOException {
        long replayFrom = tailOffset + 1;
        logger.info("replay WAL of queue#[{}] from offset [{}]", queueId, replayFrom);
        int replayCount = 0;
        try (LogReader logReader = this.logService.createReader(queueId, replayFrom)) {
            ReadLogEntry readLogEntry;
            while (!shouldStop && (readLogEntry = logReader.readNext()) != null) {
                long offset = readLogEntry.getOffset();
                LogEntry logEntry = readLogEntry.getLogEntry();
                long snapshotId = logEntry.getSnapshotId();
                OperationBatch batch = logEntry.getOperationBatch();
                this.batchSender.asyncSendWithRetry("", queueId, snapshotId, offset, batch);
                if (!batch.equals(IngestService.MARKER_BATCH)) {
                    replayCount++;
                }
            }
        }
        logger.info("replayWAL finished. total replayed [{}] records", replayCount);
    }

    public long replayDMLRecordsFrom(long offset, long timestamp) throws IOException {
        List<OperationType> types = new ArrayList<>();
        types.add(OperationType.OVERWRITE_VERTEX);
        types.add(OperationType.UPDATE_VERTEX);
        types.add(OperationType.DELETE_VERTEX);
        types.add(OperationType.OVERWRITE_EDGE);
        types.add(OperationType.UPDATE_EDGE);
        types.add(OperationType.DELETE_EDGE);
        types.add(OperationType.CLEAR_VERTEX_PROPERTIES);
        types.add(OperationType.CLEAR_EDGE_PROPERTIES);

        long batchSnapshotId = this.ingestSnapshotId.get();
        logger.info(
                "replay DML records of queue#[{}] from offset [{}], ts [{}]",
                queueId,
                offset,
                timestamp);
        int replayCount = 0;
        try (LogReader logReader = this.logService.createReader(queueId, offset, timestamp)) {
            ReadLogEntry readLogEntry;
            while (!shouldStop && (readLogEntry = logReader.readNext()) != null) {
                long entryOffset = readLogEntry.getOffset();
                LogEntry logEntry = readLogEntry.getLogEntry();
                OperationBatch batch = extractOperations(logEntry.getOperationBatch(), types);
                if (batch.getOperationCount() == 0) {
                    continue;
                }
                this.batchSender.asyncSendWithRetry(
                        "", queueId, batchSnapshotId, entryOffset, batch);
                replayCount++;
            }
        }
        logger.info("replay DML records finished. total replayed [{}] records", replayCount);
        return batchSnapshotId;
    }

    private OperationBatch extractOperations(OperationBatch input, List<OperationType> types) {
        boolean hasOtherType = false;
        for (int i = 0; i < input.getOperationCount(); ++i) {
            OperationBlob blob = input.getOperationBlob(i);
            OperationType opType = blob.getOperationType();
            if (!types.contains(opType)) {
                hasOtherType = true;
                break;
            }
        }
        if (!hasOtherType) {
            return input;
        }
        OperationBatch.Builder batchBuilder = OperationBatch.newBuilder();
        batchBuilder.setLatestSnapshotId(input.getLatestSnapshotId());
        for (int i = 0; i < input.getOperationCount(); ++i) {
            OperationBlob blob = input.getOperationBlob(i);
            OperationType opType = blob.getOperationType();
            if (types.contains(opType)) {
                batchBuilder.addOperationBlob(blob);
            }
        }
        return batchBuilder.build();
    }

    public int getQueueId() {
        return queueId;
    }

    @Override
    public void initMetrics() {
        this.totalProcessed = 0L;
        this.lastUpdateTime = System.nanoTime();
        this.lastUpdateProcessed = 0L;
        this.walBlockTimeNano = 0L;
        this.storeBlockTimeNano = 0L;
        this.ingestorRejectCount = new AtomicLong(0L);
        this.lastUpdateStoreBlockTimeNano = 0L;
        this.walBlockPerSecondMs = 0L;
        this.lastUpdateStoreBlockTimeNano = 0L;
        this.storeBlockPerSecondMs = 0L;
    }

    private void updateMetrics() {
        long currentTime = System.nanoTime();
        long processed = this.totalProcessed;
        long interval = currentTime - this.lastUpdateTime;
        this.writeRecordsPerSecond = 1000000000 * (processed - this.lastUpdateProcessed) / interval;
        long walBlockTime = this.walBlockTimeNano;
        this.walBlockPerSecondMs =
                1000 * (walBlockTime - this.lastUpdateWalBlockTimeNano) / interval;
        long storeBlockTime = this.storeBlockTimeNano;
        this.storeBlockPerSecondMs =
                1000 * (storeBlockTime - this.lastUpdateStoreBlockTimeNano) / interval;

        this.lastUpdateStoreBlockTimeNano = storeBlockTime;
        this.lastUpdateWalBlockTimeNano = walBlockTime;
        this.lastUpdateProcessed = processed;
        this.lastUpdateTime = currentTime;
    }

    @Override
    public Map<String, String> getMetrics() {
        return new HashMap<>() {
            {
                put(WRITE_RECORDS_PER_SECOND, String.valueOf(writeRecordsPerSecond));
                put(WRITE_RECORDS_TOTAL, String.valueOf(totalProcessed));
                put(WAL_BLOCK_PER_SECOND_MS, String.valueOf(walBlockPerSecondMs));
                put(STORE_BLOCK_PER_SECOND_MS, String.valueOf(storeBlockPerSecondMs));
                put(INGESTOR_REJECT_COUNT, String.valueOf(ingestorRejectCount));
                put(INGEST_BUFFER_TASKS_COUNT, String.valueOf(ingestBuffer.size()));
            }
        };
    }

    @Override
    public String[] getMetricKeys() {
        return new String[] {
            WRITE_RECORDS_PER_SECOND,
            WRITE_RECORDS_TOTAL,
            WAL_BLOCK_PER_SECOND_MS,
            STORE_BLOCK_PER_SECOND_MS,
            INGESTOR_REJECT_COUNT,
            INGEST_BUFFER_TASKS_COUNT
        };
    }
}

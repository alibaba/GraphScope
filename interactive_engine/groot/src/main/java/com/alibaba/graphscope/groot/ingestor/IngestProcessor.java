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

import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.IngestorConfig;
import com.alibaba.maxgraph.compiler.api.exception.IngestRejectException;
import com.alibaba.graphscope.groot.metrics.MetricsAgent;
import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.graphscope.groot.wal.LogEntry;
import com.alibaba.graphscope.groot.wal.LogReader;
import com.alibaba.graphscope.groot.wal.LogService;
import com.alibaba.graphscope.groot.wal.LogWriter;
import com.alibaba.graphscope.groot.wal.ReadLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
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
    public static final String WAL_BLOCK_TIME_MS = "wal.block.time.ms";
    public static final String STORE_BLOCK_TIME_MS = "store.block.time.ms";
    public static final String INGESTOR_REJECT_COUNT = "ingestor.reject.count";

    private volatile boolean shouldStop = true;
    private volatile long tailOffset;

    private int queueId;
    private int bufferSize;
    private BlockingQueue<IngestTask> ingestBuffer;
    private Thread ingestThread;
    private AtomicLong ingestSnapshotId;

    private LogService logService;
    private BatchSender batchSender;
    private volatile boolean started;

    // For metrics
    private volatile long totalProcessed;
    private volatile long lastUpdateTime;
    private volatile long lastUpdateProcessed;
    private volatile long writeRecordsPerSecond;
    private volatile long walBlockTimeNano;
    private volatile long storeBlockTimeNano;
    private AtomicLong ingestorRejectCount;

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
        initMetrics();
        metricsCollector.register(this, () -> updateMetrics());
    }

    public void start() {
        logger.info("staring ingestProcessor queue#[" + queueId + "]");
        this.ingestBuffer = new ArrayBlockingQueue<>(this.bufferSize);

        this.shouldStop = false;
        this.batchSender.start();
        this.ingestThread =
                new Thread(
                        () -> {
                            LogWriter logWriter = null;
                            while (!shouldStop) {
                                try {
                                    replayWAL(this.tailOffset);
                                    logWriter = this.logService.createWriter(this.queueId);
                                    break;
                                } catch (Exception e) {
                                    logger.error(
                                            "error occurred before ingest process, will retry after 1s",
                                            e);
                                    try {
                                        Thread.sleep(1000L);
                                    } catch (InterruptedException ie) {
                                        // Ignore
                                    }
                                }
                            }
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
        this.batchSender.stop();
        logger.info("ingestProcessor queue#[" + queueId + "] stopped");
    }

    private void checkStarted() {
        if (!started) {
            throw new IllegalStateException(
                    "IngestProcessor of queue #[" + this.queueId + "] not started yet");
        }
    }

    public void ingestBatch(
            String requestId, OperationBatch operationBatch, IngestCallback callback) {
        checkStarted();
        logger.debug("ingestBatch requestId [" + requestId + "], queueId [" + queueId + "]");
        if (this.ingestSnapshotId.get() == -1L) {
            throw new IllegalStateException("ingestor has no valid ingestSnapshotId");
        }

        boolean suc = this.ingestBuffer.offer(new IngestTask(requestId, operationBatch, callback));
        if (!suc) {
            this.ingestorRejectCount.incrementAndGet();
            throw new IngestRejectException("add ingestTask to buffer failed");
        }
    }

    private void process(LogWriter logWriter) {
        IngestTask task;
        try {
            task = this.ingestBuffer.poll(1000L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("polling ingestBuffer interrupted", e);
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
                "append batch to WAL. requestId ["
                        + task.requestId
                        + "], snapshotId ["
                        + batchSnapshotId
                        + "]");
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
        while (!shouldStop) {
            try {
                walOffset = logWriter.append(new LogEntry(batchSnapshotId, task.operationBatch));
                break;
            } catch (IOException e) {
                // write failed, just throw out to fail this task
                logger.error("write WAL failed. requestId [" + task.requestId + "]", e);
                throw e;
            } catch (Exception e) {
                // Timeout, uncertain error, etc
                logger.error("unexpected logWriter exception, will retry after 100ms", e);
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException ie) {
                    // Ignore
                }
            }
        }
        long walCompleteTimeNano = System.nanoTime();
        this.walBlockTimeNano += (walCompleteTimeNano - startTimeNano);
        if (shouldStop) {
            throw new IllegalStateException("ingestProcessor queue#[" + this.queueId + "] stopped");
        }
        this.batchSender.asyncSendWithRetry(
                task.requestId, this.queueId, batchSnapshotId, walOffset, task.operationBatch);
        long storeCompleteTimeNano = System.nanoTime();
        this.storeBlockTimeNano += (storeCompleteTimeNano - walCompleteTimeNano);
        this.totalProcessed += task.operationBatch.getOperationCount();
        return batchSnapshotId;
    }

    @Override
    public void initMetrics() {
        this.totalProcessed = 0L;
        this.lastUpdateTime = System.nanoTime();
        this.lastUpdateProcessed = 0L;
        this.walBlockTimeNano = 0L;
        this.storeBlockTimeNano = 0L;
        this.ingestorRejectCount = new AtomicLong(0L);
    }

    private void updateMetrics() {
        long currentTime = System.nanoTime();
        long propcessed = this.totalProcessed;
        this.writeRecordsPerSecond =
                1000000000
                        * (propcessed - this.lastUpdateProcessed)
                        / (currentTime - this.lastUpdateTime);
        this.lastUpdateProcessed = propcessed;
        this.lastUpdateTime = currentTime;
    }

    @Override
    public Map<String, String> getMetrics() {
        return new HashMap<String, String>() {
            {
                put(WRITE_RECORDS_PER_SECOND, String.valueOf(writeRecordsPerSecond));
                put(WRITE_RECORDS_TOTAL, String.valueOf(totalProcessed));
                put(WAL_BLOCK_TIME_MS, String.valueOf(walBlockTimeNano / 1000000));
                put(STORE_BLOCK_TIME_MS, String.valueOf(storeBlockTimeNano / 1000000));
                put(INGESTOR_REJECT_COUNT, String.valueOf(ingestorRejectCount));
            }
        };
    }

    @Override
    public String[] getMetricKeys() {
        return new String[] {
            WRITE_RECORDS_PER_SECOND,
            WRITE_RECORDS_TOTAL,
            WAL_BLOCK_TIME_MS,
            STORE_BLOCK_TIME_MS,
            INGESTOR_REJECT_COUNT
        };
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
        logger.info(
                "IngestProcessor of queue #["
                        + this.queueId
                        + "] set tail offset to ["
                        + offset
                        + "]");
        this.tailOffset = offset;
    }

    private void replayWAL(long tailOffset) throws IOException {
        long replayFrom = tailOffset + 1;
        logger.info("replay WAL of queue#[" + this.queueId + "] from offset [" + replayFrom + "]");
        LogReader logReader = this.logService.createReader(this.queueId, replayFrom);
        ReadLogEntry readLogEntry;
        int replayCount = 0;
        while (!shouldStop && (readLogEntry = logReader.readNext()) != null) {
            long offset = readLogEntry.getOffset();
            LogEntry logEntry = readLogEntry.getLogEntry();
            long snapshotId = logEntry.getSnapshotId();
            OperationBatch operationBatch = logEntry.getOperationBatch();
            this.batchSender.asyncSendWithRetry(
                    "", this.queueId, snapshotId, offset, operationBatch);
            replayCount++;
        }
        try {
            logReader.close();
        } catch (IOException e) {
            logger.warn("close logReader failed", e);
        }
        logger.info("replayWAL finished. total replayed [" + replayCount + "] records");
    }

    public int getQueueId() {
        return queueId;
    }
}

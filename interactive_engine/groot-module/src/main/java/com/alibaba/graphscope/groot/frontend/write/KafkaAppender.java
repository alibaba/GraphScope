package com.alibaba.graphscope.groot.frontend.write;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.IngestorConfig;
import com.alibaba.graphscope.groot.common.exception.IngestRejectException;
import com.alibaba.graphscope.groot.common.util.PartitionUtils;
import com.alibaba.graphscope.groot.ingestor.IngestCallback;
import com.alibaba.graphscope.groot.ingestor.IngestProcessor;
import com.alibaba.graphscope.groot.ingestor.IngestService;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.OperationBlob;
import com.alibaba.graphscope.groot.wal.LogEntry;
import com.alibaba.graphscope.groot.wal.LogService;
import com.alibaba.graphscope.groot.wal.LogWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.alibaba.graphscope.groot.ingestor.IngestService.MARKER_BATCH;

public class KafkaAppender {
    private static final Logger logger = LoggerFactory.getLogger(KafkaAppender.class);

    private final MetaService metaService;
    private final LogService logService;

    private final int storeCount;
    private final int partitionCount;
    private final int bufferSize;
    private BlockingQueue<IngestTask> ingestBuffer;
    private Thread ingestThread;

    private boolean shouldStop = false;
    private boolean started = false;

    private final AtomicLong ingestSnapshotId;

    public KafkaAppender(Configs configs,
                         MetaService metaService,
                         LogService logService) {
        this.metaService = metaService;
        this.logService = logService;
        this.storeCount = CommonConfig.STORE_NODE_COUNT.get(configs);
        this.partitionCount = metaService.getPartitionCount();
        this.bufferSize = IngestorConfig.INGESTOR_QUEUE_BUFFER_MAX_COUNT.get(configs);
        this.ingestSnapshotId = new AtomicLong(-1);
    }

    public void start() {
        logger.info("staring KafkaAppender queue#[]");
        this.ingestBuffer = new ArrayBlockingQueue<>(this.bufferSize);

        this.shouldStop = false;
        this.ingestThread =
                new Thread(
                        () -> {
                            LogWriter logWriter = this.logService.createWriter();
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
        this.started = true;
    }

    public boolean isStarted() {
        return this.started;
    }
    public void stop() {
        logger.info("stopping KafkaAppender queue#[]");
        this.shouldStop = true;
        this.started = false;
        if (this.ingestThread != null && this.ingestThread.isAlive()) {
            try {
                this.ingestThread.interrupt();
                this.ingestThread.join();
            } catch (InterruptedException e) {
                logger.warn("stop KafkaAppender queue#[] interrupted");
            }
            this.ingestThread = null;
        }
    }

    public void ingestBatch(
            String requestId, OperationBatch operationBatch, IngestCallback callback) {
        checkStarted();
        logger.debug("ingestBatch requestId [{}]", requestId);
        if (this.ingestSnapshotId.get() == -1L) {
            throw new IllegalStateException("ingestor has no valid ingestSnapshotId");
        }
        boolean suc = this.ingestBuffer.offer(new IngestTask(requestId, operationBatch, callback));
        if (!suc) {
            logger.warn("ingest buffer is full");
            throw new IngestRejectException("add ingestTask to buffer failed");
        }
    }

    private void process(LogWriter logWriter) {
        IngestTask task;
        try {
            if ((task = this.ingestBuffer.poll(1000L, TimeUnit.MILLISECONDS)) == null) {
                return;
            }
        } catch (InterruptedException e) {
            logger.warn("polling ingestBuffer interrupted");
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
        if (!shouldStop) {
            try {
                Map<Integer, OperationBatch.Builder> builderMap = splitBatch(task.operationBatch);
                for (Map.Entry<Integer, OperationBatch.Builder> entry : builderMap.entrySet()) {
                    int partitionId = entry.getKey();
                    OperationBatch batch = entry.getValue().build();
                    logWriter.append(partitionId, new LogEntry(batchSnapshotId, batch));
                }
            } catch (Exception e) {
                // write failed, just throw out to fail this task
                logger.error("write WAL failed. requestId [" + task.requestId + "]", e);
                throw e;
            }
        }
        if (shouldStop) {
            throw new IllegalStateException("ingestProcessor queue stopped");
        }
        return batchSnapshotId;
    }

    private void checkStarted() {
        if (!started) {
            throw new IllegalStateException("IngestProcessor queue not started");
        }
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

    public Map<Integer, OperationBatch.Builder> splitBatch(OperationBatch operationBatch) {
        Map<Integer, OperationBatch.Builder> storeToBatchBuilder = new HashMap<>();
        Function<Integer, OperationBatch.Builder> storeDataBatchBuilderFunc =
                k -> OperationBatch.newBuilder();
        for (OperationBlob operationBlob : operationBatch) {
            long partitionKey = operationBlob.getPartitionKey();
            if (partitionKey == -1L) {
                // replicate to all store node
                for (int i = 0; i < this.storeCount; i++) {
                    OperationBatch.Builder batchBuilder =
                            storeToBatchBuilder.computeIfAbsent(i, storeDataBatchBuilderFunc);
                    batchBuilder.addOperationBlob(operationBlob);
                }
            } else {
                int partitionId = PartitionUtils.getPartitionIdFromKey(partitionKey, partitionCount);
                int storeId = metaService.getStoreIdByPartition(partitionId);
                OperationBatch.Builder batchBuilder =
                        storeToBatchBuilder.computeIfAbsent(storeId, storeDataBatchBuilderFunc);
                batchBuilder.addOperationBlob(operationBlob);
            }
        }
        return storeToBatchBuilder;
    }

    /**
     * This method will update writeSnapshotId and returns the previous value.
     *
     * <p>SnapshotManager periodically increase the writeSnapshotId and call this method to update
     * the writeSnapshotId for each Ingestor.
     *
     * @param snapshotId
     * @return
     */
    public synchronized void advanceIngestSnapshotId(
            long snapshotId, CompletionCallback<Long> callback) {
        checkStarted();
        long previousSnapshotId = this.ingestSnapshotId.getAndUpdate(x -> Math.max(x, snapshotId));
        if (previousSnapshotId >= snapshotId) {
            throw new IllegalStateException(
                    "current ingestSnapshotId ["
                            + previousSnapshotId
                            + "], cannot update to ["
                            + snapshotId
                            + "]");
        }
            try {
                ingestBatch(
                        "marker",
                        MARKER_BATCH,
                        new IngestCallback() {
                            @Override
                            public void onSuccess(long snapshotId) {
                                callback.onCompleted(previousSnapshotId);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.warn(
                                        "ingest marker failed. snapshotId {}",
                                        snapshotId,
                                        e);
                                callback.onError(e);
                            }
                        });
            } catch (IllegalStateException e) {
                logger.warn(
                        "ingest marker failed, snapshotId {}, {}",
                        snapshotId,
                        e.getMessage());
                callback.onError(e);
            } catch (Exception e) {
                logger.warn("ingest marker failed. snapshotId {}", snapshotId, e);
                callback.onError(e);
            }
        }


}

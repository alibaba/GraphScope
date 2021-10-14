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

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.OperationBlob;
import com.alibaba.graphscope.groot.operation.StoreDataBatch;
import com.alibaba.graphscope.groot.metrics.MetricsAgent;
import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.IngestorConfig;
import com.alibaba.maxgraph.common.util.PartitionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchSender implements MetricsAgent {
    private static final Logger logger = LoggerFactory.getLogger(BatchSender.class);

    public static final String SEND_BYTES_PER_SECOND = "send.bytes.per.second";
    public static final String SEND_BYTES_TOTAL = "send.bytes.total";

    public static final String SEND_RECORDS_PER_SECOND = "send.records.per.second";
    public static final String SEND_RECORDS_TOTAL = "send.records.total";

    private MetaService metaService;
    private StoreWriter storeWriter;

    private volatile boolean shouldStop = true;

    private int bufferSize;
    private int storeCount;
    private BlockingQueue<SendTask> sendBuffer;
    private Thread sendThread;

    private volatile long totalBytes;
    private volatile long lastUpdateBytes;
    private volatile long lastUpdateTime;
    private volatile long sendBytesPerSecond;
    private volatile long totalRecords;
    private volatile long lastUpdateRecords;
    private volatile long sendRecordsPerSecond;

    public BatchSender(
            Configs configs,
            MetaService metaService,
            StoreWriter storeWriter,
            MetricsCollector metricsCollector) {
        this.metaService = metaService;
        this.storeWriter = storeWriter;
        this.storeCount = CommonConfig.STORE_NODE_COUNT.get(configs);
        this.bufferSize = IngestorConfig.INGESTOR_SENDER_BUFFER_MAX_COUNT.get(configs);
        initMetrics();
        metricsCollector.register(this, () -> updateMetrics());
    }

    public void start() {
        logger.info("starting batchSender");
        this.sendBuffer = new ArrayBlockingQueue<>(this.bufferSize);

        this.shouldStop = false;
        this.sendThread =
                new Thread(
                        () -> {
                            while (!shouldStop) {
                                try {
                                    process();
                                } catch (Exception e) {
                                    logger.warn("error occurred in send process", e);
                                }
                            }
                        });
        this.sendThread.setDaemon(true);
        this.sendThread.start();
        logger.info("batchSender started");
    }

    public void stop() {
        logger.info("stopping batchSender");
        this.shouldStop = true;
        if (this.sendThread != null && this.sendThread.isAlive()) {
            try {
                this.sendThread.interrupt();
                this.sendThread.join();
            } catch (InterruptedException e) {
                logger.warn("stop batchSender failed", e);
            }
            this.sendThread = null;
        }
        logger.info("batchSender stopped");
    }

    public void asyncSendWithRetry(
            String requestId,
            int queueId,
            long snapshotId,
            long offset,
            OperationBatch operationBatch) {
        logger.debug("asyncSendWithRetry requestId [" + requestId + "], offset [" + offset + "]");
        while (!shouldStop) {
            try {
                this.sendBuffer.put(
                        new SendTask(requestId, queueId, snapshotId, offset, operationBatch));
                break;
            } catch (InterruptedException e) {
                logger.warn("send buffer interrupted", e);
            }
        }
    }

    private void process() {
        SendTask task;
        try {
            task = this.sendBuffer.poll(1000L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("polling sendBuffer interrupted", e);
            return;
        }
        if (task == null) {
            return;
        }
        while (!shouldStop) {
            try {
                doSend(task);
                break;
            } catch (Exception e) {
                logger.warn("unexpected send batch task failed, will retry after 1s", e);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException ie) {
                    // Ignore
                }
            }
        }
    }

    private void doSend(SendTask task) throws ExecutionException, InterruptedException {
        Map<Integer, StoreDataBatch> batchNeedSend = rebatch(task);
        while (!shouldStop && batchNeedSend.size() != 0) {
            batchNeedSend = parallelSend(batchNeedSend);
        }
        this.totalRecords += task.operationBatch.getOperationCount();
    }

    private Map<Integer, StoreDataBatch> rebatch(SendTask task) {
        int partitionCount = metaService.getPartitionCount();
        Map<Integer, StoreDataBatch.Builder> storeToBatchBuilder = new HashMap<>();
        for (OperationBlob operationBlob : task.operationBatch) {
            long partitionKey = operationBlob.getPartitionKey();
            if (partitionKey == -1L) {
                // replicate to all store node
                for (int i = 0; i < this.storeCount; i++) {
                    StoreDataBatch.Builder batchBuilder =
                            storeToBatchBuilder.computeIfAbsent(
                                    i,
                                    k ->
                                            StoreDataBatch.newBuilder()
                                                    .requestId(task.requestId)
                                                    .queueId(task.queueId)
                                                    .snapshotId(task.snapshotId)
                                                    .offset(task.offset));
                    batchBuilder.addOperation(-1, operationBlob);
                }
            } else {
                int partitionId =
                        PartitionUtils.getPartitionIdFromKey(partitionKey, partitionCount);
                int storeId = metaService.getStoreIdByPartition(partitionId);
                StoreDataBatch.Builder batchBuilder =
                        storeToBatchBuilder.computeIfAbsent(
                                storeId,
                                k ->
                                        StoreDataBatch.newBuilder()
                                                .requestId(task.requestId)
                                                .queueId(task.queueId)
                                                .snapshotId(task.snapshotId)
                                                .offset(task.offset));
                batchBuilder.addOperation(partitionId, operationBlob);
            }
        }
        Map<Integer, StoreDataBatch> batchNeedSend = new HashMap<>();
        for (Map.Entry<Integer, StoreDataBatch.Builder> entry : storeToBatchBuilder.entrySet()) {
            batchNeedSend.put(entry.getKey(), entry.getValue().build());
        }
        return batchNeedSend;
    }

    private Map<Integer, StoreDataBatch> parallelSend(Map<Integer, StoreDataBatch> storeToBatch)
            throws ExecutionException, InterruptedException {
        Map<Integer, StoreDataBatch> batchNeedRetry = new ConcurrentHashMap<>();
        AtomicInteger counter = new AtomicInteger(storeToBatch.size());
        AtomicInteger totalSizeBytes = new AtomicInteger(0);
        CompletableFuture<Integer> future = new CompletableFuture<>();

        for (Map.Entry<Integer, StoreDataBatch> entry : storeToBatch.entrySet()) {
            int storeId = entry.getKey();
            StoreDataBatch storeDataBatch = entry.getValue();
            this.storeWriter.write(
                    storeId,
                    storeDataBatch,
                    new CompletionCallback<Integer>() {
                        @Override
                        public void onCompleted(Integer res) {
                            totalSizeBytes.addAndGet(res);
                            if (counter.decrementAndGet() == 0) {
                                future.complete(totalSizeBytes.get());
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            logger.warn(
                                    "send to store ["
                                            + storeId
                                            + "] failed. requestId ["
                                            + storeDataBatch.getRequestId()
                                            + "]. will retry",
                                    t);
                            batchNeedRetry.put(storeId, storeDataBatch);
                            if (counter.decrementAndGet() == 0) {
                                future.complete(null);
                            }
                        }
                    });
        }
        this.totalBytes += future.get();
        if (batchNeedRetry.size() > 0) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
        return batchNeedRetry;
    }

    @Override
    public void initMetrics() {
        this.totalBytes = 0L;
        this.lastUpdateBytes = 0L;
        this.lastUpdateTime = System.nanoTime();
        this.totalRecords = 0L;
        this.lastUpdateRecords = 0L;
    }

    private void updateMetrics() {
        long currentTime = System.nanoTime();

        long sendBytes = this.totalBytes;
        long interval = currentTime - this.lastUpdateTime;
        this.sendBytesPerSecond = 1000000000 * (sendBytes - this.lastUpdateBytes) / interval;
        this.lastUpdateBytes = sendBytes;

        long sendRecords = this.totalRecords;
        this.sendRecordsPerSecond = 1000000000 * (sendRecords - this.lastUpdateRecords) / interval;
        this.lastUpdateRecords = sendRecords;

        this.lastUpdateTime = currentTime;
    }

    @Override
    public Map<String, String> getMetrics() {
        return new HashMap<String, String>() {
            {
                put(SEND_BYTES_PER_SECOND, String.valueOf(sendBytesPerSecond));
                put(SEND_BYTES_TOTAL, String.valueOf(totalBytes));
                put(SEND_RECORDS_PER_SECOND, String.valueOf(sendRecordsPerSecond));
                put(SEND_RECORDS_TOTAL, String.valueOf(totalRecords));
            }
        };
    }

    @Override
    public String[] getMetricKeys() {
        return new String[] {
            SEND_BYTES_PER_SECOND, SEND_BYTES_TOTAL, SEND_RECORDS_PER_SECOND, SEND_RECORDS_TOTAL
        };
    }

    class SendTask {
        String requestId;
        int queueId;
        long snapshotId;
        long offset;
        OperationBatch operationBatch;

        public SendTask(
                String requestId,
                int queueId,
                long snapshotId,
                long offset,
                OperationBatch operationBatch) {
            this.requestId = requestId;
            this.queueId = queueId;
            this.snapshotId = snapshotId;
            this.offset = offset;
            this.operationBatch = operationBatch;
        }
    }
}

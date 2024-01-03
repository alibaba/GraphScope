package com.alibaba.graphscope.groot.ingestor;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.IngestorConfig;
import com.alibaba.graphscope.groot.common.config.StoreConfig;
import com.alibaba.graphscope.groot.common.util.PartitionUtils;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.metrics.AvgMetric;
import com.alibaba.graphscope.groot.metrics.MetricsAgent;
import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.OperationBlob;
import com.alibaba.graphscope.groot.operation.StoreDataBatch;
import com.alibaba.graphscope.groot.operation.StoreDataBatch.Builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BatchSender implements MetricsAgent {
    private static final Logger logger = LoggerFactory.getLogger(BatchSender.class);

    public static final String SEND_BYTES_PER_SECOND = "send.bytes.per.second";
    public static final String SEND_BYTES_TOTAL = "send.bytes.total";
    public static final String SEND_RECORDS_PER_SECOND = "send.records.per.second";
    public static final String SEND_RECORDS_TOTAL = "send.records.total";
    public static final String SEND_BUFFER_BATCH_COUNT = "send.buffer.batch.count";
    public static final String SEND_CALLBACK_LATENCY = "send.callback.latency.per.second.ms";

    private final MetaService metaService;
    private final StoreWriter storeWriter;

    private final int bufferSize;
    private final int storeCount;
    private final int sendOperationLimit;

    private List<BlockingQueue<StoreDataBatch>> storeSendBuffer;
    private BlockingQueue<SendTask> sendTasks;

    private Thread sendThread;
    private volatile boolean shouldStop = true;

    private volatile long lastUpdateTime;
    private AvgMetric sendBytesMetric;
    private AvgMetric sendRecordsMetric;
    private List<AvgMetric> callbackLatencyMetrics;
    private final int receiverQueueSize;

    public BatchSender(
            Configs configs,
            MetaService metaService,
            StoreWriter storeWriter,
            MetricsCollector metricsCollector) {
        this.metaService = metaService;
        this.storeWriter = storeWriter;

        this.storeCount = CommonConfig.STORE_NODE_COUNT.get(configs);
        this.bufferSize = IngestorConfig.INGESTOR_SENDER_BUFFER_MAX_COUNT.get(configs);
        this.sendOperationLimit = IngestorConfig.INGESTOR_SENDER_OPERATION_MAX_COUNT.get(configs);
        this.receiverQueueSize = StoreConfig.STORE_QUEUE_BUFFER_SIZE.get(configs);
        initMetrics();
        metricsCollector.register(this, this::updateMetrics);
    }

    public void start() {
        this.storeSendBuffer = new ArrayList<>(this.storeCount);
        this.sendTasks = new ArrayBlockingQueue<>(this.storeCount);
        for (int i = 0; i < this.storeCount; i++) {
            this.storeSendBuffer.add(new ArrayBlockingQueue<>(this.bufferSize));
            this.sendTasks.add(new SendTask(i, null));
        }

        this.shouldStop = false;
        this.sendThread =
                new Thread(
                        () -> {
                            while (!shouldStop) {
                                try {
                                    sendBatch();
                                } catch (Exception e) {
                                    logger.warn("error occurred in send process", e);
                                }
                            }
                        });
        this.sendThread.setDaemon(true);
        this.sendThread.start();
    }

    public void stop() {
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
    }

    public void asyncSendWithRetry(
            String requestId,
            int queueId,
            long snapshotId,
            long offset,
            OperationBatch operationBatch) {
        int partitionCount = metaService.getPartitionCount();
        Map<Integer, Builder> storeToBatchBuilder = new HashMap<>();
        Function<Integer, Builder> storeDataBatchBuilderFunc =
                k ->
                        StoreDataBatch.newBuilder()
                                .requestId(requestId)
                                .queueId(queueId)
                                .snapshotId(snapshotId)
                                .offset(offset);
        for (OperationBlob operationBlob : operationBatch) {
            long partitionKey = operationBlob.getPartitionKey();
            if (partitionKey == -1L) {
                // replicate to all store node
                for (int i = 0; i < this.storeCount; i++) {
                    StoreDataBatch.Builder batchBuilder =
                            storeToBatchBuilder.computeIfAbsent(i, storeDataBatchBuilderFunc);
                    batchBuilder.addOperation(-1, operationBlob);
                }
            } else {
                int partitionId =
                        PartitionUtils.getPartitionIdFromKey(partitionKey, partitionCount);
                int storeId = metaService.getStoreIdByPartition(partitionId);
                StoreDataBatch.Builder batchBuilder =
                        storeToBatchBuilder.computeIfAbsent(storeId, storeDataBatchBuilderFunc);
                batchBuilder.addOperation(partitionId, operationBlob);
            }
        }
        storeToBatchBuilder.forEach(
                (storeId, batchBuilder) -> {
                    while (!shouldStop) {
                        try {
                            BlockingQueue<StoreDataBatch> curBuffer = storeSendBuffer.get(storeId);
                            if (curBuffer.remainingCapacity() == 0) {
                                logger.warn("Buffer of store [{}] is full", storeId);
                            }
                            curBuffer.put(batchBuilder.build());
                            break;
                        } catch (InterruptedException e) {
                            logger.warn("send buffer interrupted");
                        }
                    }
                });
    }

    class SendTask {
        int storeId;
        int retryCount = 0;
        List<StoreDataBatch> dataToRetry;

        public SendTask(int storeId, List<StoreDataBatch> dataToRetry) {
            this(storeId, 0, dataToRetry);
        }

        public SendTask(int storeId, int retryCount, List<StoreDataBatch> dataToRetry) {
            this.storeId = storeId;
            this.retryCount = retryCount;
            this.dataToRetry = dataToRetry;
        }

        public void retry() {
            retryCount += 1;
        }
    }

    private void sendBatch() {
        SendTask sendTask;
        try {
            sendTask = this.sendTasks.poll(1000L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("polling send task interrupted");
            return;
        }
        if (sendTask == null) {
            return;
        }

        int storeId = sendTask.storeId;
        int retryCount = sendTask.retryCount;
        List<StoreDataBatch> dataToSend = sendTask.dataToRetry;

        if (retryCount > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {

            }
        }

        if (retryCount > 1000) {
            logger.error("Failed to send batch of {}", dataToSend);
            return;
        }

        if (dataToSend == null) {
            dataToSend = new ArrayList<>();
            BlockingQueue<StoreDataBatch> buffer = this.storeSendBuffer.get(storeId);
            StoreDataBatch dataBatch;
            int operationCount = 0;
            int batchCount = 0;
            while (operationCount < this.sendOperationLimit
                    && batchCount < this.receiverQueueSize) {
                try {
                    dataBatch = buffer.poll(100L, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    dataBatch = null;
                    logger.warn("polling send buffer interrupted, {}", e.getMessage());
                }
                if (dataBatch == null) {
                    break;
                }
                dataToSend.add(dataBatch);
                operationCount += dataBatch.getSize();
                batchCount++;
            }
        }

        if (dataToSend.size() > 0) {
            List<StoreDataBatch> finalDataToSend = dataToSend;
            long beforeWriteTime = System.nanoTime();
            this.storeWriter.write(
                    storeId,
                    dataToSend,
                    new CompletionCallback<Integer>() {
                        @Override
                        public void onCompleted(Integer res) {
                            sendBytesMetric.add(res);
                            sendRecordsMetric.add(
                                    finalDataToSend.stream()
                                            .collect(
                                                    Collectors.summingInt(
                                                            batch -> batch.getSize())));
                            finish(true);
                        }

                        @Override
                        public void onError(Throwable t) {
                            logger.warn(
                                    "send to store [" + storeId + "] failed. will retry later", t);
                            finish(false);
                        }

                        private void finish(boolean suc) {
                            long finishTime = System.nanoTime();
                            callbackLatencyMetrics.get(storeId).add(finishTime - beforeWriteTime);
                            if (suc) {
                                addTask(storeId, 0, null);
                            } else {
                                addTask(storeId, retryCount + 1, finalDataToSend);
                            }
                        }
                    });
        } else {
            addTask(storeId, 0, null);
        }
    }

    private void addTask(int storeId, int retryCount, List<StoreDataBatch> dataToRetry) {
        if (!sendTasks.offer(new SendTask(storeId, retryCount, dataToRetry))) {
            logger.error("failed to add task. storeId [{}]", storeId);
        }
    }

    @Override
    public void initMetrics() {
        this.lastUpdateTime = System.nanoTime();
        this.sendBytesMetric = new AvgMetric();
        this.sendRecordsMetric = new AvgMetric();
        this.callbackLatencyMetrics = new ArrayList<>(this.storeCount);
        for (int i = 0; i < this.storeCount; i++) {
            this.callbackLatencyMetrics.add(new AvgMetric());
        }
    }

    private void updateMetrics() {
        long currentTime = System.nanoTime();
        long interval = currentTime - this.lastUpdateTime;
        this.sendBytesMetric.update(interval);
        this.sendRecordsMetric.update(interval);
        this.callbackLatencyMetrics.forEach(m -> m.update(interval));
        this.lastUpdateTime = currentTime;
    }

    @Override
    public Map<String, String> getMetrics() {
        return new HashMap<String, String>() {
            {
                put(
                        SEND_BYTES_PER_SECOND,
                        String.valueOf((int) (1000000000 * sendBytesMetric.getAvg())));
                put(SEND_BYTES_TOTAL, String.valueOf(sendBytesMetric.getLastUpdateTotal()));
                put(
                        SEND_RECORDS_PER_SECOND,
                        String.valueOf((int) (1000000000 * sendRecordsMetric.getAvg())));
                put(SEND_RECORDS_TOTAL, String.valueOf(sendRecordsMetric.getLastUpdateTotal()));
                put(
                        SEND_BUFFER_BATCH_COUNT,
                        String.valueOf(
                                storeSendBuffer.stream()
                                        .map(q -> q.size())
                                        .collect(Collectors.toList())));
                put(
                        SEND_CALLBACK_LATENCY,
                        String.valueOf(
                                callbackLatencyMetrics.stream()
                                        .map(m -> (int) (1000 * m.getAvg()))
                                        .collect(Collectors.toList())));
            }
        };
    }

    @Override
    public String[] getMetricKeys() {
        return new String[] {
            SEND_BYTES_PER_SECOND,
            SEND_BYTES_TOTAL,
            SEND_RECORDS_PER_SECOND,
            SEND_RECORDS_TOTAL,
            SEND_BUFFER_BATCH_COUNT,
            SEND_CALLBACK_LATENCY
        };
    }
}

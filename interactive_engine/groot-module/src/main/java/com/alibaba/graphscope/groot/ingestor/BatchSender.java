package com.alibaba.graphscope.groot.ingestor;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.metrics.AvgMetric;
import com.alibaba.graphscope.groot.metrics.MetricsAgent;
import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.OperationBlob;
import com.alibaba.graphscope.groot.operation.StoreDataBatch;
import com.alibaba.graphscope.groot.operation.StoreDataBatch.Builder;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.IngestorConfig;
import com.alibaba.graphscope.groot.common.config.StoreConfig;
import com.alibaba.graphscope.sdkcommon.util.PartitionUtils;

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
    public static final String SEND_CALLBACK_LATENCY_PER_SECOND_MS =
            "send.callback.latency.per.second.ms";

    private MetaService metaService;
    private StoreWriter storeWriter;

    private int bufferSize;
    private int storeCount;
    private int sendOperationLimit;

    private List<BlockingQueue<StoreDataBatch>> storeSendBuffer;
    private BlockingQueue<SendTask> sendTasks;

    private Thread sendThread;
    private volatile boolean shouldStop = true;

    private volatile long lastUpdateTime;
    private AvgMetric sendBytesMetric;
    private AvgMetric sendRecordsMetric;
    private List<AvgMetric> bufferBatchCountMetrics;
    private List<AvgMetric> callbackLatencyMetrics;
    private int receiverQueueSize;

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
        metricsCollector.register(this, () -> updateMetrics());
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
                            storeSendBuffer.get(storeId).put(batchBuilder.build());
                            break;
                        } catch (InterruptedException e) {
                            logger.warn("send buffer interrupted", e);
                        }
                    }
                });
    }

    class SendTask {
        int storeId;
        List<StoreDataBatch> dataToRetry;

        public SendTask(int storeId, List<StoreDataBatch> dataToRetry) {
            this.storeId = storeId;
            this.dataToRetry = dataToRetry;
        }
    }

    private void sendBatch() {
        SendTask sendTask;
        try {
            sendTask = this.sendTasks.poll(1000L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("polling send task interrupted", e);
            return;
        }
        if (sendTask == null) {
            return;
        }

        int storeId = sendTask.storeId;
        List<StoreDataBatch> dataToSend = sendTask.dataToRetry;
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
                    logger.warn("polling send buffer interrupted", e);
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
                                addTask(storeId, null);
                            } else {
                                addTask(storeId, finalDataToSend);
                            }
                        }
                    });
        } else {
            addTask(storeId, null);
        }
    }

    private void addTask(int storeId, List<StoreDataBatch> dataToRetry) {
        if (!sendTasks.offer(new SendTask(storeId, dataToRetry))) {
            logger.error(
                    "Unexpected error, failed to add send task to queue. storeId ["
                            + storeId
                            + "]");
        }
    }

    @Override
    public void initMetrics() {
        this.lastUpdateTime = System.nanoTime();
        this.sendBytesMetric = new AvgMetric();
        this.sendRecordsMetric = new AvgMetric();
        this.bufferBatchCountMetrics = new ArrayList<>(this.storeCount);
        this.callbackLatencyMetrics = new ArrayList<>(this.storeCount);
        for (int i = 0; i < this.storeCount; i++) {
            this.bufferBatchCountMetrics.add(new AvgMetric());
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
                        SEND_CALLBACK_LATENCY_PER_SECOND_MS,
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
            SEND_CALLBACK_LATENCY_PER_SECOND_MS
        };
    }
}

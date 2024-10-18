package com.alibaba.graphscope.groot.store;

import com.alibaba.graphscope.groot.Utils;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.CoordinatorConfig;
import com.alibaba.graphscope.groot.common.config.StoreConfig;
import com.alibaba.graphscope.groot.common.exception.IllegalStateException;
import com.alibaba.graphscope.groot.common.exception.InternalException;
import com.alibaba.graphscope.groot.common.util.PartitionUtils;
import com.alibaba.graphscope.groot.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.groot.meta.FileMetaStore;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.meta.MetaStore;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.OperationBlob;
import com.alibaba.graphscope.groot.operation.OperationType;
import com.alibaba.graphscope.groot.operation.StoreDataBatch;
import com.alibaba.graphscope.groot.wal.LogEntry;
import com.alibaba.graphscope.groot.wal.LogReader;
import com.alibaba.graphscope.groot.wal.LogService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaProcessor {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProcessor.class);

    private final LogService logService;

    private AtomicReference<List<Long>> queueOffsetsRef;
    private final MetaStore metaStore;
    private final MetaService metaService;

    private final ObjectMapper objectMapper;
    private ScheduledExecutorService persistOffsetsScheduler;
    private Thread pollThread;
    private Thread processThread;
    private final boolean isSecondary;
    private final long offsetsPersistIntervalMs;
    private final WriterAgent writerAgent;
    public static final String QUEUE_OFFSETS_PATH = "queue_offsets_store";
    public static final int QUEUE_COUNT = 1;

    public static int storeId = 0;
    private volatile boolean shouldStop = true;
    List<OperationType> typesDDL;

    BlockingQueue<ConsumerRecord<LogEntry, LogEntry>> queue;

    public KafkaProcessor(
            Configs configs,
            MetaService metaService,
            WriterAgent writerAgent,
            LogService logService) {
        this.metaService = metaService;
        this.writerAgent = writerAgent;
        this.logService = logService;

        String metaPath = StoreConfig.STORE_DATA_PATH.get(configs) + "/meta";
        this.metaStore = new FileMetaStore(metaPath);
        this.objectMapper = new ObjectMapper();
        this.isSecondary = CommonConfig.SECONDARY_INSTANCE_ENABLED.get(configs);

        storeId = CommonConfig.NODE_IDX.get(configs);
        offsetsPersistIntervalMs = CoordinatorConfig.OFFSETS_PERSIST_INTERVAL_MS.get(configs);

        int queueSize = StoreConfig.STORE_QUEUE_BUFFER_SIZE.get(configs);
        queue = new ArrayBlockingQueue<>(queueSize);
    }

    public void start() {
        try {
            recover();
        } catch (IOException e) {
            throw new InternalException(e);
        }

        this.persistOffsetsScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "persist-offsets-scheduler", logger));
        this.persistOffsetsScheduler.scheduleWithFixedDelay(
                this::updateQueueOffsets,
                offsetsPersistIntervalMs,
                offsetsPersistIntervalMs,
                TimeUnit.MILLISECONDS);
        this.shouldStop = false;
        this.pollThread = new Thread(this::pollBatches);
        this.pollThread.setName("store-kafka-poller");
        this.pollThread.setDaemon(true);
        this.pollThread.start();

        this.processThread = new Thread(this::processRecords);
        this.processThread.setName("store-kafka-record-processor");
        this.processThread.setDaemon(true);
        this.processThread.start();

        logger.info("Kafka processor started");
    }

    public void stop() {
        this.shouldStop = true;
        updateQueueOffsets();
        if (this.persistOffsetsScheduler != null) {
            this.persistOffsetsScheduler.shutdown();
            try {
                boolean ignored =
                        this.persistOffsetsScheduler.awaitTermination(3000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
            this.persistOffsetsScheduler = null;
        }

        if (this.pollThread != null) {
            this.pollThread.interrupt();
            try {
                this.pollThread.join(3000);
            } catch (InterruptedException e) {
                // Do nothing
            }
            this.pollThread = null;
        }
    }

    public void recover() throws IOException {
        List<Long> offsets = new ArrayList<>(QUEUE_COUNT);
        if (!this.metaStore.exists(QUEUE_OFFSETS_PATH)) {
            for (int i = 0; i < QUEUE_COUNT; i++) {
                offsets.add(-1L);
            }
        } else {
            byte[] offsetBytes = this.metaStore.read(QUEUE_OFFSETS_PATH);
            offsets = objectMapper.readValue(offsetBytes, new TypeReference<>() {});
        }
        queueOffsetsRef = new AtomicReference<>(offsets);
        logger.info("[STORE] recovered queue offsets {}", offsets);
        if (offsets.size() != QUEUE_COUNT) {
            String msg =
                    String.format(
                            "recovered queueCount %d, expect %d", offsets.size(), QUEUE_COUNT);
            throw new IllegalStateException(msg);
        }

        long recoveredOffset = offsets.get(0);
        if (recoveredOffset != -1) { // if -1, then assume it's a fresh store
            try (LogReader ignored = logService.createReader(storeId, recoveredOffset + 1)) {
            } catch (Exception e) {
                throw new IllegalStateException(
                        "recovered queue [0] offset [" + recoveredOffset + "] is not available", e);
            }
        }
    }

    private void updateQueueOffsets() {
        List<Long> queueOffsets = this.queueOffsetsRef.get();
        List<Long> newQueueOffsets = new ArrayList<>(queueOffsets);
        boolean changed = false;
        List<Long> consumedOffsets = writerAgent.getConsumedQueueOffsets();
        for (int qId = 0; qId < queueOffsets.size(); qId++) {
            long minOffset = Math.min(consumedOffsets.get(qId), Long.MAX_VALUE);
            if (minOffset != Long.MAX_VALUE && minOffset > newQueueOffsets.get(qId)) {
                newQueueOffsets.set(qId, minOffset);
                changed = true;
            }
        }
        if (changed) {
            try {
                persistObject(newQueueOffsets, QUEUE_OFFSETS_PATH);
                this.queueOffsetsRef.set(newQueueOffsets);
            } catch (IOException e) {
                logger.error("error in updateQueueOffsets, ignore", e);
            }
        }
    }

    private void persistObject(Object value, String path) throws IOException {
        if (isSecondary) {
            return;
        }
        byte[] b = objectMapper.writeValueAsBytes(value);
        metaStore.write(path, b);
    }

    public void pollBatches() {
        typesDDL = prepareDDLTypes();
        try {
            replayWAL();
        } catch (IOException e) {
            throw new InternalException(e);
        }
        // -1 stands for poll from latest
        try (LogReader reader = logService.createReader(storeId, -1)) {
            while (!shouldStop) {
                ConsumerRecords<LogEntry, LogEntry> records = reader.getLatestUpdates();
                for (ConsumerRecord<LogEntry, LogEntry> record : records) {
                    queue.add(record);
                }
            }
        } catch (IOException e) {
            throw new InternalException(e);
        }
    }

    private void processRecord(ConsumerRecord<LogEntry, LogEntry> record) {
        int partitionCount = metaService.getPartitionCount();
        long offset = record.offset();
        LogEntry logEntry = record.value();
        OperationBatch operationBatch = logEntry.getOperationBatch();
        if (isSecondary) { // only catch up the schema updates
            operationBatch = Utils.extractOperations(operationBatch, typesDDL);
        }
        if (operationBatch.getOperationCount() == 0) {
            return;
        }
        long snapshotId = logEntry.getSnapshotId();
        StoreDataBatch.Builder builder =
                StoreDataBatch.newBuilder()
                        .requestId("")
                        .queueId(storeId)
                        .snapshotId(snapshotId)
                        .traceId(operationBatch.getTraceId())
                        .offset(offset);
        for (OperationBlob operationBlob : operationBatch) {
            long partitionKey = operationBlob.getPartitionKey();
            if (partitionKey == -1L) {
                // replicate to all store node
                builder.addOperation(-1, operationBlob);
            } else {
                int partitionId =
                        PartitionUtils.getPartitionIdFromKey(partitionKey, partitionCount);
                int batchStoreId = metaService.getStoreIdByPartition(partitionId);
                if (batchStoreId == storeId) {
                    builder.addOperation(partitionId, operationBlob);
                } else {
                    logger.error("Should not happen: {} {}", partitionId, operationBlob.toProto());
                }
            }
        }
        try {
            writerAgent.writeStore(builder.build());
        } catch (InterruptedException e) {
            throw new InternalException(e);
        }
    }

    public void replayWAL() throws IOException {
        // Only has one queue per store
        long replayFrom = queueOffsetsRef.get().get(0) + 1;
        logger.info("replay WAL of queue#[{}] from offset [{}]", storeId, replayFrom);
        if (replayFrom == 0) {
            logger.warn("It may not be expected to replay from the 0 offset, skipped");
            return;
        }
        int replayCount = 0;
        try (LogReader logReader = this.logService.createReader(storeId, replayFrom)) {
            ConsumerRecord<LogEntry, LogEntry> record;
            while ((record = logReader.readNextRecord()) != null) {
                queue.put(record);
                replayCount++;
                if (replayCount % 10000 == 0) {
                    logger.info("replayed {} records", replayCount);
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        logger.info("replayWAL finished. total replayed [{}] records", replayCount);
    }

    private void processRecords() {
        while (true) {
            try {
                ConsumerRecord<LogEntry, LogEntry> record = queue.take();
                processRecord(record);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private List<OperationType> prepareDDLTypes() {
        List<OperationType> types = new ArrayList<>();
        types.add(OperationType.CREATE_VERTEX_TYPE);
        types.add(OperationType.CREATE_EDGE_TYPE);
        types.add(OperationType.ADD_EDGE_KIND);
        types.add(OperationType.DROP_VERTEX_TYPE);
        types.add(OperationType.DROP_EDGE_TYPE);
        types.add(OperationType.REMOVE_EDGE_KIND);
        types.add(OperationType.PREPARE_DATA_LOAD);
        types.add(OperationType.COMMIT_DATA_LOAD);
        types.add(OperationType.ADD_VERTEX_TYPE_PROPERTIES);
        types.add(OperationType.ADD_EDGE_TYPE_PROPERTIES);
        types.add(OperationType.MARKER); // For advance ID
        return types;
    }
}

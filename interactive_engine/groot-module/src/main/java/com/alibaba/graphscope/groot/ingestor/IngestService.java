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
import com.alibaba.graphscope.groot.common.RoleType;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.config.IngestorConfig;
import com.alibaba.graphscope.groot.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.groot.discovery.GrootNode;
import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.operation.OperationBlob;
import com.alibaba.graphscope.groot.wal.LogService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class IngestService implements NodeDiscovery.Listener {
    private static final Logger logger = LoggerFactory.getLogger(IngestService.class);

    public static final OperationBatch MARKER_BATCH =
            OperationBatch.newBuilder()
                    .addOperationBlob(OperationBlob.MARKER_OPERATION_BLOB)
                    .build();
    private final Configs configs;
    private final NodeDiscovery discovery;
    private final MetaService metaService;
    private final LogService logService;
    private final IngestProgressFetcher ingestProgressFetcher;
    private final StoreWriter storeWriter;
    private final MetricsCollector metricsCollector;

    private final int ingestorId;
    private List<Integer> queueIds;
    private Map<Integer, IngestProcessor> queueToProcessor;
    private AtomicLong ingestSnapshotId;

    private volatile boolean storeNodeReady;
    private ScheduledExecutorService scheduler;
    private ExecutorService singleThreadExecutor;
    private volatile boolean started = false;
    private final int storeNodeCount;

    private final Set<Integer> availableNodes;

    public IngestService(
            Configs configs,
            NodeDiscovery discovery,
            MetaService metaService,
            LogService logService,
            IngestProgressFetcher ingestProgressFetcher,
            StoreWriter storeWriter,
            MetricsCollector metricsCollector) {
        this.configs = configs;
        this.discovery = discovery;
        this.metaService = metaService;
        this.logService = logService;
        this.ingestProgressFetcher = ingestProgressFetcher;
        this.storeWriter = storeWriter;
        this.metricsCollector = metricsCollector;

        this.ingestorId = CommonConfig.NODE_IDX.get(configs);
        this.storeNodeCount = CommonConfig.STORE_NODE_COUNT.get(configs);
        this.availableNodes = new HashSet<>();
    }

    public void start() {
        this.ingestSnapshotId = new AtomicLong(-1L);
        this.queueIds = this.metaService.getQueueIdsForIngestor(this.ingestorId);
        this.queueToProcessor = new HashMap<>(this.queueIds.size());
        for (int queueId : queueIds) {
            BatchSender batchSender =
                    new BatchSender(
                            this.configs,
                            this.metaService,
                            this.storeWriter,
                            this.metricsCollector);
            this.queueToProcessor.put(
                    queueId,
                    makeIngestProcessor(
                            this.configs,
                            this.logService,
                            batchSender,
                            queueId,
                            this.ingestSnapshotId,
                            this.metricsCollector));
        }
        this.storeNodeReady = false;
        this.discovery.addListener(this);
        this.singleThreadExecutor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "ingest-single-executor", logger));
        this.scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "ingest-try-start", logger));

        long delay = IngestorConfig.INGESTOR_CHECK_PROCESSOR_INTERVAL_MS.get(configs);
        this.scheduler.scheduleWithFixedDelay(
                this::tryStartProcessors, 2000, delay, TimeUnit.MILLISECONDS);
        this.started = true;
        logger.info("IngestService started");
    }

    public IngestProcessor makeIngestProcessor(
            Configs configs,
            LogService logService,
            BatchSender batchSender,
            int queueId,
            AtomicLong ingestSnapshotId,
            MetricsCollector metricsCollector) {
        return new IngestProcessor(
                configs, logService, batchSender, queueId, ingestSnapshotId, metricsCollector);
    }

    public void stop() {
        this.started = false;
        this.discovery.removeListener(this);
        if (this.scheduler != null) {
            this.scheduler.shutdown();
            try {
                this.scheduler.awaitTermination(3000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
            this.scheduler = null;
        }
        stopProcessors();
        if (this.singleThreadExecutor != null) {
            this.singleThreadExecutor.shutdown();
            try {
                this.singleThreadExecutor.awaitTermination(3000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
            this.singleThreadExecutor = null;
        }
        logger.debug("IngestService stopped");
    }

    private void checkStarted() {
        if (!started) {
            throw new IllegalStateException("IngestService not started yet");
        }
    }

    public void ingestBatch(
            String requestId, int queueId, OperationBatch operationBatch, IngestCallback callback) {
        checkStarted();
        this.queueToProcessor.get(queueId).ingestBatch(requestId, operationBatch, callback);
    }

    public List<Long> replayDMLRecordsFrom(long offset, long timestamp) throws IOException {
        List<Long> ids = new ArrayList<>();
        for (IngestProcessor processor : queueToProcessor.values()) {
            long snapshotId = processor.replayDMLRecordsFrom(offset, timestamp);
            ids.add(snapshotId);
        }
        return ids;
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
        AtomicInteger counter = new AtomicInteger(this.queueToProcessor.size());
        AtomicBoolean finished = new AtomicBoolean(false);
        for (IngestProcessor processor : this.queueToProcessor.values()) {
            int queue = processor.getQueueId();
            try {
                processor.ingestBatch(
                        "marker",
                        MARKER_BATCH,
                        new IngestCallback() {
                            @Override
                            public void onSuccess(long snapshotId) {
                                if (finished.get()) {
                                    return;
                                }
                                if (counter.decrementAndGet() == 0) {
                                    callback.onCompleted(previousSnapshotId);
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                if (finished.getAndSet(true)) {
                                    return;
                                }
                                logger.warn(
                                        "ingest marker failed. queue#{}, snapshotId {}",
                                        queue,
                                        snapshotId,
                                        e);
                                callback.onError(e);
                            }
                        });
            } catch (IllegalStateException e) {
                if (finished.getAndSet(true)) {
                    return;
                }
                logger.warn(
                        "ingest marker failed. queue#{}, snapshotId {}, {}",
                        queue,
                        snapshotId,
                        e.getMessage());
                callback.onError(e);
            } catch (Exception e) {
                if (finished.getAndSet(true)) {
                    return;
                }
                logger.warn("ingest marker failed. queue#{}, snapshotId {}", queue, snapshotId, e);
                callback.onError(e);
            }
        }
    }

    private void startProcessors() {
        this.singleThreadExecutor.execute(
                () -> {
                    if (isProcessorStarted()) {
                        return;
                    }
                    for (IngestProcessor processor : this.queueToProcessor.values()) {
                        processor.stop();
                    }
                    List<Long> tailOffsets =
                            this.ingestProgressFetcher.getTailOffsets(this.queueIds);
                    for (int i = 0; i < this.queueIds.size(); i++) {
                        int queueId = this.queueIds.get(i);
                        long offset = tailOffsets.get(i);
                        this.queueToProcessor.get(queueId).setTailOffset(offset);
                    }
                    for (IngestProcessor processor : this.queueToProcessor.values()) {
                        processor.start();
                    }
                    logger.info("processors started");
                });
    }

    private boolean isProcessorStarted() {
        for (IngestProcessor processor : this.queueToProcessor.values()) {
            if (!processor.isStarted()) {
                return false;
            }
        }
        return true;
    }

    private void stopProcessors() {
        if (this.singleThreadExecutor == null) {
            logger.warn("no executor for stop processors, ignore");
            return;
        }
        this.singleThreadExecutor.execute(
                () -> {
                    if (isProcessorStarted()) {
                        for (IngestProcessor processor : this.queueToProcessor.values()) {
                            processor.stop();
                        }
                        logger.info("processors stopped");
                    }
                });
    }

    private void graphNodesReady() {
        logger.info("Store ready");
        this.storeNodeReady = true;
    }

    private void graphNodesLost() {
        logger.info("Store lost");
        this.storeNodeReady = false;
        stopProcessors();
    }

    private void tryStartProcessors() {
        try {
            if (this.storeNodeReady) {
                startProcessors();
            } else {
                logger.warn("Store node is not ready when trying to start processors");
            }
        } catch (Exception e) {
            logger.error("tryStartProcessors failed, ignore", e);
        }
    }

    @Override
    public void nodesJoin(RoleType role, Map<Integer, GrootNode> nodes) {
        if (role == RoleType.STORE) {
            this.availableNodes.addAll(nodes.keySet());
            if (this.availableNodes.size() == storeNodeCount) {
                graphNodesReady();
            }
        }
    }

    @Override
    public void nodesLeft(RoleType role, Map<Integer, GrootNode> nodes) {
        if (role != RoleType.STORE) {
            this.availableNodes.removeAll(nodes.keySet());
            graphNodesLost();
        }
    }
}

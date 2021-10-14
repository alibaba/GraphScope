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
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.common.config.IngestorConfig;
import com.alibaba.graphscope.groot.discovery.MaxGraphNode;
import com.alibaba.graphscope.groot.discovery.NodeDiscovery;
import com.alibaba.maxgraph.common.RoleType;
import com.alibaba.graphscope.groot.metrics.MetricsCollector;
import com.alibaba.maxgraph.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.groot.wal.LogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    private Configs configs;
    private NodeDiscovery discovery;
    private MetaService metaService;
    private LogService logService;
    private IngestProgressFetcher ingestProgressFetcher;
    private StoreWriter storeWriter;
    private MetricsCollector metricsCollector;

    private int ingestorId;
    private List<Integer> queueIds;
    private Map<Integer, IngestProcessor> queueToProcessor;
    private AtomicLong ingestSnapshotId;

    private volatile boolean processorStarted;
    private volatile boolean storeNodeReady;
    private ScheduledExecutorService scheduler;
    private ExecutorService singleThreadExecutor;
    private volatile boolean started = false;
    private int storeNodeCount;
    private long checkProcessorIntervalMs;

    private Set<Integer> availableNodes;

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
        this.checkProcessorIntervalMs =
                IngestorConfig.INGESTOR_CHECK_PROCESSOR_INTERVAL_MS.get(configs);
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
        this.processorStarted = false;
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
        this.scheduler.scheduleWithFixedDelay(
                () -> tryStartProcessors(),
                this.checkProcessorIntervalMs,
                this.checkProcessorIntervalMs,
                TimeUnit.MILLISECONDS);
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
        logger.info("IngestService stopped");
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
        long previousSnapshotId =
                this.ingestSnapshotId.getAndUpdate(x -> x < snapshotId ? snapshotId : x);
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
                                        "ingest marker failed. queue#["
                                                + processor.getQueueId()
                                                + "], snapshotId ["
                                                + snapshotId
                                                + "]",
                                        e);
                                callback.onError(e);
                            }
                        });
            } catch (Exception e) {
                if (finished.getAndSet(true)) {
                    return;
                }
                logger.warn(
                        "error in ingest marker. queue#["
                                + processor.getQueueId()
                                + "], snapshotId ["
                                + snapshotId
                                + "]",
                        e);
                callback.onError(e);
            }
        }
    }

    private void startProcessors() {
        this.singleThreadExecutor.execute(
                () -> {
                    if (processorStarted) {
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
                    processorStarted = true;
                    logger.info("processors started");
                });
    }

    private void stopProcessors() {
        if (this.singleThreadExecutor == null) {
            logger.warn("no executor for stop processors, ignore");
            return;
        }
        this.singleThreadExecutor.execute(
                () -> {
                    if (!processorStarted) {
                        return;
                    }
                    for (IngestProcessor processor : this.queueToProcessor.values()) {
                        processor.stop();
                    }
                    this.processorStarted = false;
                    logger.info("processors stopped");
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
            }
        } catch (Exception e) {
            logger.error("tryStartProcessors failed, ignore", e);
        }
    }

    @Override
    public void nodesJoin(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        if (role != RoleType.STORE) {
            return;
        }
        this.availableNodes.addAll(nodes.keySet());
        if (this.availableNodes.size() == storeNodeCount) {
            graphNodesReady();
        }
    }

    @Override
    public void nodesLeft(RoleType role, Map<Integer, MaxGraphNode> nodes) {
        if (role != RoleType.STORE) {
            return;
        }
        this.availableNodes.removeAll(nodes.keySet());
        graphNodesLost();
    }
}

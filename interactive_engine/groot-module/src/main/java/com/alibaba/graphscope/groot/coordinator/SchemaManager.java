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
package com.alibaba.graphscope.groot.coordinator;

import com.alibaba.graphscope.groot.CompletionCallback;
import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.common.exception.ServiceNotReadyException;
import com.alibaba.graphscope.groot.common.schema.wrapper.GraphDef;
import com.alibaba.graphscope.groot.common.util.ThreadFactoryUtils;
import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.operation.BatchId;
import com.alibaba.graphscope.groot.operation.Operation;
import com.alibaba.graphscope.groot.operation.OperationBatch;
import com.alibaba.graphscope.groot.rpc.RoleClients;
import com.alibaba.graphscope.groot.schema.ddl.DdlExecutors;
import com.alibaba.graphscope.groot.schema.ddl.DdlResult;
import com.alibaba.graphscope.groot.schema.request.DdlRequestBatch;
import com.alibaba.graphscope.proto.groot.EdgeKindPb;
import com.alibaba.graphscope.proto.groot.LabelIdPb;
import com.alibaba.graphscope.proto.groot.Statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);

    private final SnapshotManager snapshotManager;
    private final DdlWriter ddlWriter;
    private final DdlExecutors ddlExecutors;
    private final GraphDefFetcher graphDefFetcher;

    private final RoleClients<FrontendSnapshotClient> frontendSnapshotClients;

    private final int frontendCount;

    private final AtomicReference<GraphDef> graphDefRef;
    private final AtomicReference<Statistics> graphStatistics;
    private final int partitionCount;
    private volatile boolean ready = false;

    private final boolean collectStatistics;
    private final int collectStatisticsInitialDelay;
    private final int collectStatisticsInterval;
    private ExecutorService singleThreadExecutor;
    private ScheduledExecutorService syncSchemaScheduler;

    private ScheduledExecutorService fetchStatisticsScheduler;

    public SchemaManager(
            Configs configs,
            SnapshotManager snapshotManager,
            DdlExecutors ddlExecutors,
            DdlWriter ddlWriter,
            MetaService metaService,
            GraphDefFetcher graphDefFetcher,
            RoleClients<FrontendSnapshotClient> frontendSnapshotClients) {
        this.snapshotManager = snapshotManager;
        this.ddlExecutors = ddlExecutors;
        this.ddlWriter = ddlWriter;
        this.graphDefFetcher = graphDefFetcher;
        this.frontendSnapshotClients = frontendSnapshotClients;

        this.graphDefRef = new AtomicReference<>();
        this.partitionCount = metaService.getPartitionCount();
        this.graphStatistics = new AtomicReference<>();

        this.frontendCount = CommonConfig.FRONTEND_NODE_COUNT.get(configs);
        this.collectStatistics = CommonConfig.COLLECT_STATISTICS.get(configs);
        this.collectStatisticsInitialDelay =
                CommonConfig.COLLECT_STATISTICS_INITIAL_DELAY_MIN.get(configs);
        this.collectStatisticsInterval = CommonConfig.COLLECT_STATISTICS_INTERVAL_MIN.get(configs);
    }

    public void start() {
        logger.info("starting SchemaManager...");
        this.singleThreadExecutor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "ddl-executor", logger));
        recover();
        logger.info(graphDefRef.get().toProto().toString());

        this.syncSchemaScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                "recover", logger));
        this.syncSchemaScheduler.scheduleWithFixedDelay(this::recover, 5, 2, TimeUnit.SECONDS);

        if (this.collectStatistics) {
            this.fetchStatisticsScheduler =
                    Executors.newSingleThreadScheduledExecutor(
                            ThreadFactoryUtils.daemonThreadFactoryWithLogExceptionHandler(
                                    "fetch-statistics", logger));
            this.fetchStatisticsScheduler.scheduleWithFixedDelay(
                    this::syncStatistics,
                    collectStatisticsInitialDelay,
                    collectStatisticsInterval,
                    TimeUnit.MINUTES);
        }
    }

    private void syncStatistics() {
        try {
            Map<Integer, Statistics> statisticsMap = graphDefFetcher.fetchStatistics();
            Statistics statistics = aggregateStatistics(statisticsMap);
            this.graphStatistics.set(statistics);
            logger.info("Fetched statistics from groot store to groot coordinator successfully");
        } catch (Exception e) {
            logger.error("Fetched statistics from groot store to groot coordinator failed", e);
        }
        sendStatisticsToFrontend();
    }

    private void sendStatisticsToFrontend() {
        Statistics statistics = this.graphStatistics.get();
        if (statistics != null) {
            for (int i = 0; i < frontendCount; ++i) {
                try {
                    frontendSnapshotClients.getClient(i).syncStatistics(statistics);
                    logger.info("Sent statistics from groot coordinator to frontend#{}", i);
                } catch (Exception e) {
                    logger.error("Failed to sync statistics to frontend", e);
                }
            }
        }
    }

    private Statistics aggregateStatistics(Map<Integer, Statistics> statisticsMap) {
        Statistics.Builder builder = Statistics.newBuilder();
        long numVertices = 0;
        long numEdges = 0;
        Map<Integer, Long> vertexMap = new HashMap<>();
        Map<EdgeKindPb, Long> edgeKindMap = new HashMap<>();

        for (Statistics statistics : statisticsMap.values()) {
            numVertices += statistics.getNumVertices();
            numEdges += statistics.getNumEdges();

            for (Statistics.VertexTypeStatistics subStatistics :
                    statistics.getVertexTypeStatisticsList()) {
                long count = subStatistics.getNumVertices();
                int labelId = subStatistics.getLabelId().getId();
                vertexMap.compute(labelId, (k, v) -> (v == null) ? count : v + count);
            }

            for (Statistics.EdgeTypeStatistics subStatistics :
                    statistics.getEdgeTypeStatisticsList()) {
                long count = subStatistics.getNumEdges();
                EdgeKindPb edgeKindPb = subStatistics.getEdgeKind();
                edgeKindMap.compute(edgeKindPb, (k, v) -> (v == null) ? count : v + count);
            }
        }
        builder.setSnapshotId(0); // TODO(siyuan): set this
        builder.setNumVertices(numVertices).setNumEdges(numEdges);
        for (Map.Entry<Integer, Long> entry : vertexMap.entrySet()) {
            int labelId = entry.getKey();
            LabelIdPb labelIdPb = LabelIdPb.newBuilder().setId(labelId).build();
            Long count = entry.getValue();
            builder.addVertexTypeStatistics(
                    Statistics.VertexTypeStatistics.newBuilder()
                            .setLabelId(labelIdPb)
                            .setNumVertices(count));
        }
        for (Map.Entry<EdgeKindPb, Long> entry : edgeKindMap.entrySet()) {
            EdgeKindPb edgeKindPb = entry.getKey();
            Long count = entry.getValue();
            builder.addEdgeTypeStatistics(
                    Statistics.EdgeTypeStatistics.newBuilder()
                            .setEdgeKind(edgeKindPb)
                            .setNumEdges(count));
        }
        return builder.build();
    }

    private void recover() {
        try {
            recoverInternal();
        } catch (Exception e) {
            logger.error("recover schemaManager failed", e);
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException interruptedException) {
                // Ignore
            }
            this.singleThreadExecutor.execute(this::recover);
        }
    }

    private void recoverInternal() throws IOException, ExecutionException, InterruptedException {
        logger.debug("Start to recover SchemaManager");
        long snapshotId = this.snapshotManager.increaseWriteSnapshotId();
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.snapshotManager.addSnapshotListener(snapshotId, () -> future.complete(null));
        future.get();
        GraphDef graphDef = this.graphDefFetcher.fetchGraphDef();
        this.graphDefRef.set(graphDef);
        this.ready = true;
        logger.debug("SchemaManager recovered. version [" + graphDef.getVersion() + "]");
    }

    public void stop() {
        if (this.singleThreadExecutor != null) {
            this.singleThreadExecutor.shutdown();
            try {
                this.singleThreadExecutor.awaitTermination(3000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // Ignore
            }
        }
    }

    public void submitBatchDdl(
            String requestId,
            String sessionId,
            DdlRequestBatch ddlRequestBatch,
            CompletionCallback<Long> callback) {
        String traceId = ddlRequestBatch.getTraceId();
        logger.info(
                "submitBatchDdl, traceId [{}] requestId [{}], sessionId [{}], request body [{}]",
                traceId,
                requestId,
                sessionId,
                ddlRequestBatch.toProto());
        if (!ready) {
            callback.onError(new IllegalStateException("SchemaManager is recovering"));
            return;
        }
        this.singleThreadExecutor.execute(
                () -> {
                    try {
                        if (!ready) {
                            callback.onError(
                                    new IllegalStateException("SchemaManager is recovering"));
                            return;
                        }
                        GraphDef tmpGraphDef = this.graphDefRef.get();
                        DdlResult ddlResult =
                                this.ddlExecutors.executeDdlRequestBatch(
                                        ddlRequestBatch, tmpGraphDef, this.partitionCount);
                        GraphDef graphDefResult = ddlResult.getGraphDef();
                        List<Operation> ddlOperations = ddlResult.getDdlOperations();
                        this.snapshotManager.lockWriteSnapshot();
                        BatchId batchId;
                        try {
                            long currentWriteSnapshotId =
                                    this.snapshotManager.getCurrentWriteSnapshotId();
                            OperationBatch operationBatch =
                                    OperationBatch.newBuilder(ddlOperations)
                                            .setLatestSnapshotId(currentWriteSnapshotId)
                                            .setTraceId(traceId)
                                            .build();
                            batchId = this.ddlWriter.writeOperations(requestId, operationBatch);
                        } finally {
                            this.snapshotManager.unlockWriteSnapshot();
                        }
                        long snapshotId = batchId.getSnapshotId();
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        this.snapshotManager.addSnapshotListener(
                                snapshotId,
                                () -> {
                                    this.graphDefRef.set(graphDefResult);
                                    future.complete(null);
                                });
                        future.get();
                        callback.onCompleted(snapshotId);
                    } catch (Exception e) {
                        logger.error(
                                "Error in Ddl traceId[{}], requestId [{}], sessionId [{}]",
                                traceId,
                                requestId,
                                sessionId,
                                e);
                        this.ready = false;
                        callback.onError(e);
                        this.singleThreadExecutor.execute(this::recover);
                    }
                });
    }

    public GraphDef getGraphDef() {
        if (!ready) {
            throw new ServiceNotReadyException("SchemaManager is recovering");
        }
        return this.graphDefRef.get();
    }
}
